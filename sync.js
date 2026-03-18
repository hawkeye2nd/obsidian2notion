require('dotenv').config();
const crypto = require('crypto');
const { markdownToBlocks } = require('@tryfabric/martian');
const fs = require('fs/promises');
const path = require('path');
const { getOrCreatePageForPath, fetchAllExistingPages, notion, callWithRetry } = require('./src/notion');
const { uploadFileToS3 } = require('./src/s3');
const { findMarkdownFiles } = require('./src/utils');
const config = require('./config');

const STATE_FILE = path.join(__dirname, '.sync-state.json');

// ─── State ────────────────────────────────────────────────────────────────────

async function loadState() {
    try {
        const raw = await fs.readFile(STATE_FILE, 'utf8');
        return JSON.parse(raw);
    } catch {
        return { pages: {} };
    }
}

async function saveState(state) {
    await fs.writeFile(STATE_FILE, JSON.stringify(state, null, 2), 'utf8');
}

function hashContent(content) {
    return crypto.createHash('md5').update(content).digest('hex');
}

// ─── Frontmatter ──────────────────────────────────────────────────────────────

function parseFrontmatter(content) {
    const match = content.match(/^---\r?\n([\s\S]*?)\r?\n---\r?\n?([\s\S]*)$/);
    if (!match) return { frontmatter: {}, body: content };
    const frontmatter = {};
    for (const line of match[1].split(/\r?\n/)) {
        const colonIdx = line.indexOf(':');
        if (colonIdx === -1) continue;
        const key = line.slice(0, colonIdx).trim();
        const value = line.slice(colonIdx + 1).trim();
        if (key) frontmatter[key] = value;
    }
    return { frontmatter, body: match[2] };
}

function buildFrontmatterCallout(frontmatter) {
    const entries = Object.entries(frontmatter);
    if (entries.length === 0) return null;
    return {
        object: 'block', type: 'callout',
        callout: {
            icon: { type: 'emoji', emoji: '🏷️' },
            color: 'gray_background',
            rich_text: [{ type: 'text', text: { content: entries.map(([k, v]) => `${k}: ${v}`).join('\n') } }],
        },
    };
}

// ─── Table Handling ───────────────────────────────────────────────────────────

function countTableCols(row) {
    return row.replace(/<[^>]+>/g, '').split('|').filter((_, idx, arr) => idx > 0 && idx < arr.length - 1).length;
}

function sanitizeTables(markdown) {
    const lines = markdown.split(/\r?\n/);
    const result = [];
    let i = 0;
    while (i < lines.length) {
        if (lines[i].trim().startsWith('|')) {
            const tableLines = [];
            while (i < lines.length && lines[i].trim().startsWith('|')) tableLines.push(lines[i++]);
            const colCount = countTableCols(tableLines[0]) || 1;
            for (const row of tableLines) {
                const parts = row.replace(/<[^>]+>/g, ' ').split('|');
                const inner = parts.slice(1, parts.length - 1);
                while (inner.length < colCount) inner.push('');
                result.push('|' + inner.slice(0, colCount).join('|') + '|');
            }
        } else {
            result.push(lines[i++]);
        }
    }
    return result.join('\n');
}

function tablesToCodeBlock(markdown) {
    const lines = markdown.split(/\r?\n/);
    const result = [];
    let i = 0;
    while (i < lines.length) {
        if (lines[i].trim().startsWith('|')) {
            const tableLines = [];
            while (i < lines.length && lines[i].trim().startsWith('|')) tableLines.push(lines[i++]);
            result.push('```', ...tableLines, '```');
        } else {
            result.push(lines[i++]);
        }
    }
    return result.join('\n');
}

// ─── Block Sanitization ───────────────────────────────────────────────────────

function sanitizeBlocks(blocks, depth = 0) {
    const result = [];
    const MAX_DEPTH = 2, MAX_CHILDREN = 100, MAX_RICH_TEXT = 100, MAX_EQUATION = 2000;

    const textToCodeBlocks = (text) => {
        const chunks = [];
        for (let i = 0; i < text.length; i += MAX_EQUATION) {
            chunks.push({ object: 'block', type: 'code', code: { language: 'plain text', rich_text: [{ type: 'text', text: { content: text.slice(i, i + MAX_EQUATION) } }] } });
        }
        return chunks;
    };

    for (const block of blocks) {
        const type = block.type;

        if (block[type]?.rich_text?.some(s => s.type === 'equation' && s.equation?.expression?.length > MAX_EQUATION)) {
            const text = block[type].rich_text.map(s => s.type === 'equation' ? s.equation.expression : s.text?.content || '').join('');
            result.push(...textToCodeBlocks(text));
            continue;
        }
        if (type === 'equation' && block.equation?.expression?.length > MAX_EQUATION) {
            result.push(...textToCodeBlocks(block.equation.expression));
            continue;
        }
        if (type === 'paragraph' && block.paragraph?.rich_text?.length > MAX_RICH_TEXT) {
            const segs = block.paragraph.rich_text;
            for (let i = 0; i < segs.length; i += MAX_RICH_TEXT) {
                result.push({ object: 'block', type: 'paragraph', paragraph: { rich_text: segs.slice(i, i + MAX_RICH_TEXT) } });
            }
            continue;
        }
        if (['bulleted_list_item', 'numbered_list_item'].includes(type) && depth >= MAX_DEPTH) {
            const arrows = (depth > MAX_DEPTH ? '→'.repeat(depth - MAX_DEPTH + 1) : '→') + ' ';
            result.push({ object: 'block', type: 'bulleted_list_item', bulleted_list_item: { rich_text: [{ type: 'text', text: { content: arrows + (block[type]?.rich_text?.[0]?.text?.content || '') } }] } });
            if (block[type]?.children?.length) result.push(...sanitizeBlocks(block[type].children, depth));
            continue;
        }
        if (block[type]?.children?.length) {
            const children = sanitizeBlocks(block[type].children, depth + 1);
            if (children.length > MAX_CHILDREN) {
                result.push({ ...block, [type]: { ...block[type], children: children.slice(0, MAX_CHILDREN) } });
                result.push(...children.slice(MAX_CHILDREN));
                continue;
            }
            block[type].children = children;
        }
        result.push(block);
    }
    return result;
}

// ─── Attachments ─────────────────────────────────────────────────────────────

async function resolveImagePath(imagePath, markdownFilePath) {
    const decodedPath = decodeURIComponent(imagePath);
    for (const candidate of [
        path.resolve(path.dirname(markdownFilePath), decodedPath),
        path.join(config.markdownBaseDir, config.attachmentsDir, decodedPath),
    ]) {
        try { await fs.access(candidate); return candidate; } catch {}
    }
    console.warn(`    ⚠️  Could not find image: ${decodedPath}`);
    return null;
}

async function processAttachments(markdown, filePath) {
    const regex = /!\[(.*?)\]\((?!https?:\/\/)(.*?)\)|!\[\[(.*?)(?:\|.*?)?\]\]/g;
    const matches = [...markdown.matchAll(regex)];
    if (!matches.length) return markdown;

    const results = await Promise.all(matches.map(async match => {
        const orig = match[0];
        const altText = match[2] !== undefined ? match[1] : path.basename(match[3] || '');
        const attachPath = match[2] !== undefined ? match[2] : match[3];
        if (!attachPath) return null;
        try {
            const full = await resolveImagePath(attachPath, filePath);
            if (!full) return null;
            const ext = path.extname(attachPath).toLowerCase();
            const s3Url = await uploadFileToS3(full);
            if (!s3Url) return null;
            const isImage = ['.jpg','.jpeg','.png','.gif','.webp','.bmp','.svg'].includes(ext);
            return { orig, replacement: isImage ? `![${altText}](${s3Url})` : `[📎 ${altText || ext.replace('.','').toUpperCase() + ' File'}](${s3Url})` };
        } catch (e) {
            console.error(`    ❌ ERROR processing attachment: ${e.message}`);
            return null;
        }
    }));

    for (const r of results) if (r) markdown = markdown.replace(r.orig, r.replacement);
    return markdown;
}

// ─── Notion → Markdown (for backup) ──────────────────────────────────────────

function richTextToMarkdown(richText) {
    if (!richText?.length) return '';
    return richText.map(seg => {
        let text = seg.type === 'equation' ? `$${seg.equation.expression}$` : (seg.text?.content || seg.plain_text || '');
        if (seg.annotations?.code) text = `\`${text}\``;
        if (seg.annotations?.bold) text = `**${text}**`;
        if (seg.annotations?.italic) text = `*${text}*`;
        if (seg.annotations?.strikethrough) text = `~~${text}~~`;
        if (seg.href) text = `[${text}](${seg.href})`;
        return text;
    }).join('');
}

function blocksToMarkdown(blocks, depth = 0) {
    const lines = [];
    const indent = '  '.repeat(depth);
    for (const block of blocks) {
        const type = block.type;
        const data = block[type];
        switch (type) {
            case 'paragraph': lines.push(indent + richTextToMarkdown(data.rich_text), ''); break;
            case 'heading_1': lines.push(`# ${richTextToMarkdown(data.rich_text)}`, ''); break;
            case 'heading_2': lines.push(`## ${richTextToMarkdown(data.rich_text)}`, ''); break;
            case 'heading_3': lines.push(`### ${richTextToMarkdown(data.rich_text)}`, ''); break;
            case 'bulleted_list_item':
                lines.push(`${indent}- ${richTextToMarkdown(data.rich_text)}`);
                if (block.children?.length) lines.push(...blocksToMarkdown(block.children, depth + 1));
                break;
            case 'numbered_list_item':
                lines.push(`${indent}1. ${richTextToMarkdown(data.rich_text)}`);
                if (block.children?.length) lines.push(...blocksToMarkdown(block.children, depth + 1));
                break;
            case 'to_do': lines.push(`${indent}- [${data.checked ? 'x' : ' '}] ${richTextToMarkdown(data.rich_text)}`); break;
            case 'toggle':
                lines.push(`${indent}- ${richTextToMarkdown(data.rich_text)}`);
                if (block.children?.length) lines.push(...blocksToMarkdown(block.children, depth + 1));
                break;
            case 'code': lines.push(`\`\`\`${data.language === 'plain text' ? '' : (data.language || '')}`, richTextToMarkdown(data.rich_text), '```', ''); break;
            case 'quote': lines.push(`> ${richTextToMarkdown(data.rich_text)}`, ''); break;
            case 'callout':
                if (data.icon?.emoji === '🏷️') {
                    lines.push('<!-- frontmatter -->');
                } else {
                    lines.push(`> ${data.icon?.emoji || ''} ${richTextToMarkdown(data.rich_text)}`, '');
                }
                break;
            case 'divider': lines.push('---', ''); break;
            case 'image': lines.push(`![${richTextToMarkdown(data.caption)}](${data.type === 'external' ? data.external.url : data.file?.url || ''})`, ''); break;
            case 'equation': lines.push(`$$${data.expression}$$`, ''); break;
            case 'table':
                if (block.children?.length) {
                    const rows = block.children.map(row => '| ' + (row.table_row?.cells || []).map(c => richTextToMarkdown(c)).join(' | ') + ' |');
                    if (rows.length) {
                        const colCount = (rows[0].match(/\|/g) || []).length - 1;
                        lines.push(rows[0], '| ' + Array(colCount).fill('---').join(' | ') + ' |', ...rows.slice(1), '');
                    }
                }
                break;
            case 'child_page': break;
            default: if (data?.rich_text) lines.push(richTextToMarkdown(data.rich_text), ''); break;
        }
    }
    return lines;
}

async function fetchBlocksWithChildren(blockId) {
    const blocks = [];
    let cursor;
    do {
        const response = await notion.blocks.children.list({ block_id: blockId, start_cursor: cursor, page_size: 100 });
        blocks.push(...response.results);
        cursor = response.next_cursor;
    } while (cursor);
    for (const block of blocks) {
        if (block.has_children && !['child_page'].includes(block.type)) {
            block.children = await fetchBlocksWithChildren(block.id);
        }
    }
    return blocks;
}

async function pageToMarkdown(pageId) {
    const blocks = await fetchBlocksWithChildren(pageId);
    let frontmatter = null;
    let contentBlocks = blocks;

    if (blocks[0]?.type === 'callout' && blocks[0].callout?.icon?.emoji === '🏷️') {
        const text = richTextToMarkdown(blocks[0].callout.rich_text);
        const fmObj = {};
        for (const line of text.split('\n').filter(Boolean)) {
            const colonIdx = line.indexOf(':');
            if (colonIdx !== -1) fmObj[line.slice(0, colonIdx).trim()] = line.slice(colonIdx + 1).trim();
        }
        if (Object.keys(fmObj).length > 0) { frontmatter = fmObj; contentBlocks = blocks.slice(1); }
    }

    const body = blocksToMarkdown(contentBlocks).filter(l => l !== '<!-- frontmatter -->').join('\n').trimEnd();
    if (frontmatter) {
        return `---\n${Object.entries(frontmatter).map(([k,v]) => `${k}: ${v}`).join('\n')}\n---\n\n${body}`;
    }
    return body;
}

// ─── Notion Page Operations ───────────────────────────────────────────────────

async function createPage(parentPageId, pageTitle, blocks) {
    const firstChunk = blocks.slice(0, 100);
    const remaining = [];
    for (let i = 100; i < blocks.length; i += 100) remaining.push(blocks.slice(i, i + 100));
    const newPage = await callWithRetry(() => notion.pages.create({
        parent: { page_id: parentPageId },
        properties: { title: [{ text: { content: pageTitle } }] },
        children: firstChunk,
    }));
    for (const chunk of remaining) {
        await callWithRetry(() => notion.blocks.children.append({ block_id: newPage.id, children: chunk }));
    }
    return newPage.id;
}

async function updatePage(pageId, blocks) {
    let cursor;
    do {
        const response = await callWithRetry(() => notion.blocks.children.list({ block_id: pageId, start_cursor: cursor, page_size: 100 }));
        await Promise.all(response.results.filter(b => b.type !== 'child_page').map(b => callWithRetry(() => notion.blocks.delete({ block_id: b.id }))));
        cursor = response.next_cursor;
    } while (cursor);
    for (let i = 0; i < blocks.length; i += 100) {
        await callWithRetry(() => notion.blocks.children.append({ block_id: pageId, children: blocks.slice(i, i + 100) }));
    }
}

async function deletePage(pageId) {
    await callWithRetry(() => notion.pages.update({ page_id: pageId, archived: true }));
}

// ─── Build Upload Blocks ──────────────────────────────────────────────────────

async function buildUploadBlocks(filePath, rawContent, forceStripTables = false) {
    const { frontmatter, body } = parseFrontmatter(rawContent);
    let md = forceStripTables ? tablesToCodeBlock(body) : sanitizeTables(body);
    md = await processAttachments(md, filePath);
    const callout = buildFrontmatterCallout(frontmatter);
    return sanitizeBlocks(callout ? [callout, ...markdownToBlocks(md)] : markdownToBlocks(md));
}

// ─── Pull: Notion → Obsidian ──────────────────────────────────────────────────

async function fetchAllNotionPages(pageId, localPathPrefix, results = []) {
    let cursor;
    const children = [];
    do {
        const response = await notion.blocks.children.list({ block_id: pageId, start_cursor: cursor, page_size: 100 });
        children.push(...response.results.filter(b => b.type === 'child_page'));
        cursor = response.next_cursor;
    } while (cursor);

    await Promise.all(children.map(async block => {
        const title = block.child_page.title;
        const localPath = localPathPrefix ? `${localPathPrefix}/${title}` : title;
        const page = await notion.pages.retrieve({ page_id: block.id });
        results.push({ id: block.id, title, localPath, lastEditedTime: page.last_edited_time });
        await fetchAllNotionPages(block.id, localPath, results);
    }));

    return results;
}

async function pullFromNotion(state) {
    console.log('\n📥 Pulling changes from Notion...\n');
    const parentId = process.env.NOTION_PARENT_PAGE_ID;
    const notionPages = await fetchAllNotionPages(parentId, '');
    let pulled = 0, skipped = 0, conflicts = 0;

    for (const page of notionPages) {
        // Find matching state entry by localPath
        const stateEntry = Object.entries(state.pages).find(([, v]) => v.localPath === page.localPath);
        const [stateFilePath, stateData] = stateEntry || [];

        const notionChanged = !stateData || stateData.lastEditedTime !== page.lastEditedTime;
        if (!notionChanged) { skipped++; continue; }

        try {
            // Determine local file path
            const hasChildren = notionPages.some(p => p.localPath.startsWith(page.localPath + '/'));
            let filePath;
            if (hasChildren) {
                const localDir = path.join(config.markdownBaseDir, page.localPath);
                await fs.mkdir(localDir, { recursive: true });
                filePath = path.join(localDir, `${page.title}.md`);
            } else {
                const parentDir = path.join(config.markdownBaseDir, path.dirname(page.localPath));
                await fs.mkdir(parentDir, { recursive: true });
                filePath = path.join(config.markdownBaseDir, page.localPath + '.md');
            }

            // Check for conflict — local file exists and has different hash than state
            let isConflict = false;
            try {
                const localContent = await fs.readFile(filePath, 'utf8');
                const localHash = hashContent(localContent);
                if (stateData && localHash !== stateData.hash) {
                    isConflict = true;
                }
            } catch {} // file doesn't exist locally yet, no conflict

            if (isConflict) {
                // Rename local file to (1) variant
                const ext = path.extname(filePath);
                const base = filePath.slice(0, -ext.length);
                let conflictPath = `${base} (1)${ext}`;
                let n = 1;
                while (true) {
                    try { await fs.access(conflictPath); n++; conflictPath = `${base} (${n})${ext}`; } catch { break; }
                }
                await fs.rename(filePath, conflictPath);
                console.log(`⚠️  Conflict: kept local as "${path.basename(conflictPath)}"`);
                conflicts++;
            }

            const markdown = await pageToMarkdown(page.id);
            await fs.writeFile(filePath, markdown, 'utf8');

            const hash = hashContent(markdown);
            state.pages[filePath] = {
                ...(stateData || {}),
                localPath: page.localPath,
                notionPageId: page.id,
                lastEditedTime: page.lastEditedTime,
                hash,
            };

            console.log(`✅ Pulled: ${page.localPath}`);
            pulled++;
        } catch (err) {
            console.error(`❌ ERROR pulling "${page.localPath}": ${err.message}`);
        }
    }

    console.log(`\nPull done — Downloaded: ${pulled}, Conflicts: ${conflicts}, Skipped: ${skipped}`);
    return state;
}

// ─── Push: Obsidian → Notion ──────────────────────────────────────────────────

async function pushToNotion(state) {
    console.log('\n📤 Pushing changes to Notion...\n');
    const allFiles = await findMarkdownFiles(config.markdownBaseDir);
    const allFilesSet = new Set(allFiles);
    const isFirstSync = Object.keys(state.pages).length === 0;

    // Handle deleted/renamed files
    const deletedFiles = Object.keys(state.pages).filter(f => !allFilesSet.has(f));
    for (const filePath of deletedFiles) {
        const entry = state.pages[filePath];
        if (entry?.notionPageId && !entry.isFolderNote) {
            try {
                await deletePage(entry.notionPageId);
                console.log(`🗑️  Deleted from Notion: ${path.basename(filePath, '.md')}`);
            } catch (e) {
                console.error(`❌ ERROR deleting: ${e.message}`);
            }
        }
        delete state.pages[filePath];
    }

    // On first sync, fetch existing pages to avoid duplicates
    let existingPages = new Set();
    if (isFirstSync || process.argv.includes('--full')) {
        existingPages = await fetchAllExistingPages();
    }

    let pushed = 0, skipped = 0;

    for (let i = 0; i < allFiles.length; i += config.concurrencyLimit) {
        const batch = allFiles.slice(i, i + config.concurrencyLimit);
        await Promise.all(batch.map(async filePath => {
            const pageTitle = path.basename(filePath, '.md');
            const relativePath = path.dirname(path.relative(config.markdownBaseDir, filePath));
            const parentFolderName = relativePath === '.' ? null : path.basename(relativePath);
            const isFolderNote = parentFolderName && parentFolderName === pageTitle;

            try {
                const rawContent = await fs.readFile(filePath, 'utf8');
                const hash = hashContent(rawContent);
                const prev = state.pages[filePath];

                if (prev && prev.hash === hash) { skipped++; return; }

                console.log(`${prev ? '🔄 Updating' : '➕ Creating'}: ${pageTitle}${isFolderNote ? ' (folder note)' : ''}`);

                async function syncBlocks(forceStrip = false) {
                    const blocks = await buildUploadBlocks(filePath, rawContent, forceStrip);
                    try {
                        if (isFolderNote) {
                            const folderPageId = await getOrCreatePageForPath(relativePath);
                            await updatePage(folderPageId, blocks);
                            state.pages[filePath] = { hash, notionPageId: folderPageId, isFolderNote: true, localPath: relativePath };
                        } else if (prev?.notionPageId) {
                            await updatePage(prev.notionPageId, blocks);
                            state.pages[filePath] = { ...prev, hash };
                        } else {
                            const parentPageId = await getOrCreatePageForPath(relativePath);
                            const pageId = await createPage(parentPageId, pageTitle, blocks);
                            const lp = relativePath === '.' ? pageTitle : `${relativePath.replace(/\\/g, '/')}/${pageTitle}`;
                            state.pages[filePath] = { hash, notionPageId: pageId, localPath: lp };
                        }
                    } catch (err) {
                        const errText = (err.message || '') + (err.body || '');
                        if (!forceStrip && (errText.toLowerCase().includes('table') || errText.toLowerCase().includes('content creation failed'))) {
                            console.warn(`  ⚠️  Table error in "${pageTitle}", retrying with tables as code blocks...`);
                            await syncBlocks(true);
                        } else {
                            throw err;
                        }
                    }
                }

                await syncBlocks();
                console.log(`✅ Done: ${pageTitle}`);
                pushed++;
            } catch (err) {
                console.error(`❌ ERROR syncing "${pageTitle}": ${err.message}`);
            }
        }));
    }

    console.log(`\nPush done — Uploaded: ${pushed}, Skipped: ${skipped}`);
    return state;
}

// ─── Main ─────────────────────────────────────────────────────────────────────

async function runSync() {
    try {
        console.log('🔄 Starting bidirectional sync...');
        let state = await loadState();

        // Pull first (Notion → local), then push (local → Notion)
        state = await pullFromNotion(state);
        state = await pushToNotion(state);

        state.lastSync = new Date().toISOString();
        await saveState(state);

        console.log('\n✨ Sync complete!');
    } catch (err) {
        console.error('A critical error occurred:', err);
    }
}

runSync();
