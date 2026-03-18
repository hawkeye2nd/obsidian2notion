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

// ─── State Management ─────────────────────────────────────────────────────────

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
    const lines = match[1].split(/\r?\n/);
    for (const line of lines) {
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
    const text = entries.map(([k, v]) => `${k}: ${v}`).join('\n');
    return {
        object: 'block',
        type: 'callout',
        callout: {
            icon: { type: 'emoji', emoji: '🏷️' },
            color: 'gray_background',
            rich_text: [{ type: 'text', text: { content: text } }],
        },
    };
}

// ─── Table Sanitization ───────────────────────────────────────────────────────

function countTableCols(row) {
    // Count columns by splitting on | but not inside HTML tags like <br>
    // Strip HTML tags first, then count pipes
    const stripped = row.replace(/<[^>]+>/g, '');
    return stripped.split('|').filter((_, idx, arr) => idx > 0 && idx < arr.length - 1).length;
}

function sanitizeTables(markdown) {
    const lines = markdown.split(/\r?\n/);
    const result = [];
    let i = 0;

    while (i < lines.length) {
        const line = lines[i];
        if (line.trim().startsWith('|')) {
            const tableLines = [];
            while (i < lines.length && lines[i].trim().startsWith('|')) {
                tableLines.push(lines[i]);
                i++;
            }
            // Use the first non-separator row to determine col count
            const headerRow = tableLines[0];
            const colCount = countTableCols(headerRow) || 1;
            for (const row of tableLines) {
                // Replace HTML tags that might contain pipes
                const cleanRow = row.replace(/<[^>]+>/g, ' ');
                const parts = cleanRow.split('|');
                const inner = parts.slice(1, parts.length - 1);
                while (inner.length < colCount) inner.push('');
                const trimmed = inner.slice(0, colCount);
                result.push('|' + trimmed.join('|') + '|');
            }
        } else {
            result.push(line);
            i++;
        }
    }

    return result.join('\n');
}

function tablesToCodeBlock(markdown) {
    const lines = markdown.split(/\r?\n/);
    const result = [];
    let i = 0;

    while (i < lines.length) {
        if (lines[i].includes('|')) {
            const tableLines = [];
            while (i < lines.length && lines[i].includes('|')) {
                tableLines.push(lines[i]);
                i++;
            }
            result.push('```');
            result.push(...tableLines);
            result.push('```');
        } else {
            result.push(lines[i]);
            i++;
        }
    }

    return result.join('\n');
}

// ─── Block Sanitization ───────────────────────────────────────────────────────

function sanitizeBlocks(blocks, depth = 0) {
    const result = [];
    const MAX_DEPTH = 2;
    const MAX_CHILDREN = 100;
    const MAX_RICH_TEXT = 100;
    const MAX_EQUATION = 2000;

    const textToCodeBlocks = (text) => {
        const chunks = [];
        for (let i = 0; i < text.length; i += MAX_EQUATION) {
            chunks.push({
                object: 'block',
                type: 'code',
                code: {
                    language: 'plain text',
                    rich_text: [{ type: 'text', text: { content: text.slice(i, i + MAX_EQUATION) } }],
                },
            });
        }
        return chunks;
    };

    for (const block of blocks) {
        const type = block.type;

        // Inline equation overflow → convert block to code block(s)
        if (block[type]?.rich_text) {
            const hasOversizedEquation = block[type].rich_text.some(
                seg => seg.type === 'equation' && seg.equation?.expression?.length > MAX_EQUATION
            );
            if (hasOversizedEquation) {
                const text = block[type].rich_text.map(seg => {
                    if (seg.type === 'equation') return seg.equation.expression;
                    return seg.text?.content || '';
                }).join('');
                result.push(...textToCodeBlocks(text));
                continue;
            }
        }

        // Block-level equation overflow → code block(s)
        if (type === 'equation' && block.equation?.expression?.length > MAX_EQUATION) {
            result.push(...textToCodeBlocks(block.equation.expression));
            continue;
        }

        // Paragraph rich_text overflow → split into multiple paragraphs
        if (type === 'paragraph' && block.paragraph?.rich_text?.length > MAX_RICH_TEXT) {
            const segments = block.paragraph.rich_text;
            for (let i = 0; i < segments.length; i += MAX_RICH_TEXT) {
                result.push({
                    object: 'block',
                    type: 'paragraph',
                    paragraph: { rich_text: segments.slice(i, i + MAX_RICH_TEXT) },
                });
            }
            continue;
        }

        // Bullet nesting depth exceeded → flatten with → prefix
        const listTypes = ['bulleted_list_item', 'numbered_list_item'];
        if (listTypes.includes(type) && depth >= MAX_DEPTH) {
            const arrows = depth > MAX_DEPTH ? '→'.repeat(depth - MAX_DEPTH + 1) + ' ' : '→ ';
            const originalText = block[type]?.rich_text?.[0]?.text?.content || '';
            result.push({
                object: 'block',
                type: 'bulleted_list_item',
                bulleted_list_item: {
                    rich_text: [{ type: 'text', text: { content: `${arrows}${originalText}` } }],
                },
            });
            if (block[type]?.children?.length) {
                result.push(...sanitizeBlocks(block[type].children, depth));
            }
            continue;
        }

        // Recurse into children
        if (block[type]?.children?.length) {
            const children = sanitizeBlocks(block[type].children, depth + 1);
            if (children.length > MAX_CHILDREN) {
                const kept = children.slice(0, MAX_CHILDREN);
                const overflow = children.slice(MAX_CHILDREN);
                result.push({ ...block, [type]: { ...block[type], children: kept } });
                result.push(...overflow);
                continue;
            } else {
                block[type].children = children;
            }
        }

        result.push(block);
    }

    return result;
}

// ─── Image Handling ───────────────────────────────────────────────────────────

async function resolveImagePath(imagePath, markdownFilePath) {
    const decodedPath = decodeURIComponent(imagePath);

    const relativePath = path.resolve(path.dirname(markdownFilePath), decodedPath);
    try { await fs.access(relativePath); return relativePath; } catch (e) {}

    const attachmentPath = path.join(config.markdownBaseDir, config.attachmentsDir, decodedPath);
    try { await fs.access(attachmentPath); return attachmentPath; } catch (e) {}

    console.warn(`    ⚠️  Could not find image: ${decodedPath}`);
    return null;
}

async function processAttachments(markdownContent, filePath) {
    const attachmentRegex = /!\[(.*?)\]\((?!https?:\/\/)(.*?)\)|!\[\[(.*?)(?:\|.*?)?\]\]/g;
    const attachmentMatches = [...markdownContent.matchAll(attachmentRegex)];
    if (attachmentMatches.length === 0) return markdownContent;

    const uploadPromises = attachmentMatches.map(match => (async () => {
        const originalLinkText = match[0];
        let altText = '', originalAttachmentPath = '';

        if (match[2] !== undefined) { altText = match[1]; originalAttachmentPath = match[2]; }
        else if (match[3] !== undefined) { originalAttachmentPath = match[3]; altText = path.basename(originalAttachmentPath); }
        if (!originalAttachmentPath) return null;

        try {
            const fullPath = await resolveImagePath(originalAttachmentPath, filePath);
            if (fullPath) {
                const ext = path.extname(originalAttachmentPath).toLowerCase();
                const isImage = ['.jpg', '.jpeg', '.png', '.gif', '.webp', '.bmp', '.svg'].includes(ext);
                const s3Url = await uploadFileToS3(fullPath);
                if (s3Url) {
                    if (isImage) return { original: originalLinkText, replacement: `![${altText}](${s3Url})` };
                    const fileType = ext.replace('.', '').toUpperCase();
                    return { original: originalLinkText, replacement: `[📎 ${altText || fileType + ' File'}](${s3Url})` };
                }
            }
        } catch (e) {
            console.error(`    ❌ ERROR processing attachment "${originalAttachmentPath}": ${e.message}`);
        }
        return null;
    })());

    const results = await Promise.all(uploadPromises);
    for (const r of results) {
        if (r) markdownContent = markdownContent.replace(r.original, r.replacement);
    }
    return markdownContent;
}

// ─── Build Notion Blocks ──────────────────────────────────────────────────────

async function buildBlocks(filePath, rawContent) {
    const { frontmatter, body } = parseFrontmatter(rawContent);
    let markdownContent = sanitizeTables(body);
    markdownContent = await processAttachments(markdownContent, filePath);

    const callout = buildFrontmatterCallout(frontmatter);

    async function toBlocks(md) {
        return sanitizeBlocks(callout
            ? [callout, ...markdownToBlocks(md)]
            : markdownToBlocks(md)
        );
    }

    try {
        return await toBlocks(markdownContent);
    } catch (err) {
        if (err.message && err.message.includes('table')) {
            console.warn(`  ⚠️  Table error, retrying with tables as code blocks...`);
            return await toBlocks(tablesToCodeBlock(markdownContent));
        }
        throw err;
    }
}

// ─── Notion Page Operations ───────────────────────────────────────────────────

async function createPage(parentPageId, pageTitle, blocks) {
    const firstChunk = blocks.slice(0, 100);
    const remaining = [];
    for (let i = 100; i < blocks.length; i += 100) remaining.push(blocks.slice(i, i + 100));

    const newPage = await callWithRetry(() =>
        notion.pages.create({
            parent: { page_id: parentPageId },
            properties: { title: [{ text: { content: pageTitle } }] },
            children: firstChunk,
        })
    );

    for (const chunk of remaining) {
        await callWithRetry(() =>
            notion.blocks.children.append({ block_id: newPage.id, children: chunk })
        );
    }

    return newPage.id;
}

async function updatePage(pageId, blocks) {
    // Delete all existing blocks
    let cursor;
    do {
        const response = await callWithRetry(() =>
            notion.blocks.children.list({ block_id: pageId, start_cursor: cursor, page_size: 100 })
        );
        await Promise.all(
            response.results
                .filter(b => b.type !== 'child_page') // never delete subpages
                .map(b => callWithRetry(() => notion.blocks.delete({ block_id: b.id })))
        );
        cursor = response.next_cursor;
    } while (cursor);

    // Append new blocks
    for (let i = 0; i < blocks.length; i += 100) {
        await callWithRetry(() =>
            notion.blocks.children.append({ block_id: pageId, children: blocks.slice(i, i + 100) })
        );
    }
}

async function deletePage(pageId) {
    await callWithRetry(() => notion.pages.update({ page_id: pageId, archived: true }));
}

// ─── Process Single File ──────────────────────────────────────────────────────

async function processSingleFile(filePath, state) {
    const pageTitle = path.basename(filePath, '.md');
    const relativePath = path.dirname(path.relative(config.markdownBaseDir, filePath));
    const parentFolderName = relativePath === '.' ? null : path.basename(relativePath);
    const isFolderNote = parentFolderName && parentFolderName === pageTitle;

    try {
        const rawContent = await fs.readFile(filePath, 'utf8');
        const hash = hashContent(rawContent);
        const prev = state.pages[filePath];

        if (prev && prev.hash === hash) {
            process.stdout.write(`⏭️  `);
            return; // unchanged
        }

        console.log(`${prev ? '🔄 Updating' : '➕ Creating'}: ${pageTitle}${isFolderNote ? ' (folder note)' : ''}`);

        async function getBlocks(forceStripTables = false) {
            const { frontmatter, body } = parseFrontmatter(rawContent);
            let md = forceStripTables ? tablesToCodeBlock(body) : sanitizeTables(body);
            md = await processAttachments(md, filePath);
            const callout = buildFrontmatterCallout(frontmatter);
            return sanitizeBlocks(callout ? [callout, ...markdownToBlocks(md)] : markdownToBlocks(md));
        }

        async function syncBlocks(blocks, retry = true) {
            try {
                if (isFolderNote) {
                    const folderPageId = await getOrCreatePageForPath(relativePath);
                    await updatePage(folderPageId, blocks);
                    state.pages[filePath] = { hash, notionPageId: folderPageId, isFolderNote: true };
                } else if (prev?.notionPageId) {
                    await updatePage(prev.notionPageId, blocks);
                    state.pages[filePath] = { ...prev, hash };
                } else {
                    const parentPageId = await getOrCreatePageForPath(relativePath);
                    const pageId = await createPage(parentPageId, pageTitle, blocks);
                    state.pages[filePath] = { hash, notionPageId: pageId };
                }
            } catch (err) {
                const errText = (err.message || '') + (err.body || '') + JSON.stringify(err.code || '');
                if (retry && (errText.toLowerCase().includes('table') || errText.toLowerCase().includes('number of cells') || errText.toLowerCase().includes('content creation failed'))) {
                    console.warn(`  ⚠️  Table error in "${pageTitle}", retrying with tables as code blocks...`);
                    const fallbackBlocks = await getBlocks(true);
                    await syncBlocks(fallbackBlocks, false);
                } else {
                    throw err;
                }
            }
        }

        const blocks = await getBlocks();
        await syncBlocks(blocks);

        console.log(`✅ Done: ${pageTitle}`);
    } catch (error) {
        console.error(`❌ ERROR syncing "${pageTitle}": ${error.message}`);
    }
}

// ─── Main ─────────────────────────────────────────────────────────────────────

async function processAllMarkdown() {
    try {
        const fullSync = process.argv.includes('--full');
        const allFiles = await findMarkdownFiles(config.markdownBaseDir);

        if (allFiles.length === 0) {
            console.log('No Markdown files found.');
            return;
        }

        const state = await loadState();
        const isFirstSync = Object.keys(state.pages).length === 0;

        // Find deleted/renamed files (in state but no longer on disk)
        const allFilesSet = new Set(allFiles);
        const deletedFiles = Object.keys(state.pages).filter(f => !allFilesSet.has(f));

        if (deletedFiles.length > 0) {
            console.log(`\nFound ${deletedFiles.length} deleted/renamed file(s) — removing from Notion...\n`);
            for (const filePath of deletedFiles) {
                const entry = state.pages[filePath];
                if (entry?.notionPageId && !entry.isFolderNote) {
                    try {
                        await deletePage(entry.notionPageId);
                        console.log(`🗑️  Deleted from Notion: ${path.basename(filePath, '.md')}`);
                    } catch (e) {
                        console.error(`❌ ERROR deleting "${filePath}": ${e.message}`);
                    }
                }
                delete state.pages[filePath];
            }
        }

        // On first sync or --full, verify against Notion to avoid duplicates
        if (isFirstSync || fullSync) {
            if (fullSync) console.log('--full flag: fetching existing pages from Notion...');
            const existingPages = await fetchAllExistingPages();
            // Mark already-existing pages so we don't recreate them
            // (they'll be updated on next change)
            for (const filePath of allFiles) {
                const pageTitle = path.basename(filePath, '.md');
                const relativePath = path.dirname(path.relative(config.markdownBaseDir, filePath));
                const pageKey = relativePath === '.' ? pageTitle : `${relativePath.replace(/\\/g, '/')}/${pageTitle}`;
                if (existingPages.has(pageKey) && !state.pages[filePath]) {
                    // Page exists in Notion but we don't have it in state — record it as synced
                    // We don't know the page ID without another API call, so just hash it
                    // so it won't be re-uploaded unless it changes
                    const rawContent = await fs.readFile(filePath, 'utf8').catch(() => null);
                    if (rawContent) {
                        state.pages[filePath] = { hash: hashContent(rawContent), notionPageId: null };
                    }
                }
            }
        }

        // Count files that need processing
        let toProcess = 0;
        for (const filePath of allFiles) {
            const rawContent = await fs.readFile(filePath, 'utf8');
            const hash = hashContent(rawContent);
            if (!state.pages[filePath] || state.pages[filePath].hash !== hash) toProcess++;
        }

        if (isFirstSync) {
            console.log(`\nFirst sync: processing all ${allFiles.length} files.\n`);
        } else {
            console.log(`\nIncremental sync: ${toProcess} changed / ${allFiles.length} total.\n`);
        }

        if (toProcess === 0 && deletedFiles.length === 0) {
            console.log('Nothing to sync. Run with --full to force a full check against Notion.');
            await saveState(state);
            return;
        }

        // Process files with concurrency
        for (let i = 0; i < allFiles.length; i += config.concurrencyLimit) {
            const batch = allFiles.slice(i, i + config.concurrencyLimit);
            await Promise.all(batch.map(filePath => processSingleFile(filePath, state)));
        }

        state.lastSync = new Date().toISOString();
        await saveState(state);

        console.log('\nAll files processed! 🚀');
    } catch (error) {
        console.error('A critical error occurred:', error);
    }
}

processAllMarkdown();
