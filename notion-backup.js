require('dotenv').config();
const { Client } = require('@notionhq/client');
const fs = require('fs/promises');
const path = require('path');
const config = require('./config');

const notion = new Client({ auth: process.env.NOTION_KEY });
const STATE_FILE = path.join(__dirname, '.sync-state.json');

// ─── State Management ────────────────────────────────────────────────────────

async function loadState() {
    try {
        const raw = await fs.readFile(STATE_FILE, 'utf8');
        return JSON.parse(raw);
    } catch {
        return { lastSync: null, pages: {} };
    }
}

async function saveState(state) {
    await fs.writeFile(STATE_FILE, JSON.stringify(state, null, 2), 'utf8');
}

// ─── Notion Block → Markdown ─────────────────────────────────────────────────

function richTextToMarkdown(richText) {
    if (!richText || richText.length === 0) return '';
    return richText.map(seg => {
        let text = '';
        if (seg.type === 'text') text = seg.text.content;
        else if (seg.type === 'equation') text = `$${seg.equation.expression}$`;
        else text = seg.plain_text || '';

        if (seg.annotations) {
            if (seg.annotations.code) text = `\`${text}\``;
            if (seg.annotations.bold) text = `**${text}**`;
            if (seg.annotations.italic) text = `*${text}*`;
            if (seg.annotations.strikethrough) text = `~~${text}~~`;
        }
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
            case 'paragraph':
                lines.push(indent + richTextToMarkdown(data.rich_text));
                lines.push('');
                break;

            case 'heading_1':
                lines.push(`# ${richTextToMarkdown(data.rich_text)}`);
                lines.push('');
                break;

            case 'heading_2':
                lines.push(`## ${richTextToMarkdown(data.rich_text)}`);
                lines.push('');
                break;

            case 'heading_3':
                lines.push(`### ${richTextToMarkdown(data.rich_text)}`);
                lines.push('');
                break;

            case 'bulleted_list_item': {
                const text = richTextToMarkdown(data.rich_text);
                lines.push(`${indent}- ${text}`);
                if (block.children?.length) {
                    lines.push(...blocksToMarkdown(block.children, depth + 1));
                }
                break;
            }

            case 'numbered_list_item': {
                const text = richTextToMarkdown(data.rich_text);
                lines.push(`${indent}1. ${text}`);
                if (block.children?.length) {
                    lines.push(...blocksToMarkdown(block.children, depth + 1));
                }
                break;
            }

            case 'to_do': {
                const checked = data.checked ? 'x' : ' ';
                lines.push(`${indent}- [${checked}] ${richTextToMarkdown(data.rich_text)}`);
                break;
            }

            case 'toggle': {
                lines.push(`${indent}- ${richTextToMarkdown(data.rich_text)}`);
                if (block.children?.length) {
                    lines.push(...blocksToMarkdown(block.children, depth + 1));
                }
                break;
            }

            case 'code': {
                const lang = data.language === 'plain text' ? '' : (data.language || '');
                lines.push(`\`\`\`${lang}`);
                lines.push(richTextToMarkdown(data.rich_text));
                lines.push('```');
                lines.push('');
                break;
            }

            case 'quote':
                lines.push(`> ${richTextToMarkdown(data.rich_text)}`);
                lines.push('');
                break;

            case 'callout': {
                const emoji = data.icon?.emoji || '';
                // Detect frontmatter callout written by sync script
                if (emoji === '🏷️') {
                    // Will be handled at page level, skip here
                    lines.push('<!-- frontmatter -->');
                } else {
                    lines.push(`> ${emoji} ${richTextToMarkdown(data.rich_text)}`);
                    lines.push('');
                }
                break;
            }

            case 'divider':
                lines.push('---');
                lines.push('');
                break;

            case 'image': {
                const url = data.type === 'external' ? data.external.url : data.file?.url || '';
                const caption = richTextToMarkdown(data.caption);
                lines.push(`![${caption}](${url})`);
                lines.push('');
                break;
            }

            case 'equation':
                lines.push(`$$${data.expression}$$`);
                lines.push('');
                break;

            case 'table': {
                if (block.children?.length) {
                    const rows = block.children.map(row => {
                        const cells = row.table_row?.cells || [];
                        return '| ' + cells.map(cell => richTextToMarkdown(cell)).join(' | ') + ' |';
                    });
                    if (rows.length > 0) {
                        const headerRow = rows[0];
                        const colCount = (headerRow.match(/\|/g) || []).length - 1;
                        const separator = '| ' + Array(colCount).fill('---').join(' | ') + ' |';
                        lines.push(headerRow);
                        lines.push(separator);
                        lines.push(...rows.slice(1));
                        lines.push('');
                    }
                }
                break;
            }

            case 'child_page':
                // Subpages are handled separately, skip
                break;

            default:
                if (data?.rich_text) {
                    lines.push(richTextToMarkdown(data.rich_text));
                    lines.push('');
                }
                break;
        }
    }

    return lines;
}

// ─── Fetch Page Blocks (with children) ───────────────────────────────────────

async function fetchBlocksWithChildren(blockId) {
    const blocks = [];
    let cursor;

    do {
        const response = await notion.blocks.children.list({
            block_id: blockId,
            start_cursor: cursor,
            page_size: 100,
        });
        blocks.push(...response.results);
        cursor = response.next_cursor;
    } while (cursor);

    // Fetch children for blocks that have them (except child_page)
    for (const block of blocks) {
        if (block.has_children && block.type !== 'child_page' && block.type !== 'table') {
            block.children = await fetchBlocksWithChildren(block.id);
        }
        // Tables need their rows
        if (block.type === 'table' && block.has_children) {
            block.children = await fetchBlocksWithChildren(block.id);
        }
    }

    return blocks;
}

// ─── Convert Page to Markdown ─────────────────────────────────────────────────

async function pageToMarkdown(pageId) {
    const blocks = await fetchBlocksWithChildren(pageId);

    // Extract frontmatter callout if present (first block, 🏷️ emoji)
    let frontmatter = null;
    let contentBlocks = blocks;

    const firstBlock = blocks[0];
    if (
        firstBlock?.type === 'callout' &&
        firstBlock.callout?.icon?.emoji === '🏷️'
    ) {
        const calloutText = richTextToMarkdown(firstBlock.callout.rich_text);
        const fmLines = calloutText.split('\n').filter(Boolean);
        const fmObj = {};
        for (const line of fmLines) {
            const colonIdx = line.indexOf(':');
            if (colonIdx !== -1) {
                fmObj[line.slice(0, colonIdx).trim()] = line.slice(colonIdx + 1).trim();
            }
        }
        if (Object.keys(fmObj).length > 0) {
            frontmatter = fmObj;
            contentBlocks = blocks.slice(1);
        }
    }

    const mdLines = blocksToMarkdown(contentBlocks);
    // Remove frontmatter placeholder comments
    const filtered = mdLines.filter(l => l !== '<!-- frontmatter -->');
    const body = filtered.join('\n').trimEnd();

    if (frontmatter) {
        const fmStr = Object.entries(frontmatter).map(([k, v]) => `${k}: ${v}`).join('\n');
        return `---\n${fmStr}\n---\n\n${body}`;
    }

    return body;
}

// ─── Traverse Notion Hierarchy ────────────────────────────────────────────────

async function fetchAllPages(pageId, localPathPrefix, results = []) {
    let cursor;
    const children = [];

    do {
        const response = await notion.blocks.children.list({
            block_id: pageId,
            start_cursor: cursor,
            page_size: 100,
        });
        children.push(...response.results.filter(b => b.type === 'child_page'));
        cursor = response.next_cursor;
    } while (cursor);

    await Promise.all(children.map(async block => {
        const title = block.child_page.title;
        const localPath = localPathPrefix ? `${localPathPrefix}/${title}` : title;

        // Get last_edited_time from the page object
        const page = await notion.pages.retrieve({ page_id: block.id });

        results.push({
            id: block.id,
            title,
            localPath,
            lastEditedTime: page.last_edited_time,
            parentId: pageId,
        });

        // Recurse into subpages
        await fetchAllPages(block.id, localPath, results);
    }));

    return results;
}

// ─── Main Backup Logic ────────────────────────────────────────────────────────

async function runBackup() {
    console.log('🔄 Starting Notion → Obsidian sync...\n');

    const state = await loadState();
    const parentId = process.env.NOTION_PARENT_PAGE_ID;

    console.log('Fetching page list from Notion...');
    const notionPages = await fetchAllPages(parentId, '');
    console.log(`Found ${notionPages.length} pages in Notion.\n`);

    const newState = { lastSync: new Date().toISOString(), pages: { ...state.pages } };
    let downloaded = 0;
    let skipped = 0;

    for (const page of notionPages) {
        const prev = state.pages[page.id];
        const changed = !prev || prev.lastEditedTime !== page.lastEditedTime;

        if (!changed) {
            skipped++;
            continue;
        }

        try {
            // Determine local file path
            // Check if this is a folder page (has child pages) — if so, write as FolderName/FolderName.md
            const localDir = path.join(config.markdownBaseDir, page.localPath);
            let filePath;

            // Check if a folder with this name exists locally or in Notion hierarchy
            const hasChildren = notionPages.some(p => p.localPath.startsWith(page.localPath + '/'));

            if (hasChildren) {
                // This is a folder page — write content as folder note
                await fs.mkdir(localDir, { recursive: true });
                filePath = path.join(localDir, `${page.title}.md`);
            } else {
                // Regular page — write in parent directory
                const parentDir = path.join(config.markdownBaseDir, path.dirname(page.localPath));
                await fs.mkdir(parentDir, { recursive: true });
                filePath = path.join(config.markdownBaseDir, page.localPath + '.md');
            }

            const markdown = await pageToMarkdown(page.id);
            await fs.writeFile(filePath, markdown, 'utf8');

            const stat = await fs.stat(filePath);
            newState.pages[page.id] = {
                localPath: page.localPath,
                lastEditedTime: page.lastEditedTime,
                mtime: stat.mtime.toISOString(),
                filePath: filePath,
            };

            console.log(`✅ Downloaded: ${page.localPath}`);
            downloaded++;
        } catch (err) {
            console.error(`❌ ERROR downloading "${page.localPath}": ${err.message}`);
        }
    }

    // Remove state entries for pages that no longer exist in Notion
    const notionPageIds = new Set(notionPages.map(p => p.id));
    for (const id of Object.keys(newState.pages)) {
        if (!notionPageIds.has(id)) {
            console.log(`🗑️  Removed from state (deleted in Notion): ${newState.pages[id].localPath}`);
            delete newState.pages[id];
        }
    }

    await saveState(newState);

    console.log(`\nDone! Downloaded: ${downloaded}, Skipped (unchanged): ${skipped} 🚀`);
}

runBackup().catch(err => console.error('A critical error occurred:', err));
