require('dotenv').config();
const { markdownToBlocks } = require('@tryfabric/martian');
const fs = require('fs/promises');
const path = require('path');
const { getOrCreatePageForPath, fetchAllExistingPages, notion, callWithRetry } = require('./src/notion');
const { uploadFileToS3 } = require('./src/s3');
const { findMarkdownFiles } = require('./src/utils');
const config = require('./config');

/**
 * Parses YAML frontmatter from markdown content.
 */
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

/**
 * Builds a callout block from frontmatter key/value pairs.
 */
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

/**
 * Normalizes all markdown tables so every row has the same number of columns as the header.
 */
function sanitizeTables(markdown) {
    const lines = markdown.split(/\r?\n/);
    const result = [];
    let i = 0;

    while (i < lines.length) {
        const line = lines[i];
        if (line.includes('|')) {
            const tableLines = [];
            while (i < lines.length && lines[i].includes('|')) {
                tableLines.push(lines[i]);
                i++;
            }
            const headerCells = tableLines[0].split('|').filter((_, idx, arr) => idx > 0 && idx < arr.length - 1);
            const colCount = headerCells.length || 1;
            for (const row of tableLines) {
                const parts = row.split('|');
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

/**
 * Strips all markdown tables, replacing them with a notice.
 */
function stripTables(markdown) {
    const lines = markdown.split(/\r?\n/);
    const result = [];
    let i = 0;
    let stripped = false;

    while (i < lines.length) {
        if (lines[i].includes('|')) {
            while (i < lines.length && lines[i].includes('|')) i++;
            if (!stripped) {
                result.push('> ⚠️ One or more tables were removed due to formatting issues.');
                stripped = true;
            }
        } else {
            result.push(lines[i]);
            i++;
        }
    }

    return result.join('\n');
}

/**
 * Resolves the full path of an image.
 */
async function resolveImagePath(imagePath, markdownFilePath) {
    const decodedPath = decodeURIComponent(imagePath);

    const relativePath = path.resolve(path.dirname(markdownFilePath), decodedPath);
    try {
        await fs.access(relativePath);
        return relativePath;
    } catch (e) {}

    const attachmentPath = path.join(config.markdownBaseDir, config.attachmentsDir, decodedPath);
    try {
        await fs.access(attachmentPath);
        return attachmentPath;
    } catch (e) {}

    console.warn(`    ⚠️  Could not find image: ${decodedPath}`);
    return null;
}

/**
 * Appends blocks to an existing page (for folder notes).
 */
async function appendBlocksToPage(pageId, blocks) {
    const chunks = [];
    for (let i = 0; i < blocks.length; i += 100) {
        chunks.push(blocks.slice(i, i + 100));
    }
    for (const chunk of chunks) {
        await callWithRetry(() =>
            notion.blocks.children.append({ block_id: pageId, children: chunk })
        );
    }
}

/**
 * Processes a single markdown file and creates or updates a Notion page.
 * If the file is a folder note (same name as parent folder), its content
 * is written to the folder page itself instead of creating a child page.
 */
async function processSingleFile(filePath, existingPages) {
    const pageTitle = path.basename(filePath, '.md');
    const relativePath = path.dirname(path.relative(config.markdownBaseDir, filePath));
    const parentFolderName = relativePath === '.' ? null : path.basename(relativePath);
    const isFolderNote = parentFolderName && parentFolderName === pageTitle;
    const pageKey = relativePath === '.' ? pageTitle : `${relativePath.replace(/\\/g, '/')}/${pageTitle}`;

    if (existingPages.has(pageKey)) {
        console.log(`⏭️  Skipping (already exists): ${pageTitle}`);
        return;
    }

    try {
        console.log(`Processing: ${pageTitle}${isFolderNote ? ' (folder note)' : ''}`);
        const rawContent = await fs.readFile(filePath, 'utf8');
        const { frontmatter, body } = parseFrontmatter(rawContent);
        let markdownContent = sanitizeTables(body);

        // Handle attachments
        const attachmentRegex = /!\[(.*?)\]\((?!https?:\/\/)(.*?)\)|!\[\[(.*?)(?:\|.*?)?\]\]/g;
        const attachmentMatches = [...markdownContent.matchAll(attachmentRegex)];

        if (attachmentMatches.length > 0) {
            const uploadPromises = attachmentMatches.map(match => {
                return (async () => {
                    const originalLinkText = match[0];
                    let altText = '';
                    let originalAttachmentPath = '';

                    if (match[2] !== undefined) {
                        altText = match[1];
                        originalAttachmentPath = match[2];
                    } else if (match[3] !== undefined) {
                        originalAttachmentPath = match[3];
                        altText = path.basename(originalAttachmentPath);
                    }

                    if (!originalAttachmentPath) return null;

                    try {
                        const fullAttachmentPath = await resolveImagePath(originalAttachmentPath, filePath);
                        if (fullAttachmentPath) {
                            const fileExtension = path.extname(originalAttachmentPath).toLowerCase();
                            const isImage = ['.jpg', '.jpeg', '.png', '.gif', '.webp', '.bmp', '.svg'].includes(fileExtension);
                            const s3Url = await uploadFileToS3(fullAttachmentPath);
                            if (s3Url) {
                                if (isImage) {
                                    return { original: originalLinkText, replacement: `![${altText}](${s3Url})` };
                                } else {
                                    const fileType = fileExtension.replace('.', '').toUpperCase();
                                    const displayName = altText || `${fileType} File`;
                                    return { original: originalLinkText, replacement: `[📎 ${displayName}](${s3Url})` };
                                }
                            }
                        }
                    } catch (e) {
                        console.error(`    ❌ ERROR processing attachment "${originalAttachmentPath}": ${e.message}`);
                    }
                    return null;
                })();
            });

            const uploadResults = await Promise.all(uploadPromises);
            for (const result of uploadResults) {
                if (result) markdownContent = markdownContent.replace(result.original, result.replacement);
            }
        }

        const callout = buildFrontmatterCallout(frontmatter);

        async function buildAndSendBlocks(md) {
            const notionBlocks = markdownToBlocks(md);
            return callout ? [callout, ...notionBlocks] : notionBlocks;
        }

        if (isFolderNote) {
            // Write content onto the folder page itself
            const folderPageId = await getOrCreatePageForPath(relativePath);
            try {
                const blocks = await buildAndSendBlocks(markdownContent);
                await appendBlocksToPage(folderPageId, blocks);
            } catch (err) {
                if (err.message && err.message.includes('table')) {
                    console.warn(`  ⚠️  Table error in "${pageTitle}", retrying with tables stripped...`);
                    const blocks = await buildAndSendBlocks(stripTables(markdownContent));
                    await appendBlocksToPage(folderPageId, blocks);
                } else {
                    throw err;
                }
            }
        } else {
            // Create a new child page under the parent
            const parentPageId = await getOrCreatePageForPath(relativePath);

            async function tryCreatePage(md) {
                const allBlocks = await buildAndSendBlocks(md);
                const firstChunk = allBlocks.slice(0, 100);
                const remainingChunks = [];
                for (let i = 100; i < allBlocks.length; i += 100) {
                    remainingChunks.push(allBlocks.slice(i, i + 100));
                }

                const newPage = await callWithRetry(() =>
                    notion.pages.create({
                        parent: { page_id: parentPageId },
                        properties: {
                            title: [{ text: { content: pageTitle } }],
                        },
                        children: firstChunk,
                    })
                );

                for (const chunk of remainingChunks) {
                    await callWithRetry(() =>
                        notion.blocks.children.append({ block_id: newPage.id, children: chunk })
                    );
                }
            }

            try {
                await tryCreatePage(markdownContent);
            } catch (err) {
                if (err.message && err.message.includes('table')) {
                    console.warn(`  ⚠️  Table error in "${pageTitle}", retrying with tables stripped...`);
                    await tryCreatePage(stripTables(markdownContent));
                } else {
                    throw err;
                }
            }
        }

        console.log(`✅ Synced: ${pageTitle}`);
    } catch (error) {
        console.error(`❌ ERROR syncing "${pageTitle}": ${error.message}`);
    }
}

/**
 * Main function.
 */
async function processAllMarkdown() {
    try {
        const allFiles = await findMarkdownFiles(config.markdownBaseDir);

        if (allFiles.length === 0) {
            console.log('No Markdown files found.');
            return;
        }

        const existingPages = await fetchAllExistingPages();

        console.log(`\nFound ${allFiles.length} local files. Starting sync with concurrency of ${config.concurrencyLimit}...\n`);

        for (let i = 0; i < allFiles.length; i += config.concurrencyLimit) {
            const batch = allFiles.slice(i, i + config.concurrencyLimit);
            const promises = batch.map(filePath => processSingleFile(filePath, existingPages));
            await Promise.all(promises);
        }

        console.log('\nAll files processed! 🚀');
    } catch (error) {
        console.error('A critical error occurred:', error);
    }
}

processAllMarkdown();
