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
 * Returns { frontmatter: {}, body: '' }
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
 * Sanitizes markdown tables so all rows have the same number of columns as the header.
 */
function sanitizeTables(markdown) {
    return markdown.replace(
        /((?:\|.*\|\r?\n)+)/g,
        (tableBlock) => {
            const rows = tableBlock.trimEnd().split(/\r?\n/);
            const headerCols = (rows[0].match(/\|/g) || []).length - 1;
            if (headerCols <= 0) return tableBlock;

            const sanitized = rows.map(row => {
                const cells = row.split('|');
                const inner = cells.slice(1, cells.length - 1);
                while (inner.length < headerCols) inner.push('');
                const trimmed = inner.slice(0, headerCols);
                return '|' + trimmed.join('|') + '|';
            });

            return sanitized.join('\n') + '\n';
        }
    );
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
 * Processes a single markdown file and creates a Notion page under the appropriate folder page.
 */
async function processSingleFile(filePath, existingPages) {
    const pageTitle = path.basename(filePath, '.md');
    const relativePath = path.dirname(path.relative(config.markdownBaseDir, filePath));
    const pageKey = relativePath === '.' ? pageTitle : `${relativePath.replace(/\\/g, '/')}/${pageTitle}`;

    if (existingPages.has(pageKey)) {
        console.log(`⏭️  Skipping (already exists): ${pageTitle}`);
        return;
    }

    try {
        const parentPageId = await getOrCreatePageForPath(relativePath);

        console.log(`Processing: ${pageTitle}`);
        const rawContent = await fs.readFile(filePath, 'utf8');
        const { frontmatter, body } = parseFrontmatter(rawContent);
        let markdownContent = sanitizeTables(body);

        // Handle attachments
        const attachmentRegex = /!\[(.*?)\]\((?!https?:\/\/)(.*?)\)|!\[\[(.*?)(?:\|.*?)?\]\]/g;
        const attachmentMatches = [...markdownContent.matchAll(attachmentRegex)];

        if (attachmentMatches.length > 0) {
            console.log(`  Found ${attachmentMatches.length} local attachment(s) in ${pageTitle}`);
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
                if (result) {
                    markdownContent = markdownContent.replace(result.original, result.replacement);
                }
            }
        }

        const notionBlocks = markdownToBlocks(markdownContent);

        // Prepend frontmatter callout if present
        const callout = buildFrontmatterCallout(frontmatter);
        const allBlocks = callout ? [callout, ...notionBlocks] : notionBlocks;

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
                notion.blocks.children.append({
                    block_id: newPage.id,
                    children: chunk,
                })
            );
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
