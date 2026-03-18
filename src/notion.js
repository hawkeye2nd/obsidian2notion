const { Client } = require('@notionhq/client');
const { callWithRetry } = require('./utils');

const notion = new Client({ auth: process.env.NOTION_KEY });

const pageCache = new Map();

/**
 * Gets or creates a folder page under a given parent.
 */
async function getOrCreateFolderPage(title, parentPageId) {
    const cacheKey = `${parentPageId}/${title}`;
    if (pageCache.has(cacheKey)) {
        return pageCache.get(cacheKey);
    }

    const response = await callWithRetry(() =>
        notion.blocks.children.list({ block_id: parentPageId })
    );
    const existing = response.results.find(
        block => block.type === 'child_page' && block.child_page.title === title
    );

    if (existing) {
        console.log(`  Found existing folder page: "${title}"`);
        pageCache.set(cacheKey, existing.id);
        return existing.id;
    }

    console.log(`  Creating new folder page: "${title}"`);
    const newPage = await callWithRetry(() =>
        notion.pages.create({
            parent: { page_id: parentPageId },
            properties: {
                title: [{ text: { content: title } }],
            },
        })
    );

    pageCache.set(cacheKey, newPage.id);
    return newPage.id;
}

/**
 * Resolves the parent page ID for a given relative path, creating folder pages as needed.
 */
async function getOrCreatePageForPath(relativePath) {
    if (relativePath === '.') {
        return process.env.NOTION_PARENT_PAGE_ID;
    }

    const parts = relativePath.split(/[\\/]/);
    let currentParentId = process.env.NOTION_PARENT_PAGE_ID;

    for (const part of parts) {
        currentParentId = await getOrCreateFolderPage(part, currentParentId);
    }

    return currentParentId;
}

/**
 * Traverses the page hierarchy and returns a set of existing page title paths.
 */
async function fetchAllExistingPages() {
    console.log('Fetching all existing pages from Notion to speed up sync...');
    const existingKeys = new Set();

    async function traverse(pageId, pathPrefix) {
        const response = await callWithRetry(() =>
            notion.blocks.children.list({ block_id: pageId })
        );
        for (const block of response.results) {
            if (block.type === 'child_page') {
                const title = block.child_page.title;
                const fullPath = pathPrefix ? `${pathPrefix}/${title}` : title;
                existingKeys.add(fullPath);
                await traverse(block.id, fullPath);
            }
        }
    }

    await traverse(process.env.NOTION_PARENT_PAGE_ID, '');
    console.log(`Found ${existingKeys.size} existing pages.`);
    return existingKeys;
}

module.exports = {
    notion,
    getOrCreatePageForPath,
    callWithRetry,
    fetchAllExistingPages,
};
