const { Client } = require('@notionhq/client');
const { callWithRetry } = require('./utils');

const notion = new Client({ auth: process.env.NOTION_KEY });

// Cache promises (not just results) to prevent race conditions with concurrent requests
const pagePromiseCache = new Map();

/**
 * Gets or creates a folder page under a given parent.
 */
function getOrCreateFolderPage(title, parentPageId) {
    const cacheKey = `${parentPageId}/${title}`;

    if (pagePromiseCache.has(cacheKey)) {
        return pagePromiseCache.get(cacheKey);
    }

    const promise = (async () => {
        const response = await callWithRetry(() =>
            notion.blocks.children.list({ block_id: parentPageId })
        );
        const existing = response.results.find(
            block => block.type === 'child_page' && block.child_page.title === title
        );

        if (existing) {
            console.log(`  Found existing folder page: "${title}"`);
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
        return newPage.id;
    })();

    pagePromiseCache.set(cacheKey, promise);
    return promise;
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

    // Paginated fetch of all child blocks under a page
    async function listAllChildren(pageId) {
        const results = [];
        let cursor = undefined;
        do {
            const response = await callWithRetry(() =>
                notion.blocks.children.list({
                    block_id: pageId,
                    start_cursor: cursor,
                    page_size: 100,
                })
            );
            results.push(...response.results);
            cursor = response.next_cursor;
        } while (cursor);
        return results;
    }

    // Traverse concurrently at each level instead of one-by-one
    async function traverse(pageId, pathPrefix) {
        const blocks = await listAllChildren(pageId);
        const childPages = blocks.filter(b => b.type === 'child_page');

        // Register all pages at this level first
        for (const block of childPages) {
            const title = block.child_page.title;
            const fullPath = pathPrefix ? `${pathPrefix}/${title}` : title;
            existingKeys.add(fullPath);
        }

        // Then recurse into all children concurrently
        await Promise.all(childPages.map(block => {
            const title = block.child_page.title;
            const fullPath = pathPrefix ? `${pathPrefix}/${title}` : title;
            return traverse(block.id, fullPath);
        }));
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
