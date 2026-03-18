const { Client } = require('@notionhq/client');
const { callWithRetry } = require('./utils');

const notion = new Client({ auth: process.env.NOTION_KEY });

const databasePromiseCache = new Map();
const databasePropertyCache = new Map();

/**
 * Creates a new Notion database inside the parent page.
 */
async function createNotionDatabase(title) {
    console.log(`  Creating new Notion database titled: "${title}"`);
    const response = await callWithRetry(() => notion.databases.create({
        parent: { page_id: process.env.NOTION_PARENT_PAGE_ID },
        title: [{ type: 'text', text: { content: title } }],
        properties: {
            'Name': { title: {} },
            'Created Date': { date: {} },
        },
    }));
    databasePropertyCache.set(response.id, new Set(['Name', 'Created Date']));
    return response.id;
}

/**
 * Ensures a database has all required properties, adding any that are missing as rich_text.
 */
async function ensureDatabaseProperties(databaseId, requiredProps) {
    if (!databasePropertyCache.has(databaseId)) {
        const db = await callWithRetry(() => notion.databases.retrieve({ database_id: databaseId }));
        databasePropertyCache.set(databaseId, new Set(Object.keys(db.properties)));
    }

    const existingProps = databasePropertyCache.get(databaseId);
    const missingProps = requiredProps.filter(p => !existingProps.has(p));

    if (missingProps.length === 0) return;

    console.log(`  Adding new properties to database: ${missingProps.join(', ')}`);
    const newProperties = {};
    for (const prop of missingProps) {
        newProperties[prop] = { rich_text: {} };
    }

    await callWithRetry(() => notion.databases.update({
        database_id: databaseId,
        properties: newProperties,
    }));

    for (const prop of missingProps) {
        existingProps.add(prop);
    }
}

/**
 * Gets the ID of a Notion database for a given path, creating it if it doesn't exist.
 */
function getOrCreateDatabaseForPath(relativePath) {
    const dbTitle = relativePath === '.' ? 'Root' : relativePath;

    if (databasePromiseCache.has(dbTitle)) {
        return databasePromiseCache.get(dbTitle);
    }

    const promise = (async () => {
        const response = await callWithRetry(() =>
            notion.blocks.children.list({ block_id: process.env.NOTION_PARENT_PAGE_ID })
        );
        const existingDb = response.results.find(
            block => block.type === 'child_database' && block.child_database.title === dbTitle
        );

        if (existingDb) {
            console.log(`  Found existing database for path: "${dbTitle}"`);
            return existingDb.id;
        } else {
            return await createNotionDatabase(dbTitle);
        }
    })();

    databasePromiseCache.set(dbTitle, promise);
    return promise;
}

/**
 * Fetches all pages from all databases under the parent page and returns a set of unique keys.
 */
async function fetchAllExistingPages() {
    console.log('Fetching all existing pages from Notion to speed up sync...');
    const existingKeys = new Set();

    const dbsResponse = await callWithRetry(() =>
        notion.blocks.children.list({ block_id: process.env.NOTION_PARENT_PAGE_ID })
    );
    const databaseBlocks = dbsResponse.results.filter(block => block.type === 'child_database');

    for (const dbBlock of databaseBlocks) {
        const dbTitle = dbBlock.child_database.title;
        const dbId = dbBlock.id;
        let nextCursor = undefined;

        do {
            const response = await notion.databases.query({
                database_id: dbId,
                start_cursor: nextCursor,
                page_size: 100,
            });

            for (const page of response.results) {
                const pageTitle = page.properties.Name?.title?.[0]?.plain_text;
                if (pageTitle) {
                    existingKeys.add(`${dbTitle}/${pageTitle}`);
                }
            }
            nextCursor = response.next_cursor;
        } while (nextCursor);
    }

    console.log(`Found ${existingKeys.size} existing pages across ${databaseBlocks.length} databases.`);
    return existingKeys;
}

module.exports = {
    notion,
    getOrCreateDatabaseForPath,
    ensureDatabaseProperties,
    callWithRetry,
    fetchAllExistingPages,
};
