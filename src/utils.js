const fs = require('fs/promises');
const path = require('path');

/**
 * A utility function to add a delay.
 */
const delay = (ms) => new Promise(resolve => setTimeout(resolve, ms));

/**
 * A wrapper for Notion API calls to handle rate limiting, conflicts, and temporary server errors.
 */
async function callWithRetry(apiCall, retries = 5) {
    let attempt = 0;
    while (attempt < retries) {
        try {
            return await apiCall();
        } catch (error) {
            if (error.code === 'conflict_error' || error.code === 'rate_limited' || (error.status && error.status >= 500)) {
                attempt++;
                if (attempt >= retries) throw error;
                const waitTime = Math.pow(2, attempt) * 1000;
                console.log(`  ... Notion API error (${error.status || error.code}). Retrying in ${waitTime/1000}s (Attempt ${attempt}/${retries-1})`);
                await delay(waitTime);
            } else {
                throw error;
            }
        }
    }
}

/**
 * Recursively finds all Markdown files in a directory.
 * Skips hidden folders (starting with '.') and any folders in config.excludedFolders.
 */
async function findMarkdownFiles(dir) {
    const config = require('./config');
    const excluded = config.excludedFolders || [];
    let markdownFiles = [];
    try {
        const items = await fs.readdir(dir);
        for (const item of items) {
            if (item.startsWith('.')) continue;
            if (excluded.includes(item)) continue;

            const fullPath = path.join(dir, item);
            const stat = await fs.stat(fullPath);
            if (stat.isDirectory()) {
                markdownFiles = markdownFiles.concat(await findMarkdownFiles(fullPath));
            } else if (path.extname(item).toLowerCase() === '.md') {
                markdownFiles.push(fullPath);
            }
        }
    } catch (error) {
        console.error(`Error reading directory ${dir}: ${error.message}`);
    }
    return markdownFiles;
}

module.exports = {
    delay,
    callWithRetry,
    findMarkdownFiles,
};
