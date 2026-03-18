const path = require('path');

module.exports = {
  // The base directory for your markdown files
markdownBaseDir: path.join('C:\\Users\\A\\Documents\\Verse'),

  // The name of the folder where global attachments are stored
  attachmentsDir: 'Attachments',

  // The number of files to process concurrently
  concurrencyLimit: 5,

  // The name for the database that holds notes from the root directory
  rootDatabaseName: 'Root',
};
