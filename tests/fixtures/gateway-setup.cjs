// Disposable CI settings only: no production credentials or CouchDB instance.
// Use the published gateway's pinned upstream encoder, matching its CLI format.
const {createRequire} = require('node:module');
const {pathToFileURL} = require('node:url');
const appRequire = createRequire('/app/bootstrap.cjs');
async function createSetupURI(passphrase) {
    const {encodeSettingsToSetupURI} = await import(pathToFileURL(appRequire.resolve('@vrtmrz/livesync-commonlib/compat/API/processSetting')).href);
    return await encodeSettingsToSetupURI({
        isConfigured: true, couchDB_URI: 'http://127.0.0.1:1', couchDB_DBNAME: 'test',
        couchDB_USER: 'test', couchDB_PASSWORD: 'test', encrypt: false,
    }, passphrase);
}
module.exports = {createSetupURI};
if (require.main === module) {
    createSetupURI(process.env.LIVESYNC_SETUP_PASSPHRASE)
        .then(uri => process.stdout.write(uri.trim()))
        .catch(() => {console.error('Could not generate disposable gateway Setup URI.'); process.exitCode = 1;});
}
