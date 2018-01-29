'use strict';
const Datastore = require('@google-cloud/datastore');
const config = require('../system/config');

const ds = new Datastore({ apiEndpoint: config['DATASTORE_ENDPOINT'], projectId: 'project-test' });

/**
 * @param query
 * @returns Promise.<DatastoreKey[]>
 */
function runKeysOnlyQuery(query) {
    return query.select(KEYS_ONLY_SELECTOR).run().get(0).then(results => keyUtil.mapToKeys(results));
}

Datastore.prototype.runKeysOnlyQuery = runKeysOnlyQuery;

/**
 * @param {DatastoreKey} key
 * @returns {Promise.<boolean>}
 */
function doesKeyExist(key) {
    const query = this.createQuery(key.kind).filter(KEYS_ONLY_SELECTOR, key);
    return runKeysOnlyQuery(query).then(keys => keys.length > 0);
}

Datastore.prototype.doesKeyExist = doesKeyExist;

Datastore.ds = ds;

module.exports = ds;
