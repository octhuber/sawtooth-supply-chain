/**
 * Copyright 2017 Intel Corporation
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 * ----------------------------------------------------------------------------
 */
'use strict';

const _ = require('lodash');
const ds = require('@google-cloud/datastore').ds;

const addBlockState = (tableName, indexName, indexValue, doc, blockNum) => {
    // todo should be in tx
    return ds.createQuery(tableName)
             .filter(indexName, '=', indexValue)
             .filter('endBlockNum', '=', Number.MAX_SAFE_INTEGER)
             .run()
             .then(([results]) => {

                 console.log(tableName);
                 console.log('Results', results);

                 // If there are duplicates, do nothing
                 if (results.some(r => r.startBlockNum === blockNum)) {
                     return results;
                 }

                 // Otherwise, update the end block on any old docs,
                 // and insert the new one

                 const updatedBlockStates = results.map(bs => {
                     bs.endBlockNum = blockNum;
                     return bs;
                 });

                 const newBlockState = _.assign({}, doc, {
                     startBlockNum: blockNum,
                     endBlockNum: Number.MAX_SAFE_INTEGER
                 });
                 newBlockState[ds.KEY] = ds.key([tableName]);

                 return ds.upsert([...updatedBlockStates, newBlockState]);
             })
             .catch(err => {
                 console.error(err);
                 return err;
             });
};

const addAgent = (agent, blockNum) => {
    console.log('adding agent');
    console.log(agent);


    return addBlockState('agents', 'publicKey', agent.publicKey,
                         agent, blockNum
    );
};

const addRecord = (record, blockNum) => {
    return addBlockState('records', 'recordId', record.recordId,
                         record, blockNum
    );
};

const addRecordType = (type, blockNum) => {
    return addBlockState('recordTypes', 'name', type.name,
                         type, blockNum
    );
};

const addProperty = (property, blockNum) => {
    return addBlockState('properties', 'attributes',
                         ['name', 'recordId'].map(k => property[k]),
                         property, blockNum
    );
};

const addPropertyPage = (page, blockNum) => {
    return addBlockState('propertyPages', 'attributes',
                         ['name', 'recordId', 'pageNum'].map(k => page[k]),
                         page, blockNum
    );
};

const addProposal = (proposal, blockNum) => {
    return addBlockState(
        'proposals', 'attributes',
        ['recordId', 'timestamp', 'receivingAgent', 'role'].map(k => proposal[k]),
        proposal, blockNum
    );
};

module.exports = {
    addAgent,
    addRecord,
    addRecordType,
    addProperty,
    addPropertyPage,
    addProposal
};
