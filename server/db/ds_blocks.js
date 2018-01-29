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

const ds = require('@google-cloud/datastore').ds;

const insert = block => {

    console.log(block);

    const blockPayload = {
        key: ds.key(['blocks', block.blockNum.toString()]),
        data: block
    };

    return ds.insert(blockPayload)
             .then(() => block)
             .catch(err => {
                 // TODO HANDLE FORKS
                 console.debug(err);
                 console.warn('Unhandled duplicate block');
                 return block;
             });
};

module.exports = {
    insert
};
