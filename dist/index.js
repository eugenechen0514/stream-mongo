"use strict";
var __createBinding = (this && this.__createBinding) || (Object.create ? (function(o, m, k, k2) {
    if (k2 === undefined) k2 = k;
    Object.defineProperty(o, k2, { enumerable: true, get: function() { return m[k]; } });
}) : (function(o, m, k, k2) {
    if (k2 === undefined) k2 = k;
    o[k2] = m[k];
}));
var __exportStar = (this && this.__exportStar) || function(m, exports) {
    for (var p in m) if (p !== "default" && !exports.hasOwnProperty(p)) __createBinding(exports, m, p);
};
var __awaiter = (this && this.__awaiter) || function (thisArg, _arguments, P, generator) {
    function adopt(value) { return value instanceof P ? value : new P(function (resolve) { resolve(value); }); }
    return new (P || (P = Promise))(function (resolve, reject) {
        function fulfilled(value) { try { step(generator.next(value)); } catch (e) { reject(e); } }
        function rejected(value) { try { step(generator["throw"](value)); } catch (e) { reject(e); } }
        function step(result) { result.done ? resolve(result.value) : adopt(result.value).then(fulfilled, rejected); }
        step((generator = generator.apply(thisArg, _arguments || [])).next());
    });
};
var __importDefault = (this && this.__importDefault) || function (mod) {
    return (mod && mod.__esModule) ? mod : { "default": mod };
};
Object.defineProperty(exports, "__esModule", { value: true });
exports.createLogChangeStreamTransform = exports.cloneCollection = exports.createLogDocIdTransform = exports.createAllStream = exports.mirrorChangeStreamToMongoDB = exports.cloneStreamToMongoDB = exports.insertStreamToMongoDB = void 0;
const assert_1 = __importDefault(require("assert"));
const stream_1 = require("stream");
const mongodb_1 = require("mongodb");
const MongoTypes_1 = require("./MongoTypes");
__exportStar(require("./MongoCollectionMirrorSync"), exports);
__exportStar(require("./MongoTypes"), exports);
/**
 *
 * Source code from: https://github.com/AbdullahAli/node-stream-to-mongo-db
 */
function insertStreamToMongoDB(options = { dbURL: '', collection: '' }) {
    assert_1.default(options.dbURL, '"dbURL" should no empty');
    assert_1.default(options.collection, '"collection" should no empty');
    const config = Object.assign(
    // default config
    {
        batchSize: 1,
        insertOptions: { w: 1 },
    }, 
    // overrided options
    options);
    // those variables can't be initialized without Promises, so we wait first drain
    let client;
    let dbConnection;
    let collection;
    let records = [];
    // this function is usefull to insert records and reset the records array
    const insert = () => __awaiter(this, void 0, void 0, function* () {
        yield collection.insertMany(records, config.insertOptions);
        records = [];
    });
    const close = () => __awaiter(this, void 0, void 0, function* () {
        if (!config.dbConnection && client) {
            yield client.close();
        }
    });
    // stream
    const writable = new stream_1.Writable({
        objectMode: true,
        write: (record, encoding, next) => __awaiter(this, void 0, void 0, function* () {
            try {
                // connection
                if (!dbConnection) {
                    if (config.dbConnection) {
                        dbConnection = config.dbConnection; // eslint-disable-line prefer-destructuring
                    }
                    else {
                        client = yield mongodb_1.MongoClient.connect(config.dbURL, { useNewUrlParser: true });
                        dbConnection = yield client.db();
                    }
                }
                if (!collection)
                    collection = yield dbConnection.collection(config.collection);
                // add to batch records
                records.push(record);
                // insert and reset batch recors
                if (records.length >= config.batchSize) {
                    yield insert();
                }
                // next stream
                next();
            }
            catch (error) {
                yield close();
                writable.emit('error', error);
            }
        })
    });
    writable.on('finish', () => __awaiter(this, void 0, void 0, function* () {
        try {
            if (records.length > 0)
                yield insert();
            yield close();
            writable.emit('close');
        }
        catch (error) {
            yield close();
            writable.emit('error', error);
        }
    }));
    return writable;
}
exports.insertStreamToMongoDB = insertStreamToMongoDB;
function cloneStreamToMongoDB(options = { dbURL: '', collection: '' }) {
    assert_1.default(options.dbURL, '"dbURL" should no empty');
    assert_1.default(options.collection, '"collection" should no empty');
    const config = Object.assign(
    // default config
    {
        batchSize: 1,
        insertOptions: { w: 1 },
    }, 
    // overrided options
    options);
    // those variables can't be initialized without Promises, so we wait first drain
    let client;
    let dbConnection;
    let collection;
    let records = [];
    // this function is usefull to insert records and reset the records array
    const upsertFun = () => __awaiter(this, void 0, void 0, function* () {
        const ops = records.map(doc => {
            assert_1.default(doc._id);
            return {
                replaceOne: {
                    filter: { _id: doc._id },
                    replacement: doc,
                    upsert: true,
                }
            };
        });
        yield collection.bulkWrite(ops);
        records = [];
    });
    const close = () => __awaiter(this, void 0, void 0, function* () {
        if (!config.dbConnection && client) {
            yield client.close();
        }
    });
    // stream
    const writable = new stream_1.Writable({
        objectMode: true,
        write: (record, encoding, next) => __awaiter(this, void 0, void 0, function* () {
            try {
                // connection
                if (!dbConnection) {
                    if (config.dbConnection) {
                        dbConnection = config.dbConnection; // eslint-disable-line prefer-destructuring
                    }
                    else {
                        client = yield mongodb_1.MongoClient.connect(config.dbURL, { useNewUrlParser: true });
                        dbConnection = yield client.db();
                    }
                }
                if (!collection)
                    collection = yield dbConnection.collection(config.collection);
                // add to batch records
                records.push(record);
                // insert and reset batch recors
                if (records.length >= config.batchSize) {
                    yield upsertFun();
                }
                // next stream
                next();
            }
            catch (error) {
                yield close();
                writable.emit('error', error);
            }
        })
    });
    writable.on('finish', () => __awaiter(this, void 0, void 0, function* () {
        try {
            if (records.length > 0)
                yield upsertFun();
            yield close();
            writable.emit('close');
        }
        catch (error) {
            yield close();
            writable.emit('error', error);
        }
    }));
    return writable;
}
exports.cloneStreamToMongoDB = cloneStreamToMongoDB;
function mirrorChangeStreamToMongoDB(options = { dbURL: '', collection: '' }) {
    assert_1.default(options.dbURL, '"dbURL" should no empty');
    assert_1.default(options.collection, '"collection" should no empty');
    const config = Object.assign(
    // default config
    {
        batchSize: 1,
        insertOptions: { w: 1 },
    }, 
    // overrided options
    options);
    // TODO: Fix the limitation. When batchSize > 1, residue data may not be committed
    assert_1.default(config.batchSize === 1, 'only support batchSize = 1');
    // those variables can't be initialized without Promises, so we wait first drain
    let client;
    let dbConnection;
    let collection;
    let records = [];
    // this function is useful to insert records and reset the records array
    const bulkWriteFun = () => __awaiter(this, void 0, void 0, function* () {
        const ops = records.map(doc => {
            if (doc.operationType === MongoTypes_1.ChangeOpType.insert) {
                assert_1.default(doc.fullDocument);
                return {
                    insertOne: {
                        document: doc.fullDocument
                    }
                };
            }
            if (doc.operationType === MongoTypes_1.ChangeOpType.delete) {
                assert_1.default(doc.documentKey._id);
                return {
                    deleteOne: {
                        filter: { _id: doc.documentKey._id }
                    }
                };
            }
            if (doc.operationType === MongoTypes_1.ChangeOpType.replace) {
                assert_1.default(doc.fullDocument);
                assert_1.default(doc.documentKey._id);
                return {
                    replaceOne: {
                        filter: { _id: doc.documentKey._id },
                        replacement: doc.fullDocument
                    }
                };
            }
            if (doc.operationType === MongoTypes_1.ChangeOpType.update) {
                const updateDoc = doc;
                assert_1.default(doc.documentKey._id);
                if (doc.fullDocument) {
                    return {
                        updateOne: {
                            filter: { _id: doc.documentKey._id },
                            update: { $set: doc.fullDocument }
                        }
                    };
                }
                assert_1.default(updateDoc.updateDescription);
                const { updatedFields = {}, removedFields = [] } = updateDoc.updateDescription;
                const updateObj = {};
                if (Object.keys(updatedFields).length > 0) {
                    updateObj['$set'] = updatedFields;
                }
                if (removedFields.length > 0) {
                    const unsetObj = {};
                    removedFields.forEach(f => {
                        unsetObj[f] = '';
                    });
                    updateObj['$unset'] = unsetObj;
                }
                return {
                    updateOne: {
                        filter: { _id: doc.documentKey._id },
                        update: updateObj,
                    }
                };
            }
            throw new Error('invalid operationType');
        });
        // @ts-ignore
        yield collection.bulkWrite(ops);
        records = [];
    });
    const close = () => __awaiter(this, void 0, void 0, function* () {
        if (!config.dbConnection && client) {
            yield client.close();
        }
    });
    // stream
    const writable = new stream_1.Writable({
        objectMode: true,
        write: (record, encoding, next) => __awaiter(this, void 0, void 0, function* () {
            try {
                // connection
                if (!dbConnection) {
                    if (config.dbConnection) {
                        dbConnection = config.dbConnection;
                    }
                    else {
                        client = yield mongodb_1.MongoClient.connect(config.dbURL, { useNewUrlParser: true });
                        dbConnection = yield client.db();
                    }
                }
                if (!collection)
                    collection = yield dbConnection.collection(config.collection);
                // add to batch records
                records.push(record);
                // insert and reset batch recors
                if (records.length >= config.batchSize) {
                    yield bulkWriteFun();
                }
                // next stream
                next();
            }
            catch (error) {
                yield close();
                writable.emit('error', error);
            }
        })
    });
    writable.on('finish', () => __awaiter(this, void 0, void 0, function* () {
        try {
            if (records.length > 0) {
                yield bulkWriteFun();
            }
            yield close();
            writable.emit('close');
        }
        catch (error) {
            yield close();
            writable.emit('error', error);
        }
    }));
    return writable;
}
exports.mirrorChangeStreamToMongoDB = mirrorChangeStreamToMongoDB;
function createAllStream(sourceUrl, sourceCollection, options = { query: {} }) {
    return __awaiter(this, void 0, void 0, function* () {
        const { query = {} } = options;
        const mongoClient = yield mongodb_1.MongoClient.connect(sourceUrl);
        return mongoClient.db().collection(sourceCollection).find(query).stream();
    });
}
exports.createAllStream = createAllStream;
function createLogDocIdTransform() {
    return __awaiter(this, void 0, void 0, function* () {
        const myTransform = new stream_1.Transform({
            objectMode: true,
            transform(obj, encoding, callback) {
                console.log(`Clone ${obj._id}`);
                callback(null, obj);
            }
        });
        return myTransform;
    });
}
exports.createLogDocIdTransform = createLogDocIdTransform;
function cloneCollection(sourceUrl, sourceCollection, targetUrl, targetCollection, options = {}) {
    return __awaiter(this, void 0, void 0, function* () {
        const inputStream = yield createAllStream(sourceUrl, sourceCollection, { query: options.query });
        const transformStream = yield createLogDocIdTransform();
        const outputStream = yield cloneStreamToMongoDB({ dbURL: targetUrl, collection: targetCollection });
        const stream = inputStream.pipe(transformStream).pipe(outputStream);
        return new Promise((resolve, reject) => {
            stream.on('finish', () => {
                resolve();
            });
            stream.on('error', (err) => {
                reject(err);
            });
        });
    });
}
exports.cloneCollection = cloneCollection;
function createLogChangeStreamTransform() {
    return __awaiter(this, void 0, void 0, function* () {
        const myTransform = new stream_1.Transform({
            objectMode: true,
            transform(obj, encoding, callback) {
                console.log(`${obj.operationType} -> ${JSON.stringify(obj.documentKey)}`);
                callback(null, obj);
            }
        });
        return myTransform;
    });
}
exports.createLogChangeStreamTransform = createLogChangeStreamTransform;
//# sourceMappingURL=index.js.map