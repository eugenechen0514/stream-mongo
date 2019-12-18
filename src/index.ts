import assert from 'assert';
import {Transform, Writable} from 'stream';
import {Collection, CollectionInsertManyOptions, Db, MongoClient, ObjectId} from 'mongodb';
import {ChangeOpType, ChangeStreamEventObject, UpdateEventObject} from "./MongoTypes";

export * from './MongoCollectionMirrorSync';
export * from './MongoTypes';

export interface InsertStreamToMongoDBOptions {
    batchSize?: number,
    insertOptions?: CollectionInsertManyOptions,
    dbConnection?: Db,
    dbURL: string,
    collection: string,
}

export interface MirrorStreamToMongoDBOptions {
    batchSize?: number,
    dbConnection?: Db,
    dbURL: string,
    collection: string,
}

interface UnsetObject {
    [key: string] : string
}

/**
 *
 * Source code from: https://github.com/AbdullahAli/node-stream-to-mongo-db
 */
export function insertStreamToMongoDB(options: InsertStreamToMongoDBOptions = {dbURL: '', collection: ''}) {
    assert(options.dbURL, '"dbURL" should no empty');
    assert(options.collection, '"collection" should no empty');

    const config = Object.assign(
        // default config
        {
            batchSize: 1,
            insertOptions: { w: 1 },
        },
        // overrided options
        options,
    );

    // those variables can't be initialized without Promises, so we wait first drain
    let client: MongoClient;
    let dbConnection: Db;
    let collection: Collection;
    let records: any[] = [];

    // this function is usefull to insert records and reset the records array
    const insert = async () => {
        await collection.insertMany(records, config.insertOptions);
        records = [];
    };

    const close = async () => {
        if (!config.dbConnection && client) {
            await client.close();
        }
    };

    // stream
    const writable = new Writable({
        objectMode: true,
        write: async (record, encoding, next) => {
            try {
                // connection
                if (!dbConnection) {
                    if (config.dbConnection) {
                        dbConnection = config.dbConnection; // eslint-disable-line prefer-destructuring
                    } else {
                        client = await MongoClient.connect(config.dbURL, { useNewUrlParser: true });
                        dbConnection = await client.db();
                    }
                }
                if (!collection) collection = await dbConnection.collection(config.collection);

                // add to batch records
                records.push(record);

                // insert and reset batch recors
                if (records.length >= config.batchSize){
                    await insert();
                }

                // next stream
                next();
            } catch (error) {
                await close();
                writable.emit('error', error);
            }
        }
    });

    writable.on('finish', async () => {
        try {
            if (records.length > 0) await insert();
            await close();

            writable.emit('close');
        } catch(error) {
            await close();

            writable.emit('error', error);
        }
    });

    return writable;
}

export function cloneStreamToMongoDB(options: InsertStreamToMongoDBOptions = {dbURL: '', collection: ''}) {
    assert(options.dbURL, '"dbURL" should no empty');
    assert(options.collection, '"collection" should no empty');

    const config = Object.assign(
        // default config
        {
            batchSize: 1,
            insertOptions: { w: 1 },
        },
        // overrided options
        options,
    );

    // those variables can't be initialized without Promises, so we wait first drain
    let client: MongoClient;
    let dbConnection: Db;
    let collection: Collection;
    let records: any[] = [];

    // this function is usefull to insert records and reset the records array
    const upsertFun = async () => {
        const ops = records.map(doc => {
            assert(doc._id);
            return {
                replaceOne: {
                    filter: {_id: doc._id},
                    replacement: doc,
                    upsert: true,
                }} ;
        });
        await collection.bulkWrite(ops);
        records = [];
    };

    const close = async () => {
        if (!config.dbConnection && client) {
            await client.close();
        }
    };

    // stream
    const writable = new Writable({
        objectMode: true,
        write: async (record, encoding, next) => {
            try {
                // connection
                if (!dbConnection) {
                    if (config.dbConnection) {
                        dbConnection = config.dbConnection; // eslint-disable-line prefer-destructuring
                    } else {
                        client = await MongoClient.connect(config.dbURL, { useNewUrlParser: true });
                        dbConnection = await client.db();
                    }
                }
                if (!collection) collection = await dbConnection.collection(config.collection);

                // add to batch records
                records.push(record);

                // insert and reset batch recors
                if (records.length >= config.batchSize){
                    await upsertFun();
                }

                // next stream
                next();
            } catch (error) {
                await close();
                writable.emit('error', error);
            }
        }
    });

    writable.on('finish', async () => {
        try {
            if (records.length > 0) await upsertFun();
            await close();

            writable.emit('close');
        } catch(error) {
            await close();

            writable.emit('error', error);
        }
    });

    return writable;
}

export function mirrorChangeStreamToMongoDB(options: MirrorStreamToMongoDBOptions = {dbURL: '', collection: ''}) {
    assert(options.dbURL, '"dbURL" should no empty');
    assert(options.collection, '"collection" should no empty');

    const config = Object.assign(
        // default config
        {
            batchSize: 1,
            insertOptions: { w: 1 },
        },
        // overrided options
        options,
    );

    // TODO: Fix the limitation. When batchSize > 1, residue data may not be committed
    assert(config.batchSize === 1, 'only support batchSize = 1');

    // those variables can't be initialized without Promises, so we wait first drain
    let client: MongoClient;
    let dbConnection: Db;
    let collection: Collection;
    let records: ChangeStreamEventObject[] = [];

    // this function is useful to insert records and reset the records array
    const bulkWriteFun = async () => {
        const ops = records.map(doc => {
            if (doc.operationType === ChangeOpType.insert) {
                assert(doc.fullDocument);
                return {
                    insertOne: {
                        document: doc.fullDocument
                    }} ;
            }
            if (doc.operationType === ChangeOpType.delete) {
                assert(doc.documentKey._id);
                return {
                    deleteOne: {
                        filter: {_id: doc.documentKey._id}
                    }} ;
            }
            if (doc.operationType === ChangeOpType.replace) {
                assert(doc.fullDocument);
                assert(doc.documentKey._id);
                return {
                    replaceOne: {
                        filter: {_id: doc.documentKey._id},
                        replacement: doc.fullDocument
                    }} ;
            }
            if (doc.operationType === ChangeOpType.update) {
                const updateDoc = doc as UpdateEventObject;
                assert(doc.documentKey._id);
                if(doc.fullDocument) {
                    return {
                        updateOne: {
                            filter: {_id: doc.documentKey._id},
                            update: { $set : doc.fullDocument }
                        }} ;
                }

                assert(updateDoc.updateDescription);
                const {updatedFields = {}, removedFields = []} = updateDoc.updateDescription;

                const updateObj: {$set?: object, $unset?: object} = {};

                if(Object.keys(updatedFields).length > 0) {
                    updateObj['$set'] = updatedFields;
                }

                if(removedFields.length > 0) {
                    const unsetObj: UnsetObject = {};
                    removedFields.forEach(f => {
                        unsetObj[f] = '';
                    });
                    updateObj['$unset'] = unsetObj;
                }

                return {
                    updateOne: {
                        filter: {_id: doc.documentKey._id},
                        update: updateObj,
                    }} ;
            }
            throw new Error('invalid operationType');
        });
        await collection.bulkWrite(ops);
        records = [];
    };

    const close = async () => {
        if (!config.dbConnection && client) {
            await client.close();
        }
    };

    // stream
    const writable = new Writable({
        objectMode: true,
        write: async (record, encoding, next) => {
            try {
                // connection
                if (!dbConnection) {
                    if (config.dbConnection) {
                        dbConnection = config.dbConnection;
                    } else {
                        client = await MongoClient.connect(config.dbURL, { useNewUrlParser: true });
                        dbConnection = await client.db();
                    }
                }
                if (!collection) collection = await dbConnection.collection(config.collection);

                // add to batch records
                records.push(record);

                // insert and reset batch recors
                if (records.length >= config.batchSize){
                    await bulkWriteFun();
                }

                // next stream
                next();
            } catch (error) {
                await close();
                writable.emit('error', error);
            }
        }
    });

    writable.on('finish', async () => {
        try {
            if (records.length > 0) {
                await bulkWriteFun();
            }
            await close();
            writable.emit('close');
        } catch(error) {
            await close();
            writable.emit('error', error);
        }
    });

    return writable;
}

export async function createAllStream(sourceUrl: string, sourceCollection: string, options: {query?: any} = {query: {}}) {
    const {query = {}} = options;
    const mongoClient = await MongoClient.connect(sourceUrl);
    return mongoClient.db().collection(sourceCollection).find(query).stream();
}

export async function createLogDocIdTransform() {
    const myTransform = new Transform({
        objectMode: true,
        transform(obj: object & {_id: string | ObjectId}, encoding, callback) {
            console.log(`Clone ${obj._id}`);
            callback(null, obj)
        }
    });
    return myTransform
}

export async function cloneCollection(sourceUrl: string, sourceCollection: string , targetUrl: string, targetCollection: string) {
    const inputStream = await createAllStream(sourceUrl, sourceCollection);
    const transformStream = await createLogDocIdTransform();
    const outputStream = await cloneStreamToMongoDB({dbURL: targetUrl, collection: targetCollection});

    const stream = inputStream.pipe(transformStream).pipe(outputStream);
    return new Promise((resolve, reject) => {
        stream.on('finish', () => {
            resolve();
        });
        stream.on('error', (err) => {
            reject(err);
        });
    });
}

export async function createLogChangeStreamTransform() {
    const myTransform = new Transform({
        objectMode: true,
        transform(obj: ChangeStreamEventObject, encoding, callback) {
            console.log(`${obj.operationType} -> ${JSON.stringify(obj.documentKey)}`);
            callback(null, obj)
        }
    });
    return myTransform
}
