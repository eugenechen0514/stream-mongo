import {ChangeStream, MongoClient} from "mongodb";
import {Writable} from "stream";
import assert from 'assert';
import event from 'events';
import {
    cloneCollection,
    createLogChangeStreamTransform,
    mirrorChangeStreamToMongoDB
} from "./index";

export interface MongoCollectionSyncOptions {
    sourceUrl: string,
    sourceCollection: string,
    targetUrl: string,
    targetCollection: string,
    cloneBeforeWatch?: boolean,
}

enum MongoCollectionMirrorSyncEvent {
    start = 'start',
    stop = 'stop',
}

export class MongoCollectionMirrorSync extends event.EventEmitter {
    options: MongoCollectionSyncOptions;
    sourceClient?: MongoClient;
    targetClient?: MongoClient;
    sourceStream?: ChangeStream;
    targetStream?: Writable;

    constructor(options: MongoCollectionSyncOptions) {
        super();
        this.options = Object.assign({cloneBeforeWatch: false}, options);
    }

    async __createChangeStream(sourceUrl: string, sourceCollection: string) {
        const mongoClient = await MongoClient.connect(sourceUrl);
        const changeStream = mongoClient.db().collection(sourceCollection).watch(undefined);

        this.sourceClient = mongoClient;
        this.sourceStream = changeStream;
        return changeStream;
    }

    async __createWriteStream(targetUrl: string, targetCollection: string) {
        const mongoClient = await MongoClient.connect(targetUrl);
        const db = mongoClient.db();
        const writable = mirrorChangeStreamToMongoDB({dbURL: targetUrl, collection: targetCollection, dbConnection: db});

        this.targetClient = mongoClient;
        this.targetStream = writable;
        return writable;
    }

    async startWatch() {
        const sourceUrl = this.options.sourceUrl;
        const sourceCollection = this.options.sourceCollection;
        const targetUrl = this.options.targetUrl;
        const targetCollection = this.options.targetCollection;

        if(this.options.cloneBeforeWatch) {
            console.log('Start clone...');
            await cloneCollection(sourceUrl, sourceCollection, targetUrl, targetCollection);
            console.log('Clone done');
        }

        // Create clients
        const changeStream = await this.__createChangeStream(sourceUrl, sourceCollection);
        const writable = await this.__createWriteStream(targetUrl, targetCollection);

        const transform = await createLogChangeStreamTransform();

        changeStream.pipe(transform).pipe(writable);
        console.log('start watching...');

        this.emit(MongoCollectionMirrorSyncEvent.start);

        // TODO: Error handling: close/error/retry
    }


    async stopWatch() {
        try {
            this.sourceStream && await this.sourceStream.close();
            this.emit(MongoCollectionMirrorSyncEvent.stop);
        } catch (e) {
            return e;
        }
    }

    async status() {
        return {
            source: this.sourceClient && this.sourceClient.isConnected(),
            target: this.targetClient && this.targetClient.isConnected(),
        }
    }
}

export default MongoCollectionMirrorSync;
