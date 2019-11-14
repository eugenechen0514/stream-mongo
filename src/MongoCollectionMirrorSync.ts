import {
    cloneCollection,
    createLogChangeStreamTransform,
    mirrorChangeStreamToMongoDB
} from "./index";
import {MongoClient} from "mongodb";

export interface MongoCollectionSyncOptions {
    sourceUrl: string,
    sourceCollection: string,
    targetUrl: string,
    targetCollection: string,
    cloneBeforeWatch?: boolean,
}

export class MongoCollectionMirrorSync {
    options: MongoCollectionSyncOptions;
    constructor(options: MongoCollectionSyncOptions) {
        this.options = Object.assign({cloneBeforeWatch: true}, options);
    }

    async __createChangeStream(sourceUrl: string, sourceCollection: string) {
        const mongoClient = await MongoClient.connect(sourceUrl);
        const changeStream = mongoClient.db().collection(sourceCollection).watch(undefined);
        return changeStream;
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

        const changeStream = await this.__createChangeStream(sourceUrl, sourceCollection);
        const transform = await createLogChangeStreamTransform();
        const writable = mirrorChangeStreamToMongoDB({dbURL: targetUrl, collection: targetCollection});
        changeStream.pipe(transform).pipe(writable);
        console.log('start watching...')

        // TODO: Error handling: close/error/retry
    }
}

export default MongoCollectionMirrorSync;
