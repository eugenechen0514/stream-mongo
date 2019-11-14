"use strict";
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
const mongodb_1 = require("mongodb");
const assert_1 = __importDefault(require("assert"));
const events_1 = __importDefault(require("events"));
const index_1 = require("./index");
var MongoCollectionMirrorSyncEvent;
(function (MongoCollectionMirrorSyncEvent) {
    MongoCollectionMirrorSyncEvent["start"] = "start";
    MongoCollectionMirrorSyncEvent["stop"] = "stop";
})(MongoCollectionMirrorSyncEvent || (MongoCollectionMirrorSyncEvent = {}));
class MongoCollectionMirrorSync extends events_1.default.EventEmitter {
    constructor(options) {
        super();
        this.options = Object.assign({ cloneBeforeWatch: false }, options);
    }
    __createChangeStream(sourceUrl, sourceCollection) {
        return __awaiter(this, void 0, void 0, function* () {
            assert_1.default(sourceUrl);
            assert_1.default(sourceCollection);
            const mongoClient = yield mongodb_1.MongoClient.connect(sourceUrl);
            const changeStream = mongoClient.db().collection(sourceCollection).watch(undefined);
            this.sourceClient = mongoClient;
            this.sourceStream = changeStream;
            return changeStream;
        });
    }
    __createWriteStream(targetUrl, targetCollection) {
        return __awaiter(this, void 0, void 0, function* () {
            assert_1.default(targetUrl);
            assert_1.default(targetCollection);
            const mongoClient = yield mongodb_1.MongoClient.connect(targetUrl);
            const db = mongoClient.db();
            const writable = index_1.mirrorChangeStreamToMongoDB({ dbURL: targetUrl, collection: targetCollection, dbConnection: db });
            this.targetClient = mongoClient;
            this.targetStream = writable;
            return writable;
        });
    }
    startWatch() {
        return __awaiter(this, void 0, void 0, function* () {
            const sourceUrl = this.options.sourceUrl;
            const sourceCollection = this.options.sourceCollection;
            const targetUrl = this.options.targetUrl;
            const targetCollection = this.options.targetCollection;
            if (this.options.cloneBeforeWatch) {
                console.log('Start clone...');
                yield index_1.cloneCollection(sourceUrl, sourceCollection, targetUrl, targetCollection);
                console.log('Clone done');
            }
            // Create clients
            const changeStream = yield this.__createChangeStream(sourceUrl, sourceCollection);
            const writable = yield this.__createWriteStream(targetUrl, targetCollection);
            const transform = yield index_1.createLogChangeStreamTransform();
            changeStream.pipe(transform).pipe(writable);
            console.log('start watching...');
            this.emit(MongoCollectionMirrorSyncEvent.start);
            // TODO: Error handling: close/error/retry
        });
    }
    stopWatch() {
        return __awaiter(this, void 0, void 0, function* () {
            try {
                yield Promise.all([this.targetClient && this.targetClient.close(), this.sourceClient && this.sourceClient.close()]);
                this.emit(MongoCollectionMirrorSyncEvent.stop);
            }
            catch (e) {
                return e;
            }
        });
    }
    status() {
        return __awaiter(this, void 0, void 0, function* () {
            return {
                source: this.sourceClient && this.sourceClient.isConnected(),
                target: this.targetClient && this.targetClient.isConnected(),
            };
        });
    }
}
exports.MongoCollectionMirrorSync = MongoCollectionMirrorSync;
exports.default = MongoCollectionMirrorSync;
//# sourceMappingURL=MongoCollectionMirrorSync.js.map