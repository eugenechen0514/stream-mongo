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
Object.defineProperty(exports, "__esModule", { value: true });
const index_1 = require("./index");
const mongodb_1 = require("mongodb");
class MongoCollectionMirrorSync {
    constructor(options) {
        this.options = Object.assign({ cloneBeforeWatch: true }, options);
    }
    __createChangeStream(sourceUrl, sourceCollection) {
        return __awaiter(this, void 0, void 0, function* () {
            const mongoClient = yield mongodb_1.MongoClient.connect(sourceUrl);
            const changeStream = mongoClient.db().collection(sourceCollection).watch(undefined);
            return changeStream;
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
            const changeStream = yield this.__createChangeStream(sourceUrl, sourceCollection);
            const transform = yield index_1.createLogChangeStreamTransform();
            const writable = index_1.mirrorChangeStreamToMongoDB({ dbURL: targetUrl, collection: targetCollection });
            changeStream.pipe(transform).pipe(writable);
            console.log('start watching...');
            // TODO: Error handling: close/error/retry
        });
    }
}
exports.MongoCollectionMirrorSync = MongoCollectionMirrorSync;
exports.default = MongoCollectionMirrorSync;
//# sourceMappingURL=MongoCollectionMirrorSync.js.map