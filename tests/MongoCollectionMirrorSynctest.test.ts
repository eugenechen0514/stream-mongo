import chai from 'chai';
const expect = chai.expect;

import MongoCollectionMirrorSync from "../src/MongoCollectionMirrorSync";

describe('MongoCollectionMirrorSync', function () {
    this.timeout(6000);

    it('start watch and then stop watch', (done) => {
        const sourceUrl = 'mongodb://eugene:1234@127.0.0.1/source';
        const sourceCollection = 'SourceCollection';

        const targetUrl = 'mongodb://eugene:1234@127.0.0.1/target';
        const targetCollection = 'TargetCollection';

        const sync = new MongoCollectionMirrorSync({
            cloneBeforeWatch: true,
            sourceUrl,
            sourceCollection,
            targetUrl,
            targetCollection
        });

        sync.startWatch()
            .catch(console.log);

        setTimeout(() => {
            sync.stopWatch();
            done();
        }, 5000);
    });
});
