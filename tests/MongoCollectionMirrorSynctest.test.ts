import chai from 'chai';
const expect = chai.expect;

import MongoCollectionMirrorSync from "../src/MongoCollectionMirrorSync";

describe('MongoCollectionMirrorSync', function () {
    this.timeout(3000);
    const sourceUrl = 'mongodb://eugene:1234@127.0.0.1/source';
    const sourceCollection = 'SourceCollection';

    const targetUrl = 'mongodb://eugene:1234@127.0.0.1/target';
    const targetCollection = 'TargetCollection';

    it('start watch and then stop watch', (done) => {
        const sync = new MongoCollectionMirrorSync({
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
        }, 1000);
    });

    it('when start watching, should get "start" event', (done) => {
        let startEvent = false;
        let stopEvent = false;

        const sync = new MongoCollectionMirrorSync({
            sourceUrl,
            sourceCollection,
            targetUrl,
            targetCollection
        });

        sync.startWatch()
            .catch(console.log);

        sync.on('start', () => {
            startEvent = true;
        });

        sync.stopWatch();

        setTimeout(() => {
            expect(startEvent).be.eq(true);
            done();
        }, 1000);
    });

    it('when start watching, status should be {source: true, target: true}', (done) => {

        const sync = new MongoCollectionMirrorSync({
            sourceUrl,
            sourceCollection,
            targetUrl,
            targetCollection
        });

        sync.startWatch()
            .catch(console.log);

        setTimeout(async () => {
            const status = await sync.status();
            expect(status).be.deep.eq({source: true, target: true});
            sync.stopWatch();
            done();
        }, 1000);
    });

    it('when stop watching, should get "stop" event', (done) => {
        let stopEvent = false;

        const sync = new MongoCollectionMirrorSync({
            sourceUrl,
            sourceCollection,
            targetUrl,
            targetCollection
        });

        sync.startWatch()
            .catch(console.log);

        sync.on('stop', () => {
            stopEvent = true;
        });

        sync.stopWatch();

        setTimeout(() => {
            expect(stopEvent).be.eq(true);
            done();
        }, 1000);
    });
});
