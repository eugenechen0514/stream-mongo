import chai from 'chai';
const expect = chai.expect;

import MongoCollectionMirrorSync from "../src/MongoCollectionMirrorSync";

describe('MongoCollectionMirrorSync connection', function () {
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

        setTimeout(async () => {
            await sync.stopWatch();
            done();
        }, 1000);
    });

    it('when start watching, should get "start" event', (done) => {
        let startEvent = false;

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

        setTimeout(async () => {
            expect(startEvent).be.eq(true);
            await sync.stopWatch();
            done();
        }, 1000);
    });

    it('when start watching, "status" should be {source: true, target: true}', (done) => {

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
            await sync.stopWatch();
            done();
        }, 1000);
    });

    it('when stop watching, "status" should be {source: false, target: false}', (done) => {
        const sync = new MongoCollectionMirrorSync({
            sourceUrl,
            sourceCollection,
            targetUrl,
            targetCollection
        });

        sync.startWatch()
            .catch(console.log);


        setTimeout(async () => {
            await sync.stopWatch();

            const status = await sync.status();
            expect(status).be.deep.eq({source: false, target: false});
            done();
        }, 2000);
    });

    it('when start watching and close source client, "status" should be {source: false, target: true}', (done) => {
        const sync = new MongoCollectionMirrorSync({
            sourceUrl,
            sourceCollection,
            targetUrl,
            targetCollection
        });

        sync.startWatch()
            .catch(console.log);


        setTimeout(async () => {
             sync.sourceClient && await sync.sourceClient.close();

            const status = await sync.status();
            expect(status).be.deep.eq({source: false, target: true});

            await sync.stopWatch();
            done();
        }, 2000);
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

        setTimeout(async () => {
            await sync.stopWatch();
            expect(stopEvent).be.eq(true);
            done();
        }, 1000);
    });
});
