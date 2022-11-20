const { UbiiClientNode } = require('../src/index');
const config = require('./testConfig.json');

(async function () {
  let ubiiNode = new UbiiClientNode('test-node-nodejs', config.masterNode.services, config.masterNode.topicdata);
  await ubiiNode.initialize();

  const testTopic = ubiiNode.id + '/test-topic/int32';
  ubiiNode.subscribeTopic(testTopic, (record) => {
    console.info('received "' + record.topic + '": ' + record.int32);
  });
  let counter = 0;
  let intervalPublishTestTopic = setInterval(() => {
    counter++;
    ubiiNode.publishRecord({
      topic: testTopic,
      int32: counter
    })
  }, 1000);
})();
