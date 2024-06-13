const { exit } = require('shelljs');
const namida = require('@tum-far/namida');

const { UbiiClientNode } = require('../../src/index');
const config = require('./testConfigZMQ.json');
const BaseTest = require('./tests/baseTest');
const TestPubSubTopic = require('./tests/testPubSubTopic');
const TestSetPublishInterval = require('./tests/testSetPublishInterval');
const TestLargeTopicData = require('./tests/testLargeTopicData');

let runTest = async (test) => {
  const testResult = await test.run();
  if (testResult === BaseTest.CONSTANTS.STATUS.SUCCESS) {
    namida.logSuccess(test.getName(), test.getStatus());
  } else if (testResult === BaseTest.CONSTANTS.STATUS.FAILED) {
    namida.logFailure(test.getName(), test.getStatus());
  } else {
    namida.log(test.getName(), test.getStatus());
  }

  return testResult;
};

(async function () {
  let ubiiNode = new UbiiClientNode('test-node-nodejs', config.masterNode.services, config.masterNode.topicdata, 5);
  await ubiiNode.initialize();

  let testBasicPublishSubscribe = new TestPubSubTopic(ubiiNode);
  await runTest(testBasicPublishSubscribe);

  let testLargeTopicData = new TestLargeTopicData(ubiiNode);
  await runTest(testLargeTopicData);

  let testSetPublishInterval = new TestSetPublishInterval(ubiiNode);
  await runTest(testSetPublishInterval);

  await ubiiNode.deinitialize();

  exit();
})();
