const { exit } = require('shelljs');
const namida = require('@tum-far/namida');

const { UbiiClientNode } = require('../../src/index');
const config = require('./testConfigZMQ.json');
const BaseTest = require('./tests/baseTest');
const TestBasicPublishSubscribe = require('./tests/testBasicPublishSubscribe');
const TestSetPublishInterval = require('./tests/testSetPublishInterval');

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
  let ubiiNode = new UbiiClientNode('test-node-nodejs', config.masterNode.services, config.masterNode.topicdata);
  await ubiiNode.initialize();

  let testBasicPublishSubscribe = new TestBasicPublishSubscribe(ubiiNode);
  await runTest(testBasicPublishSubscribe);

  let testSetPublishInterval = new TestSetPublishInterval(ubiiNode);
  await runTest(testSetPublishInterval);

  exit();
})();
