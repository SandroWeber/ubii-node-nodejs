const { v4: uuidv4 } = require('uuid');

const BaseTest = require('./baseTest');

const LOG_TAG = 'TestBasicPublishSubscribe';

class TestBasicPublishSubscribe extends BaseTest {
  constructor(ubiiNode) {
    super(ubiiNode);
    this.status = BaseTest.CONSTANTS.STATUS.CREATED;
    this.name = LOG_TAG;
  }

  async run() {
    let result = undefined;
    try {
      result = await this.executeTest();
    } catch (error) {
      result = error;
    }
    this.finishTest(result);
    return result;
  }

  async abort() {
    this.finishTest(BaseTest.CONSTANTS.STATUS.ABORTED);
  }

  executeTest() {
    return new Promise((resolve, reject) => {
      const PUBLISH_INTERVAL_MS = 100;
      const MAX_COUNTER = 10;
      const testTopic = uuidv4();
      let counter = 0;

      this.ubiiNode.subscribeTopic(testTopic, (record) => {
        if (record.int32 !== this.expectedCounter) {
          reject(BaseTest.CONSTANTS.STATUS.FAILED);
        }
      });

      this.intervalPublishTestTopic = setInterval(() => {
        counter++;
        if (counter === MAX_COUNTER) {
          resolve(BaseTest.CONSTANTS.STATUS.SUCCESS);
        } else {
          this.expectedCounter = counter;
          this.ubiiNode.publishRecordImmediately({
            topic: testTopic,
            int32: counter
          });
        }
      }, PUBLISH_INTERVAL_MS);
    });
  }

  finishTest(status) {
    this.intervalPublishTestTopic && clearInterval(this.intervalPublishTestTopic);
    this.status = status;
  }
}

module.exports = TestBasicPublishSubscribe;
