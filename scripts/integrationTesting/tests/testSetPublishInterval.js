const { v4: uuidv4 } = require('uuid');
const namida = require('@tum-far/namida');

const BaseTest = require('./baseTest');

const LOG_TAG = 'TestSetPublishInterval';

class TestSetPublishInterval extends BaseTest {
  constructor(ubiiNode) {
    super(ubiiNode);
    this.status = BaseTest.CONSTANTS.STATUS.CREATED;
    this.name = LOG_TAG;
  }

  async run() {
    const originalPublishInterval = this.ubiiNode.publishIntervalMs;
    let result = undefined;
    try {
      result = await this.executeTest();
    } catch (error) {
      result = error;
    }
    this.finishTest(result);
    this.ubiiNode.setPublishIntervalMs(originalPublishInterval);
    return result;
  }

  async abort() {
    this.finishTest(BaseTest.CONSTANTS.STATUS.ABORTED);
  }

  executeTest() {
    return new Promise((resolve, reject) => {
      /* windows process sceduler causes this test to fail for any publish interval below 55ms, on linux all rates work fine */
      const PUBLISH_INTERVAL_MS = 60;
      const MAX_COUNTER = 100;
      const testTopic = uuidv4();
      let msgCounter = 0;
      const EXPECTED_DURATION = PUBLISH_INTERVAL_MS * MAX_COUNTER;

      this.ubiiNode.setPublishIntervalMs(PUBLISH_INTERVAL_MS);

      this.ubiiNode.subscribeTopic(testTopic, () => {
        msgCounter++;
      });

      this.intervalPublishTestTopic = setInterval(() => {
        if (msgCounter === 0) {
          this.testStart = Date.now();
        }

        if (msgCounter === MAX_COUNTER) {
          this.testStop = Date.now();
          const testDuration = this.testStop - this.testStart;
          const toleranceDuration = 1.1 * EXPECTED_DURATION;
          let resultMsg =
            'duration=' +
            testDuration +
            'ms (max tolerance ' +
            toleranceDuration +
            'ms) | actual rate=' +
            testDuration / msgCounter +
            'ms (target ' +
            PUBLISH_INTERVAL_MS +
            'ms)';
          if (testDuration > toleranceDuration) {
            namida.logFailure(LOG_TAG, resultMsg);
            reject(BaseTest.CONSTANTS.STATUS.FAILED);
          } else {
            namida.logSuccess(LOG_TAG, resultMsg);
            resolve(BaseTest.CONSTANTS.STATUS.SUCCESS);
          }
        } else {
          this.ubiiNode.publishRecord({
            topic: testTopic,
            int32: 1
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

module.exports = TestSetPublishInterval;
