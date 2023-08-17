const { v4: uuidv4 } = require('uuid');

const BaseTest = require('./baseTest');
const largeJson = require('../assets/auronautical-airspaces_2023-8-14_17-18-57.json');
const namida = require('@tum-far/namida');

const LOG_TAG = 'TestLargeTopicData';

class TestLargeTopicData extends BaseTest {
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
      const testTopic = uuidv4();
      let counter = 0;

      this.ubiiNode.subscribeTopic(testTopic, (record) => {
        //const receivedJSON = record.string && JSON.parse(record.string);
        if (record.string !== JSON.stringify(largeJson)) {
          namida.logFailure(LOG_TAG, 'received JSON does not equal original');
          reject(BaseTest.CONSTANTS.STATUS.FAILED);
        } else {
          resolve(BaseTest.CONSTANTS.STATUS.SUCCESS);
        }
      });

      this.ubiiNode.publishRecordImmediately({
        topic: testTopic,
        string: JSON.stringify(largeJson)
      });
    });
  }

  finishTest(status) {
    this.status = status;
  }
}

module.exports = TestLargeTopicData;
