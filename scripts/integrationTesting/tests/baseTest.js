class BaseTest {
  constructor(ubiiNode) {
    if (this.constructor.name === 'BaseTest') {
      throw new Error('This is a base class and must be extended for actual test implementations!');
    }

    this.ubiiNode = ubiiNode;
  }

  async run() {
    throw new Error('Method must be overwritten!');
  }

  async abort() {
    throw new Error('Method must be overwritten!');
  }

  getStatus() {
    return this.status;
  }

  getName() {
    return this.name;
  }
}

BaseTest.CONSTANTS = Object.freeze({
  STATUS: {
    CREATED: 'created',
    STARTED: 'started',
    RUNNING: 'running',
    STOPPED: 'stopped',
    ABORTED: 'aborted',
    SUCCESS: 'success',
    FAILED: 'failed'
  }
});

module.exports = BaseTest;
