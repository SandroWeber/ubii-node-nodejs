const namida = require('@tum-far/namida');
const TopicMuxer = require('./topicMuxer');

let _instance = null;
const SINGLETON_ENFORCER = Symbol();

class DeviceManager {
  constructor(enforcer) {
    if (enforcer !== SINGLETON_ENFORCER) {
      throw new Error('Use ' + this.constructor.name + '.instance');
    }

    this.devices = new Map();
    this.muxers = new Map();
  }

  static get instance() {
    if (_instance == null) {
      _instance = new DeviceManager(SINGLETON_ENFORCER);
    }

    return _instance;
  }

  setTopicDataBuffer(buffer) {
    this.topicDataBuffer = buffer;
  }

  addDevice(device) {
    if (!device.id) {
      namida.logFailure('DeviceManager', 'can not add device "' + device.name + '", missing ID');
      return false;
    }

    if (this.devices.has(device.id)) {
      namida.logFailure('DeviceManager', 'can not add device "' + device.name + '", ID already exists');
      return false;
    }

    this.devices.set(device.id, device);
    return true;
  }

  createTopicMux(specs, topicDataBuffer = this.topicDataBuffer) {
    if (!specs.id) {
      namida.logFailure('DeviceManager', 'can not create TopicMuxer "' + specs.name + '", missing ID');
      return false;
    }

    if (this.muxers.has(specs.id)) {
      namida.logFailure('DeviceManager', 'can not create TopicMuxer "' + specs.name + '", ID already exists');
      return false;
    }

    let muxer = new TopicMuxer(specs, topicDataBuffer);
    muxer.init();
    this.muxers.set(specs.id, muxer);

    return muxer;
  }

  getTopicMux(id) {
      return this.muxers.get(id);
  }
}

module.exports = DeviceManager;
