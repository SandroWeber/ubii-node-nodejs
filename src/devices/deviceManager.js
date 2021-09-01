const namida = require('@tum-far/namida');
const TopicMuxer = require('./topicMuxer');
const TopicDemuxer = require('./topicDemuxer');

let _instance = null;
const SINGLETON_ENFORCER = Symbol();

class DeviceManager {
  constructor(enforcer) {
    if (enforcer !== SINGLETON_ENFORCER) {
      throw new Error('Use ' + this.constructor.name + '.instance');
    }

    this.devices = new Map();
    this.muxers = new Map();
    this.demuxers = new Map();
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

  registerDevice(specs) {
    //TODO
  }

  getTopicMuxer(id) {
    return this.muxers.get(id);
  }

  async createTopicMuxer(specs, topicDataBuffer = this.topicDataBuffer) {
    if (!specs.id) {
      namida.logFailure('DeviceManager', 'can not create TopicMuxer "' + specs.name + '", missing ID');
      return;
    }

    if (this.muxers.has(specs.id)) {
      namida.logFailure('DeviceManager', 'can not create TopicMuxer "' + specs.name + '", ID already exists');
      return;
    }

    let muxer = new TopicMuxer(specs, topicDataBuffer);
    await muxer.init();
    this.muxers.set(specs.id, muxer);

    return muxer;
  }

  getTopicDemuxer(id) {
    return this.demuxers.get(id);
  }

  async createTopicDemuxer(specs, topicDataBuffer = this.topicDataBuffer) {
    if (!specs.id) {
      namida.logFailure('DeviceManager', 'can not create TopicDemuxer "' + specs.name + '", missing ID');
      return;
    }

    if (this.demuxers.has(specs.id)) {
      namida.logFailure('DeviceManager', 'can not create TopicDemuxer "' + specs.name + '", ID already exists');
      return;
    }

    let demuxer = new TopicDemuxer(specs, topicDataBuffer);
    this.demuxers.set(specs.id, demuxer);

    return demuxer;
  }
}

module.exports = DeviceManager;
