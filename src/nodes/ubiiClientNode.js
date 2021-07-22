const namida = require('@tum-far/namida/src/namida');
const { ProtobufTranslator, MSG_TYPES, DEFAULT_TOPICS } = require('@tum-far/ubii-msg-formats');
const { RuntimeTopicData } = require('@tum-far/ubii-topic-data');

const ZmqDealer = require('../networking/zmqDealer');
const ZmqRequest = require('../networking/zmqRequest');

const ProcessingModuleManager = require('../processing/processingModuleManager');
const ProcessingModuleStorage = require('../storage/processingModuleStorage');
const TopicDataProxy = require('./topicDataProxy');

class UbiiClientNode {
  constructor(name, masterNodeIP, masterNodeServicePort) {
    this.name = name;
    this.masterNodeIP = masterNodeIP;
    this.masterNodeServicePort = masterNodeServicePort;

    //this.topicSubscriptions = new Map();
    this.topicDataRegexCallbacks = new Map();

    this.topicdata = new RuntimeTopicData();
    //TODO: for now we prevent direct publishing to local topicdata buffer
    // until smart distinguishing of topics owned by this node vs remote topics
    // is implemented
    /*this.originalTopicdataPublish = this.topicdata.publish;
    this.topicdata.publish = this.publishTopicdataReplacement;*/

    this.proxyTopicData = new TopicDataProxy(this.topicdata, this);
  }

  get id() {
    return this.clientSpecification && this.clientSpecification.id;
  }

  async initialize() {
    this.connectServiceSocket();

    // get server config
    let replyServerSpec = await this.callService({
      topic: DEFAULT_TOPICS.SERVICES.SERVER_CONFIG
    });
    if (replyServerSpec.server) {
      this.serverSpecification = replyServerSpec.server;
    } else {
      namida.logFailure('UbiiNode.initialize()', 'server config request failed');
      return;
    }

    // register as client node
    let replyClientRegistration = await this.callService({
      topic: DEFAULT_TOPICS.SERVICES.CLIENT_REGISTRATION,
      client: {
        name: this.name,
        isDedicatedProcessingNode: true,
        processingModules: ProcessingModuleStorage.instance.getAllSpecs()
      }
    });
    if (replyClientRegistration.client) {
      this.clientSpecification = replyClientRegistration.client;
    } else {
      namida.logFailure('UbiiNode.initialize()', 'client registration failed');
      return;
    }

    console.info(this.clientSpecification);

    this.processingModuleManager = new ProcessingModuleManager(this.id, undefined, this.proxyTopicData);

    this.connectTopicdataSocket();

    await this.proxyTopicData.proxySubscribeTopic(DEFAULT_TOPICS.INFO_TOPICS.START_SESSION, (record) => {
      this._onStartSession(record.session);
    });
    await this.proxyTopicData.proxySubscribeTopic(DEFAULT_TOPICS.INFO_TOPICS.STOP_SESSION, (record) => {
      this._onStopSession(record.session);
    });
  }

  connectServiceSocket() {
    this.serviceRequestTranslator = new ProtobufTranslator(MSG_TYPES.SERVICE_REQUEST);
    this.serviceReplyTranslator = new ProtobufTranslator(MSG_TYPES.SERVICE_REPLY);

    this.zmqRequest = new ZmqRequest('tcp', this.masterNodeIP + ':' + this.masterNodeServicePort);
  }

  connectTopicdataSocket() {
    if (!this.serverSpecification || !this.clientSpecification) {
      namida.logFailure(
        'Ubii Node',
        "can't connect topic data socket, missing specifications for port and client configuration"
      );
    }

    this.translatorTopicData = new ProtobufTranslator(MSG_TYPES.TOPIC_DATA);
    this.zmqDealer = new ZmqDealer(
      this.clientSpecification.id,
      'tcp',
      this.masterNodeIP + ':' + this.serverSpecification.portTopicDataZmq
    );
    this.zmqDealer.onMessageReceived(this._onTopicDataMessageReceived.bind(this));
  }

  _onTopicDataMessageReceived(messageBuffer) {
    try {
      let topicdata = this.translatorTopicData.createMessageFromBuffer(messageBuffer);
      if (!topicdata) {
        namida.logFailure('Ubii node', 'could not parse topic data message from buffer');
        return;
      }

      let records = topicdata.topicDataRecordList ? topicdata.topicDataRecordList.elements : [];
      if (topicdata.topicDataRecord) records.push(topicdata.topicDataRecord);

      records.forEach((record) => {
        this.topicdata.publish(record.topic, record);
      });
    } catch (error) {
      namida.logFailure('Ubii node', error);
    }
  }

  async _onStartSession(msgSession) {
    let localPMs = [];
    msgSession.processingModules.forEach((pm) => {
      if (pm.nodeId === this.id) {
        let newModule = this.processingModuleManager.createModule(pm);
        if (newModule) localPMs.push(newModule);
      }
    });

    let pmRuntimeAddRequest = {
      topic: DEFAULT_TOPICS.SERVICES.PM_RUNTIME_ADD,
      processingModuleList: {
        elements: localPMs.map((pm) => {
          return pm.toProtobuf();
        })
      }
    };
    let response = await this.callService(pmRuntimeAddRequest);

    if (response.success) {
      this.processingModuleManager.applyIOMappings(msgSession.ioMappings, msgSession.id);

      localPMs.forEach((pm) => {
        this.processingModuleManager.startModule(pm);
      });
    }
  }

  async _onStopSession(msgSession) {
    this.processingModuleManager.processingModules.forEach((pm) => {
      if (pm.sessionId === msgSession.id) {
        this.processingModuleManager.stopModule(pm);
        this.processingModuleManager.removeModule(pm);
      }
    });
  }

  callService(request) {
    return new Promise(async (resolve, reject) => {
      try {
        let buffer = this.serviceRequestTranslator.createBufferFromPayload(request);
        this.zmqRequest.sendRequest(buffer, (response) => {
          let reply = this.serviceReplyTranslator.createMessageFromBuffer(response);
          resolve(reply);
        });
      } catch (error) {
        reject(error);
      }
    });
  }

  /**
   * Generate a timestamp for topic data.
   */
  generateTimestamp() {
    return { millis: Date.now() };
  }
}

module.exports = UbiiClientNode;
