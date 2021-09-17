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

    this.topicDataBuffer = new RuntimeTopicData();
    //TODO: for now we prevent direct publishing to local topicdata buffer
    // until smart distinguishing of topics owned by this node vs remote topics
    // is implemented
    /*this.originalTopicdataPublish = this.topicdata.publish;
    this.topicdata.publish = this.publishTopicdataReplacement;*/

    this.proxyTopicData = new TopicDataProxy(this.topicDataBuffer, this);
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
      namida.logSuccess(this.toString(), 'successfully registered at master node');
    } else {
      namida.logFailure('UbiiNode.initialize()', 'client registration failed');
      return;
    }

    this.processingModuleManager = new ProcessingModuleManager(this.id, this.proxyTopicData);

    this.connectTopicdataSocket();

    await this.proxyTopicData.proxySubscribeTopic(DEFAULT_TOPICS.INFO_TOPICS.START_SESSION, (record) => {
      this._onStartSession(record.session);
    });
    /*await this.proxyTopicData.proxySubscribeTopic(DEFAULT_TOPICS.INFO_TOPICS.NEW_SESSION, (record) => {
      this._onNewSession(record.session);
    });*/
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
    let topicdataMsg = this.translatorTopicData.createPayloadFromBuffer(messageBuffer);
    if (!topicdataMsg) {
      namida.logFailure('TopicData received', 'could not parse topic data message from buffer');
      return;
    }

    let records = topicdataMsg.topicDataRecordList ? topicdataMsg.topicDataRecordList.elements : [];
    if (topicdataMsg.topicDataRecord) records.push(topicdataMsg.topicDataRecord);

    records.forEach((record) => {
      try {
        this.topicDataBuffer.publish(record.topic, record);
      } catch (error) {
        namida.logFailure('TopicData received', 'topic "' + record.topic + '"\n' + error);
      }
    });
  }

  async _onStartSession(msgSession) {
    let localPMs = [];
    //TODO: after adding PM_RUNTIME_START to msg-formats, clean split between "_onNewSession" and "_onStartSession"
    msgSession.processingModules.forEach((pm) => {
      if (pm.nodeId === this.id) {
        let newModule = this.processingModuleManager.createModule(pm);
        if (newModule) localPMs.push(newModule);
      }
    });

    await this.processingModuleManager.applyIOMappings(msgSession.ioMappings, msgSession.id);

    for (let pm of localPMs) {
      await this.processingModuleManager.startModule(pm);
    }

    let pmRuntimeAddRequest = {
      topic: DEFAULT_TOPICS.SERVICES.PM_RUNTIME_ADD,
      processingModuleList: {
        elements: localPMs.map((pm) => {
          return pm.toProtobuf();
        })
      }
    };
    let response = await this.callService(pmRuntimeAddRequest);
    if (response.error) {
      namida.logFailure('Start Session Error', response.error);
    }
  }

  /*async _onNewSession(msgSession) {
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
      await this.processingModuleManager.applyIOMappings(msgSession.ioMappings, msgSession.id);

      localPMs.forEach((pm) => {
        this.processingModuleManager.startModule(pm);
      });
    }
  }*/

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

  toString() {
    let output = this.name;
    if (this.clientSpecification && this.clientSpecification.id) {
      output += ' (ID ' + this.clientSpecification.id + ')';
    } else {
      output += ' (no ID, unregistered)';
    }

    return output;
  }
}

module.exports = UbiiClientNode;
