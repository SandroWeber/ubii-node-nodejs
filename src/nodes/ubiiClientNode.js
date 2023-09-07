const namida = require('@tum-far/namida/src/namida');
const { ProtobufTranslator, MSG_TYPES, DEFAULT_TOPICS } = require('@tum-far/ubii-msg-formats');
const { RuntimeTopicData } = require('@tum-far/ubii-topic-data');
const ServiceClientHTTP = require('../networking/serviceClientHttp.js');
const TopicDataClientWS = require('../networking/topicDataClientWS.js');

const ZmqDealer = require('../networking/zmqDealer');
const ZmqRequest = require('../networking/zmqRequest');

const ProcessingModuleManager = require('../processing/processingModuleManager');
const ProcessingModuleStorage = require('../storage/processingModuleStorage');
const TopicDataProxy = require('./topicDataProxy');

const LOG_TAG = 'Node';

class UbiiClientNode {
  constructor(name, serviceConnection, topicDataConnection, publishIntervalMs = 5) {
    this.name = name;
    this.serviceConnection = serviceConnection;
    this.topicDataConnection = topicDataConnection;
    this.publishIntervalMs = publishIntervalMs;

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
      namida.logFailure(LOG_TAG, 'server config request failed');
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

    await this.proxyTopicData.subscribeTopic(DEFAULT_TOPICS.INFO_TOPICS.START_SESSION, (record) => {
      this._onStartSession(record.session);
    });
    /*await this.proxyTopicData.subscribeTopic(DEFAULT_TOPICS.INFO_TOPICS.NEW_SESSION, (record) => {
      this._onNewSession(record.session);
    });*/
    await this.proxyTopicData.subscribeTopic(DEFAULT_TOPICS.INFO_TOPICS.STOP_SESSION, (record) => {
      this._onStopSession(record.session);
    });

    this.setPublishIntervalMs(this.publishIntervalMs);
  }

  async deinitialize() {
    this.proxyTopicData.intervalPublishRecords && clearInterval(this.proxyTopicData.intervalPublishRecords);
  }

  callService(request) {
    return new Promise(async (resolve, reject) => {
      try {
        //let buffer = this.serviceRequestTranslator.createBufferFromPayload(request);
        this.serviceClient.sendRequest(request, (response) => {
          //let reply = this.serviceReplyTranslator.createMessageFromBuffer(response);
          resolve(response);
        });
      } catch (error) {
        reject(error);
      }
    });
  }

  async subscribeTopic(topic, callback) {
    return await this.proxyTopicData.subscribeTopic(topic, (record) => {
      callback(record);
    });
  }

  async subscribeRegex(regex, callback) {
    return await this.proxyTopicData.subscribeRegex(regex, (record) => {
      callback(record);
    });
  }

  async unsubscribe(token) {
    return await this.proxyTopicData.unsubscribe(token);
  }

  setPublishIntervalMs(ms) {
    this.publishIntervalMs = ms;
    this.proxyTopicData.setPublishIntervalMs(this.publishIntervalMs);
  }

  publishRecord(record) {
    this.proxyTopicData.publishRecord(record);
  }

  publishRecordList(recordList) {
    this.proxyTopicData.publishRecordList(recordList);
  }

  publishRecordImmediately(record) {
    this.proxyTopicData.publishRecordImmediately(record);
  }

  connectServiceSocket() {
    //this.serviceRequestTranslator = new ProtobufTranslator(MSG_TYPES.SERVICE_REQUEST);
    //this.serviceReplyTranslator = new ProtobufTranslator(MSG_TYPES.SERVICE_REPLY);

    if (this.serviceConnection.address.startsWith('tcp://')) {
      if (this.serviceConnection.format) {
        namida.warn(LOG_TAG, `config parameter "format" not supported for tcp protocol, always uses binary`);
      }
      let [protocol, address] = this.serviceConnection.address.split('://');
      this.serviceClient = new ZmqRequest(protocol, address);
    } else if (this.serviceConnection.address.startsWith('http')) {
      this.serviceClient = new ServiceClientHTTP(
        this.serviceConnection.address,
        this.serviceConnection.format.toUpperCase()
      );
    }
  }

  connectTopicdataSocket() {
    if (!this.serverSpecification || !this.clientSpecification) {
      namida.logFailure(
        'Ubii Node',
        "can't connect topic data socket, missing specifications for port and client configuration"
      );
    }

    this.translatorTopicData = new ProtobufTranslator(MSG_TYPES.TOPIC_DATA);
    if (this.topicDataConnection.address.startsWith('tcp://')) {
      let [protocol, address] = this.topicDataConnection.address.split('://');
      this.topicDataClient = new ZmqDealer(this.clientSpecification.id, protocol, address);
      this.topicDataClient.setCallbackOnMessage(this._onTopicDataMessageReceived.bind(this));
    } else if (this.topicDataConnection.address.startsWith('ws')) {
      this.topicDataClient = new TopicDataClientWS(this.clientSpecification.id, this.topicDataConnection.address);
      this.topicDataClient.setCbOnMessageReceived(this._onTopicDataMessageReceived.bind(this));
    } else {
      namida.logFailure(LOG_TAG, `topic data address ${this.topicDataConnection.address} protocol not recognized`);
    }
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
      namida.logFailure('PM_RUNTIME_ADD error', response.error);
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
    let pmsRemoved = [];
    for (let pm of this.processingModuleManager.processingModules.values()) {
      let pmSpecs = pm.toProtobuf();
      if (pmSpecs.sessionId === msgSession.id) {
        await this.processingModuleManager.stopModule(pmSpecs);
        this.processingModuleManager.removeModule(pmSpecs);
        pmsRemoved.push(pmSpecs);
      }
    }

    let pmRuntimeRemoveRequest = {
      topic: DEFAULT_TOPICS.SERVICES.PM_RUNTIME_REMOVE,
      processingModuleList: {
        elements: pmsRemoved
      }
    };
    let response = await this.callService(pmRuntimeRemoveRequest);
    if (response.error) {
      namida.logFailure('PM_RUNTIME_REMOVE error', response.error);
    }
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
