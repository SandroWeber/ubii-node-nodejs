const namida = require('@tum-far/namida/src/namida');
const { ProtobufTranslator, MSG_TYPES, DEFAULT_TOPICS } = require('@tum-far/ubii-msg-formats');
const { RuntimeTopicData } = require('@tum-far/ubii-topic-data');

const ZmqDealer = require('./networking/zmqDealer');
const ZmqRequest = require('./networking/zmqRequest');

const ProcessingModuleManager = require('./processing/processingModuleManager');

class UbiiNode {
  constructor(name, masterNodeIP, masterNodeServicePort) {
    this.name = name;
    this.masterNodeIP = masterNodeIP;
    this.masterNodeServicePort = masterNodeServicePort;

    this.topicSubscriptions = new Map();
    this.topicDataRegexCallbacks = new Map();
    this.topicdata = new RuntimeTopicData();
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
        name: this.name
      }
    });
    if (replyClientRegistration.client) {
      this.clientSpecification = replyClientRegistration.client;
    } else {
      namida.logFailure('UbiiNode.initialize()', 'client registration failed');
      return;
    }

    //console.info(this.serverSpecification);
    console.info(this.clientSpecification);

    this.processingModuleManager = new ProcessingModuleManager(this.id, undefined, this.topicdata);

    this.connectTopicdataSocket();

    await this.subscribeTopic(DEFAULT_TOPICS.INFO_TOPICS.START_SESSION, async (msgSession) => {
      let localPMs = [];
      msgSession.processingModules.forEach((pm) => {
        if (pm.nodeId === this.id) {
          let newModule = this.processingModuleManager.createModule(pm);
          if (newModule) localPMs.push(newModule);
        }
      });

      this.processingModuleManager.applyIOMappings(msgSession.ioMappings, msgSession.id);

      localPMs.forEach((pm) => {
        pm.start();
      });

      let pmRuntimeAddRequest = {
        topic: DEFAULT_TOPICS.SERVICES.PM_RUNTIME_ADD,
        processingModuleList: {
          elements: localPMs.map((pm) => {
            return pm.toProtobuf();
          })
        }
      };
      await this.callService(pmRuntimeAddRequest);
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
        /*if (record && record.topic) {
        let callbacks = this.topicDataCallbacks.get(record.topic);
        if (!callbacks) {
          this.topicDataRegexCallbacks.forEach((value) => {
            let regex = value.regex;
            if (regex.test(record.topic)) {
              callbacks = value.callbacks;
            }
          });
        }
        callbacks &&
          callbacks.forEach((cb) => {
            cb(record[record.type], record.topic);
          });
      }*/
        this.topicdata.publish(record.topic, record[record.type], record.type, record.timestamp);
      });
    } catch (error) {
      namida.logFailure('Ubii node', error);
    }
  }

  callService(request) {
    return new Promise(async (resolve, reject) => {
      try {
        let buffer = this.serviceRequestTranslator.createBufferFromPayload(request);
        await this.zmqRequest.sendRequest(buffer, (response) => {
          let reply = this.serviceReplyTranslator.createMessageFromBuffer(response);
          resolve(reply);
        });
      } catch (error) {
        reject(error);
      }
    });
  }

  /**
   * Subscribe a callback to a given topic.
   * @param {string} topic
   * @param {function} callback
   *
   * @returns {object} Subscription token, save to later unsubscribe
   */
  subscribeTopic(topic, callback) {
    return new Promise((resolve, reject) => {
      let message = {
        topic: DEFAULT_TOPICS.SERVICES.TOPIC_SUBSCRIPTION,
        topicSubscription: {
          clientId: this.clientSpecification.id,
          subscribeTopics: [topic]
        }
      };

      this.callService(message).then(
        (reply) => {
          if (reply.success !== undefined && reply.success !== null) {
            let token = this.topicdata.subscribe(topic, (topic, entry) => {
              callback(entry.data, entry.type, entry.timestamp);
            });

            let callbacks = this.topicSubscriptions.get(topic);
            if (callbacks && callbacks.length > 0) {
              callbacks.push(callback);
            } else {
              this.topicSubscriptions.set(topic, [token]);
            }

            resolve(token);
          } else {
            namida.logFailure('Ubii Node', 'subscribe failed (' + topic + ')\n' + reply);
            reject(reply.error);
          }
        },
        (error) => {
          namida.logFailure('Ubii Node', error);
          reject(error);
        }
      );
    });
  }

  /**
   * Unsubscribe a given callback from a given topic.
   * @param {object} subscriptionToken - the token returned upon successful subscription
   */
  async unsubscribeTopic(subscriptionToken) {
    this.topicdata.unsubscribe(subscriptionToken);

    let topic = subscriptionToken.topic;
    let tokens = this.topicSubscriptions.get(topic);
    if (tokens && tokens.length > 0) {
      if (!subscriptionToken) {
        this.topicSubscriptions.delete(topic);
      } else {
        let index = tokens.indexOf(subscriptionToken);
        if (index !== -1) tokens.splice(index, 1);
      }
    }

    if (tokens && tokens.length === 0) {
      this.topicSubscriptions.delete(topic);

      let message = {
        topic: DEFAULT_TOPICS.SERVICES.TOPIC_SUBSCRIPTION,
        topicSubscription: {
          clientId: this.clientSpecification.id,
          unsubscribeTopics: [topic]
        }
      };
      let response = await this.callService(message);
      if (response.success) {
        return true;
      } else {
        return false;
      }
    }

    return true;
  }

  /**
   * Subscribe to the specified regex.
   * @param {*} regexString
   * @param {*} callback
   */
  async subscribeRegex(regexString, callback) {
    // already subscribed to regexString, add callback to list
    let registeredRegex = this.topicDataRegexCallbacks.get(regexString);
    if (registeredRegex) {
      if (registeredRegex.callbacks && Array.isArray(registeredRegex.callbacks)) {
        registeredRegex.callbacks.push(callback);
      } else {
        registeredRegex.callbacks = [callback];
      }
    }
    // need to subscribe at backend
    else {
      let message = {
        topic: DEFAULT_TOPICS.SERVICES.TOPIC_SUBSCRIPTION,
        topicSubscription: {
          clientId: this.clientSpecification.id,
          subscribeTopicRegexp: [regexString]
        }
      };

      try {
        let reply = await this.callService(message);
        if (reply.success !== undefined && reply.success !== null) {
          let newRegex = {
            callbacks: [callback],
            regex: new RegExp(regexString)
          };
          this.topicDataRegexCallbacks.set(regexString, newRegex);
        } else {
          // another component subscribed in the meantime?
          let registeredRegex = this.topicDataRegexCallbacks.get(regexString);
          if (registeredRegex && registeredRegex.callbacks.length > 0) {
            registeredRegex.callbacks.push(callback);
          } else {
            console.error('ClientNodeWeb - could not subscribe to regex ' + regexString + ', response:\n' + reply);
            return false;
          }
        }
      } catch (error) {
        console.error('ClientNodeWeb - subscribeRegex(' + regexString + ') failed: \n' + error);
        return false;
      }
    }

    return true;
  }

  /**
   * Unsubscribe from the specified regex.
   * @param {*} regexString
   * @param {*} callback
   */
  async unsubscribeRegex(regexString, callback) {
    let registeredRegex = this.topicDataRegexCallbacks.get(regexString);
    if (registeredRegex === undefined) {
      return false;
    }

    // remove callback from list of callbacks
    let index = registeredRegex.callbacks.indexOf(callback);
    if (index >= 0) {
      registeredRegex.callbacks.splice(index, 1);
    }

    // if no callbacks left, unsubscribe at backend
    if (registeredRegex.callbacks.length === 0) {
      let message = {
        topic: DEFAULT_TOPICS.SERVICES.TOPIC_SUBSCRIPTION,
        topicSubscription: {
          clientId: this.clientSpecification.id,
          unsubscribeTopicRegexp: [regexString]
        }
      };

      try {
        let reply = await this.callService(message);
        if (reply.success !== undefined && reply.success !== null) {
          this.topicDataRegexCallbacks.delete(regexString);
        } else {
          console.error('ClientNodeWeb - could not unsubscribe from regex ' + regexString + ', response:\n' + reply);
          return false;
        }
      } catch (error) {
        console.error('ClientNodeWeb - unsubscribeRegex(' + regexString + ') failed: \n' + error);
        return false;
      }
    }

    return true;
  }

  /**
   * Publish some TopicData.
   * @param {ubii.topicData.TopicData} topicData
   */
  publish(topicData) {
    let buffer = this.translatorTopicData.createBufferFromPayload(topicData);

    this.zmqDealer.send(buffer);
    //TODO: as soon as master node has smart distinction of topic ownership for clients and will not send back
    // topic data to clients that published it we can publish on local topic data here as well
  }

  /**
   * Generate a timestamp for topic data.
   */
  generateTimestamp() {
    let now = Date.now();
    let seconds = Math.floor(now / 1000);
    let nanos = (now - seconds * 1000) * 1000000;
    return {
      seconds: seconds,
      nanos: nanos
    };
  }
}

module.exports = UbiiNode;
