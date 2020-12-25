const namida = require('@tum-far/namida/src/namida');
const { ProtobufTranslator, MSG_TYPES, DEFAULT_TOPICS } = require('@tum-far/ubii-msg-formats');
const { RuntimeTopicData } = require('@tum-far/ubii-topic-data');

const ZmqDealer = require('./networking/zmqDealer');
const ZmqRequest = require('./networking/zmqRequest');

class UbiiNode {
  constructor(name, masterNodeIP, masterNodeServicePort) {
    this.name = name;
    this.masterNodeIP = masterNodeIP;
    this.masterNodeServicePort = masterNodeServicePort;

    this.topicDataCallbacks = new Map();
    this.topicDataRegexCallbacks = new Map();
    this.topicdata = new RuntimeTopicData();

    this.initialize();
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

    this.topicdataTranslator = new ProtobufTranslator(MSG_TYPES.TOPIC_DATA);
    this.zmqDealer = new ZmqDealer(
      this.clientSpecification.id,
      'tcp',
      this.masterNodeIP + ':' + this.serverSpecification.portTopicDataZmq
    );
    this.zmqDealer.onMessageReceived((messageBuffer) => {
      let topicdata = this.topicdataTranslator.createPayloadFromBuffer(messageBuffer);
      this._onTopicDataMessageReceived(topicdata);
    });
  }

  _onTopicDataMessageReceived(message) {
    let records = message.topicDataRecordList || [];
    if (message.topicDataRecord) records.push(message.topicDataRecord);

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
  }

  callService(request) {
    return new Promise((resolve, reject) => {
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
   * Subscribe a callback to a given topic.
   * @param {string} topic
   * @param {function} callback
   */
  subscribeTopic(topic, callback) {
    let message = {
      topic: DEFAULT_TOPICS.SERVICES.TOPIC_SUBSCRIPTION,
      topicSubscription: {
        clientId: this.clientSpecification.id,
        subscribeTopics: [topic]
      }
    };

    return this.callService(message).then(
      (reply) => {
        if (reply.success !== undefined && reply.success !== null) {
          let callbacks = this.topicDataCallbacks.get(topic);
          if (callbacks && callbacks.length > 0) {
            callbacks.push(callback);
          } else {
            this.topicDataCallbacks.set(topic, [callback]);
          }
        } else {
          console.error('ClientNodeWeb - subscribe failed (' + topic + ')\n' + reply);
        }
      },
      (error) => {
        console.error(error);
      }
    );
  }

  /**
   * Unsubscribe a given callback from a given topic.
   * @param {string} topic
   * @param {function} callback
   */
  async unsubscribeTopic(topic, callback = undefined) {
    let currentCallbacks = this.topicDataCallbacks.get(topic);
    if (currentCallbacks && currentCallbacks.length > 0) {
      if (!callback) {
        this.topicDataCallbacks.delete(topic);
      } else {
        let index = currentCallbacks.indexOf(callback);
        if (index !== -1) currentCallbacks.splice(index, 1);
      }
    }

    if (currentCallbacks && currentCallbacks.length === 0) {
      this.topicDataCallbacks.delete(topic);

      let message = {
        topic: DEFAULT_TOPICS.SERVICES.TOPIC_SUBSCRIPTION,
        topicSubscription: {
          clientId: this.clientSpecification.id,
          unsubscribeTopics: [topic]
        }
      };
      this.callService(message);
    }
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
}

module.exports = UbiiNode;
