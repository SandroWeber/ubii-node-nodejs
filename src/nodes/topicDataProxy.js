const namida = require('@tum-far/namida/src/namida');
const { DEFAULT_TOPICS } = require('@tum-far/ubii-msg-formats');
const { SUBSCRIPTION_TYPES } = require('@tum-far/ubii-topic-data');

class TopicDataProxy {
  constructor(topicData, ubiiNode) {
    this.topicData = topicData;
    this.ubiiNode = ubiiNode;
    this.regexSubs = [];

    this.recordsToPublish = [];
  }

  pull(topic) {
    return this.topicData.pull(topic);
  }

  /**
   * Subscribe a callback to a given topic.
   * @param {string} topic
   * @param {function} callback
   *
   * @returns {object} Subscription token, save to later unsubscribe
   */
  async subscribeTopic(topic, callback) {
    let subscriptions = this.topicData.getSubscriptionTokensForTopic(topic);
    if (!subscriptions || subscriptions.length === 0) {
      let message = {
        topic: DEFAULT_TOPICS.SERVICES.TOPIC_SUBSCRIPTION,
        topicSubscription: {
          clientId: this.ubiiNode.id,
          subscribeTopics: [topic]
        }
      };

      try {
        let replySubscribe = await this.ubiiNode.callService(message);
        if (replySubscribe.error) {
          namida.logFailure('TopicDataProxy', 'server error during subscribe to "' + topic + '": ' + replySubscribe.error);
          return replySubscribe.error;
        }
      } catch (error) {
        namida.logFailure('TopicDataProxy', 'local error during subscribe to "' + topic + '": ' +  error);
        return error;
      }
    }

    let token = this.topicData.subscribeTopic(topic, (record) => {
      callback(record);
    });

    return token;
  }

  /**
   * Subscribe to the specified regex.
   * @param {*} regexString
   * @param {*} callback
   */
  async subscribeRegex(regex, callback) {
    let subscriptions = this.topicData.getSubscriptionTokensForRegex(regex);
    if (!subscriptions || subscriptions.length === 0) {
      let message = {
        topic: DEFAULT_TOPICS.SERVICES.TOPIC_SUBSCRIPTION,
        topicSubscription: {
          clientId: this.ubiiNode.id,
          subscribeTopicRegexp: [regex]
        }
      };

      try {
        let replySubscribe = await this.ubiiNode.callService(message);
        if (replySubscribe.error) {
          return replySubscribe.error;
        }
      } catch (error) {
        namida.logFailure('TopicDataProxy', error);
        return error;
      }
    }

    let token = this.topicData.subscribeRegex(regex, (record) => {
      callback(record);
    });

    return token;
  }

  /**
   * Unsubscribe at topicdata and possibly at master node.
   * @param {*} token
   */
  async unsubscribe(token) {
    let result = this.topicData.unsubscribe(token);

    let subs = undefined;
    if (token.type === SUBSCRIPTION_TYPES.TOPIC) {
      subs = this.topicData.getSubscriptionTokensForTopic(token.topic);
    } else if (token.type === SUBSCRIPTION_TYPES.REGEX) {
      subs = this.topicData.getSubscriptionTokensForRegex(token.topic);
    }

    if (!subs || subs.length === 0) {
      let message = {
        topic: DEFAULT_TOPICS.SERVICES.TOPIC_SUBSCRIPTION,
        topicSubscription: {
          clientId: this.ubiiNode.id
        }
      };
      if (token.type === SUBSCRIPTION_TYPES.TOPIC) {
        message.topicSubscription.unsubscribeTopics = [token.topic];
      } else if (token.type === SUBSCRIPTION_TYPES.REGEX) {
        message.topicSubscription.unsubscribeTopicRegexp = [token.topic];
      }

      try {
        let replySubscribe = await this.ubiiNode.callService(message);
        if (replySubscribe.error) {
          return replySubscribe.error;
        }
      } catch (error) {
        namida.logFailure('TopicData Proxy', error);
        return error;
      }
    }

    return result;
  }

  publishRecord(record) {
    this.recordsToPublish.push(record);
  }

  publishRecordList(recordList) {
    this.recordsToPublish.push(...recordList);
  }

  setPublishIntervalMs(intervalMs) {
    this.intervalPublishRecords && clearInterval(this.intervalPublishRecords);

    this.intervalPublishRecords = setInterval(() => this.flushRecordsToPublish(), intervalMs);
  }

  flushRecordsToPublish() {
    if (this.recordsToPublish.length === 0) return;

    let buffer = this.ubiiNode.translatorTopicData.createBufferFromPayload({
      topicDataRecordList: {
        elements: this.recordsToPublish
      }
    });
    this.ubiiNode.topicDataClient.send(buffer);

    this.recordsToPublish = [];
  }

  /**
   * Publish record without delay.
   * @param {ubii.topicData.TopicData} topicData
   */
  publishRecordImmediately(record) {
    try {
      let buffer = this.ubiiNode.translatorTopicData.createBufferFromPayload({
        topicDataRecord: record
      });
      this.ubiiNode.topicDataClient.send(buffer);
    } catch (error) {
      namida.logFailure('TopicDataProxy', 'failed to send data: ' + error);
    }

    //TODO: as soon as master node has smart distinction of topic ownership for clients and will not send back
    // topic data to clients that published it we can publish on local topic data here as well
  }
}

module.exports = TopicDataProxy;
