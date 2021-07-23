const namida = require('@tum-far/namida/src/namida');
const { DEFAULT_TOPICS } = require('@tum-far/ubii-msg-formats');
const { SUBSCRIPTION_TYPES } = require('@tum-far/ubii-topic-data');

class TopicDataProxy {
  constructor(topicData, ubiiNode) {
    this.topicData = topicData;
    this.ubiiNode = ubiiNode;
    this.regexSubs = [];
  }

  publish(topic, record) {
    let msgTopicData = {
      topicDataRecord: record
    };
    this.proxyPublish(msgTopicData);
  }

  pull(topic) {
    return this.topicData.pull(topic);
  }

  async subscribe(topic, callback) {
    return await this.proxySubscribeTopic(topic, callback);
  }

  async subscribeRegex(regex, callback) {
    return await this.proxySubscribeRegex(regex, callback);
  }

  async unsubscribe(token) {
    return await this.proxyUnsubscribe(token);
  }

  /**
   * Subscribe a callback to a given topic.
   * @param {string} topic
   * @param {function} callback
   *
   * @returns {object} Subscription token, save to later unsubscribe
   */
  async proxySubscribeTopic(topic, callback) {
    let subscriptions = this.topicData.getSubscriptionTokensForTopic(topic);
    console.info('TopicDataProxy.proxySubscribeTopic() - subscriptions: ' + subscriptions);
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
        console.info(replySubscribe);
        if (replySubscribe.error) {
          namida.logFailure('TopicDataProxy', 'server error during subscribe to "' + topic + '": ' + replySubscribe.error);
          return replySubscribe.error;
        }
      } catch (error) {
        namida.logFailure('TopicDataProxy', 'local error during subscribe to "' + topic + '": ' +  error);
        return error;
      }
    }

    let token = this.topicData.subscribe(topic, (record) => {
      callback(record);
    });
    console.info('TopicDataProxy.proxySubscribeTopic() - topic: ' + token.topic);

    return token;
  }

  /**
   * Subscribe to the specified regex.
   * @param {*} regexString
   * @param {*} callback
   */
  async proxySubscribeRegex(regex, callback) {
    console.info('TopicDataProxy.proxySubscribeRegex: ' + regex);
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
        namida.logFailure('Ubii Node', error);
        return error;
      }
    }

    let token = this.topicData.subscribeRegex(topic, (record) => {
      callback(record);
    });

    return token;
  }

  /**
   * Unsubscribe at topicdata and possibly at master node.
   * @param {*} token
   */
  async proxyUnsubscribe(token) {
    console.info('TopicDataProxy.proxyUnsubscribe: ' + token.topic);
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
        namida.logFailure('Ubii Node', error);
        return error;
      }
    }

    return result;
  }

  /**
   * Publish some TopicData.
   * @param {ubii.topicData.TopicData} topicData
   */
  proxyPublish(topicData) {
    //TODO: this should be refactored
    let buffer = this.ubiiNode.translatorTopicData.createBufferFromPayload(topicData);
    this.ubiiNode.zmqDealer.send(buffer);

    //TODO: as soon as master node has smart distinction of topic ownership for clients and will not send back
    // topic data to clients that published it we can publish on local topic data here as well
  }
}

module.exports = TopicDataProxy;
