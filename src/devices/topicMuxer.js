const e = require('express');

class TopicMuxer {
  constructor(specs, topicDataBuffer) {
    this.specs = specs;
    this.topicDataBuffer = topicDataBuffer;
    this.records = [];

    if (this.specs.identityMatchPattern) {
      this.identityRegex = new RegExp(this.specs.identityMatchPattern);
    }
  }

  init() {
    this.subscriptionToken = this.topicDataBuffer.subscribeRegex(this.specs.topicSelector, (record) => {
      this.onTopicData(record);
    });
  }

  deInit() {
    this.topicDataBuffer.unsubscribe(this.subscriptionToken);
  }

  onTopicData(record) {
    let existingRecord = this.records.find((entry) => entry.topic === record.topic);
    if (!existingRecord) {
      this.records.push(record);
      if (this.identityRegex) {
        let identityMatches = this.identityRegex.exec(record.topic);
        if (identityMatches && identityMatches.length > 0) {
          record.identity = identityMatches[0];
        }
      }
    } else {
      existingRecord.timestamp = record.timestamp;
      existingRecord[existingRecord.type] = record[record.type];
    }
  }

  get() {
    return {
      elements: this.records
    };
  }
}

module.exports = TopicMuxer;
