class TopicMuxer {
  constructor(specs, topicDataBuffer) {
    this.specs = specs;
    this.topicDataBuffer = topicDataBuffer;

    this.records = [];

    if (this.specs.identityMatchPattern) {
      this.identityRegex = new RegExp(this.specs.identityMatchPattern);
    }
  }

  onTopicData(record) {
    // if a data type is specified and the record matches the topic selector regex but not the data type, discard
    if (this.specs.dataType && record.type !== this.specs.dataType) {
      return;
    }

    let existingRecord = this.records.find((entry) => entry.topic === record.topic);
    if (!existingRecord) {
      record.type = record.type || this.specs.dataType;
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

  getRecords() {
    //console.info('Muxer.getRecords():');
    //console.info(this.records);
    return {
      elements: this.records
    };
  }

  getSubscriptionToken() {
    return this.subscriptionToken;
  }

  toString() {
    return 'TopicMuxer ' + this.specs.name + ' (ID ' + this.specs.id + ')';
  }
}

module.exports = TopicMuxer;
