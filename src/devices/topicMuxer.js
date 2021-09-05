class TopicMuxer {
  constructor(specs, topicDataBuffer) {
    this.specs = specs;
    this.topicDataBuffer = topicDataBuffer;
    this.records = [];

    if (this.specs.identityMatchPattern) {
      this.identityRegex = new RegExp(this.specs.identityMatchPattern);
    }
  }

  async init() {
    console.info('\n' + this.toString());
    console.info(this.specs);
    this.subscriptionToken = await this.topicDataBuffer.subscribeRegex(this.specs.topicSelector, (record) => {
      this.onTopicData(record);
    });
  }

  async deInit() {
    await this.topicDataBuffer.unsubscribe(this.subscriptionToken);
  }

  onTopicData(record) {
    console.info(this.toString() + ' onTopicData()');
    console.info(record);
    // if a data type is specified and the record matches the topic selector regex but not the data type, discard
    if (this.specs.dataType && record.type !== this.specs.dataType) {
      return;
    }

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
    console.info(this.toString() + ' get()');
    console.info(this.records);
    return {
      elements: this.records
    };
  }

  toString() {
    return 'TopicMuxer ' + this.specs.name + ' (ID ' + this.specs.id + ')';
  }
}

module.exports = TopicMuxer;
