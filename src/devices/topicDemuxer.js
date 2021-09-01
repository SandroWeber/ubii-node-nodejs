class TopicDemuxer {
  constructor(specs, topicDataBuffer) {
    this.specs = specs;
    this.topicDataBuffer = topicDataBuffer;
    this.records = [];

    if (this.specs.identityMatchPattern) {
      this.identityRegex = new RegExp(this.specs.identityMatchPattern);
    }
  }

  publish(recordList) {
    console.info(recordList);
  }

  toString() {
    return 'TopicDemuxer ' + this.specs.name + ' (ID ' + this.specs.id + ')';
  }
}

module.exports = TopicDemuxer;
