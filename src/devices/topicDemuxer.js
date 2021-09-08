class TopicDemuxer {
  static DEFAULT_REGEX_OUTPUT_PARAM = /{{#[0-9]+}}/g;

  constructor(specs, topicDataBuffer) {
    this.specs = specs;
    this.topicDataBuffer = topicDataBuffer;
    this.records = [];

    if (this.specs.identityMatchPattern) {
      this.identityRegex = new RegExp(this.specs.identityMatchPattern);
    }

    console.info(this.specs);
    this.outputParamMatches = new Map();
    let outputParamMatch = undefined;
    while (outputParamMatch = TopicDemuxer.DEFAULT_REGEX_OUTPUT_PARAM.exec(this.specs.outputTopicFormat)) {
      let matchString = outputParamMatch[0];
      outputParamMatch.paramIndex = parseInt(matchString.substring(3, matchString.length - 2));
      console.info(outputParamMatch);
      this.outputParamMatches.set(outputParamMatch.paramIndex, outputParamMatch);
    }
  }

  publish(recordList) {
    console.info('TopicDemuxer.publish():');
    console.info(this.outputParamMatches);
    for (let record of recordList.elements) {
      console.info(record);
      record.topic = this.specs.outputTopicFormat.slice(); //copy
      for (let i = 0; i < record.outputTopicParams.length; i++) {
        if (this.outputParamMatches.has(i)) {
          console.info(this.outputParamMatches.get(i));
          record.topic = record.topic.replace(this.outputParamMatches.get(i)[0], record.outputTopicParams[i]);
        }
      }
      record.type = this.specs.dataType;
      console.info(record);
      this.topicDataBuffer.publish(record.topic, record);
    }
  }

  toString() {
    return 'TopicDemuxer ' + this.specs.name + ' (ID ' + this.specs.id + ')';
  }
}

module.exports = TopicDemuxer;
