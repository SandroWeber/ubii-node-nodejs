const UbiiNode = require('../src/ubiiNode');
const { DEFAULT_TOPICS } = require('@tum-far/ubii-msg-formats/dist/js/constants');

const PMCoCoSSDObjectDetection = require('../database/processing/pm-coco-ssd-object-detection');
const { ubii } = require('@tum-far/ubii-msg-formats/dist/js/protobuf');

const topicTestPubSub = '/test/topic/pub-sub';
const topicImages = '/test/topic/image';
const topicPredictions = '/test/topic/object_predictions';
const pmSpecs = PMCoCoSSDObjectDetection.specs;

const sessionSpecs = {
  name: 'test-session-node-nodejs',
  processingModules: [],
  ioMappings: [
    {
      processingModuleName: pmSpecs.name,
      inputMappings: [
        {
          inputName: pmSpecs.inputs[0].internalName,
          topic: topicImages
        }
      ],
      outputMappings: [
        {
          outputName: pmSpecs.outputs[0].internalName,
          topic: topicPredictions
        }
      ]
    }
  ]
};

let wait = (ms) => {
  return new Promise((resolve) => {
    setTimeout(resolve, ms);
  });
};

let testPublishSubscribe = async (ubiiNode) => {
  let topic = '/' + ubiiNode.id + topicTestPubSub;
  let token = await ubiiNode.subscribeTopic(topic, (msg) => {
    console.info('\ntestPublishSubscribe\ntopic: ' + topic + '\nmsg: ' + msg);
  });
  console.info('\testPublishSubscribe() - sub token:');
  console.info(token);

  ubiiNode.publishTopicdata({
    topicDataRecord: {
      topic: topic,
      timestamp: ubiiNode.generateTimestamp(),
      string: 'some test string'
    }
  });

  await wait(1000);

  ubiiNode.publishTopicdata({
    topicDataRecordList: {
      elements: [
        {
          topic: topic,
          timestamp: ubiiNode.generateTimestamp(),
          string: 'some other string'
        }
      ]
    }
  });

  await wait(1000);

  await ubiiNode.unsubscribeTopic(token);
};

let testSessionStartStop = async (ubiiNode, testSessionSpecs) => {
  //console.info('\ntestSessionStartStop');
  //console.info(testSessionSpecs);
  let response = await ubiiNode.callService({
    topic: DEFAULT_TOPICS.SERVICES.SESSION_RUNTIME_START,
    session: testSessionSpecs
  });
  if (response.session) {
    testSessionSpecs = response.session;
  }

  await wait(1000);

  let reply = await ubiiNode.callService({
    topic: DEFAULT_TOPICS.SERVICES.SESSION_RUNTIME_STOP,
    session: testSessionSpecs
  });
  console.info(reply);
};

(async function () {
  let ubiiNode = new UbiiNode('test-node-nodejs', 'localhost', '8101');
  await ubiiNode.initialize();

  // test PM and session specifications
  let testPMSpecs = Object.assign({}, pmSpecs);
  testPMSpecs.nodeId = ubiiNode.clientSpecification.id;
  //console.info('\ntestPMSpecs');
  //console.info(testPMSpecs);
  let testSessionSpecs = Object.assign({}, sessionSpecs);
  testSessionSpecs.processingModules.push(testPMSpecs);

  /* TESTING */
  //await testPublishSubscribe(ubiiNode);
  //await testSessionStartStop(ubiiNode, testSessionSpecs);

  let waitCycle = () => {
    setTimeout(waitCycle, 1000);
  };
  waitCycle();
})();
