const UbiiNode = require('../src/ubiiNode');
const { DEFAULT_TOPICS } = require('@tum-far/ubii-msg-formats/dist/js/constants');

const PMCoCoSSDObjectDetection = require('../database/processing/pm-coco-ssd-object-detection');

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
        inputMappings: [{
          inputName: pmSpecs.inputs[0].internalName,
          topic: topicImages
        }],
        outputMappings: [{
          outputName: pmSpecs.outputs[0].internalName,
          topic: topicPredictions
        }]
      }
    ]
};

let testPublishSubscribe = async (ubiiNode) => {
  let token = await ubiiNode.subscribeTopic(topicTestPubSub, (msg) => {
    //console.info('\ncustom sub callback - topic: ' + msg);
  });
  console.info('\ninit() - sub token:');
  console.info(token);

  ubiiNode.publish({
    topicDataRecord: {
      topic: topicTestPubSub,
      timestamp: ubiiNode.generateTimestamp(),
      string: 'some test string'
    }
  });

  setTimeout(() => {
    ubiiNode.publish({
      topicDataRecordList: {
        elements: [
          {
            topic: topicTestPubSub,
            timestamp: ubiiNode.generateTimestamp(),
            string: 'some other string'
          }
        ]
      }
    });

    ubiiNode.unsubscribeTopic(token);
  }, 1000);
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

  await ubiiNode.callService({
    topic: DEFAULT_TOPICS.SERVICES.SESSION_RUNTIME_STOP,
    session: testSessionSpecs
  });
}

(async function () {
  let ubiiNode = new UbiiNode('test-node-nodejs', '192.168.178.39', '8101');
  await ubiiNode.initialize();

  // test PM and session specifications
  let testPMSpecs = Object.assign({}, pmSpecs);
  testPMSpecs.nodeId = ubiiNode.clientSpecification.id;
  //console.info('\ntestPMSpecs');
  //console.info(testPMSpecs);
  let testSessionSpecs = Object.assign({}, sessionSpecs);
  testSessionSpecs.processingModules.push(testPMSpecs);

  /* TESTING */
  await testPublishSubscribe(ubiiNode);
  await testSessionStartStop(ubiiNode, testSessionSpecs);
})();
