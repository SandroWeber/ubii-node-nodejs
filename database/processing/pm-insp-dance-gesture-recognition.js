const { MSG_TYPES } = require('@tum-far/ubii-msg-formats');
const { ProcessingModule } = require('../../src/processing/processingModule.js');

class InspectorDanceGestureRecognitionPM extends ProcessingModule {
  static specs = {
    name: 'inspector-dance-gesture-recognition',
    authors: ['Nina Cordes (nina.cordes@tum.de)', 'Sandro Weber (webers@in.tum.de)'],
    tags: ['imu', 'gesture recognition', 'game'],

    processingMode: {
      frequency: {
        hertz: 1
      }
    },
    inputs: [
      {
        internalName: 'listAccelerationData',
        messageFormat: MSG_TYPES.TOPIC_DATA_RECORD_LIST
      }
    ]
  };

  constructor(specs) {
    super(specs);
    Object.assign(this, InspectorDanceGestureRecognitionPM.specs);
    //console.info('hi this is InspectorDanceGestureRecognitionPM.constructor()');
  }

  onCreated() {
    //console.info('hi this is InspectorDanceGestureRecognitionPM.onCreated()');
  }

  async onProcessing(deltaTime, inputs, outputs, state) {
    let listAccelerationData = inputs.listAccelerationData;
    //console.info(listAccelerationData);
    //console.info('hi this is InspectorDanceGestureRecognitionPM.onProcessing()');
  }
}

module.exports = InspectorDanceGestureRecognitionPM;
