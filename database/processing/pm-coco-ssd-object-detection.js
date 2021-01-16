const tf = require('@tensorflow/tfjs-node');
const cocoSsd = require('@tensorflow-models/coco-ssd');
const { MSG_TYPES } = require('@tum-far/ubii-msg-formats');

const { ProcessingModule } = require('../../src/processing/processingModule.js');

class PMCoCoSSDObjectDetection extends ProcessingModule {
  static specs = {
    name: 'coco-ssd-object-detection',
    tags: ['coco', 'ssd', 'object-detection', 'tensorflow'],
    description:
      'All credit goes to https://github.com/tensorflow/tfjs-models/tree/master/coco-ssd. Processes RGB8 image and returns SSD predictions.',

    inputs: [
      {
        internalName: 'image',
        messageFormat: MSG_TYPES.DATASTRUCTURE_IMAGE
      }
    ],
    outputs: [
      {
        internalName: 'predictions',
        messageFormat: MSG_TYPES.DATASTRUCTURE_OBJECT2D_LIST
      }
    ],

    processingMode: {
      frequency: {
        hertz: 10
      }
    }
  };

  constructor(specs) {
    super(specs);

    Object.assign(this, PMCoCoSSDObjectDetection.specs);
  }

  onCreated() {
    let prepareModel = async () => {
      this.state.model = await cocoSsd.load();
    };
    prepareModel();
  }

  async predict(image) {
    let imgTensor = tf.tensor3d(image.data, [image.height, image.width, 3], 'int32');
    let predictions = await this.state.model.detect(imgTensor); // line causes memory leaks
    return [];//predictions;
  }

  onProcessing() {
    let image = this.image;
    if (image && this.state.model) {
      // make predictions
      this.predict(image).then((predictions) => {
        // generate output list
        /*let outputList = [];
        predictions.forEach((prediction) => {
          let pos = { x: prediction.bbox[0] / image.width, y: prediction.bbox[1] / image.height };
          outputList.push({
            id: prediction.class,
            pose: { position: pos },
            size: { x: prediction.bbox[2] / image.width, y: prediction.bbox[3] / image.height }
          });
        });
        // write output
        this.predictions = { elements: outputList };*/
      });
    }
  }
}

module.exports = PMCoCoSSDObjectDetection;
