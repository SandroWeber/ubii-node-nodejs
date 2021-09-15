const EventEmitter = require('events');
const { v4: uuidv4 } = require('uuid');
const { proto, ProtobufTranslator, MSG_TYPES } = require('@tum-far/ubii-msg-formats');
const ProcessingModuleProto = proto.ubii.processing.ProcessingModule;
const namida = require('@tum-far/namida');

const ExternalLibrariesService = require('./externalLibrariesService');
const Utils = require('../utilities');

class ProcessingModule extends EventEmitter {
  constructor(specs = {}) {
    super();

    // take over specs
    //TODO: refactor to this.specs = specs and getters
    specs && Object.assign(this, specs);
    // new instance is getting new ID
    this.id = this.id || uuidv4();
    this.inputs = this.inputs || [];
    this.outputs = this.outputs || [];
    // check that language specification for module is correct
    if (this.language === undefined) this.language = ProcessingModuleProto.Language.JS;
    if (this.language !== ProcessingModuleProto.Language.JS) {
      namida.error(
        'ProcessingModule ' + this.toString(),
        'trying to create module under javascript, but specification says ' +
          ProcessingModuleProto.Language[this.language]
      );
      throw new Error(
        'Incompatible language specifications (javascript vs. ' + ProcessingModuleProto.Language[this.language] + ')'
      );
    }
    // default processing mode
    if (!this.processingMode) {
      this.processingMode = { frequency: { hertz: 30 } };
    }

    if (this.onCreatedStringified) {
      this.onCreated = Utils.createFunctionFromString(this.onCreatedStringified);
    }
    if (this.onProcessingStringified) {
      this.onProcessing = Utils.createFunctionFromString(this.onProcessingStringified);
    }
    if (this.onHaltedStringified) {
      this.onHalted = Utils.createFunctionFromString(this.onHaltedStringified);
    }
    if (this.onDestroyedStringified) {
      this.onDestroyed = Utils.createFunctionFromString(this.onDestroyedStringified);
    }

    this.status = ProcessingModuleProto.Status.CREATED;

    this.ioProxy = {};

    //TODO: refactor away from old "interactions" setup
    // only kept for backwards compatibility testing
    this.state = {};
    Object.defineProperty(this.state, 'modules', {
      // modules are read-only
      get: () => {
        return ExternalLibrariesService.instance.getExternalLibraries();
      },
      configurable: true
    });

    this.translatorProtobuf = new ProtobufTranslator(MSG_TYPES.PM);
  }

  /* execution control */

  start() {
    if (this.workerPool) {
      this.openWorkerpoolExecutions = [];
    }

    if (this.processingMode && this.processingMode.frequency) {
      this.startProcessingByFrequency();
    } else if (this.processingMode && this.processingMode.triggerOnInput) {
      this.startProcessingByTriggerOnInput();
    } else if (this.processingMode && this.processingMode.lockstep) {
      this.startProcessingByLockstep();
    } else if (this.processingMode === undefined) {
      namida.logFailure(this.toString(), 'no processing mode specified, can not start processing');
    }

    if (this.status === ProcessingModuleProto.Status.PROCESSING) {
      let message = 'started';
      if (this.workerPool) {
        message += ' (using workerpool)';
      } else {
        message += ' (without workerpool)';
      }
      namida.logSuccess(this.toString(), message);
      return true;
    }

    return false;
  }

  stop() {
    if (this.status === ProcessingModuleProto.Status.HALTED) {
      return;
    }

    this.onHalted && this.onHalted();
    this.status = ProcessingModuleProto.Status.HALTED;

    this.removeAllListeners(ProcessingModule.EVENTS.NEW_INPUT);
    this.onProcessingLockstepPass = () => {
      return undefined;
    };

    if (this.workerPool) {
      for (let exec of this.openWorkerpoolExecutions) {
        exec.cancel();
      }
    }

    namida.logSuccess(this.toString(), 'stopped');
  }

  startProcessingByFrequency() {
    this.status = ProcessingModuleProto.Status.PROCESSING;

    let tLastProcess = Date.now();
    let msFrequency = 1000 / this.processingMode.frequency.hertz;

    let processingPass = () => {
      // timing
      let tNow = Date.now();
      let deltaTime = tNow - tLastProcess;
      tLastProcess = tNow;

      // processing
      let inputs = this.readAllInputData();
      let { outputs, state } = this.onProcessing(deltaTime, inputs, this.state);
      this.writeAllOutputData(outputs);
      this.state = state ? state : this.state;

      if (this.status === ProcessingModuleProto.Status.PROCESSING) {
        setTimeout(() => {
          processingPass();
        }, msFrequency);
      }
    };
    processingPass();
  }

  startProcessingByTriggerOnInput() {
    this.status = ProcessingModuleProto.Status.PROCESSING;

    let allInputsNeedUpdate = this.processingMode.triggerOnInput.allInputsNeedUpdate;
    let minDelayMs = this.processingMode.triggerOnInput.minDelayMs;

    let tLastProcess = Date.now();

    let processingPass = () => {
      // timing
      let tNow = Date.now();
      let deltaTime = tNow - tLastProcess;
      tLastProcess = tNow;
      // processing
      let inputData = this.readAllInputData();
      for (let inputTriggerName of this.inputTriggerNames) {
        if (!inputData[inputTriggerName]) {
          namida.logFailure(
            this.toString(),
            'input trigger for "' + inputTriggerName + '", but input data is ' + inputData[inputTriggerName]
          );
        } else {
          this.state.inputTriggerNames = [...this.inputTriggerNames]; // copy those input names that received update trigger to state
        }
      }
      this.inputTriggerNames = [];

      let { outputs, state } = this.onProcessing(deltaTime, inputData, this.state);

      this.writeAllOutputData(outputs);
      this.state = state ? state : this.state;
    };

    let checkProcessingNeeded = false;
    let checkProcessing = () => {
      if (this.status !== ProcessingModuleProto.Status.PROCESSING) return;

      let inputUpdatesFulfilled =
        !allInputsNeedUpdate || this.inputs.every((element) => this.inputTriggerNames.includes(element.internalName));
      let minDelayFulfilled = !minDelayMs || Date.now() - tLastProcess >= minDelayMs;
      if (inputUpdatesFulfilled && minDelayFulfilled) {
        processingPass();
      }
      checkProcessingNeeded = false;
    };

    this.inputTriggerNames = [];
    this.on(ProcessingModule.EVENTS.NEW_INPUT, (inputName) => {
      if (!this.inputTriggerNames.includes(inputName)) this.inputTriggerNames.push(inputName);
      if (!checkProcessingNeeded) {
        checkProcessingNeeded = true;
        setImmediate(() => {
          checkProcessing();
        });
      }
    });
  }

  startProcessingByLockstep() {
    this.status = ProcessingModuleProto.Status.PROCESSING;

    this.onProcessingLockstepPass = (deltaTime, inputs = this.readAllInputData()) => {
      return new Promise((resolve, reject) => {
        try {
          let outputData = this.onProcessing(deltaTime, inputs, this.state);
          return resolve(outputData);
        } catch (error) {
          return reject(error);
        }
      });
    };
  }

  async setWorkerPool(workerPool) {
    let viable = await this.isWorkerpoolViable(workerPool);

    if (viable) {
      this.workerPool = workerPool;

      // redefine onProcessing to be executed via workerpool
      this.originalOnProcessing = this.onProcessing;
      let workerpoolOnProcessing = (deltaTime, inputs, state) => {
        let wpExecPromise = this.workerPool
          .exec(this.originalOnProcessing, [deltaTime, inputs, state])
          .then((outputData) => {
            /*this.outputs.forEach((output) => {
              if (outputData && outputData.hasOwnProperty(output.internalName)) {
                this.ioProxy[output.internalName] = outputData[output.internalName];
              }
            });*/
            this.writeAllOutputData(outputData);
            this.openWorkerpoolExecutions.splice(this.openWorkerpoolExecutions.indexOf(wpExecPromise), 1);
          })
          .catch((error) => {
            if (!error.message || error.message !== 'promise cancelled') {
              // executuion was not just cancelled via workerpool API
              namida.logFailure(this.toString(), 'workerpool execution failed - ' + error + '\n' + error.stack);
            }
          });
        this.openWorkerpoolExecutions.push(wpExecPromise);
      };
      this.onProcessing = workerpoolOnProcessing;
    } else {
      namida.warn(this.toString(), 'not viable to be executed via workerpool, might slow down system significantly');
    }
  }

  async isWorkerpoolViable(workerPool) {
    try {
      await workerPool.exec(this.onProcessing, [1, this.readAllInputData(), this.state]);
      return true;
    } catch (error) {
      return false;
    }
  }

  /* execution control end */

  /* lifecycle functions */

  setOnCreated(callback) {
    this.onCreated = callback;
  }

  setOnProcessing(callback) {
    this.onProcessing = callback;
  }

  setOnHalted(callback) {
    this.onHalted = callback;
  }

  setOnDestroyed(callback) {
    this.onDestroyed = callback;
  }

  onCreated() {}

  /**
   * Lifecycle function to be called when module is supposed to process data.
   * Needs to be overwritten when extending this class, specified as a stringified version for the constructor or
   * set via setOnProcessing() before onProcessing() is called.
   */
  onProcessing(deltaTime, inputs, outputs, state) {
    let errorMsg =
      'onProcessing callback is not specified / overwritten, called with' +
      '\ndeltaTime: ' +
      deltaTime +
      '\ninputs:\n' +
      inputs +
      '\noutputs:\n' +
      outputs +
      '\nstate:\n' +
      state;
    namida.error(this.toString(), errorMsg);
    throw new Error(this.toString() + ' - onProcessing() callback is not specified');
  }

  onProcessingLockstepPass() {
    return undefined;
  }

  onHalted() {}

  onDestroyed() {}

  /* lifecycle functions end */

  /* I/O functions */

  setInputGetter(internalName, getter, overwrite = false) {
    // check internal naming is viable
    if (!this.checkInternalName(internalName, overwrite)) {
      return false;
    }

    // make sure getter is defined
    if (getter === undefined) {
      namida.error(this.toString(), 'trying to set input getter for ' + internalName + ' but getter is undefined');
      return false;
    }
    // make sure getter is a function
    if (typeof getter !== 'function') {
      namida.error(this.toString(), 'trying to set input getter for ' + internalName + ' but getter is not a function');
      return false;
    }

    // make sure we're clean
    this.removeIOAccessor(internalName);
    // define getter for both ioProxy and module itself (as shortcut), input is read-only
    [this.ioProxy, this].forEach((object) => {
      Object.defineProperty(object, internalName, {
        get: () => {
          return getter();
        },
        configurable: true,
        enumerable: true
      });
    });

    return true;
  }

  setOutputSetter(internalName, setter, overwrite = false) {
    // check internal naming is viable
    if (!this.checkInternalName(internalName, overwrite)) {
      return false;
    }

    // make sure setter is defined
    if (setter === undefined) {
      namida.error(this.toString(), 'trying to set output setter for ' + internalName + ' but setter is undefined');
      return false;
    }
    // make sure setter is a function
    if (typeof setter !== 'function') {
      namida.error(
        this.toString(),
        'trying to set output setter for ' + internalName + ' but setter is not a function'
      );
      return false;
    }

    // make sure we're clean
    this.removeIOAccessor(internalName);
    // define setter for both ioProxy and module itself (as shortcut), output is write-only
    [this.ioProxy, this].forEach((object) => {
      Object.defineProperty(object, internalName, {
        set: (value) => {
          setter(value);
        },
        configurable: true,
        enumerable: true
      });
    });

    return true;
  }

  removeIOAccessor(internalName) {
    if (this.ioProxy.hasOwnProperty(internalName)) {
      delete this.ioProxy[internalName];
      delete this[internalName];
    }
  }

  removeAllIOAccessors() {
    for (let key in this.ioProxy) {
      this.removeIOAccessor(key);
    }
  }

  checkInternalName(internalName, overwrite = false) {
    // case: name that is a property of this class and explicitly not an otherwise viable internal name
    // and should therefore never be overwritten
    if (this.hasOwnProperty(internalName) && !this.ioProxy.hasOwnProperty(internalName)) {
      namida.error(
        this.toString(),
        'the internal I/O naming "' + internalName + '" should not be used as it conflicts with internal properties'
      );
      return false;
    }
    // case: we're not using an already defined name without specifying to overwrite
    if (this.ioProxy.hasOwnProperty(internalName) && !overwrite) {
      namida.error(
        this.toString(),
        'the internal I/O naming "' + internalName + '" is already defined (overwrite not specified)'
      );
      return false;
    }
    // case: the internal name is empty
    if (internalName === '') {
      namida.error(this.toString(), 'the internal I/O naming "' + internalName + '" can\'t be used (empty)');
      return false;
    }

    return true;
  }

  readInput(name) {
    return this.ioProxy[name];
  }

  writeOutput(name, value) {
    this.ioProxy[name] = value;
  }

  /* I/O functions end */

  /* helper functions */

  readAllInputData() {
    let inputData = {};
    for (let input of this.inputs) {
      inputData[input.internalName] = this.ioProxy[input.internalName];
    }

    return inputData;
  }

  writeAllOutputData(outputData) {
    if (!outputData) return;

    for (let outputSpec of this.outputs) {
      let output = outputData[outputSpec.internalName];
      if (output) {
        this.ioProxy[outputSpec.internalName] = output;
      }
    }
  }

  getIOMessageFormat(name) {
    let ios = [...this.inputs, ...this.outputs];
    let io = ios.find((io) => {
      return io.internalName === name;
    });

    return io.messageFormat;
  }

  toString() {
    return 'ProcessingModule ' + this.name + ' (ID ' + this.id + ')';
  }

  toProtobuf() {
    return this.translatorProtobuf.createMessageFromPayload(this);
  }

  /* helper functions end */
}

ProcessingModule.EVENTS = Object.freeze({
  NEW_INPUT: 1,
  LOCKSTEP_PASS: 2,
  PROCESSED: 3
});

module.exports = { ProcessingModule };
