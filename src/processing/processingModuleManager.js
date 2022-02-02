const EventEmitter = require('events');
const workerpool = require('workerpool');

const namida = require('@tum-far/namida/src/namida');
const { RuntimeTopicData } = require('@tum-far/ubii-topic-data');
const { proto } = require('@tum-far/ubii-msg-formats');
const ProcessingModuleProto = proto.ubii.processing.ProcessingModule;

const Utils = require('../utilities');
const { ProcessingModule } = require('./processingModule');
const ProcessingModuleStorage = require('../storage/processingModuleStorage');
const DeviceManager = require('../devices/deviceManager');

class ProcessingModuleManager extends EventEmitter {
  constructor(nodeID, topicData = undefined) {
    super();

    this.nodeID = nodeID;
    this.deviceManager = DeviceManager.instance;
    this.topicData = topicData;

    this.processingModules = new Map();
    this.ioMappings = new Map();
    this.pmTopicSubscriptions = new Map();

    //TODO: optimize for use without real TopicData, avoiding write/read cycles for each lockstep request/reply
    this.lockstepTopicData = new RuntimeTopicData();
    /*this.lockstepInputTopicdata = {
      records: []
    };
    this.lockstepOutputTopicdata = {
      records: []
    };*/

    this.workerPool = workerpool.pool();
  }

  createModule(specs) {
    if (specs.id && this.processingModules.has(specs.id)) {
      namida.logFailure(
        'ProcessingModuleManager',
        "can't create module " + specs.name + ', ID already exists: ' + specs.id
      );
    }

    let pm = undefined;
    if (ProcessingModuleStorage.instance.hasEntry(specs.name)) {
      pm = ProcessingModuleStorage.instance.createInstance(specs);
    } else {
      // create new module based on specs
      if (!specs.onProcessingStringified) {
        namida.logFailure(
          'ProcessingModuleManager',
          'can\'t create PM "' + specs.name + '" based on specs, missing onProcessing definition.'
        );
        return undefined;
      }
      pm = new ProcessingModule(specs);
    }
    pm.nodeId = this.nodeID;

    let success = this.addModule(pm);
    if (!success) {
      return undefined;
    } else {
      pm.initialized = this.initializeModule(pm);
      return pm;
    }
  }

  async initializeModule(pm) {
    try {
      pm.onCreated && (await pm.onCreated(pm.state));
      await pm.setWorkerPool(this.workerPool);

      return true;
    } catch (error) {
      namida.logFailure(this.toString(), 'PM initialization error:\n' + error);
      return false;
    }
  }

  addModule(pm) {
    if (!pm.id) {
      namida.logFailure('ProcessingModuleManager', 'module ' + pm.name + " does not have an ID, can't add");
      return false;
    }
    this.processingModules.set(pm.id, pm);
    return true;
  }

  removeModule(pmSpecs) {
    if (!pmSpecs.id) {
      namida.logFailure('ProcessingModuleManager', 'module ' + pmSpecs.name + " does not have an ID, can't remove");
      return false;
    }

    if (this.pmTopicSubscriptions.has(pmSpecs.id)) {
      let subscriptionTokens = this.pmTopicSubscriptions.get(pmSpecs.id);
      subscriptionTokens.forEach((token) => {
        this.topicData.unsubscribe(token);
      });
      this.pmTopicSubscriptions.delete(pmSpecs.id);
    }

    this.processingModules.delete(pmSpecs.id);
  }

  hasModuleID(id) {
    return this.processingModules.has(id);
  }

  getModuleBySpecs(pmSpecs, sessionID) {
    return this.getModuleByID(pmSpecs.id) || this.getModuleByName(pmSpecs.name, sessionID);
  }

  getModuleByID(id) {
    return this.processingModules.get(id);
  }

  getModuleByName(name, sessionID) {
    let candidates = [];
    this.processingModules.forEach((pm) => {
      if (pm.name === name) {
        candidates.push(pm);
      }
    });

    if (sessionID) {
      candidates = candidates.filter((element) => element.sessionId === sessionID);
    }

    if (candidates.length > 1) {
      namida.logFailure(
        'ProcessingModuleManager',
        'trying to get PM by name (' + name + ') resulted in multiple candidates'
      );
    } else {
      return candidates[0];
    }
  }

  getModulesProcessing() {
    return this.getModulesByStatus(ProcessingModuleProto.Status.PROCESSING);
  }

  getModulesByStatus(status) {
    return Array.from(this.processingModules)
      .map((keyValue) => {
        return keyValue[1];
      })
      .filter((pm) => pm.status === status);
  }

  /* running modules */

  async startModule(pmSpec) {
    let pm = this.processingModules.get(pmSpec.id);
    await pm.initialized;
    pm && pm.start();
    this.emit(ProcessingModuleManager.EVENTS.PM_STARTED, pmSpec);
  }

  async stopModule(pmSpec) {
    let pm = this.processingModules.get(pmSpec.id);
    pm && (await pm.stop());
    this.emit(ProcessingModuleManager.EVENTS.PM_STOPPED, pmSpec);
    let subs = this.pmTopicSubscriptions.get(pmSpec.id);
    subs &&
      subs.forEach((token) => {
        this.topicData.unsubscribe(token);
      });
  }

  async startAllSessionModules(session) {
    for (let pm of this.processingModules) {
      if (pm.sessionId === session.id) {
        await this.startModule({ id: pm.id });
      }
    }
  }

  async stopAllSessionModules(session) {
    for (let pm of this.processingModules) {
      if (pm.sessionId === session.id) {
        await this.stopModule({ id: pm.id });
      }
    }
  }

  /* I/O <-> topic mapping functions */

  async applyIOMappings(ioMappings, sessionID) {
    // filter out I/O mappings for PMs that run on this node
    let applicableIOMappings = ioMappings.filter((ioMapping) =>
      this.processingModules.has(ioMapping.processingModuleId)
    );

    for (let mapping of applicableIOMappings) {
      this.ioMappings.set(mapping.processingModuleId, mapping);
      let processingModule =
        this.getModuleByID(mapping.processingModuleId) || this.getModuleByName(mapping.processingModuleName, sessionID);
      if (!processingModule) {
        namida.logFailure(
          'ProcessingModuleManager',
          "can't find processing module for I/O mapping, given: ID = " +
            mapping.processingModuleId +
            ', name = ' +
            mapping.processingModuleName +
            ', session ID = ' +
            sessionID
        );
        return;
      }

      // connect inputs
      mapping.inputMappings && (await this.applyInputMappings(processingModule, mapping.inputMappings));

      // connect outputs
      mapping.outputMappings && this.applyOutputMappings(processingModule, mapping.outputMappings);
    }
  }

  async applyInputMappings(processingModule, inputMappings) {
    let isLockstep = processingModule.processingMode && processingModule.processingMode.lockstep;
    let topicDataBuffer = isLockstep ? this.lockstepTopicData : this.topicData;

    for (let inputMapping of inputMappings) {
      if (!this.isValidIOMapping(processingModule, inputMapping)) {
        namida.logFailure(
          'ProcessingModuleManager',
          'IO-Mapping for module ' + processingModule.name + '->' + inputMapping.inputName + ' is invalid'
        );
        return;
      }

      let topicSource =
        inputMapping[inputMapping.topicSource] ||
        inputMapping.topicSource ||
        inputMapping.topic ||
        inputMapping.topicMux;

      let isTopicMuxer = typeof topicSource === 'object';

      // set approriate input getter
      let inputGetterCallback = undefined;
      let multiplexer = undefined;
      // single topic input
      if (!isTopicMuxer) {
        inputGetterCallback = () => {
          return topicDataBuffer.pull(topicSource);
        };
      }
      // topic muxer input
      else if (isTopicMuxer) {
        multiplexer =
          this.deviceManager.getTopicMuxer(topicSource.id) ||
          (await this.deviceManager.createTopicMuxer(topicSource, topicDataBuffer));

        inputGetterCallback = () => {
          return multiplexer.getRecords();
        };
      }
      processingModule.setInputGetter(inputMapping.inputName, inputGetterCallback);

      // subscribe to topics necessary for PM (if not lockstep), set input event emitter in case of trigger on input mode
      if (!isLockstep) {
        // if mode frequency, we do nothing but subscribe nonetheless to indicate our PM on this node needs the topic
        //TODO: allow undefined callbacks? potential ambiguous scenarios?
        let callback = () => {};
        // if PM is triggered on input, notify PM for new input
        //TODO: needs to be done for topic muxer too? does it make sense for accumulated topics to trigger processing?
        // use-case seems not to match but leaving opportunity open could be nice
        if (processingModule.processingMode && processingModule.processingMode.triggerOnInput) {
          if (!isTopicMuxer) {
            callback = () => {
              processingModule.emit(ProcessingModule.EVENTS.NEW_INPUT, inputMapping.inputName);
            };
          } else if (isTopicMuxer) {
            callback = (record) => {
              multiplexer.onTopicData(record);
              processingModule.emit(ProcessingModule.EVENTS.NEW_INPUT, inputMapping.inputName);
            };
          }
        }

        // single topic input
        if (!isTopicMuxer) {
          let subscriptionToken = await topicDataBuffer.subscribe(topicSource, callback);
          if (!this.pmTopicSubscriptions.has(processingModule.id)) {
            this.pmTopicSubscriptions.set(processingModule.id, []);
          }
          this.pmTopicSubscriptions.get(processingModule.id).push(subscriptionToken);
        }
        // topic muxer input
        else if (isTopicMuxer) {
          let subscriptionToken = await topicDataBuffer.subscribeRegex(topicSource.topicSelector, callback);
          if (!this.pmTopicSubscriptions.has(processingModule.id)) {
            this.pmTopicSubscriptions.set(processingModule.id, []);
          }
          this.pmTopicSubscriptions.get(processingModule.id).push(subscriptionToken);
        }
      }
    }
  }

  applyOutputMappings(processingModule, outputMappings) {
    let isLockstep = processingModule.processingMode && processingModule.processingMode.lockstep;
    let topicDataBuffer = isLockstep ? this.lockstepTopicData : this.topicData;

    for (let outputMapping of outputMappings) {
      if (!this.isValidIOMapping(processingModule, outputMapping)) {
        namida.logFailure(
          'ProcessingModuleManager',
          'OutputMapping for module ' +
            processingModule.toString() +
            ' -> "' +
            outputMapping.outputName +
            '" is invalid'
        );
        namida.logFailure('ProcessingModuleManager', outputMapping);
        return;
      }

      let topicDestination =
        outputMapping[outputMapping.topicDestination] ||
        outputMapping.topicDestination ||
        outputMapping.topic ||
        outputMapping.topicDemux;
      // single topic output
      if (typeof topicDestination === 'string') {
        let messageFormat = processingModule.getIOMessageFormat(outputMapping.outputName);
        let type = Utils.getTopicDataTypeFromMessageFormat(messageFormat);

        processingModule.setOutputSetter(outputMapping.outputName, (record) => {
          if (!record[type]) {
            namida.logFailure(
              processingModule.toString(),
              'Output "' +
                outputMapping.outputName +
                '" (topic=' +
                topicDestination +
                ') returned without any value for "' +
                type +
                '" after processing.'
            );
            return;
          }

          record.topic = topicDestination;
          record.type = type;
          record.timestamp = Utils.generateTimestamp();
          topicDataBuffer.publish(record.topic, record);
        });

        /*// lockstep mode
          if (isLockstep) {
            processingModule.setOutputSetter(inputMapping.inputName, (value) => {
              let record = { topic: topicDestination };
              record.type = type;
              record[type] = value;
              this.lockstepOutputTopicdata.records.push(record);
            });
          }
          // all async modes (immediate cycles, frequency, input trigger) - directly publish to topicdata buffer
          processingModule.setOutputSetter(outputMapping.outputName, (value) => {
            this.topicData.publish(topicDestination, value, type);
          });*/
      }
      // topic demuxer output
      else if (typeof topicDestination === 'object') {
        let demultiplexer =
          this.deviceManager.getTopicDemuxer(topicDestination.id) ||
          this.deviceManager.createTopicDemuxer(topicDestination, topicDataBuffer);

        processingModule.setOutputSetter(outputMapping.outputName, (demuxerRecordList) => {
          demultiplexer.publish(demuxerRecordList);
        });
      }
    }
  }

  isValidIOMapping(processingModule, ioMapping) {
    if (ioMapping.inputName) {
      return processingModule.inputs.some((element) => element.internalName === ioMapping.inputName);
    } else if (ioMapping.outputName) {
      return processingModule.outputs.some((element) => element.internalName === ioMapping.outputName);
    }

    return false;
  }

  /* I/O <-> topic mapping functions end */

  /* lockstep processing functions */

  sendLockstepProcessingRequest(nodeId, request) {
    if (nodeId === this.nodeID) {
      // server side PM
      return new Promise((resolve, reject) => {
        // assign input
        request.records.forEach((record) => {
          //TODO: refactor without use of extra topicdata to avoid write/read cycles
          this.lockstepTopicData.publish(record.topic, record);
        });
        //this.lockstepInputTopicdata.records = request.records;
        //TODO: need to map input names for lockstepInputTopicdata to records so it can be passed as second argument to onProcessingLockstepPass()
        // clear output
        //this.lockstepOutputTopicdata.records = [];

        // lockstep pass calls to PMs
        let lockstepPasses = [];
        request.processingModuleIds.forEach((id) => {
          lockstepPasses.push(this.processingModules.get(id).onProcessingLockstepPass(request.deltaTimeMs));
        });

        Promise.all(lockstepPasses).then(() => {
          let reply = this.produceLockstepProcessingReply(request);
          return resolve(reply);
        });
      });
    }
  }

  produceLockstepProcessingReply(lockstepProcessingRequest) {
    let lockstepProcessingReply = {
      processingModuleIds: [],
      records: []
    };
    lockstepProcessingRequest.processingModuleIds.forEach((id) => {
      lockstepProcessingReply.processingModuleIds.push(id);
      this.processingModules.get(id).outputs.forEach((pmOutput) => {
        let outputMapping = this.ioMappings
          .get(id)
          .outputMappings.find((mapping) => mapping.outputName === pmOutput.internalName);
        let destination = outputMapping[outputMapping.topicDestination] || outputMapping.topicDestination;

        // single topic
        let topicdataEntry = this.lockstepTopicData.pull(destination);
        if (topicdataEntry) {
          let record = {
            topic: destination,
            type: topicdataEntry.type
          };
          record[record.type] = topicdataEntry.data;
          lockstepProcessingReply.records.push(record);
        }
        //TODO: handle demuxer output
      });
    });

    return lockstepProcessingReply;
  }

  /* lockstep processing functions end */
}

ProcessingModuleManager.EVENTS = Object.freeze({
  PM_STARTED: 'PM_STARTED',
  PM_STOPPED: 'PM_STOPPED'
});

module.exports = ProcessingModuleManager;
