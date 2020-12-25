const { ProtobufTranslator, MSG_TYPES, DEFAULT_TOPICS } = require('@tum-far/ubii-msg-formats');

const ZmqRequest = require('./zmqRequest');

class NetworkService {
  constructor(name, masterNodeIP, masterNodeServicePort) {
    this.name = name;
    this.masterNodeIP = masterNodeIP;
    this.masterNodeServicePort = masterNodeServicePort;
  }

  connect() {
    this.serviceRequestTranslator = new ProtobufTranslator(MSG_TYPES.SERVICE_REQUEST);
    this.serviceReplyTranslator = new ProtobufTranslator(MSG_TYPES.SERVICE_REPLY);

    this.zmqRequest = new ZmqRequest('tcp', this.masterNodeIP + ':' + this.masterNodeServicePort);
  }

  callService(request) {
    console.info('callService')
    console.info(request);
    return new Promise((resolve, reject) => {
      try {
        let buffer = this.serviceRequestTranslator.createBufferFromPayload(request);
        this.zmqRequest.sendRequest(buffer, (response) => {
          let reply = this.serviceReplyTranslator.createMessageFromBuffer(response);
          console.info('reply');
          console.info(reply);
          resolve(reply);
        });
      } catch (error) {
        reject(error);
      }
    });
  }
}

module.exports = NetworkService;
