const zmq = require('zeromq');

const namida = require('@tum-far/namida/src/namida');
const { ProtobufTranslator, MSG_TYPES } = require('@tum-far/ubii-msg-formats');

class ZmqRequest {
  /**
   * Communication endpoint implementing the zmq reply pattern.
   * @param {*} transportProtocol Transport protocol to bind to.
   * @param {*} address Address to bind to.
   * @param {*} autoBind Should the socket bind directly after the initialization of the object?
   * If not, the start method must be called manually.
   */
  constructor(transportProtocol = 'tcp', address = '*:5555', autoBind = true) {
    this.transportProtocol = transportProtocol;
    this.address = address;

    this.ready = false;
    this.socket = {};
    this.requestQueue = [];

    this.serviceRequestTranslator = new ProtobufTranslator(MSG_TYPES.SERVICE_REQUEST);
    this.serviceReplyTranslator = new ProtobufTranslator(MSG_TYPES.SERVICE_REPLY);

    if (autoBind) {
      this.start();
    }
  }

  start() {
    // init
    this.socket = zmq.socket('req');

    // add callbacks
    this.socket.on('message', (response) => {
      if (!this.onResponse) {
        namida.logFailure('ZMQ request socket', 'no callback for response handling set!');
      } else {
        let responseMsg = this.serviceReplyTranslator.createMessageFromBuffer(response);
        this.onResponse(responseMsg);
        this.pendingRequest = false;
      }
    });

    // bind
    this.endpoint = this.transportProtocol + '://' + this.address;
    this.socket.connect(this.endpoint, (err) => {
      if (err) {
        console.log('Error: ' + err);
      } else {
        this.open = true;
      }
    });

    // start request handling
    let requestQueueWorker = () => {
      if (this.requestQueue.length > 0) {
        this.handleNextRequest();
      }
      setTimeout(requestQueueWorker, 1);
    }
    requestQueueWorker();
  }

  stop() {
    this.ready = false;
    this.socket.close();
  }

  sendRequest(request, onResponseCallback) {
    //let requestBuffer = this.serviceRequestTranslator.createBufferFromPayload(request);
    this.requestQueue.push({request, onResponseCallback});

    this.handleNextRequest();
  }

  handleNextRequest() {
    if (this.pendingRequest) {
      return;
    }
    this.pendingRequest = true;

    let next = this.requestQueue.splice(0,1)[0];
    this.onResponse = next.onResponseCallback;
    let requestBuffer = this.serviceRequestTranslator.createBufferFromPayload(next.request);
    this.socket.send(requestBuffer);
  }

  toString() {
    let status = this.ready ? 'ready' : 'not ready';

    return 'ZMQ-Service | ' + status + ' | ZMQ-REQUEST at ' + this.endpoint;
  }
}

module.exports = ZmqRequest;
