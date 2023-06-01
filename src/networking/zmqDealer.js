const zmq = require('zeromq');

const { PING_MESSAGE, PONG_MESSAGE } = require('./constants');

class ZmqDealer {
  /**
   * Communication endpoint implementing the zmq router pattern.
   * @param {*} identity ID string to uniquely identify this object. This id is used to route messages to this socket.
   * @param {*} transportProtocol Transport protocol to bind to.
   * @param {*} address Address to bind to.
   * @param {*} autoBind Should the socket bind directly after the initialization of the object?
   * If not, the start method must be called manually.
   */
  constructor(identity, transportProtocol = 'tcp', address = '*:6666', autoBind = true) {
    this.identity = identity;
    this.transportProtocol = transportProtocol;
    this.address = address;
    this.ready = false;

    this.socket = {};

    this.waitingPongCallbacks = new Map();

    if (autoBind) {
      this.start();
    }
  }

  /**
   * Start the router.
   */
  start() {
    // init
    this.socket = zmq.socket('dealer');
    this.socket.identity = this.identity;

    // add callbacks
    this.socket.on('message', (envelope, payload) => {
      try {
        if (payload.toString() === PING_MESSAGE) {
          let bytes = Buffer.from(PONG_MESSAGE);
          this.send(bytes);
          return;
        }
      } catch (error) {
        console.error(error);
      }

      if (!this.onMessage) {
        namida.logFailure('ZMQ router socket', 'no callback for message handling set!');
      } else {
        this.onMessage(payload);
      }
    });

    // bind
    this.endpoint = this.transportProtocol + '://' + this.address;
    this.socket.connect(this.endpoint, (err) => {
      if (err) {
        console.error('Error: ' + err);
      } else {
        this.ready = true;
      }
    });
  }

  /**
   * Stop the router and close the socket.
   */
  stop() {
    this.ready = false;
    this.socket.close();
  }

  /**
   * Set the message handling function to be called upon receiving a message. Also marks the this socket as ready to receive.
   * @param {*} callback Callback function that is called when a new message is received from a dealer socket.
   * Callback should accept an envelope parameter containing the client identity and a message parameter with the received message buffer.
   */
  setCallbackOnMessage(callback) {
    this.onMessage = callback;
    this.ready = true;
  }

  /**
   * Send a payload (string or Buffer object) to the specified dealer client.
   * @param {(string|Buffer)} payload
   */
  send(payload) {
    //const tStart = Date.now();
    this.socket.send(payload);
    //const tEndSend = Date.now();
    //console.info('ZeroMQ send time (ms): ' + (tEndSend - tStart));
  }

  toString() {
    let status = this.ready ? 'ready' : 'not ready';

    return this.identity + ' | ' + status + ' | ZMQ-DEALER ' + this.endpoint;
  }
}

module.exports = ZmqDealer;
