/* eslint-disable no-console */
const namida = require('@tum-far/namida');
const WebSocket = require('ws');

const MSG_PING = 'PING';
const MSG_PONG = 'PONG';

const LOG_TAG = 'TopicData Client WS';

class TopicDataClientWS {
  /**
   * Communication endpoint implementing websocket.
   * @param {string} identity ID string to uniquely identify this object. This id is used to route messages to this socket.
   * @param {string} url URL to connect to.
   * @param {boolean} autoconnect Should the socket connect directly after the initialization of the object?
   * If not, the start method must be called manually.
   */
  constructor(identity, url, autoconnect = true) {
    this.identity = identity;
    this.url = url;

    if (autoconnect) {
      try {
        this.start();
      } catch (error) {
        console.error(error);
      }
    }
  }

  /**
   * Start the websocket client.
   */
  start() {
    try {
      this.websocket = new WebSocket(this.url + `?clientID=${this.identity}`);
    } catch (error) {
      console.error(error);
    }
    this.websocket.binaryType = 'arraybuffer';

    // add callbacks
    this.websocket.onmessage = (message) => {
      // process pings
      if (message.data === MSG_PING) {
        this.send(MSG_PONG);
        return;
      }

      if (!this.processMessage) {
        namida.logFailure(LOG_TAG, 'message processing callback not set, use setCbOnMessageReceived()');
      } else {
        this.processMessage(new Uint8Array(message.data));
      }
    };

    this.websocket.onerror = (error) => {
      throw error;
    };
  }

  setCbOnMessageReceived(callback) {
    this.processMessage = callback;
  }

  /**
   * Send a payload (string or Buffer object) to the server.
   * @param {(string|Buffer)} message
   */
  send(message) {
    //const tStart = Date.now();
    this.websocket.send(message);
    //const tEndSend = Date.now();
    //console.info('Websocket send time (ms): ' + (tEndSend - tStart));
  }

  stop() {
    this.websocket.close();
  }
}

module.exports = TopicDataClientWS;
