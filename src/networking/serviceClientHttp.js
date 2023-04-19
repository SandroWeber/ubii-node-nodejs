/* eslint-disable no-console */
//import fetch from 'node-fetch';
const axios = require('axios');

const { ProtobufTranslator, MSG_TYPES } = require('@tum-far/ubii-msg-formats');
const namida = require("@tum-far/namida");

const LOG_TAG = 'ServiceClientHTTP';

class ServiceClientHTTP {
  /**
   * Communication endpoint implementing REST pattern.
   * @param {*} address Address of the master node HTTP service endpoint.
   */
  constructor(address, format) {
    this.address = address;
    this.format = format;
    this.useHTTPS = process.env.NODE_ENV === 'production' ? true : false;

    if (this.format === ServiceClientHTTP.CONSTANTS.MSG_FORMAT_BINARY) {
      this.serviceRequestTranslator = new ProtobufTranslator(MSG_TYPES.SERVICE_REQUEST);
      this.serviceReplyTranslator = new ProtobufTranslator(MSG_TYPES.SERVICE_REPLY);
      this.sendRequest = this.sendBinary;
    } else if (this.format === ServiceClientHTTP.CONSTANTS.MSG_FORMAT_JSON) {
      this.sendRequest = this.sendJSON;
    } else {
      namida.logFailure(LOG_TAG, `format "${format}" not recognized, must be "${ServiceClientHTTP.CONSTANTS.MSG_FORMAT_BINARY}" or "${ServiceClientHTTP.CONSTANTS.MSG_FORMAT_JSON}"`);
      throw new Error('message format not recognized');
    }
  }

  sendRequest() {
    throw new Error('must be replaced depending on format config');
  }

  sendBinary() {
    throw new Error('not implemented');
  }

  sendJSON(message, onResponseCallback) {
    /*let url = this.useHTTPS ? 'https://' : 'http://';
    url += this.address;*/

    return new Promise(async (resolve, reject) => {
      /*const request = {
        method: 'POST',
        mode: 'cors',
        headers: {
          'Content-Type': 'application/json'
        },
        body: JSON.stringify(message)
      };*/

      try {
        const response = await axios.post(this.address, message);
        if (!response.status === 200) {
          console.error(response);
          return reject(response);
        }
        onResponseCallback(response.data);
        return resolve(response.data);
      } catch (error) {
        console.error(error);
        return reject(error);
      }
    });
  }
}

ServiceClientHTTP.CONSTANTS = Object.freeze({
  MSG_FORMAT_BINARY: 'BINARY',
  MSG_FORMAT_JSON: 'JSON'
});

module.exports = ServiceClientHTTP;
