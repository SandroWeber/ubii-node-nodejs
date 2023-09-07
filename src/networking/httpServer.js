const express = require('express');
const http = require('http');
const https = require('https');
const bodyParser = require('body-parser');
const fs = require('fs');

const NetworkConfigManager = require('./networkConfigManager');

const ConfigService = require('../config/configService');

class HTTPServer {
  /**
   * Communication endpoint implementing the zmq reply pattern.
   * @param {*} port Port to bind.
   * @param {*} autoBind Should the socket bind directly after the initialization of the object?
   * If not, the start method must be called manually.
   */
  constructor(port = 5555, autoBind = true) {
    this.port = port;

    this.allowedOrigins = ConfigService.instance.getAllowedOrigins();
    this.allowedOrigins = this.allowedOrigins.map((string) => new RegExp(string));

    this.allowedHosts = ConfigService.instance.getAllowedHosts();
    this.allowedHosts = this.allowedHosts.map((string) => new RegExp(string));

    this.endpointServices = [];

    this.ready = false;

    if (autoBind) {
      this.start();
    }
  }

  start() {
    // init
    this.app = express();

    if (ConfigService.instance.useHTTPS()) {
      var credentials = {
        //ca: [fs.readFileSync(PATH_TO_BUNDLE_CERT_1), fs.readFileSync(PATH_TO_BUNDLE_CERT_2)],
        key: fs.readFileSync(ConfigService.instance.getPathPrivateKey())
      };
      let certificatePath = ConfigService.instance.getPathCertificate();
      if (certificatePath) {
        credentials.cert = fs.readFileSync(ConfigService.instance.getPathCertificate());
      }

      this.server = https.createServer(credentials, this.app);
      this.endpoint = 'https://*:' + this.port;
    } else {
      this.server = http.createServer(this.app);
      this.endpoint = 'http://*:' + this.port;
    }

    // CORS
    this.app.use((req, res, next) => {
      let allowed = false;
      if (!req.headers.origin && !req.headers.host) {
        console.error('Request missing origin and host, ignoring it.');
        return;
      }

      if (req.headers.origin) {
        allowed = this.allowedOrigins.some((originRegex) => originRegex.test(req.headers.origin));
      } else if (req.headers.host) {
        allowed = this.allowedHosts.some((hostRegex) => hostRegex.test(req.headers.host));
      }

      if (allowed) {
        res.header('Access-Control-Allow-Origin', req.headers.origin);
        res.header('Access-Control-Allow-Headers', 'Origin, X-Requested-With, Content-Type, Accept');
      } else {
        console.warn('Request from "' + req.headers.origin + '" is not in the list of allowed origins');
      }

      next();
    });

    this.app.use(bodyParser.urlencoded({ extended: true }));

    // VARIANT A: PROTOBUF BINARY
    this.app.use(bodyParser.raw({
      type: 'application/octet-stream',
      limit: '10mb'
    }));
    /// VARIANT B: PROTOBUF JSON
    this.app.use(bodyParser.json());

    this.server.listen(this.port, () => {
      this.open = true;
    });
  }

  stop() {
    this.ready = false;
    this.server.close();
  }

  /**
   * Set the message handling function to be called upon receiving a message. Also marks the this socket as ready to receive.
   * @param {*} route The route suffix after the endpoint where messages can be POSTed.
   * @param {*} callback Callback function that is called when a new message is received.
   * Callback should accept a request parameter and a response paramter.
   */
  setServiceRoute(route, callback) {
    this.setRoutePOST(route, callback);
    this.endpointServices.push(this.endpoint + route);
    this.ready = true;
  }

  setRoutePOST(route, callback) {
    this.app.post(route, callback);
  }

  toString() {
    let status = this.ready ? 'ready' : 'not ready';

    return 'HTTP(S)-Service | ' + status + ' | POST at ' + this.endpointServices.toString();
  }
}

module.exports = HTTPServer;
