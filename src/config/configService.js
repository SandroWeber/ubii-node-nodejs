const fs = require('fs');
const path = require('path');

let _instance = null;
const SINGLETON_ENFORCER = Symbol();

const {
  DEFAULT_PORT_SERVICE_ZMQ,
  DEFAULT_PORT_SERVICE_REST,
  DEFAULT_PORT_TOPICDATA_ZMQ,
  DEFAULT_PORT_TOPICDATA_WS,
  DEFAULT_USE_HTTPS
} = require('../networking/constants');

class ConfigService {
  constructor(enforcer) {
    if (enforcer !== SINGLETON_ENFORCER) {
      throw new Error('Use ' + this.constructor.name + '.instance');
    }

    let appRoot = this.getRootPath();
    let pathConfig = path.join(appRoot, 'config.json').normalize();

    if (fs.existsSync(pathConfig)) {
      this.config = JSON.parse(fs.readFileSync(pathConfig));
    } else {
      console.error('config.json not found! Expected to be at "' + pathConfig + '".');
    }
  }

  static get instance() {
    if (_instance == null) {
      _instance = new ConfigService(SINGLETON_ENFORCER);
    }

    return _instance;
  }

  useHTTPS() {
    return typeof this.config.https.enabled !== 'undefined' ? this.config.https.enabled : DEFAULT_USE_HTTPS;
  }

  getPathCertificate() {
    if (this.config.https && this.config.https.pathCert) {
      return this.getFullFilePath(this.config.https.pathCert);
    }
  }

  getPathPrivateKey() {
    if (this.config.https && this.config.https.pathPrivateKey) {
      return this.getFullFilePath(this.config.https.pathPrivateKey);
    }
  }

  getPathPublicKey() {
    if (this.config.https && this.config.https.pathPublicKey) {
      return this.getFullFilePath(this.config.https.pathPublicKey);
    }
  }

  getFullFilePath(pathRelativeOrAbsolute) {
    if (path.isAbsolute(pathRelativeOrAbsolute)) {
      return path.join(pathRelativeOrAbsolute, '');
    } else {
      return path.join(this.getRootPath(), pathRelativeOrAbsolute);
    }
  }

  getAllowedOrigins() {
    return this.config.https.allowedOrigins;
  }

  getAllowedHosts() {
    return this.config.allowedHosts;
  }

  getPortServiceZMQ() {
    return typeof this.config.ports.serviceZMQ !== 'undefined'
      ? this.config.ports.serviceZMQ
      : DEFAULT_PORT_SERVICE_ZMQ;
  }

  getPortServiceREST() {
    return typeof this.config.ports.serviceREST !== 'undefined'
      ? this.config.ports.serviceREST
      : DEFAULT_PORT_SERVICE_REST;
  }

  getPortTopicdataZMQ() {
    return typeof this.config.ports.topicdataZMQ !== 'undefined'
      ? this.config.ports.topicdataZMQ
      : DEFAULT_PORT_TOPICDATA_ZMQ;
  }

  getPortTopicdataWS() {
    return typeof this.config.ports.topicdataWS !== 'undefined'
      ? this.config.ports.topicdataWS
      : DEFAULT_PORT_TOPICDATA_WS;
  }

  getRootPath() {
    let appRoot = __dirname;
    if (appRoot.includes('node_modules')) {
      appRoot = appRoot.substring(0, appRoot.search('node_modules*'));
    }
    if (appRoot.includes('scripts')) {
      appRoot = appRoot.substring(0, appRoot.search('scripts*'));
    }

    return appRoot;
  }
}

module.exports = ConfigService;
