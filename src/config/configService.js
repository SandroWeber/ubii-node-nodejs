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
const namida = require('@tum-far/namida');

const LOG_TAG = 'ConfigService';

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
    } else {
      namida.warn(
        LOG_TAG,
        'config.json is missing a path for SSL certificate files (config.https.pathCert) - check config.json(.template)!'
      );
    }
  }

  getPathPrivateKey() {
    if (this.config.https && this.config.https.pathPrivateKey) {
      return this.getFullFilePath(this.config.https.pathPrivateKey);
    } else {
      namida.warn(
        LOG_TAG,
        'config.json is missing a path for SSL private key (config.https.pathPrivateKey) - check config.json(.template)!'
      );
    }
  }

  getPathPublicKey() {
    if (this.config.https && this.config.https.pathPublicKey) {
      return this.getFullFilePath(this.config.https.pathPublicKey);
    } else {
      namida.warn(
        LOG_TAG,
        'config.json is missing a path for SSL public key (config.https.pathPublicKey) - check config.json(.template)!'
      );
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
    if (this.config.https && this.config.https.allowedOrigins) {
      return this.config.https.allowedOrigins;
    } else {
      namida.warn(
        LOG_TAG,
        'config.json is missing settting for allowed origins (config.https.allowedOrigins) - check config.json(.template)!'
      );
    }
  }

  getAllowedHosts() {
    if (this.config.allowedHosts) {
      return this.config.allowedHosts;
    } else {
      namida.warn(
        LOG_TAG,
        'config.json is missing settting for allowed origins (config.allowedHosts) - check config.json(.template)!'
      );
    }
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
