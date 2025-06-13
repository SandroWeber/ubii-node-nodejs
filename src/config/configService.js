const fs = require('fs');
const path = require('path');

const LoggingService = require('../loggingService');

let _instance = null;
const SINGLETON_ENFORCER = Symbol();

const {
  DEFAULT_PORT_SERVICE_TCP,
  DEFAULT_PORT_TOPICDATA_TCP,
  DEFAULT_PORT_SERVICE_HTTP,
  DEFAULT_PORT_TOPICDATA_WS,
  DEFAULT_USE_HTTPS
} = require('../networking/constants');

const LOG_TAG = '[UBII ConfigService] ';
const logger = LoggingService.instance.logger;

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
      logger.warn({
        label: LOG_TAG,
        message:
          'config.json is missing a path for SSL certificate files (config.https.pathCert) - check config.json(.template)!'
      });
    }
  }

  getPathPrivateKey() {
    if (this.config.https && this.config.https.pathPrivateKey) {
      return this.getFullFilePath(this.config.https.pathPrivateKey);
    } else {
      logger.warn({
        label: LOG_TAG,
        message:
          'config.json is missing a path for SSL private key (config.https.pathPrivateKey) - check config.json(.template)!'
      });
    }
  }

  getPathPublicKey() {
    if (this.config.https && this.config.https.pathPublicKey) {
      return this.getFullFilePath(this.config.https.pathPublicKey);
    } else {
      logger.warn({
        label: LOG_TAG,
        message:
          'config.json is missing a path for SSL public key (config.https.pathPublicKey) - check config.json(.template)!'
      });
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
      logger.warn({
        label: LOG_TAG,
        message:
          'config.json is missing settting for allowed origins (config.https.allowedOrigins) - check config.json(.template)!'
      });
    }
  }

  getAllowedHosts() {
    if (this.config.allowedHosts) {
      return this.config.allowedHosts;
    } else {
      logger.warn({
        label: LOG_TAG,
        message:
          'config.json is missing settting for allowed origins (config.allowedHosts) - check config.json(.template)!'
      });
    }
  }

  getPortServiceTCP() {
    return typeof this.config.ports.serviceTCP !== 'undefined'
      ? this.config.ports.serviceTCP
      : DEFAULT_PORT_SERVICE_TCP;
  }

  getPortServiceREST() {
    return typeof this.config.ports.serviceREST !== 'undefined'
      ? this.config.ports.serviceREST
      : DEFAULT_PORT_SERVICE_HTTP;
  }

  getPortTopicdataTCP() {
    return typeof this.config.ports.topicdataTCP !== 'undefined'
      ? this.config.ports.topicdataTCP
      : DEFAULT_PORT_TOPICDATA_TCP;
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
