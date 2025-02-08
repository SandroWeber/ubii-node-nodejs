const winston = require('winston');
const { combine, timestamp, label, prettyPrint } = winston.format;

let _instance = null;
const SINGLETON_ENFORCER = Symbol();

const LOG_TAG = 'LoggingService';

class LoggingService {
  constructor(enforcer) {
    if (enforcer !== SINGLETON_ENFORCER) {
      throw new Error('Use ' + this.constructor.name + '.instance');
    }

    this.logger = winston.createLogger({
      /*level: 'info',
      defaultMeta: { service: 'user-service' },*/
      format: combine(timestamp(), prettyPrint()),
      transports: [new winston.transports.Console()]
    });
  }

  static get instance() {
    if (_instance == null) {
      _instance = new LoggingService(SINGLETON_ENFORCER);
    }

    return _instance;
  }
}

module.exports = LoggingService;
