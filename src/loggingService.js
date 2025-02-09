const winston = require('winston');
const { combine, timestamp, colorize, printf } = winston.format;

let _instance = null;
const SINGLETON_ENFORCER = Symbol();

const LOG_TAG = 'LoggingService';

class LoggingService {
  constructor(enforcer) {
    if (enforcer !== SINGLETON_ENFORCER) {
      throw new Error('Use ' + this.constructor.name + '.instance');
    }

    this.logger = winston.createLogger({
      transports: [
        new winston.transports.Console({
          timestamp: function () {
            return Date.now();
          },
          format: combine(
            winston.format((log) => ({ ...log, level: log.level.toUpperCase() }))(),
            colorize(),
            timestamp({
              format: 'YYYY-MM-DD HH:mm:ss.SSS'
            }),
            winston.format.errors({ stack: true }),
            printf((log) => `[${log.timestamp}] ${log.level} ${log.label} ${log.message}`)
          )
        })
      ]
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
