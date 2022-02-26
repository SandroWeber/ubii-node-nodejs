const UbiiClientNode = require('./nodes/ubiiClientNode');

const NetworkConnectionsManager = require('./networking/networkConnectionsManager');
const HTTPClient = require('./networking/httpClient');
const HTTPServer = require('./networking/httpServer');
const WebsocketServer = require('./networking/websocketServer');
const ZmqDealer = require('./networking/zmqDealer');
const ZmqReply = require('./networking/zmqReply');
const ZmqRequest = require('./networking/zmqRequest');
const ZmqRouter = require('./networking/zmqRouter');

const ExternalLibrariesService = require('./processing/externalLibrariesService');
const { ProcessingModule } = require('./processing/processingModule');
const ProcessingModuleManager = require('./processing/processingModuleManager');

const ProcessingModuleStorage = require('./storage/processingModuleStorage');

const Utils = require('./utilities');

module.exports = {
  UbiiClientNode,
  NetworkConnectionsManager,
  HTTPClient: HTTPClient,
  HTTPServer: HTTPServer,
  WebsocketServer,
  ZmqDealer,
  ZmqReply,
  ZmqRequest,
  ZmqRouter,
  ExternalLibrariesService,
  ProcessingModule,
  ProcessingModuleManager,
  ProcessingModuleStorage,
  Utils
};
