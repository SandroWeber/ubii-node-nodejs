const namida = require('@tum-far/namida/src/namida');
const { DEFAULT_TOPICS } = require('@tum-far/ubii-msg-formats/dist/js/constants');
const NetworkService = require('./networking/networkService');

class UbiiNode {
  constructor(name, masterNodeIP, masterNodeServicePort) {
    this.name = name;
    this.masterNodeIP = masterNodeIP;
    this.masterNodeServicePort = masterNodeServicePort;

    this.networkService = new NetworkService(name, masterNodeIP, masterNodeServicePort);
    this.networkService.connect();

    this.initialize();
  }

  async initialize() {
    // get server config
    let replyServerSpec = await this.networkService.callService({
      topic: DEFAULT_TOPICS.SERVICES.SERVER_CONFIG
    });
    if (replyServerSpec.server) {
      this.serverSpecification = replyServerSpec.server;
    } else {
      namida.logFailure('UbiiNode.initialize()', 'server config request failed');
      return;
    }

    // register as client node
    let replyClientRegistration = await this.networkService.callService({
      topic: DEFAULT_TOPICS.SERVICES.CLIENT_REGISTRATION,
      client: {
        name: this.name
      }
    });
    if (replyClientRegistration.client) {
      this.clientSpecification = replyClientRegistration.client;
    } else {
      namida.logFailure('UbiiNode.initialize()', 'client registration failed');
      return;
    }
  }
}

module.exports = UbiiNode;
