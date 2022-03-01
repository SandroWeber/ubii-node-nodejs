const { UbiiClientNode } = require('../src/index');
const config = require('./config.json');

(async function () {
  let ubiiNode = new UbiiClientNode('test-node-nodejs', config.masterNode.services, config.masterNode.topicdata);
  await ubiiNode.initialize();
})();
