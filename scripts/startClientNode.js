const UbiiClientNode = require('../src/nodes/ubiiClientNode');

(async function () {
  let ubiiNode = new UbiiClientNode('test-node-nodejs', 'localhost', '8101');
  await ubiiNode.initialize();
})();
