const os = require('os');

let _instance = null;
const SINGLETON_ENFORCER = Symbol();

class NetworkConfigService {
  constructor(enforcer) {
    if (enforcer !== SINGLETON_ENFORCER) {
      throw new Error('Use ' + this.constructor.name + '.instance');
    }
    
    this.getIPConfig();
  }

  static get instance() {
    if (_instance == null) {
      _instance = new ConfigService(SINGLETON_ENFORCER);
    }

    return _instance;
  }

  getIPConfig() {
    this.hostAdresses = {
      ethernet: '',
      wifi: ''
    };

    let ifaces = os.networkInterfaces();
    Object.keys(ifaces).forEach(ifname => {
      ifaces[ifname].forEach(iface => {
        if (iface.family === 'IPv4' && !iface.internal && ifname.indexOf('Default Switch') === -1) {
          if (
            ifname.indexOf('en') === 0 ||
            ifname.indexOf('vEthernet') === 0 ||
            ifname.indexOf('Ethernet') === 0
          ) {
            this.hostAdresses.ethernet = iface.address;
          } else if (ifname.indexOf('wl') === 0 || ifname.indexOf('Wi-Fi') === 0) {
            this.hostAdresses.wifi = iface.address;
          }
        }
      });
    });
  }
}

module.exports = NetworkConfigService;
