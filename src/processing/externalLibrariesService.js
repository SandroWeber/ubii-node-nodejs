let _instance = null;
const SINGLETON_ENFORCER = Symbol();

class ExternalLibrariesService {
  constructor(enforcer) {
    if (enforcer !== SINGLETON_ENFORCER) {
      throw new Error('Use ' + this.constructor.name + '.instance');
    }
    this.libraries = {};
  }

  static get instance() {
    if (_instance == null) {
      _instance = new ExternalLibrariesService(SINGLETON_ENFORCER);
    }

    return _instance;
  }

  addExternalLibrary(name, library) {
    if (this.libraries.hasOwnProperty(name)) {
      console.error(
        'InteractionModulesService.addModule() - module named "' + name + '" already exists'
      );
      return;
    }

    this.libraries[name] = library;
  }

  getExternalLibraries() {
    return this.libraries;
  }
}

module.exports = ExternalLibrariesService;
