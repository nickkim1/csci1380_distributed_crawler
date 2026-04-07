// @ts-check
/**
 * @typedef {import("../types.js").Callback} Callback
 * @typedef {import("../types.js").Config} Config
 * @typedef {import("../types.js").Hasher} Hasher
 * @typedef {import("../types.js").Node} Node
 */
const distribution = globalThis.distribution;
const id = distribution.util.id;


/**
 * @typedef {Object} StoreConfig
 * @property {string | null} key
 * @property {string} gid
 *
 * @typedef {StoreConfig | string | null} SimpleConfig
 */


/**
 * @param {Config} config
 */
function store(config) {
  const context = {
    gid: config.gid || 'all',
    hash: config.hash || globalThis.distribution.util.id.naiveHash,
    subset: config.subset,
  };

  function buildConfig(cnfg, state) {
    cnfg = cnfg || id.getID(state);
    cnfg = typeof cnfg === 'string' ? {key: cnfg, gid: context.gid} : cnfg;
    if (!cnfg.gid) {
      cnfg.gid = context.gid;
    }
    return cnfg;
  }

  function getNode(key, group) {
    const kid = id.getID(key);
    const nids = Object.keys(group);
    const nid = context.hash(kid, nids);
    const node = group[nid];
    return node;
  }

  /**
   * @param {SimpleConfig} configuration
   * @param {Callback} callback
   */
  function get(configuration, callback) {
    if (!callback) {
      callback = (e, v) => console.log('no callback provided');
    }
    if (!configuration) {
      return callback(Error('no config provided'));
    }
    distribution.local.groups.get(context.gid, (e, v) => {
      if (e) {
        return callback(e, null);
      }
      configuration = buildConfig(configuration);
      const node = getNode(configuration.key, v);
      const remote = {node: node, service: 'store', method: 'get'};
      const message = [configuration];
      distribution.local.comm.send(message, remote, (e, v) => {
        if (e) {
          return callback(e, null);
        }
        return callback(null, v);
      });
    });
  }

  /**
   * @param {any} state
   * @param {SimpleConfig} configuration
   * @param {Callback} callback
   */
  function put(state, configuration, callback) {
    distribution.local.groups.get(context.gid, (e, v) => {
      if (e) {
        return callback(e, null);
      }
      configuration = buildConfig(configuration, state);
      const node = getNode(configuration.key, v);
      const remote = {node: node, service: 'store', method: 'put'};
      const message = [state, configuration];
      distribution.local.comm.send(message, remote, (e, v) => {
        if (e) {
          return callback(e, null);
        }
        return callback(null, v);
      });
    });
  }

  /**
   * @param {any} state
   * @param {SimpleConfig} configuration
   * @param {Callback} callback
   */
  function append(state, configuration, callback) {
    if (!callback) {
      callback = (e, v) => console.log('no callback provided');
    }
    distribution.local.groups.get(context.gid, (e, v) => {
      if (e) {
        return callback(e, null);
      }
      configuration = buildConfig(configuration, state);
      const node = getNode(configuration.key, v);
      const remote = {node: node, service: 'store', method: 'append'};
      const message = [state, configuration];
      distribution.local.comm.send(message, remote, (e, v) => {
        if (e) {
          return callback(e, null);
        }
        return callback(null, v);
      });
    });
  }

  /**
   * @param {SimpleConfig} configuration
   * @param {Callback} callback
   */
  function del(configuration, callback) {
    if (!callback) {
      callback = (e, v) => console.log('no callback provided');
    }
    if (!configuration) {
      return callback(Error('no config provided'));
    }
    distribution.local.groups.get(context.gid, (e, v) => {
      if (e) {
        return callback(e, null);
      }
      configuration = buildConfig(configuration);
      const node = getNode(configuration.key, v);
      const remote = {node: node, service: 'store', method: 'del'};
      const message = [configuration];
      distribution.local.comm.send(message, remote, (e, v) => {
        if (e) {
          return callback(e, null);
        }
        return callback(null, v);
      });
    });
  }

  /**
   * @param {Object.<string, Node>} configuration
   * @param {Callback} callback
   */
  function reconf(configuration, callback) {
    return callback(new Error('store.reconf not implemented'));
  }

  /* For the distributed store service, the configuration will
          always be a string */
  return {get, put, append, del, reconf};
}

module.exports = store;
