// @ts-check
/**
 * @typedef {import("../types.js").Callback} Callback
 * @typedef {import("../types.js").Config} Config
 * @typedef {import("../types.js").Node} Node
 */


/**
 * @typedef {Object} StoreConfig
 * @property {string | null} key
 * @property {string} gid
 *
 * @typedef {StoreConfig | string | null} SimpleConfig
 *
 * @typedef {Object} Mem
 * @property {(configuration: SimpleConfig, callback: Callback) => void} get
 * @property {(state: any, configuration: SimpleConfig, callback: Callback) => void} put
 * @property {(state: any, configuration: SimpleConfig, callback: Callback) => void} append
 * @property {(configuration: SimpleConfig, callback: Callback) => void} del
 * @property {(configuration: Object.<string, Node>, callback: Callback) => void} reconf
 */
const distribution = globalThis.distribution;
const id = distribution.util.id;

/**
 * @param {Config} config
 * @returns {Mem}
 */
function mem(config) {
  const context = {};
  context.gid = config.gid || 'all';
  context.hash = config.hash || globalThis.distribution.util.id.naiveHash;

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
      const remote = {node: node, service: 'mem', method: 'get'};
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
    if (!callback) {
      callback = (e, v) => console.log('no callback provided');
    }
    distribution.local.groups.get(context.gid, (e, v) => {
      if (e) {
        return callback(e, null);
      }
      configuration = buildConfig(configuration, state);
      const node = getNode(configuration.key, v);
      const remote = {node: node, service: 'mem', method: 'put'};
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
      const remote = {node: node, service: 'mem', method: 'append'};
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
      const remote = {node: node, service: 'mem', method: 'del'};
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
    return callback(new Error('mem.reconf not implemented'));
  }
  /* For the distributed mem service, the configuration will
          always be a string */
  return {
    get,
    put,
    append,
    del,
    reconf,
  };
}

module.exports = mem;
