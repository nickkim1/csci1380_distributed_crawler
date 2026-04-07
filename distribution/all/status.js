// @ts-check
/**
 * @typedef {import("../types.js").Callback} Callback
 * @typedef {import("../types.js").Config} Config
 * @typedef {import("../util/id.js").Node} Node
 *
 * @typedef {Object} Status
 * @property {(configuration: string, callback: Callback) => void} get
 * @property {(configuration: Node, callback: Callback) => void} spawn
 * @property {(callback: Callback) => void} stop
 */
const distribution = globalThis.distribution;

/**
 * @param {Config} config
 * @returns {Status}
 */
function status(config) {
  const context = {};
  context.gid = config.gid || 'all';

  /**
   * @param {string} configuration
   * @param {Callback} callback
   */
  function get(configuration, callback) {
    const remote = {service: 'status', method: 'get'};
    distribution[context.gid].comm.send([configuration], remote, (e, v) => {
      if (Object.keys(e).length) {
        return callback(e, {});
      }

      switch (configuration) {
        case 'heapTotal':
          const heapTotal = Object.values(v).reduce((acc, val) => acc + val, 0);
          return callback({}, heapTotal);
        case 'nid':
          const nids = Object.values(v);
          return callback({}, nids);
        case 'sid':
          const sids = Object.values(v);
          return callback({}, sids);
        default:
          return callback({}, v);
      }
    });
  }

  /**
   * @param {Node} configuration
   * @param {Callback} callback
   */
  function spawn(configuration, callback) {
    distribution.local.status.spawn(configuration, (e, v) => {
      if (e) {
        return callback(e, null);
      }
      distribution.all.groups.add(context.gid, configuration, (e, v) => {
        if (e) {
          return callback(e, null);
        }
        return callback(null, configuration);
      });
    });
  }

  /**
   * @param {Callback} callback
   */
  function stop(callback) {
    const remote = {service: 'status', method: 'stop'};
    distribution[context.gid].comm.send([], remote, (e, v) => {
      if (Object.keys(e).length) {
        return callback(e, {});
      }
      return callback(null, null);
    });
  }

  return {get, stop, spawn};
}

module.exports = status;
