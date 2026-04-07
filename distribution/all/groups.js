// @ts-check
/**
 * @typedef {import("../types.js").Callback} Callback
 * @typedef {import("../types.js").Config} Config
 * @typedef {import("../util/id.js").Node} Node
 *
 * @typedef {Object} Groups
 * @property {(config: Config | string, group: Object.<string, Node>, callback: Callback) => void} put
 * @property {(name: string, callback: Callback) => void} del
 * @property {(name: string, callback: Callback) => void} get
 * @property {(name: string, node: Node, callback: Callback) => void} add
 * @property {(name: string, node: string, callback: Callback) => void} rem
 */
const distribution = globalThis.distribution;

/**
 * @param {Config} config
 * @returns {Groups}
 */
function groups(config) {
  const context = {gid: config.gid || 'all'};

  /**
   * @param {Config | string} config
   * @param {Object.<string, Node>} group
   * @param {Callback} callback
   */
  function put(config, group, callback) {
    const remote = {service: 'groups', method: 'put'};
    distribution[context.gid].comm.send([config, group], remote, (e, v) => {
      if (Object.keys(e).length) {
        return callback(e, {});
      }
      return callback({}, v);
    });
  }

  /**
   * @param {string} name
   * @param {Callback} callback
   */
  function del(name, callback) {
    const remote = {service: 'groups', method: 'del'};
    distribution[context.gid].comm.send([name], remote, (e, v) => {
      if (Object.keys(e).length) {
        return callback(e, {});
      }
      return callback({}, v);
    });
  }

  /**
   * @param {string} name
   * @param {Callback} callback
   */
  function get(name, callback) {
    const remote = {service: 'groups', method: 'get'};
    distribution[context.gid].comm.send([name], remote, (e, v) => {
      if (Object.keys(e).length) {
        return callback(e, {});
      }
      return callback({}, v);
    });
  }

  /**
   * @param {string} name
   * @param {Node} node
   * @param {Callback} callback
   */
  function add(name, node, callback) {
    const remote = {service: 'groups', method: 'add'};
    distribution[context.gid].comm.send([name, node], remote, (e, v) => {
      if (Object.keys(e).length) {
        return callback(e, {});
      }
      return callback({}, v);
    });
  }

  /**
   * @param {string} name
   * @param {string} node
   * @param {Callback} callback
   */
  function rem(name, node, callback) {
    const remote = {service: 'groups', method: 'rem'};
    distribution[context.gid].comm.send([name, node], remote, (e, v) => {
      if (Object.keys(e).length) {
        return callback(e, {});
      }
      return callback({}, v);
    });
  }

  return {
    put, del, get, add, rem,
  };
}

module.exports = groups;
