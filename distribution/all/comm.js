// @ts-check
/**
 * @typedef {import("../types.js").Callback} Callback
 * @typedef {import("../types.js").Config} Config
 */
const distribution = globalThis.distribution;

/**
 * NOTE: This Target is slightly different from local.all.Target
 * @typedef {Object} Target
 * @property {string} service
 * @property {string} method
 * @property {string} [gid]
 *
 * @typedef {Object} Comm
 * @property {(message: any[], configuration: Target, callback: Callback) => void} send
 */

/**
 * @param {Config} config
 * @returns {Comm}
 */
function comm(config) {
  const context = {};
  context.gid = config.gid || 'all';

  /**
   * @param {any[]} message
   * @param {Target} configuration
   * @param {Callback} callback
   */
  function send(message, configuration, callback) {
    const gid = configuration.gid || context.gid;
    distribution.local.groups.get(gid, (e, v) => {
      if (e) {
        return callback(e, null);
      }

      const nodes = Object.entries(v);
      let count = nodes.length;
      const errorMap = {};
      const valueMap = {};

      if (count === 0) {
        return callback(Error('empty group'), null);
      }

      nodes.forEach(([sid, node]) => {
        const nodeConfig = {...configuration, node: node};
        distribution.local.comm.send(message, nodeConfig, (e, v) => {
          if (e) {
            errorMap[sid] = e;
          } else {
            valueMap[sid] = v;
          }

          count--;

          if (count == 0) {
            return callback(errorMap, valueMap);
          }
        });
      });
    });
  }

  return {send};
}

module.exports = comm;
