// @ts-check
/**
 * @typedef {import("../types.js").Callback} Callback
 * @typedef {import("../types.js").Config} Config
 *
 * @typedef {Object} Routes
 * @property {(service: object, name: string, callback: Callback) => void} put
 * @property {(configuration: string, callback: Callback) => void} rem
 */
const distribution = globalThis.distribution;

/**
 * @param {Config} config
 * @returns {Routes}
 */
function routes(config) {
  const context = {};
  context.gid = config.gid || 'all';

  /**
   * @param {object} service
   * @param {string} name
   * @param {Callback} callback
   */
  function put(service, name, callback) {
    const remote = {service: 'routes', method: 'put'};
    distribution[context.gid].comm.send([service, name], remote, (e, v) => {
      if (Object.keys(e).length) {
        return callback(e, {});
      }
      return callback({}, v);
    });
  }

  /**
   * @param {string} configuration
   * @param {Callback} callback
   */
  function rem(configuration, callback) {
    const remote = {service: 'routes', method: 'rem'};
    distribution[context.gid].comm.send([configuration], remote, (e, v) => {
      if (Object.keys(e).length) {
        return callback(e, {});
      }
      return callback({}, v);
    });
  }

  return {put, rem};
}

module.exports = routes;
