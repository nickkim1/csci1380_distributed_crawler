/**
 * @typedef {import("../types").Callback} Callback
 * @typedef {string} ServiceName
 */
const distribution = globalThis.distribution;
const routesStore = new Map();

/**
 * @param {ServiceName | {service: ServiceName, gid?: string}} configuration
 * @param {Callback} callback
 * @returns {void}
 */
function get(configuration, callback) {
  if (!callback) {
    callback = (e, v) => console.log('no callback provided');
  }
  if (!configuration) {
    return callback(Error('no configuration provided'));
  }

  if (typeof configuration === 'string') {
    configuration = {service: configuration};
  }

  const serviceName = configuration.service;
  const gid = configuration.gid;

  if (gid && gid !== 'local') {
    const group = distribution[gid];
    if (!group || !(serviceName in group)) {
      return callback(Error(`service ${serviceName} not found`), null);
    }
    return callback(null, group[serviceName]);
  }

  if (routesStore.has(serviceName)) {
    return callback(null, routesStore.get(serviceName));
  }

  const rpc = globalThis.toLocal?.get(serviceName);
  if (rpc) {
    return callback(null, {call: rpc});
  }

  return callback(Error(`service ${serviceName} not found`), null);
}

/**
 * @param {object} service
 * @param {string} configuration
 * @param {Callback} callback
 * @returns {void}
 */
function put(service, configuration, callback) {
  if (!callback) {
    callback = (e, v) => console.log('no callback provided');
  }
  routesStore.set(configuration, service);
  return callback(null, configuration);
}

/**
 * @param {string} configuration
 * @param {Callback} callback
 */
function rem(configuration, callback) {
  if (!callback) {
    callback = (e, v) => console.log('no callback provided');
  }
  const ret = routesStore.get(configuration);
  routesStore.delete(configuration);
  return callback(null, ret);
}

module.exports = {get, put, rem};
