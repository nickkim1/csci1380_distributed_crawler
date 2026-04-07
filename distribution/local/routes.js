/**
 * @typedef {import("../types").Callback} Callback
 * @typedef {string} ServiceName
 */
const distribution = globalThis.distribution;

/**
 * @param {ServiceName | {service: ServiceName, gid?: string}} configuration
 * @param {Callback} callback
 * @returns {void}
 */
function get(configuration, callback) {
  if (!callback) {
    callback = (e, v) => console.log('no callback provided');
  }
  let group = distribution.local;
  if (typeof configuration !== 'string') {
    if ('gid' in configuration) {
      group = distribution[configuration.gid];
    }
    configuration = configuration.service;
  }
  let ret = null;
  let err = null;
  if (configuration in group) {
    ret = group[configuration];
  } else {
    err = Error(`service ${configuration} not found`);
  }
  return callback(err, ret);
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
  const local = distribution.local;
  local[configuration] = service;
  return callback(null, null);
}

/**
 * @param {string} configuration
 * @param {Callback} callback
 */
function rem(configuration, callback) {
  if (!callback) {
    callback = (e, v) => console.log('no callback provided');
  }
  const local = distribution.local;
  let ret = null;
  let err = null;
  if (configuration in local) {
    ret = local[configuration];
    delete local[configuration];
  } else {
    err = Error(`service ${configuration} not found`);
  }
  return callback(err, ret);
}

module.exports = {get, put, rem};
