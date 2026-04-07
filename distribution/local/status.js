// @ts-check
/**
 * @typedef {import("../types.js").Callback} Callback
 * @typedef {import("../types.js").Node} Node
 */
const distribution = globalThis.distribution;
const config = distribution.node.config;
const id = distribution.util.id;

/**
 * @param {string} configuration
 * @param {Callback} callback
 */
function get(configuration, callback) {
  if (!callback) {
    callback = (e, v) => console.log('no callback provided');
  }
  let ret = null;
  let err = null;
  switch (configuration) {
    case 'nid':
      ret = id.getNID(config);
      break;
    case 'sid':
      ret = id.getSID(config);
      break;
    case 'ip':
      ret = config.ip;
      break;
    case 'port':
      ret = config.port;
      break;
    case 'counts':
      ret = distribution.node.counts || 0;
      break;
    case 'heapTotal':
      ret = process.memoryUsage().heapTotal;
      break;
    case 'heapUsed':
      ret = process.memoryUsage().heapUsed;
      break;
    default:
      err = Error(`${configuration} not a valid configuration`);
  }
  return callback(err, ret);
};


/**
 * @param {Node} configuration
 * @param {Callback} callback
 */
function spawn(configuration, callback) {
  callback(new Error('status.spawn not implemented'));
}

/**
 * @param {Callback} callback
 */
function stop(callback) {
  callback(new Error('status.stop not implemented'));
}

module.exports = {get, spawn, stop};
