// @ts-check
/**
 * @typedef {import("../types.js").Callback} Callback
 *
 * @typedef {Object} StoreConfig
 * @property {string | null} key
 * @property {string | null} gid
 *
 * @typedef {StoreConfig | string | null} SimpleConfig
 */
const distribution = globalThis.distribution;
const id = distribution.util.id;

const kvstore = {};

function buildKey(config, state) {
  config = config || id.getID(state);
  config = typeof config === 'string' ? {key: config, gid: 'local'} : config;
  if (!config.gid) {
    config.gid = 'local';
  }
  return [config.key, config.gid];
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
  let [key, config] = buildKey(configuration);
  key = key + config;
  kvstore[key] = state;
  return callback(null, state);
};

/**
 * @param {any} state
 * @param {SimpleConfig} configuration
 * @param {Callback} callback
 */
function append(state, configuration, callback) {
  if (!callback) {
    callback = (e, v) => console.log('no callback provided');
  }
  const [rawKey, config] = buildKey(configuration);
  const key = rawKey + config;
  const existing = kvstore[key];
  const arr = Array.isArray(existing) ? existing : (existing ? [existing] : []);
  kvstore[key] = arr.concat(state);
  if (key !== config) { // this is for when we retrieve the metadata in case that matters for some reason
    const metaArr = kvstore[config] || [];
    metaArr.push(rawKey);
    kvstore[config] = metaArr;
  }
  callback(null, state);
};

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
  let [key, config] = buildKey(configuration);
  key = key + config;
  if (key in kvstore) {
    return callback(null, kvstore[key]);
  }
  return callback(Error(`key ${key} not found`), null);
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
  let [key, config] = buildKey(configuration);
  key = key + config;
  if (key in kvstore) {
    const val = kvstore[key];
    delete kvstore[key];
    return callback(null, val);
  }
  return callback(Error(`key ${key} not found`), null);
};

module.exports = {put, get, del, append};
