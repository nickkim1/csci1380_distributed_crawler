// @ts-check
/**
 * @typedef {import("../types.js").Callback} Callback
 *
 * @typedef {Object} StoreConfig
 * @property {?string} key
 * @property {?string} gid
 *
 * @typedef {StoreConfig | string | null} SimpleConfig
 */

/* Notes/Tips:

- Use absolute paths to make sure they are agnostic to where your code is running from!
  Use the `path` module for that.
*/
const distribution = globalThis.distribution;
const util = distribution.util;

const fs = require('fs');
const path = require('path');

function buildFile(config, state) {
  config = config || util.id.getID(state);
  config = typeof config === 'string' ? {key: config, gid: 'local'} : config;
  const key = (config.key + config.gid).replace(/[^a-zA-Z0-9]/g, '');
  const file = path.join(__dirname, '../../store/', key);
  return file;
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
  const file = buildFile(configuration, state);
  const data = util.serialize(state);
  fs.writeFile(file, data, (err) => {
    if (err) {
      return callback(Error(`write error: ${err}`), null);
    }
    return callback(null, state);
  });
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
  const file = buildFile(configuration);
  fs.readFile(file, 'utf8', (err, data) => {
    if (err) {
      return callback(Error(`read error: ${err}`), null);
    }
    const obj = util.deserialize(data);
    return callback(null, obj);
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
  const file = buildFile(configuration);
  fs.readFile(file, 'utf8', (err, data) => {
    if (err) {
      return callback(Error(`read error: ${err}`), null);
    }
    const obj = util.deserialize(data);
    fs.unlink(file, (err) => {
      if (err) {
        return callback(Error(`unlink error: ${err}`), null);
      }
      return callback(null, obj);
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
  const file = buildFile(configuration, state);
  try {
    let current = [];

    if (fs.existsSync(file)) {
      const raw = fs.readFileSync(file, 'utf8');
      const existing = util.deserialize(raw);
      current = Array.isArray(existing) ? existing : [existing];
    }

    current.push(state);
    fs.writeFileSync(file, util.serialize(current), 'utf8');
    return callback(null, state);
  } catch (err) {
    return callback(Error(`append error: ${err}`), null);
  }
}

module.exports = {put, get, del, append};
