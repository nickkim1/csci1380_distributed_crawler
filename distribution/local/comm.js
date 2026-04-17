// @ts-check
/**
 * @typedef {import("../types.js").Callback} Callback
 * @typedef {import("../types.js").Node} Node
 */

const http = require('node:http');
const distribution = globalThis.distribution;
const util = distribution.util;

/**
 * @typedef {Object} Target
 * @property {string} service
 * @property {string} method
 * @property {Node} node
 * @property {string} [gid]
 */

/**
 * @param {Array<any>} message
 * @param {Target} remote
 * @param {(error: Error, value?: any) => void} callback
 * @returns {void}
 */
function send(message, remote, callback) {
  if (!callback) {
    callback = (e, v) => console.log('no callback provided');
  }

  const node = remote.node;
  if (!node.ip) {
    return callback(Error('node missing ip'));
  } else if (!node.port) {
    return callback(Error('node missing port'));
  } else if (!(message instanceof Array)) {
    return callback(Error('message not array'));
  }

  const payload = util.serialize(message);
  const gid = remote.gid || 'local';

  const options = {
    hostname: node.ip,
    port: node.port,
    path: `/${gid}/${remote.service}/${remote.method}`,
    method: 'PUT',
    headers: {
      'Content-Type': 'application/json',
      'Content-Length': Buffer.byteLength(payload),
    },
  };

  const maxAttempts = 3;
  const retryableCodes = new Set([
    'ECONNRESET',
    'EPIPE',
    'ECONNREFUSED',
    'ETIMEDOUT',
  ]);

  /**
   * @param {number} attempt
   */
  const sendAttempt = (attempt) => {
    let settled = false;
    const req = http.request(options, (res) => {
      let data = '';

      res.on('data', (chunk) => {
        data += chunk;
      });

      res.on('end', () => {
        if (settled) {
          return;
        }
        settled = true;
        try {
          const parsed = util.deserialize(data);
          return callback(...parsed);
        } catch (e) {
          return callback(e, null);
        }
      });
    });

    req.setTimeout(15000, () => {
      req.destroy(Object.assign(new Error('request timeout'), {code: 'ETIMEDOUT'}));
    });

    req.on('error', (err) => {
      if (settled) {
        return;
      }

      const canRetry =
        attempt < maxAttempts &&
        err &&
        typeof err === 'object' &&
        retryableCodes.has(err.code);

      if (canRetry) {
        const delayMs = 25 * attempt;
        return setTimeout(() => sendAttempt(attempt + 1), delayMs);
      }

      settled = true;
      callback(err, null);
    });

    req.write(payload);
    req.end();
  };

  sendAttempt(1);
}

module.exports = {send};
