// @ts-check
/**
 * @typedef {import("../types.js").Callback} Callback
 * @typedef {import("../types.js").Config} Config
 * @typedef {import("../util/id.js").NID} NID
 */

const distribution = globalThis.distribution;
const id = distribution.util.id;

/**
 * Map functions used for mapreduce
 * @callback Mapper
 * @param {string} key
 * @param {any} value
 * @returns {object[]}
 */

/**
 * Reduce functions used for mapreduce
 * @callback Reducer
 * @param {string} key
 * @param {any[]} value
 * @returns {object}
 */

/**
 * @typedef {Object} MRConfig
 * @property {Mapper} map
 * @property {Reducer} reduce
 * @property {string[]} keys
 *
 * @typedef {Object} Mr
 * @property {(configuration: MRConfig, callback: Callback) => void} exec
 */


/*
  Note: The only method explicitly exposed in the `mr` service is `exec`.
  Other methods, such as `map`, `shuffle`, and `reduce`, should be dynamically
  installed on the remote nodes and not necessarily exposed to the user.
*/

/**
 * @param {Config} config
 * @returns {Mr}
 */
function mr(config) {
  const context = {
    gid: config.gid || 'all',
  };

  /**
   * @param {MRConfig} configuration
   * @param {Callback} callback
   * @returns {void}
   */
  function exec(configuration, callback) {
    const mrID = id.getID(`${configuration}${Date.now()}`);
    const mrGid = `mr${mrID}`;
    let finished = false;

    /*
      MapReduce steps:
      1) Setup: register a service `mr-<id>` on all nodes in the group. The service implements the map, shuffle, and reduce methods.
      2) Map: make each node run map on its local data and store them locally, under a different gid, to be used in the shuffle step.
      3) Shuffle: group values by key using store.append.
      4) Reduce: make each node run reduce on its local grouped values.
      5) Cleanup: remove the `mr-<id>` service and return the final output.

      Note: Comments inside the stencil describe a possible implementation---you should feel free to make low- and mid-level adjustments as needed.
    */
    const mrService = {
      coordNode: distribution.node.config,
      mapper: configuration.map,
      reducer: configuration.reduce,
      map: function(
          /** @type {string} */ gid,
          /** @type {string} */ mrID,
          /** @type {string[]} */ keysPartition,
          /** @type {Callback} */ callback,
      ) {
        // Map should read the node's local keys under the mrGid gid and write to store under gid `${mrID}_map`.
        // Expected output: array of objects with a single key per object.
        const sid = distribution.util.id.getSID(distribution.node.config);
        const mapKey = `${sid}_map`;
        const out = [];

        const finishMap = () => {
          distribution.local.store.put(out, {key: mapKey, gid: `${mrID}_map`}, (e) => {
            if (e) return callback(e, null);
            distribution.local.comm.send(
                [{phase: 'map', sid, node: distribution.node.config, mapKey}],
                {node: this.coordNode, gid: 'local', service: `mr${mrID}`, method: 'notify'},
                (e) => callback(e || null, 'ok'),
            );
          });
        };

        // Fast path for empty partitions so every worker still notifies once.
        if (keysPartition.length === 0) {
          finishMap();
          return;
        }

        let pendingKeys = keysPartition.length;
        keysPartition.forEach((key) => {
          distribution[gid].store.get(key, (e, v) => {
            const hasErr = e && (e instanceof Error || (typeof e === 'object' && Object.keys(e).length > 0));
            if (!hasErr) {
              const mapped = this.mapper(key, v) || [];
              out.push(...mapped);
            }
            pendingKeys--;
            if (pendingKeys === 0) {
              finishMap();
            }
          });
        });
      },
      shuffle: function(
          /** @type {string} */ gid,
          /** @type {string} */ mrID,
          /** @type {Callback} */ callback,
      ) {
        // Fetch the mapped values from the local store
        // Shuffle groups values by key (via store.append).
        const sid = distribution.util.id.getSID(distribution.node.config);
        const mapKey = `${sid}_map`;
        distribution.local.store.get({key: mapKey, gid: `${mrID}_map`}, (e, objs) => {
          if (e) return callback(e, null);

          const mappedObjs = Array.isArray(objs) ? objs : [];
          let pendingAppends = 0;
          let done = false;

          const finishShuffle = (err) => {
            if (done) {
              return;
            }
            done = true;
            if (err) {
              return callback(err, null);
            }
            distribution.local.comm.send(
                [{phase: 'shuffle', sid, node: distribution.node.config}],
                {node: this.coordNode, gid: 'local', service: `mr${mrID}`, method: 'notify'},
                (notifyErr) => callback(notifyErr || null, 'ok'),
            );
          };

          mappedObjs.forEach((obj) => {
            const key = Object.keys(obj)[0];
            if (key === undefined) {
              return;
            }
            const vals = Array.isArray(obj[key]) ? obj[key] : [obj[key]];
            vals.forEach((val) => {
              pendingAppends++;
              distribution[gid].mem.append(val, {key: key, gid: `${mrID}_shfl`}, (appendErr) => {
                const hasAppendErr = appendErr && (appendErr instanceof Error || (typeof appendErr === 'object' && Object.keys(appendErr).length > 0));
                if (hasAppendErr) return finishShuffle(appendErr);
                pendingAppends--;
                if (pendingAppends === 0) {
                  finishShuffle(null);
                }
              });
            });
          });

          if (pendingAppends === 0) {
            return finishShuffle(null);
          }
        });
      },
      reduce: function(
          /** @type {string} */ gid,
          /** @type {string} */ mrID,
          /** @type {Callback} */ callback,
      ) {
        // fetch grouped values from mem using metadata, apply reducer, and return final output.
        const sid = distribution.util.id.getSID(distribution.node.config);
        const out = [];

        const finishReduce = (err) => {
          distribution.local.comm.send(
              [{phase: 'reduce', sid, node: distribution.node.config, out}],
              {node: this.coordNode, gid: 'local', service: `mr${mrID}`, method: 'notify'},
              (notifyErr) => callback(err || notifyErr || null, 'ok'),
          );
        };

        // get list of keys appended locally during shuffle (metadata stored under gid key)
        distribution.local.mem.get({key: '', gid: `${mrID}_shfl`}, (e, keyList) => {
          if (e || !keyList || keyList.length === 0) {
            return finishReduce(e || Error('reduce: no shuffle keys'));
          }

          const uniqueKeys = Array.from(new Set(keyList));

          let pendingKeys = uniqueKeys.length;
          uniqueKeys.forEach((keyName) => {
            distribution.local.mem.get({key: keyName, gid: `${mrID}_shfl`}, (memErr, values) => {
              const hasMemErr = memErr && (memErr instanceof Error || (typeof memErr === 'object' && Object.keys(memErr).length > 0));
              if (!hasMemErr && values) {
                out.push(this.reducer(keyName, values));
              }
              pendingKeys--;
              if (pendingKeys === 0) {
                return finishReduce();
              }
            });
          });
        });
      },
    };

    /*
    function reduceOnCoordinator(reports, mrID, reducer, finish) {
      let pending = reports.length;
      const grouped = {};
      if (pending === 0) return finish(null, []);

      reports.forEach(({node, mapKey}) => {
        distribution.local.comm.send(
            [{key: mapKey, gid: `${mrID}_map`}],
            {node, service: 'store', method: 'get', gid: 'local'},
            (e, arr) => {
              if (e) return finish(e, null);
              for (const obj of arr || []) {
                const k = Object.keys(obj)[0];
                const vals = obj[k] instanceof Array ? obj[k] : [obj[k]];
                grouped[k] = (grouped[k] || []).concat(vals);
              }
              pending -= 1;
              if (pending === 0) {
                const out = Object.entries(grouped).map(([k, vals]) => reducer(k, vals));
                cleanupAndFinish(null, out);
              }
            },
        );
      });
    } */

    function finish(err, value) {
      if (finished) return;
      finished = true;
      return callback(err, value);
    }

    function cleanupAndFinish(err, value) {
      distribution[context.gid].routes.rem(mrGid, (e) => {
        const groupErr = e && Object.keys(e).length ? e : null;
        distribution.local.routes.rem(mrGid, (e2) => {
          err = err || groupErr || e2 || null;
          return finish(err, err ? null : value);
        });
      });
    }

    let expectedNodes = 0; // number of nodes in context.gid, set later
    let currentPhase = 'map';
    const phaseReports = {
      map: new Map(),
      shuffle: new Map(),
      reduce: new Map(),
    };

    const notifyService = {
      notify: function(message, notifyCallback) {
        notifyCallback(null, 'ack');
        if (!message || !message.phase || !message.sid) {
          return;
        }

        // count only notifications for the currently active phase
        if (message.phase !== currentPhase) {
          return;
        }

        const reports = phaseReports[currentPhase];
        // ignore duplicate notifications from the same worker in a phase
        if (reports.has(message.sid)) {
          return;
        }
        reports.set(message.sid, message);

        // all notifs received!
        if (reports.size === expectedNodes) {
          if (currentPhase === 'map') {
            currentPhase = 'shuffle';
            distribution[context.gid].comm.send(
                [context.gid, mrID],
                {service: mrGid, method: 'shuffle'}, (e) => {
                  if (e && (e instanceof Error || Object.keys(e).length)) {
                    return cleanupAndFinish(e, null);
                  }
                },
            );
          } else if (currentPhase === 'shuffle') {
            currentPhase = 'reduce';
            // reduceOnCoordinator(Array.from(phaseReports.map.values()), mrID, configuration.reduce,
            distribution[context.gid].comm.send(
                [context.gid, mrID],
                {service: mrGid, method: 'reduce'}, (e) => {
                  if (e && (e instanceof Error || Object.keys(e).length)) {
                    return cleanupAndFinish(e, null);
                  }
                });
          } else if (currentPhase === 'reduce') {
            const reduceReports = Array.from(phaseReports.reduce.values());
            const flatOut = reduceReports.flatMap((report) => report.out || []);
            cleanupAndFinish(null, flatOut);
          }
        }
      },
    };

    // Register the mr service on all nodes in the group and execute in sequence: map, shuffle, reduce.
    // get # nodes expected
    distribution.local.groups.get(context.gid, (e, v) => {
      if (e) return finish(e, null);
      expectedNodes = Object.keys(v).length;
      if (expectedNodes === 0) return finish(Error('empty group'), null);
      // install coordinator notify service
      distribution.local.routes.put(notifyService, mrGid, (e) => {
        if (e) return finish(e, null);
        // install group mr service
        distribution[context.gid].routes.put(mrService, mrGid, (e) => {
          if (Object.keys(e).length) return finish(e, null);
          // partition keys among workers
          const nodesList = Object.values(v);
          const keysPerNode = Math.ceil(configuration.keys.length / nodesList.length);
          // send map message to each worker with its key partition
          nodesList.forEach((node, idx) => {
            const startIdx = idx * keysPerNode;
            const endIdx = Math.min(startIdx + keysPerNode, configuration.keys.length);
            const keysForNode = configuration.keys.slice(startIdx, endIdx);
            // start map phase
            distribution.local.comm.send(
                [context.gid, mrID, keysForNode],
                {node, service: mrGid, method: 'map'}, (e) => {
                  if (e) {
                    return cleanupAndFinish(e, null);
                  }
                },
            );
          });
        });
      });
    });
  }

  return {exec};
}

module.exports = mr;
