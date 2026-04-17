function hasRealError(e) {
  if (!e) {
    return false;
  }
  if (e instanceof Error) {
    return true;
  }
  if (typeof e === 'object') {
    return Object.keys(e).length > 0;
  }
  return true;
}

function getNodes() {
  if (process.env.M6_NODES) {
    const nodes = process.env.M6_NODES
        .split(',')
        .map((pair) => pair.trim())
        .filter((pair) => pair.length > 0)
        .map((pair) => {
          const [ip, portStr] = pair.split(':');
          return {ip, port: Number(portStr)};
        })
        .filter((node) => node.ip && Number.isInteger(node.port) && node.port > 0);

    if (nodes.length > 0) {
      return nodes;
    }
  }

  return [
    {ip: '127.0.0.1', port: 7210},
    {ip: '127.0.0.1', port: 7211},
    {ip: '127.0.0.1', port: 7212},
  ];
}

function stopNodes(distribution, nodes, callback) {
  const remote = {service: 'status', method: 'stop'};
  let i = 0;

  function next() {
    if (i >= nodes.length) {
      callback();
      return;
    }
    remote.node = nodes[i];
    distribution.local.comm.send([], remote, () => {
      i += 1;
      next();
    });
  }

  next();
}

function spawnNodes(distribution, nodes, callback) {
  let i = 0;

  function next() {
    if (i >= nodes.length) {
      callback();
      return;
    }

    distribution.local.status.spawn(nodes[i], () => {
      i += 1;
      next();
    });
  }

  next();
}

function setupGroup(distribution, nodes, gid, callback) {
  const id = distribution.util.id;
  const group = {};

  nodes.forEach((node) => {
    group[id.getSID(node)] = node;
  });

  const config = {gid};
  distribution.local.groups.put(config, group, (e) => {
    if (hasRealError(e)) {
      callback(e);
      return;
    }

    distribution[gid].groups.put(config, group, callback);
  });
}

function setupGroups(distribution, nodes, gids, callback) {
  let i = 0;

  function next() {
    if (i >= gids.length) {
      callback(null);
      return;
    }

    setupGroup(distribution, nodes, gids[i], (e) => {
      if (hasRealError(e)) {
        callback(e);
        return;
      }
      i += 1;
      next();
    });
  }

  next();
}

function shutdown(distribution, nodes, code) {
  stopNodes(distribution, nodes, () => {
    if (globalThis.distribution.node.server) {
      globalThis.distribution.node.server.close();
    }
    process.exit(code);
  });
}

module.exports = {
  hasRealError,
  getNodes,
  stopNodes,
  spawnNodes,
  setupGroup,
  setupGroups,
  shutdown,
};
