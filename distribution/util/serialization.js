// @ts-check

/**
 * @param {any} object
 * @returns {string}
 */
function serialize(object) {
  const metaObject = {};
  switch (typeof object) {
    case 'number':
      metaObject['t'] = 'num';
      metaObject['v'] = object.toString();
      break;
    case 'string':
      metaObject['t'] = 'str';
      metaObject['v'] = object;
      break;
    case 'boolean':
      metaObject['t'] = 'bool';
      metaObject['v'] = object;
      break;
    case 'function':
      metaObject['t'] = 'fn';
      metaObject['v'] = object.toString();
      break;
    case 'undefined':
      metaObject['t'] = 'undef';
      break;
    case 'object':
      if (object === null) {
        metaObject['t'] = 'null';
      } else if (object instanceof Array) {
        metaObject['t'] = 'arr';
        metaObject['v'] = Array.from(object, (elt) => serialize(elt));
      } else if (object instanceof Date) {
        metaObject['t'] = 'date';
        metaObject['v'] = object.toISOString();
      } else if (object instanceof Error) {
        metaObject['t'] = 'err';
        metaObject['v'] = {
          name: object.name,
          message: object.message,
          stack: object.stack,
        };
      } else if (object.constructor === Object) {
        metaObject['t'] = 'obj';
        metaObject['v'] = Object.fromEntries(Object.entries(object)
            .map(([key, val]) => [key, serialize(val)]));
      } else {
        throw Error('got unexpected object instance');
      }
      break;
    default:
      throw Error('got unexpected type');
  }
  return JSON.stringify(metaObject);
}


/**
 * @param {string} string
 * @returns {any}
 */
function deserialize(string) {
  if (typeof string !== 'string') {
    throw new Error(`Invalid argument type: ${typeof string}.`);
  }
  const metaObject = JSON.parse(string);
  switch (metaObject['t']) {
    case 'num':
      return Number(metaObject['v']);
    case 'str':
      return metaObject['v'];
    case 'bool':
      return metaObject['v'];
    case 'fn':
      return Function('return ' + metaObject['v'])();
    case 'undef':
      return undefined;
    case 'null':
      return null;
    case 'arr':
      return Array.from(metaObject['v'], (elt) => deserialize(elt));
    case 'date':
      return new Date(metaObject['v']);
    case 'err':
      return Object.assign(Error(), metaObject['v']);
    case 'obj':
      return Object.fromEntries(Object.entries(metaObject['v'])
          .map(([key, val]) => [key, deserialize(val)]));
    default:
      throw Error('got unexpected type');
  }
}

module.exports = {
  serialize,
  deserialize,
};
