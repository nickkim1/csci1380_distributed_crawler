# distribution

This is the distribution library. 

## Environment Setup

We recommend using the prepared [container image](https://github.com/brown-cs1380/container).

## Installation

After you have setup your environment, you can start using the distribution library.
When loaded, distribution introduces functionality supporting the distributed execution of programs. To download it:

```sh
$ npm i '@brown-ds/distribution'
```

This command downloads and installs the distribution library.

## Testing

There are several categories of tests:
  *	Regular Tests (`*.test.js`)
  *	Scenario Tests (`*.scenario.js`)
  *	Extra Credit Tests (`*.extra.test.js`)
  * Student Tests (`*.student.test.js`) - inside `test/test-student`

### Running Tests

By default, all regular tests are run. Use the options below to run different sets of tests:

1. Run all regular tests (default): `$ npm test` or `$ npm test -- -t`
2. Run scenario tests: `$ npm test -- -c` 
3. Run extra credit tests: `$ npm test -- -ec`
4. Run the `non-distribution` tests: `$ npm test -- -nd`
5. Combine options: `$ npm test -- -c -ec -nd -t`

## Usage

To try out the distribution library inside an interactive Node.js session, run:

```sh
$ node
```

Then, load the distribution library:

```js
> let distribution = require("@brown-ds/distribution")();
> distribution.node.start(console.log);
```

Now you have access to the full distribution library. You can start off by serializing some values. 

```js
> s = distribution.util.serialize(1); // '{"type":"number","value":"1"}'
> n = distribution.util.deserialize(s); // 1
```

You can inspect information about the current node (for example its `sid`) by running:

```js
> distribution.local.status.get('sid', console.log); // null 8cf1b (null here is the error value; meaning there is no error)
```

You can also store and retrieve values from the local memory:

```js
> distribution.local.mem.put({name: 'nikos'}, 'key', console.log); // null {name: 'nikos'} (again, null is the error value) 
> distribution.local.mem.get('key', console.log); // null {name: 'nikos'}

> distribution.local.mem.get('wrong-key', console.log); // Error('Key not found') undefined
```

You can also spawn a new node:

```js
> node = { ip: '127.0.0.1', port: 8080 };
> distribution.local.status.spawn(node, console.log);
```

Using the `distribution.all` set of services will allow you to act 
on the full set of nodes created as if they were a single one.

```js
> distribution.all.status.get('sid', console.log); // {} { '8cf1b': '8cf1b', '8cf1c': '8cf1c' } (now, errors are per-node and form an object)
```

You can also send messages to other nodes:

```js
> distribution.local.comm.send(['sid'], {node: node, service: 'status', method: 'get'}, console.log); // null 8cf1c
```

Most methods in the distribution library are asynchronous and take a callback as their last argument.
This callback is invoked when the method completes, with the first argument being an error (if any) and the second argument being the result.
The following runs the sequence of commands described above inside a script (note the nested callbacks):

```js
let distribution = require("@brown-ds/distribution")();
// Now we're only doing a few of the things we did above
const out = (cb) => {
  distribution.local.status.stop(cb); // Shut down the local node
};
distribution.node.start(() => {
  // This will run only after the node has started
  const node = {ip: '127.0.0.1', port: 8765};
  distribution.local.status.spawn(node, (e, v) => {
    if (e) {
      return out(console.log);
    }
    // This will run only after the new node has been spawned
    distribution.all.status.get('sid', (e, v) => {
      // This will run only after we communicated with all nodes and got their sids
      console.log(v); // { '8cf1b': '8cf1b', '8cf1c': '8cf1c' }
      // Shut down the remote node
      distribution.local.comm.send([], {service: 'status', method: 'stop', node: node}, () => {
        // Finally, stop the local node
        out(console.log); // null, {ip: '127.0.0.1', port: 1380}
      });
    });
  });
});
```

# Results and Reflections

## M1: Serialization / Deserialization


### Summary

> Summarize your implementation, including key challenges you encountered. Remember to update the `report` section of the `package.json` file with the total number of hours it took you to complete each task of M1 (`hours`) and the lines of code per task.

My implementation comprises `4` software components, totaling `300` lines of code. Key challenges included:
```
1. Serializing functions. Just calling toString() then reconstruction with Function() rendered it uncallable. This required appending 'return ' to the serialized function so it would evaluate correctly.
2. Serializing recursive structures. To serialize arrays or objects, each element must be serialized in a way that allows this nested operation to be deserialized in a similar fashion.
3. Error and Date types. These instances of object do not have fields that can be extracted the same way as Object objects, thus required some API digging to figure out how to unpack and repack them neatly.
```

### Correctness & Performance Characterization

> Describe how you characterized the correctness and performance of your implementation
```
I created 100 objects each of a base type (number), functions, and a recursive structure (objects). I measured how long each serialization/deserialization took then divided this sum by the number of objects done (300) to calculate the latency, and vice versa for throughput. I used the high-resolution timer library and put this in a separate test in the student tests for cleanliness and to not slow down production code.
```

*Correctness*: I wrote `8` tests; these tests take `0.795s` to execute. This includes objects with:
```
- negative floats
- fields that imitate other objects (e.g. Error)
- complex named functions
- empty and unicode strings
- unimplemented types/instances
```

*Performance*: The latency of various subsystems is described in the `"latency"` portion of package.json. The characteristics of my development machines are summarized in the `"dev"` portion of package.json.

`NOTE: units for latency and throughput are ms and msgs/ms, respectively`

## M2: Actors and Remote Procedure Calls (RPC)


### Summary

> Summarize your implementation, including key challenges you encountered. Remember to update the `report` section of the `package.json` file with the total number of hours it took you to complete each task of M2 (`hours`) and the lines of code per task.


My implementation comprises `6` software components, totaling `300` lines of code. Key challenges included.

```
1. Grasping the project structure. At first I wasn't sure how the pieces were supposed to fit together, especially for things like where additional routes were supposed to "go" programmatically and how all the components were related. Eventually I realized that we treat `local` as the base store of routes, and add things to it dynamically as we call routes.put.
2. Error handling. Due to the slight underspecification of certain inputs it was difficult to know how robust some checks should be. I found missing callback handling to be a consistent thing to check, and between the type annotations and tests I was able to figure out sufficient constraints on other inputs.
3. HTTP communication. I had to do a bit of research on the JS http package to know how I was expected to use it to communicate between nodes. It helped to look at the comm tests and the scaffolding of start() in node.js. It turned out to be relatively intuitive although it was still tricky to figure out how and when to pass things back and forth or to callbacks.
```


### Correctness & Performance Characterization

> Describe how you characterized the correctness and performance of your implementation


*Correctness*: I wrote `6` tests; these tests take `1.862s` to execute.


*Performance*: I characterized the performance of comm and RPC by sending 1000 service requests in a tight loop. Average throughput and latency is recorded in `package.json`.

```
In particular, I did 1000 for throughput and latency each, then averaged between the comm measurement and the RPC measurement. The throughput and latency tests differed in that the former was done concurrently while the latency tests were done sequentially to measure each message separately.
```

### Key Feature

> How would you explain the implementation of `createRPC` to someone who has no background in computer science — i.e., with the minimum jargon possible?

```
I think one analogy is like when you ask your dad something and he says "let me ask your mom." In this case your dad is deferring the responsibility of answering your question by allowing your mom to answer it for him, then using that as his answer. In this case you are the caller, your mom is the remote, and your dad is the orchestrator that calls createRPC() and then asks the remote (mom) the answer that the caller (you) needed.
```

## M3: Node Groups & Gossip Protocols


### Summary

> Summarize your implementation, including key challenges you encountered. Remember to update the `report` section of the `package.json` file with the total number of hours it took you to complete each task of M3 (`hours`) and the lines of code per task.


My implementation comprises `7` new software components, totaling `400` added lines of code over the previous implementation. Key challenges included:

```
1. In local.groups, I was not sure how to attach the distributed services to each object. I found the setup() function which allowed me to initialize the group with the correct gid.

2. In all.com, my first intuition was to send the messages sequentially. However I realized it is much faster for this to be concurrently and the counter can still be reliably used to call the final callback.
```


### Correctness & Performance Characterization

> Describe how you characterized the correctness and performance of your implementation


*Correctness* -- number of tests and time they take.

```
5 tests which take 0.879s

```

*Performance* -- spawn times (all students) and gossip (lab/ec-only).

```
Listed in package.json. It is commented out becuase I couldn't figure out how to make it not hang Jest.
```


### Key Feature

> What is the point of having a gossip protocol? Why doesn't a node just send the message to _all_ other nodes in its group?

```
Gossip protocols help support an at least once semantics for dissemination of information. As groups become large, messaging to all other nodes would be too expensive when messaging a group ~log(N) converges comparatively quickly.
```

## M4: Distributed Storage


### Summary

> Summarize your implementation, including key challenges you encountered

```
- to make keys unique to groups i just concatenate the group and given key for each kv pair
- i had some trouble with the JS quirks (mainly sorting BigInts) with the hashers so I had to do some research on that
- this was also my first time communicating to aws nodes remotely so i had some difficulty communicating with them
```

Remember to update the `report` section of the `package.json` file with the total number of hours it took you to complete each task of M4 (`hours`) and the lines of code per task.


### Correctness & Performance Characterization

> Describe how you characterized the correctness and performance of your implementation


*Correctness* -- number of tests and time they take.

```
5 tests, take 0.457s
```

*Performance* -- insertion and retrieval.

```
I randomly generate 1000 objects then store them in a distributed, in-memory kv store. I then retrieve them, measuring the latency for each store and retrieval separately to avoid overlap. I then averaged these values to get the final result.
```

### Key Feature

> Why is the `reconf` method designed to first identify all the keys to be relocated and then relocate individual objects instead of fetching all the objects immediately and then pushing them to their corresponding locations?

```
It would be very computationally/storage expensive to retrieve all the objects instead of operating only on their keys.
```

## M5: Distributed Execution Engine

### Summary

> Summarize your implementation, including key challenges you encountered. Remember to update the `report` section of the `package.json` file with the total number of hours it took you to complete each task of M5 (`hours`) and the lines of code per task.

My implementation comprises `5` new software components, totaling `800` added lines of code over the previous implementation. Key challenges included:

```
- designing the communication scheme. i did not understand the need for the worker nodes to have a notify method, so i just had the coordinator notify method communicate directly with the mapreduce service methods instead.
- managing the callbacks and avoiding multiple terminations. the finish method allowed me to ensure the final callback and cleanup was only done at most once.
- giving the reduce phase access to the keys shuffled to a node. i solved this by having mem.append store node-local metadata that would track the keys allocated to each node.
```

### Correctness & Performance Characterization

> Describe how you characterized the correctness and performance of your implementation

*Correctness*: I wrote `7` cases testing `the append methods and various failure modes of mapreduce`.

*Performance*: My `word count mapreduce` can sustain `0.000125` `runs`/second on `300 documents`, with an average latency of `8.024` seconds per `run`.

### Key Feature

> Which extra features did you implement and how?

`I guess I partially did in-memory operation but mixing storage methods avoided the main difficulties of this.`