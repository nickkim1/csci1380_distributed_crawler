// @ts-check

/**
 * @typedef {import("../types.js").Callback} Callback
 * @typedef {import("../types.js").Config} Config
 */

const distribution = globalThis.distribution;

/**
 * @typedef {Object} CrawlerConfig
 * This should probably just be a struct that is of the form:
 *  { urls: [] } 
 * where [] is just a list of seed URLs to start crawling.
 */

function crawler(config) {
  const context = {
    gid: config.gid || "all",
  };

  /**
   * @param {CrawlerConfig} configuration
   * @param {Callback} callback
   * @returns {void}
   *
   * First MR exec flow:
   * 1) Setup: first, parse out the seed URLs from the configuration object.
   * Don't bother with error checking right now for malformed URLs, but that should
   * be a todo for a later point.
   * 2) MR: take those seed URLs and pass them to the mr exec function.
   *
   * mapper: this should be a function that is of the form:
   * (key : hash(url), value : url) => {
   * return [{key : hash(outgoing url), value : outgoing url} ...] (note though this is
   * flattened before added to the out results!!! ALSO here the result should NOT be emitted
   * IF it's already present in the visited set (check the global cross-node visited file set))
   * }
   *
   * reducer: this should be a function that is of the form:
   * (key : hash(outgoing url), value : [outgoing url1, outgoing url2...] ) = {
   * return {key : hash(outgoing url), value: text for outgoing url}
   * }
   *
   * In the map phase proper:
   *    a) Crawl the HTML text for anchor tags/other links (similar to getURLs.js)
   *    b) Get all the URLs and collect them into a list of outgoing objects
   *    c) In principle, just takes in 1 tuple -> outputs (potentially) many tuples
   *    NOTE: the map results will be stored into a single file with name like: <sid>map<gid>
   *    as an array of mapped objects/results, per the existing implementation.
   *
   * In the shuffle phase proper:
   *    Based on the existing implementation, this will fetch all the mapped obj results and for each of them
   *    it'll call local.mem.append() to collate them by unique key (in this case, the hash of the outgoing url).
   *    And put each unique URL on its own node for the reducer to run, since the operative assumption is for all vals
   *    for a given URL to live on same (to-be reducer) node.
   *
   * In the reducer phase proper:
   *    Based on the existing implementation, it'll just get all the unique keys from mem.get
   *    then run mem.get to get the corresponding values for each uq key then will call the reducer
   *    function on these. Then it'll return the node-level results back up to the group mr call which will collate these
   *    by node into a node map blah blah.
   *
   * 3) Store the reduced results into persistent storage i.e., in smth like: <sid>reduce<gid>
   * as a list of reduced objects: [{ key: hash(outgoing url), value: text for outgoing url}, {...}... ]
   *
   * Second MR exec flow
   * 1) mapper: (key : hash(outgoing url), value: text for outgoing url) -> { key: term, value : <outgoing url> <count of term in outgoing url> }
   * 2) shuffle - groups by hash(term) and sends to appropriate node via consistent hashing.
   * 3) reducer: ( key : term, value : [<outgoing url> <count of term in outgoing url>, ...] ) ->
   *    { key : term, value : sorted([<outgoing url> <count of term in outgoing url>, ...]) }
   *    
   * Query service:
   *    The end of this second reducer stage should be post-processed such that each node stores a small text
   *    file as an inverted index that we can subsequently query. A separate query service will chunk
   *    the query into component terms, hash them, then route them to the corresponding node which will
   *    theoretically for the same term hash have a bunch of terms mapping to it to comprise an overall
   *    inverted index (e.g,. if hash(apple) -> 4 and hash(me) -> 4 then the inverted index will comprise
   *    {apple : ...}, {me : ... }, etc. ). Then the query service can collect the relevant results at
   *    the end, sort them, and present them back to the user. I guess you can retrofit the query service
   *    to MR but that defeats the point, querying isn't a data processing service it's supposed to be fast
   *    and it's a read-only (not write) task which MR isn't. 
   * 
   */
  function exec(configuration, callback) {}
}