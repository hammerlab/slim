
var mixinMongoMethods = require("../mongo/record").mixinMongoMethods;

function Graph(app, stageId) {
  this.app = app;
  this.appId = app.id;
  this.stageId = stageId;

  this.init(['appId', 'stageId']);
}

mixinMongoMethods(Graph, 'Graph', 'Graphs');

Graph.prototype.fromDAG = function(dag) {
  return this.set({
    'jobId': dag['jobId'],
    'dotFile': dag['dotFile'],
    'submitted': dag['submitted'],
    'childSubmitted': dag['childSubmitted'],
    'cachedRDDs': dag['cachedRDDs'],
    'incomingEdges': dag['incomingEdges'],
    'outgoingEdges': dag['outgoingEdges']
  });
}

// Used to update current stage as submitted, this forces 'skipped' status
Graph.prototype.update = function(dag) {
  return this.set('submitted', dag['submitted'], allowExtant = true);
}

// Used to update current stage child as submitted, meaning, if it is not submitted itself,
// it must be skipped
Graph.prototype.check = function(dag) {
  return this.set('childSubmitted', dag['childSubmitted'], allowExtant = true);
}

module.exports.Graph = Graph;
