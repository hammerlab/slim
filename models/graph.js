
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
    'skipped': dag['skipped'],
    'cachedRDDs': dag['cachedRDDs'],
    'incomingEdges': dag['incomingEdges'],
    'outgoingEdges': dag['outgoingEdges']
  });
}

module.exports.Graph = Graph;
