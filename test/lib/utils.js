
module.exports.sortObjs = {
  Applications: { id: 1 },
  Jobs: { appId: 1, id: 1 },
  Graphs: {appId: 1, jobId: 1, stageId: 1},
  Stages: { appId: 1, id: 1 },
  StageAttempts: { appId: 1, stageId: 1, id: 1 },
  StageExecutors: { appId: 1, stageId: 1, execId: 1 },
  RDDs: { appId: 1, id: 1 },
  RDDExecutors: { appId: 1, rddId: 1, execId: 1 },
  NonRddBlocks: { appId: 1, execId: 1, id: 1 },
  RddBlocks: { appId: 1, rddId: 1, id: 1, execId: 1 },
  Executors: { appId: 1, id: 1 },
  Tasks: { appId: 1, stageId: 1, stageAttemptId: 1, id: 1 },
  TaskAttempts: { appId: 1, id: 1 },
  Environment: { appId: 1 },
  StageSummaryMetrics: { appId: 1, stageId: 1, stageAttemptId: 1, id: 1 }
};
