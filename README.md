# `slim`
`slim` is a Node server that:

* listens to [Spark][] events (as emitted by [`spark-json-relay`][]), 
* aggregates various statistics about Spark applications described by those events, and 
* writes those statistics to Mongo for consumption by [`spree`][].

[The `spree` documentation][`spree`] has lots more information about using these components together, which is the intended use case.

If you're just interested in running `slim`, e.g. to persist Spark events to Mongo, you can:
* run a `mongod` process
* run `slim`:

  ```
  $ node slim.js
  ```

* run a Spark application with a [`JsonRelay`][] registered as a listener

  ```
  $ $SPARK_HOME/bin/spark-{submit,shell} \
      --driver-class-path /path/to/json-relay/client/target/json-relay-client-with-deps-1.0-SNAPSHOT.jar \
      --conf spark.extraListeners=org.apache.spark.JsonRelay
  ```

## Cmdline Options
* `-h`/`--mongo-host`; default `localhost`.
* `-p`/`--mongo-port`; default `3001`.
* `-d`/`--mongo-db`; default `meteor`.
* `-u`/`--mongo-url`: full Mongo URL, starting with `mongodb://`; overrides the above three options if present.
* `-P`/`--port`: default `8123`; port to listen for `JsonRelay` clients on.
  * Should match `--conf spark.slim.port` arg passed to Spark (which also defaults to `8123`, conveniently).
* `--log`: if present, all events received from `JsonRelay` clients will be written to this file.
  * Enclosing directory is created if it doesn't exist.
  * See [`test/data/small/input/events.json`][] for an example.
* `-l`: default `info`; log-level passed to [`tracer`][] logging utility.
  * Can be `debug`, `info`, `warn`, `error`.

## Mongo Collections / Schemas
`slim` writes out 13 (!) types of records, each of which corresponds to a noun from the Spark universe (or a "join" of two such nouns):

* Applications
  * Jobs
  * Stages
    * Stage attempts
      * Tasks
      * Task Attempts
      * {Stage-attempt, Executor} joins
  * Executors
    * Non-RDD blocks
  * RDDs
    * {RDD, Executor} joins
    * RDD blocks
  * Environment

The nesting above indicates which records are stored and accessed via instances of other record types. For example, when `slim` sees a `TaskEnd` event, e.g.:

```
{
  "Event": "SparkListenerTaskEnd",
  "appId": "local-1437323845748",
  "Stage ID": 1,
  "Stage Attempt ID": 0,
  "Task Info": {
    "Task ID": 6,
    "Index": 2,
    "Attempt": 0,
    …
  },
  "Task Metrics": { … },
  …
}
```

…it first looks up the [`App`][] with `appId` equal to `"local-1437323845748"` (this field is [added to every event by `JsonRelay`](https://github.com/hammerlab/spark-json-relay/blob/abfea947334a6185cfd43e64a552806094c4c584/client/src/main/scala/org/apache/spark/JsonRelay.scala#L61)), then asks that `App` for a [`Stage`][] matching the `"Stage ID"` field, etc.

### At a glance
Probably the easiest way to grok `slim`'s record types and schemas is to peek at the [`test/data/small/output`][] directory.

## Other utilities

### [`dump-records.js`][]
This script reads in a file, specified by the `--in` flag, that contains logged `slim` events (see the `--log` option above).

It runs those events through a `slim` server (which writes info about them to Mongo) and then dumps the resulting Mongo contents into per-collection JSON files in an output directory specified either as a cmdline argument (if one is present) or by swapping `output` for `input` in the path to the input log file.

For example, this command will read [`test/data/small/input/events.json`][] and produce [`test/data/small/output/*`][`test/data/small/output`]:
```
$ node test/lib/dump-records.js --in test/data/small
```

This script was used to generate the `test/data/*/output` directories (modulo a few manual tweaks to the resulting JSON).

## Implementation Details

### [`record.js`][]
This file defines a simple Mongo ORM for the classes defined in the [`models`][] directory. 

It mixes-in methods that allow instances of the `models` classes to be manipulated in ways that should be familiar to Mongo users, e.g. `.set`, `.inc`, `.dec`, etc.

In an effort to keep `slim` lean, it is set up to basically only write to Mongo, assuming its in-memory representation of all objects' state matches what is in Mongo. However, on receiving events that refer to an application it doesn't currently have in memory, it pages in any and all Mongo records from all collections that pertain to that application, in case e.g. a previous instance of `slim` had processed this applications events before going down.

#### Synchronization
Currently each Mongo record limits itself to one "in-flight" Mongo-upsert-request at a time; any time one finishes, that record checks whether it has additional queued up changes that have accrued since that request took off, and fires off another upsert if so. 

This is necessary because e.g. `$set` operations are not commutative. At this point, much of the work has been moved to `$inc`/`$dec` operators, but a few things still risk ending up in the wrong state if allowed to race, e.g. records' `status` field that describes whether they are `RUNNING`, `SUCCEEDED`, `FAILED`, etc.

Additionally, this constraint is useful for easing Mongo thrashing during bursts of activity, e.g. stages that finish large numbers of tasks quickly, so relaxing it is not even really a goal.

### Denormalization
Much of the logic in `slim` has to do with maintaining various aggregate statistics that it collects. To this end, many fields, typically counts/"totals", are denormalized onto several different record types.

The quintessential example of this is tasks' "metrics" fields; denormalized totals of these are rolled up on to StageAttempts, Executors, Stage-Executor joins, Jobs, and Applications (the latter storing e.g. the sum of all shuffle read bytes across all tasks in all stages).

Two other notable examples are memory/disk/tachyon usage (stored on blocks, denormalized onto Executors, RDDs, Executor-RDD joins, Applications) and durations (total duration of all tasks is rolled up onto all "parent" records of task attempts).

### Tests

[`slim`'s test cases](https://github.com/hammerlab/slim/tree/69307377f9f5f8534e5385b530fd60be3be48e5d/test/data) and [attendant tooling](https://github.com/hammerlab/slim/tree/69307377f9f5f8534e5385b530fd60be3be48e5d/test/lib) provide a good way to check assumptions about what it will do with various Spark-event input data; checking them out is recommended.

## Contributing

Please file issues if you have any trouble using `slim` or have any questions!

[Spark]: https://spark.apache.org/
[`spark-json-relay`]: https://github.com/hammerlab/spark-json-relay
[`spree`]: https://github.com/hammerlab/spree
[`JsonRelay`]: https://github.com/hammerlab/spark-json-relay/blob/abfea947334a6185cfd43e64a552806094c4c584/client/src/main/scala/org/apache/spark/JsonRelay.scala
[`tracer`]: https://github.com/baryon/tracer
[`dump-records.js`]: https://github.com/hammerlab/slim/blob/69307377f9f5f8534e5385b530fd60be3be48e5d/test/lib/dump-records.js
[`test/data/small/input/events.json`]: https://github.com/hammerlab/slim/blob/69307377f9f5f8534e5385b530fd60be3be48e5d/test/data/small/input/events.json
[`App`]: https://github.com/hammerlab/slim/blob/69307377f9f5f8534e5385b530fd60be3be48e5d/models/app.js#L19
[`Stage`]: https://github.com/hammerlab/slim/blob/69307377f9f5f8534e5385b530fd60be3be48e5d/models/stage.js
[`test/data/small/output`]: https://github.com/hammerlab/slim/tree/69307377f9f5f8534e5385b530fd60be3be48e5d/test/data/small/output
[`record.js`]: https://github.com/hammerlab/slim/blob/69307377f9f5f8534e5385b530fd60be3be48e5d/mongo/record.js
[`models`]: https://github.com/hammerlab/slim/tree/69307377f9f5f8534e5385b530fd60be3be48e5d/models
