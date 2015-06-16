# `slim`
`slim` is a Node server that:

* listens to Spark events (as emitted by [`spark-json-relay`][]), 
* aggregates various statistics, and 
* writes them to Mongo for consumption by [`spree`][].

[The `spree` documentation][`spree`] has more information.

[`spark-json-relay`]: https://github.com/hammerlab/spark-json-relay
[`spree`]: https://github.com/hammerlab/spree
