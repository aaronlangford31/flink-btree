# What is this Repository?
The Apache Flink project provides several managed state interfaces right out of the box (`ValueState`, `ListState`, `MapState` to name a few). This project aims to provide a library that offers another managed state structure backed by a B+Tree implementation. I'm calling it BTreeState, however it may be useful to think of this as a basic key-value data structure (supporting the standard `get`, `put`, `delete` operations) that also supports prefix and range scans on keys.

# Motivation

The use case that led me to believe this library would be useful is performing infinite state joins in Flink. Flink already has really great support for many of the kind of joins you would want to do in real-time (unbounded stream) or batch (bounded stream):
- Windowed Joins
- Enrichment Joins
- Batch Joins

In my case, my teams were developing applications that joined many input streams. We had a handful of sources that included CDC streams, HTTP request logs, and business events. The two main outputs from these data were indexes (materialized views/documents/etc) and feature vectors to serve to some ML model for labeling/scoring in real time (and training later). I consider feature vectors for ML models to be a special case of materialized views, so I will simply refer to these use cases under the umbrella term _materialized views_.

In many of the prototype applications, it was common to see teams deploy prototype Flink topologies with more than 50 operators (this is not parallel operators). Most of these prototypes really struggled to scale on large data sets. As it turns out, materialized views that involve joins are really greedy when it comes to state size. In other words, we were asking Flink to hold on to a LOT of state indefinitely.

In theory, running Flink with a RocksDB state backend should be able to scale with the amount of disk available to the cluster. Yet we were still observing terrible throughput and frequent job failures due to YARN killing containers for exceeding memory limits. As we took a deep dive in tuning these jobs, we found two root causes for this: bad use of data structures on our part and incurring a significant amount of RocksDB overhead.

## List Types as Values
- BAD: Left & Right sides of join stored in `Map<ID, LIST<RECORD>>`
- GOOD: Key Stream on Join Key, store left and right sides in `Map<ID, RECORD>` or just `Value<RECORD>`

## RocksDB and Other Overhead
- Issue remains: 1 operator per join -> Lots of operators
- Each parallel operator has an instance of RocksDB, which eats up the "Cut Off Space" memory in a TaskManager very quickly
- Requires lots of tuning to maximize memory resources, make sure RocksDB behaves
- Records of 1 type may be involved in many joins, and therefore cause write amplification issues
  - 1 record must be copied and shuffled (sent from one operator to another) for each join it is involved in

## Enter BTree
What if we could reduce the number of operators required to perform joins? If we could identify a parent key that would group all records in a set of joins together (like tenant id or shard id), then we might be able to get some good performance gains by using less RocksDB instances and by requiring less shuffles for keys.

In order to accomplish this, we need a data structure in Flink which supports:
- fast inserts/updates
- fast deletes
- fast key look-ups
- fast prefix scans
Since we're assuming a RocksDB state backend, we have to factor in serialization and RocksDB operations into the analysis for a good tool.

A BTree/B+Tree is a classic solution to this kind of problem!

TODO: analysis on BTree vs existing Managed State APIs

# Implementation

# Road Map
In some particular order
- Implement the delete operation
- Implement serializers
- Tests, tests, tests!
- Convert page objects to byte stores, allowing for rapid serialization and deserialization