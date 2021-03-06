# What is this Repository?
The Apache Flink project provides several managed state interfaces right out of the box (`ValueState`, `ListState`, `MapState` to name a few). This project aims to provide a library that offers another managed state structure backed by a B+Tree implementation. I'm calling it BTreeState, however it may be useful to think of this as a basic key-value data structure (supporting the standard `get`, `put`, `delete` operations) that also supports range scans on keys.

# Motivation

The use case that led me to believe this library would be useful is performing infinite state joins in Flink. Flink already has really great support for many of the kind of joins you would want to do in real-time (unbounded stream) or batch (bounded stream):
- Windowed Joins - joins which are scoped to a given time window between two streams
- Enrichment Joins - [link](https://training.ververica.com/exercises/eventTimeJoin.html)
- Batch Joins - joins on bounded streams, same as a join in Spark

## Background

In my case, my teams were developing applications that joined many input streams. We had a handful of sources that included CDC streams, HTTP request logs, and business events. The two main outputs from these data were indexes (materialized views/documents/etc) and feature vectors to serve to some ML model for labeling/scoring in real time (and training later). I consider feature vectors for ML models to be a special case of materialized views, so I will simply refer to these use cases under the umbrella term _materialized views_.

In many of the prototype applications, it was common to see teams deploy prototype Flink topologies with more than 50 operators (this is not parallel operators). Most of these prototypes really struggled to scale on large data sets. As it turns out, materialized views that involve joins are really greedy when it comes to state size. In other words, we were asking Flink to hold on to a LOT of state indefinitely.

In theory, running Flink with a RocksDB state backend should be able to scale with the amount of disk available to the cluster. Yet we were still observing terrible throughput and frequent job failures due to YARN killing containers for exceeding memory limits. As we took a deep dive in tuning these jobs, we found two root causes for this: bad use of data structures on our part and incurring a significant amount of RocksDB overhead.

## Typical Approach to Infinite Joins
The typical way to perform a join is to model each side of the join as a DataStream<T>, key each stream by the join key, then connect the streams and apply a RichCoFlatMapFunction to the connected streams. The RichCoFlatMapFunction's state can be modeled as two MapState<PRIMARY_KEY, RECORD> for the right and left sides of the join. As each record arrives, it "upserts" itself into the appropriate MapState, then applies a combine/project function against all records in the other MapState, and finally sends each new projection downstream.

Some implementations model the output as a append/retract stream. See [this link](https://ci.apache.org/projects/flink/flink-docs-stable/dev/table/streaming/dynamic_tables.html#table-to-stream-conversion) for more details.

Flink tends to perform really well for simple cases in this model. However, when the number of join operations in a streaming topology gets fairly large/complex, Flink begins to struggle.

## Operator Overhead
- Issue remains: 1 operator per join -> Lots of operators
- Flink tasks have overhead from the framework
- Each parallel operator has an instance of RocksDB, which eats up the "Cut Off Space" memory in a TaskManager very quickly
- Requires lots of tuning to maximize memory resources, make sure RocksDB behaves
- Records of 1 type may be involved in many joins, and therefore cause write amplification issues
  - 1 record must be copied and shuffled (sent from one operator to another) for each join it is involved in

## Enter BTree
What if we could reduce the number of operators required to perform joins? If we could identify a parent key that would group all records in a set of joins together (like tenant id or shard id), and this parent key could produce partitions of data that could fit on a single machine, then we might be able to get some good performance gains by using less RocksDB instances and by requiring less shuffles for keys.

This is an important assumption and thus it is worth stating again: the sets of data we are looking to join together should be able to share one parent key. So across a CDC stream, HTTP request log stream, and a business event stream, there should be some property that allows me to derive a key that groups things together. This might be a domain name, a region ID, or a shard ID. The point is that all of the data that we are interested in joining must be able to share a common key so that Flink can route the records to the right stateful operator instance in a streaming topology.

In order to accomplish this, we need a data structure in Flink which supports:
- fast inserts/updates
- fast deletes
- fast key look-ups
- fast prefix scans
- ability to spill to disk

Since we're assuming a RocksDB state backend, we have to factor in serialization and RocksDB operations into the analysis for a good tool.

A BTree/B+Tree is a classic solution to this kind of problem!

Complexity Analysis on BTree (roughly)
- Any given look up is M*(log(N) + S) + (log(P) + S)
    - 1 binary search per page internal page, M is height of B-Tree excluding leaf level, N is number of keys per internal page
    - 1 binary search on the leaf page, P is the number of records in a leaf page
    - S is the cost of deserializing a page from RocksDB state backend (can be non trivial)
 
# Implementation
- B+Tree, all records are in leaf pages
- Root Page stored in ValueState
- Internal pages stored in MapState
- Leaf pages stored in MapState
- Flink deals with serializing pages of the tree

# Example of Use Case
Given I want to materialize the following view from a change data capture stream:
```sql
SELECT
    c.id,
    c.name,
    e.id,
    e.enrolled_at,
    u.id,
    u.name
FROM
    courses c
INNER JOIN
    enrollments e ON c.id = e.course_id AND c.shard_id = e.shard_id
INNER JOIN
    users u ON u.id = e.user_id AND u.shard_id = e.shard_id
```

- Courses are one-to-many with enrollments
- Users are one-to-many with enrollments

Get a unioned stream of all records and key the unioned stream by `shard_id`.

Psuedo-code for each record from the unioned stream:
```
tree = initializeTree()

def project(course, enrollment, user):
    return [
        course.getField("id"),
        course.getField("name"),
        enrollment.getField("id"),
        enrollment.getField("enrolledAt"),
        user.getField("id"),
        user.getField("name")
    ]

def processElement(el):
    if isCourse(el):
        key = "courses-" + el.getField("id")
        tree.insert(key, el)

        enrollments = tree.getByPrefix("enrollments-course-ix" + el.getField("id"))
        joinedRecords = enrollments.flatMap(enrollment => 
            user = tree.get("users-" + enrollment.getField("user_id")
            return project(el, enrollment, user)
        )

        return joinedRecords

    if isEnrollment(el):
        key1 = "enrollments-course-ix-" + el.getField("course_id")  "-" + el.getField("id")
        key2 = "enrollments-user-ix-" + el.getField("user_id")  "-" + el.getField("id")

        tree.insert(key1, el)
        tree.insert(key2, el)

        course = tree.get("courses-" + el.getField("course_id"))
        user = tree.getByPrefix("users-" + el.getField("user_id"))

        return [project(course, el, user)]

    if isUser(el):
        key = "users-" + el.getField("id")

        tree.insert(key, el)

        enrollments = tree.getByPrefix("enrollments-user-ix" + el.getField("id"))
        joinedRecords = enrollments.flatMap(enrollment => 
            course = tree.get("courses-" + enrollment.getField("course_id")
            return project(course, enrollment, el)
        )
        
        return joinedRecords
```

For this join, you could probably do better by eagerly projecting each record into it's output, rather than lazily storing each input record in the btree structure. This is possible because the each input can be mapped to the output it belongs to by it's primary key.

# Alternative Approaches
- RocksDB natively supports a prefix/range scan operation. Work could be done to surface this functionality to the existing managed state APIs.

# Road Map/Wishful Thinking
In some particular order:
- Implement the delete operation
- Merge pages when pages fall below capacity
- Convert page objects to byte stores, allowing for rapid serialization and deserialization, byte operations for insert, get, delete