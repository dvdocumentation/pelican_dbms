# pelican_dbms

Pelican is a lightning-fast serverless JSON-based DBMS with improved performance for all atomic CRUD operations. Allows you to work with large collections without losing performance due to data volume. That is, the speed of basic operations does not depend on size of database.

The strength of NoSQL document-oriented databases is their natural simplicity, but they are usually not very fast (unless they are serious backend databases like MongoDB). Pelican solves performance problems in all critical areas - insert, upsert, update, delete, get

Instant insert, upsert, update, delete and get operations due to a special storage architecture (records are appended to the end, data is stored in binary form). The operation time does not depend on how many records are already in the collection and what the file size is
Object versioning
Additional approaches for even greater speed
Support for transactions (sessions), use of custom handlers in a transaction
Pointers to data are always stored in RAM with concurrent change tracking: data is recalculated from disk only if it has been modified by another process.
Blocking files for writing for a short time (data is pre-prepared) which makes it easier to work in multi-threaded mode
ACID for multi-user and multi-threaded operation
Two types of indexes for key types of queries - hash index and special B-tree for full-text search.
Syntax similar to MongoDB, incl. 100% similar query language
Written in pure Python, about 2000 lines in total.
Why Pelican?
Written for situations when you need to organize a local database without a server with a JSON-oriented interface. For example, in a mobile solution. But at the same time, there are increased performance requirements: large collections (1,000,000+ documents in a collection) require fast, almost instantaneous execution of some operations:

Adding a new (changing, deleting) document to a collection with 1,000,000 documents – 1-2 milliseconds,running time does not depend on collection size
Find an element by equality in a collection with more than 1,000,000 entries in 1-2 microseconds.
Organize a real-time search for the occurrence of a string in a large collection without friezes
