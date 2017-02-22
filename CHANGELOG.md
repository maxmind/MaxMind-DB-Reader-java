CHANGELOG
=========

1.2.2 (2017-02-22)
------------------

* Remove the version range. As of today, `jackson-databind` is no longer
  resolved correctly when a range is used. GitHub #28.

1.2.1 (2016-04-15)
------------------

* Specify a hard minimum dependency for `jackson-databind`. This API will not
  work with versions earlier than 2.7.0, and Maven's nearest-first resolution
  rule often pulled in older versions.

1.2.0 (2016-01-13)
------------------

* `JsonNode` containers returned by the `get(ip)` are now backed by
  unmodifiable collections. Any mutation done to them will fail with an
  `UnsupportedOperationException` exception. This allows safe caching of the
  nodes to be done without doing a deep copy of the cached data. Pull request
  by Viktor Szathmáry. GitHub #24.

1.1.0 (2016-01-04)
------------------

* The reader now supports pluggable caching of the decoded data. By default,
  no caching is performed. Please see the `README.md` file or the API docs
  for information on how to enable caching. Pull requests by Viktor Szathmáry.
  GitHub #21.
* This release also includes several additional performance enhancements as
  well as code cleanup from Viktor Szathmáry. GitHub #18, #19, #20, #22,and
  #23.

1.0.1 (2015-12-17)
------------------

* Several optimizations have been made to reduce allocations when decoding a
  record. Pull requests by Viktor Szathmáry. GitHub #16 & #17.


1.0.0 (2014-09-29)
------------------

* First production release.

0.4.0 (2014-09-23)
------------------

* Made `com.maxmind.db.Metadata` public and added public getters for most
  of the interesting metadata. This is accessible through the `getMetadata()`
  method on a `Reader` object.

0.3.4 (2014-08-27)
------------------

* Previously the Reader would hold onto the underlying file and FileChannel,
  not closing them until the Reader was closed. This was unnecessary; they
  are now closed immediately after they are used. Fix by Andrew Snare; GitHub
  issue #7.
* The Reader now discards the reference to the underlying buffer when
  `close()` is called. This is done to help ensure that the buffer is garbage
  collected sooner, which may mitigate file locking issues that some users
  have experienced on Windows when updating the database. Patch by Andrew
  Snare; GitHub issue #8.

0.3.3 (2014-06-02)
------------------

* A potential (small) resource leak when using this library with a thread
  pool was fixed.

0.3.2 (2014-04-02)
------------------

* Added tests and documentation for multi-threaded use.

0.3.1 (2013-11-05)
------------------

* An `InputStream` constructor was added to the `Reader` class. This reads the
  stream into memory as if it was using `FileMode.MEMORY`. Patch by Matthew
  Daniel.
* The source code is now attached during packaging. Patch by Matthew Daniel.
* The artifact ID was changed to `maxmind-db` in order to increase naming
  consistency.

0.3.0 (2013-10-17)
------------------

* IMPORTANT: The package name was changed to `com.maxmind.db`. The
  `MaxMindDbReader` class was renamed to `Reader`.
* Improved error handling and test coverage.
* Performance improvements.

0.2.0 (2013-07-08)
------------------

* The reader and database format now uses IEEE 754 doubles and floats.
* FileMode.IN_MEMORY was renamed to FileMode.MEMORY.
* Cache Type enum values array.

0.1.0 (2013-06-14)
------------------

* Initial release
