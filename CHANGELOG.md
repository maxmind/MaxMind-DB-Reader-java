CHANGELOG
=========

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
