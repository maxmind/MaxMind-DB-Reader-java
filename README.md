# MaxMind DB Reader #
[![Build Status](https://travis-ci.org/maxmind/MaxMind-DB-Reader-java.png?branch=master)](https://travis-ci.org/maxmind/MaxMind-DB-Reader-java)

## Description ##

This is the Java API for reading MaxMind DB files. MaxMind DB is a binary file
format that stores data indexed by IP address subnets (IPv4 or IPv6).

## Installation ##

### Maven ###

We recommend installing this package with [Maven](http://maven.apache.org/).
To do this, add the dependency to your pom.xml:

```xml
    <dependency>
        <groupId>com.maxmind.db</groupId>
        <artifactId>maxmind-db</artifactId>
        <version>1.3.1</version>
    </dependency>
```

### Gradle ###

Add the following to your `build.gradle` file:

```
repositories {
    mavenCentral()
}
dependencies {
    compile 'com.maxmind.db:maxmind-db:1.3.1'
}
```

## Usage ##

*Note:* For accessing MaxMind GeoIP2 databases, we generally recommend using
the [GeoIP2 Java API](http://maxmind.github.io/GeoIP2-java/) rather than using
this package directly.

To use the API, you must first create a `Reader` object. The constructor for
the reader object takes a `File` representing your MaxMind DB. Optionally you
may pass a second parameter with a `FileMode` with a value of `MEMORY_MAP` or
`MEMORY`. The default mode is `MEMORY_MAP`, which maps the file to virtual
memory. This often provides performance comparable to loading the file into
real memory with `MEMORY`.

To look up an IP address, pass the address as an `InetAddress` to the `get`
method on `Reader`. This method will return the result as a
`com.fasterxml.jackson.databind.JsonNode` object. `JsonNode` objects are used
as they provide a convenient representation of multi-type data structures and
the databind package of Jackson 2 supplies many tools for interacting with the
data in this format.

We recommend reusing the `Reader` object rather than creating a new one for
each lookup. The creation of this object is relatively expensive as it must
read in metadata for the file.

## Example ##

```java
import com.fasterxml.jackson.databind.JsonNode;
import com.maxmind.db.Reader;
import com.maxmind.db.Record;

import java.io.File;
import java.io.IOException;
import java.net.InetAddress;

public class Lookup {
    public static void main(String[] args) throws IOException {
        File database = new File("/path/to/database/GeoIP2-City.mmdb");
        try (Reader reader = new Reader(database)) {

            InetAddress address = InetAddress.getByName("24.24.24.24");

            // get() returns just the data for the associated record
            JsonNode recordData = reader.get(address);

            System.out.println(recordData);

            // getRecord() returns a Record class that contains both
            // the data for the record and associated metadata.
            Record record = reader.getRecord(address);

            System.out.println(record.getData());
            System.out.println(record.getNetwork());
        }
    }
}
```

### Caching ###

The database API supports pluggable caching (by default, no caching is
performed). A simple implementation is provided by `com.maxmind.db.CHMCache`.
Using this cache, lookup performance is significantly improved at the cost of
a small (~2MB) memory overhead.

Usage:

```java
Reader reader = new Reader(database, new CHMCache());
```

## Multi-Threaded Use ##

This API fully supports use in multi-threaded applications. In such
applications, we suggest creating one `Reader` object and sharing that among
threads.

## Common Problems ##

### File Lock on Windows ###

By default, this API uses the `MEMORY_MAP` mode, which memory maps the file.
On Windows, this may create an exclusive lock on the file that prevents it
from being renamed or deleted. Due to the implementation of memory mapping in
Java, this lock will not be released when the `DatabaseReader` is closed; it
will only be released when the object and the `MappedByteBuffer` it uses are
garbage collected. Older JVM versions may also not release the lock on exit.

To work around this problem, use the `MEMORY` mode or try upgrading your JVM
version. You may also call `System.gc()` after dereferencing the
`DatabaseReader` object to encourage the JVM to garbage collect sooner.

### Packaging Database in a JAR ###

If you are packaging the database file as a resource in a JAR file using
Maven, you must
[disable binary file filtering](http://maven.apache.org/plugins/maven-resources-plugin/examples/binaries-filtering.html).
Failure to do so will result in `InvalidDatabaseException` exceptions being
thrown when querying the database.

## Format ##

The MaxMind DB format is an open format for quickly mapping IP addresses to
records. The
[specification](https://github.com/maxmind/MaxMind-DB/blob/master/MaxMind-DB-spec.md)
is available as part of our
[Perl writer](https://github.com/maxmind/MaxMind-DB-Writer-perl) for the
format.

## Bug Tracker ##

Please report all issues with this code using the [GitHub issue tracker]
(https://github.com/maxmind/MaxMind-DB-Reader-java/issues).

If you are having an issue with a MaxMind database or service that is not
specific to this reader, please [contact MaxMind support]
(http://www.maxmind.com/en/support).

## Requirements  ##

This API requires Java 8 or greater.

## Contributing ##

Patches and pull requests are encouraged. Please include unit tests whenever
possible.

## Versioning ##

The MaxMind DB Reader API uses [Semantic Versioning](http://semver.org/).

## Copyright and License ##

This software is Copyright (c) 2014 by MaxMind, Inc.

This is free software, licensed under the Apache License, Version 2.0.
