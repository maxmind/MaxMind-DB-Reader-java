# MaxMind DB Reader #

## Description ##

This is the Java API for reading MaxMind DB files. MaxMind DB is a binary file
format that stores data indexed by IP address subnets (IPv4 or IPv6).

## Installation ##

### Maven ###

We recommend installing this package with [Maven](https://maven.apache.org/).
To do this, add the dependency to your pom.xml:

```xml
    <dependency>
        <groupId>com.maxmind.db</groupId>
        <artifactId>maxmind-db</artifactId>
        <version>2.0.0</version>
    </dependency>
```

### Gradle ###

Add the following to your `build.gradle` file:

```
repositories {
    mavenCentral()
}
dependencies {
    compile 'com.maxmind.db:maxmind-db:2.0.0'
}
```

## Usage ##

*Note:* For accessing MaxMind GeoIP2 databases, we generally recommend using
the [GeoIP2 Java API](https://maxmind.github.io/GeoIP2-java/) rather than using
this package directly.

To use the API, you must first create a `Reader` object. The constructor for
the reader object takes a `File` representing your MaxMind DB. Optionally you
may pass a second parameter with a `FileMode` with a value of `MEMORY_MAP` or
`MEMORY`. The default mode is `MEMORY_MAP`, which maps the file to virtual
memory. This often provides performance comparable to loading the file into
real memory with `MEMORY`.

To look up an IP address, pass the address as an `InetAddress` to the `get`
method on `Reader`, along with the class of the object you want to
deserialize into. This method will create an instance of the class and
populate it. See examples below.

We recommend reusing the `Reader` object rather than creating a new one for
each lookup. The creation of this object is relatively expensive as it must
read in metadata for the file.

## Example ##

```java
import com.maxmind.db.MaxMindDbConstructor;
import com.maxmind.db.MaxMindDbParameter;
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
            LookupResult result = reader.get(address, LookupResult.class);

            System.out.println(result.getCountry().getIsoCode());

            // getRecord() returns a Record class that contains both
            // the data for the record and associated metadata.
            Record<LookupResult> record
                = reader.getRecord(address, LookupResult.class);

            System.out.println(record.getData().getCountry().getIsoCode());
            System.out.println(record.getNetwork());
        }
    }

    public static class LookupResult {
        private final Country country;

        @MaxMindDbConstructor
        public LookupResult (
            @MaxMindDbParameter(name="country") Country country
        ) {
            this.country = country;
        }

        public Country getCountry() {
            return this.country;
        }
    }

    public static class Country {
        private final String isoCode;

        @MaxMindDbConstructor
        public Country (
            @MaxMindDbParameter(name="iso_code") String isoCode
        ) {
            this.isoCode = isoCode;
        }

        public String getIsoCode() {
            return this.isoCode;
        }
    }
}
```

## Caching ##

The database API supports pluggable caching (by default, no caching is
performed). A simple implementation is provided by `com.maxmind.db.CHMCache`.
Using this cache, lookup performance is significantly improved at the cost of
a small (~2MB) memory overhead.

Usage:

```java
Reader reader = new Reader(database, new CHMCache());
```

Please note that the cache will hold references to the objects created
during the lookup. If you mutate the objects, the mutated objects will be
returned from the cache on subsequent lookups.

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
[disable binary file filtering](https://maven.apache.org/plugins/maven-resources-plugin/examples/binaries-filtering.html).
Failure to do so will result in `InvalidDatabaseException` exceptions being
thrown when querying the database.

## Format ##

The MaxMind DB format is an open format for quickly mapping IP addresses to
records. The
[specification](https://github.com/maxmind/MaxMind-DB/blob/master/MaxMind-DB-spec.md)
is available, as is our
[Perl writer](https://github.com/maxmind/MaxMind-DB-Writer-perl) for the
format.

## Bug Tracker ##

Please report all issues with this code using the [GitHub issue
tracker](https://github.com/maxmind/MaxMind-DB-Reader-java/issues).

If you are having an issue with a MaxMind database or service that is not
specific to this reader, please [contact MaxMind support]
(https://www.maxmind.com/en/support).

## Requirements  ##

This API requires Java 8 or greater.

## Contributing ##

Patches and pull requests are encouraged. Please include unit tests whenever
possible.

## Versioning ##

The MaxMind DB Reader API uses [Semantic Versioning](https://semver.org/).

## Copyright and License ##

This software is Copyright (c) 2014-2020 by MaxMind, Inc.

This is free software, licensed under the Apache License, Version 2.0.
