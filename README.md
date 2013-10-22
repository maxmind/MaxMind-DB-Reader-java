# MaxMind DB Reader #

**NOTE**: This is a beta release, and the API may change before the final
production release.

## Description ##

This is the Java API for reading MaxMind DB files. MaxMind DB is a binary file
format that stores data indexed by IP address subnets (IPv4 or IPv6).

## Installation ##

### Define Your Dependencies ###

We recommend installing this package with [Maven](http://maven.apache.org/).
To do this, add the dependency to your pom.xml:

```xml
    <dependency>
        <groupId>com.maxmind.db</groupId>
        <artifactId>maxminddb</artifactId>
        <version>0.3.0</version>
    </dependency>
```

## Usage ##

*Note:* For accessing MaxMind GeoIP2 databases, we generally recommend using
the GeoIP2 Java API rather than using this package directly.

To use the API, you must first create a `Reader` object. The constructor for
the reader object takes a `File` representing your MaxMind DB. Optionally you
may pass a second parameter with a `FileMode` with a valueof `MEMORY_MAP` or
`MEMORY`. The default mode is `MEMORY_MAP`, which maps the file to virtual
memory. This often provides performance comparable to loading the file into
real memory with `MEMORY`.

To look up an IP address, pass the address as an `InetAddress` to the `get`
method on `Reader`. This method will return the result as a
`com.fasterxml.jackson.databind.JsonNode` object. `JsonNode` objects are used
as they provide a convenient representation of multi-type data structures and
the databind package of Jackson 2 supplies many tools for interacting with the
data in this format.

## Example ##

```java

File database = new File("/path/to/database/GeoIP2-City.mmdb");
Reader reader = new Reader(database);

InetAddress address = InetAddress.getByName("24.24.24.24");

JsonNode response = reader.get(address);

System.out.println(response);

reader.close();

```

## Format ##

The MaxMind DB format is an open format for quickly mapping IP addresses to
records. The [specification](https://github.com/maxmind/MaxMind-DB-perl/blob/master/docs/MaxMind-IPDB-spec.md)
is available as part of our [Perl writer](https://github.com/maxmind/MaxMind-
DB-perl) for the format.

## Bug Tracker ##

Please report all issues with this code using the [GitHub issue tracker]
(https://github.com/maxmind/MaxMind-DB-Reader-java/issues).

If you are having an issue with a MaxMind database or service that is not
specific to this reader, please [contact MaxMind support]
(http://www.maxmind.com/en/support).

## Requirements  ##

MaxMind has tested this API with Java 6 and above. Reasonable patches for Java
5 will be accepted. Patches for 1.4 or earlier will not be accepted.

## Contributing ##

Patches and pull requests are encouraged. Please include unit tests whenever
possible.

## Versioning ##

The MaxMind DB Reader API uses [Semantic Versioning](http://semver.org/).

## Copyright and License ##

This software is Copyright (c) 2013 by MaxMind, Inc.

This is free software, licensed under the Apache License, Version 2.0.
