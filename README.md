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
        <version>3.2.0</version>
    </dependency>
```

### Gradle ###

Add the following to your `build.gradle` file:

```
repositories {
    mavenCentral()
}
dependencies {
    compile 'com.maxmind.db:maxmind-db:3.2.0'
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
import com.maxmind.db.DatabaseRecord;

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

            // getRecord() returns a DatabaseRecord class that contains both
            // the data for the record and associated metadata.
            DatabaseRecord<LookupResult> record
                = reader.getRecord(address, LookupResult.class);

            System.out.println(record.data().getCountry().getIsoCode());
            System.out.println(record.network());
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

### Constructor and parameter selection

- Preferred: annotate a constructor with `@MaxMindDbConstructor` and its
  parameters with `@MaxMindDbParameter(name = "...")`.
- Records: if no constructor is annotated, the canonical record constructor is
  used automatically. Record component names are used as field names.
- Classes with a single public constructor: if no constructor is annotated,
  that constructor is used automatically.
- Unannotated parameters: when a parameter is not annotated, the reader falls
  back to the parameter name. For records, this is the component name; for
  classes, this is the Java parameter name. To use Java parameter names at
  runtime, compile your model classes with the `-parameters` flag (Maven:
  `maven-compiler-plugin` with `<parameters>true</parameters>`).
  If Java parameter names are unavailable (no `-parameters`) and there is no
  `@MaxMindDbParameter` annotation, the reader throws a
  `ParameterNotFoundException` with guidance.

Defaults for missing values

- Provide a default with
  `@MaxMindDbParameter(name = "...", useDefault = true, defaultValue = "...")`.
- Supports primitives, boxed types, and `String`. If `defaultValue` is empty
  and `useDefault` is true, Java defaults are used (0, false, 0.0, empty
  string).
- Example:

  ```java
  @MaxMindDbConstructor
  Example(
      @MaxMindDbParameter(name = "count", useDefault = true, defaultValue = "0")
      int count,
      @MaxMindDbParameter(
          name = "enabled",
          useDefault = true,
          defaultValue = "true"
      )
      boolean enabled
  ) { }
  ```

Lookup context injection

- Use `@MaxMindDbIpAddress` to inject the IP address being decoded.
  Supported parameter types are `InetAddress` and `String`.
- Use `@MaxMindDbNetwork` to inject the network of the resulting record.
  Supported parameter types are `Network` and `String`.
- Context annotations cannot be combined with `@MaxMindDbParameter` on the same
  constructor argument. Values are populated for every lookup without being
  cached between different IPs.

Custom deserialization

- Use `@MaxMindDbCreator` to mark a static factory method or constructor that
  should be used for custom deserialization of a type from a MaxMind DB file.
- This annotation is similar to Jackson's `@JsonCreator` and is useful for
  types that need custom deserialization logic, such as enums with non-standard
  string representations or types that require special initialization.
- The annotation can be applied to both constructors and static factory methods.
- Example with an enum:

  ```java
  public enum ConnectionType {
      DIALUP("Dialup"),
      CABLE_DSL("Cable/DSL");

      private final String name;

      ConnectionType(String name) {
          this.name = name;
      }

      @MaxMindDbCreator
      public static ConnectionType fromString(String s) {
          return switch (s) {
              case "Dialup" -> DIALUP;
              case "Cable/DSL" -> CABLE_DSL;
              default -> null;
          };
      }
  }
  ```

You can also use the reader object to iterate over the database.
The `reader.networks()` and `reader.networksWithin()` methods can
be used for this purpose.

```java
Reader reader = new Reader(file);
Networks networks = reader.networks(Map.class);

while(networks.hasNext()) {
    DatabaseRecord<Map<String, String>> iteration = networks.next();

    // Get the data.
    Map<String, String> data = iteration.data();

    // The IP Address
    InetAddress ipAddress = InetAddress.getByName(data.get("ip"));

    // ...
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
[specification](https://github.com/maxmind/MaxMind-DB/blob/main/MaxMind-DB-spec.md)
is available, as is our
[Perl writer](https://github.com/maxmind/MaxMind-DB-Writer-perl) for the
format.

## Bug Tracker ##

Please report all issues with this code using the [GitHub issue
tracker](https://github.com/maxmind/MaxMind-DB-Reader-java/issues).

If you are having an issue with a MaxMind database or service that is not
specific to this reader, please [contact MaxMind support](https://www.maxmind.com/en/support).

## Requirements  ##

This API requires Java 17 or greater.

## Contributing ##

Patches and pull requests are encouraged. Please include unit tests whenever
possible.

## Versioning ##

The MaxMind DB Reader API uses [Semantic Versioning](https://semver.org/).

## Copyright and License ##

This software is Copyright (c) 2014-2025 by MaxMind, Inc.

This is free software, licensed under the Apache License, Version 2.0.
