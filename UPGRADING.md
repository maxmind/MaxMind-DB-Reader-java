# Upgrading to 4.0.0

This guide covers the breaking changes introduced in version 4.0.0 and how to
update your code.

## Java Version Requirement

**Java 17 or greater is now required.** If you are using Java 11, you must
upgrade to Java 17 or later before upgrading to 4.0.0.

## Record Conversion and Method Changes

The `DatabaseRecord`, `Metadata`, and `Network` classes have been converted to
Java records. This means getter methods have been replaced with record accessor
methods.

### DatabaseRecord Changes

**Before (3.x):**

```java
DatabaseRecord<MyData> record = reader.getRecord(address, MyData.class);
MyData data = record.getData();
Network network = record.getNetwork();
```

**After (4.0.0):**

```java
DatabaseRecord<MyData> record = reader.getRecord(address, MyData.class);
MyData data = record.data();
Network network = record.network();
```

**Find and replace:**

- `record.getData()` → `record.data()`
- `record.getNetwork()` → `record.network()`

### Network Changes

**Before (3.x):**

```java
InetAddress address = network.getNetworkAddress();
int prefixLength = network.getPrefixLength();
```

**After (4.0.0):**

```java
InetAddress address = network.networkAddress();
int prefixLength = network.prefixLength();
```

**Find and replace:**

- `network.getNetworkAddress()` → `network.networkAddress()`
- `network.getPrefixLength()` → `network.prefixLength()`

### Metadata Changes

All getter methods on `Metadata` have been replaced with record accessor
methods. Remove the `get` prefix and use camelCase instead.

**Before (3.x):**

```java
Metadata metadata = reader.getMetadata();
int majorVersion = metadata.getBinaryFormatMajorVersion();
String dbType = metadata.getDatabaseType();
List<String> languages = metadata.getLanguages();
```

**After (4.0.0):**

```java
Metadata metadata = reader.getMetadata();
int majorVersion = metadata.binaryFormatMajorVersion();
String dbType = metadata.databaseType();
List<String> languages = metadata.languages();
```

**Common replacements:**

- `getBinaryFormatMajorVersion()` → `binaryFormatMajorVersion()`
- `getBinaryFormatMinorVersion()` → `binaryFormatMinorVersion()`
- `getDatabaseType()` → `databaseType()`
- `getLanguages()` → `languages()`
- `getDescription()` → `description()`
- `getIpVersion()` → `ipVersion()`
- `getNodeCount()` → `nodeCount()`
- `getRecordSize()` → `recordSize()`

### Build Time Change (Date → Instant)

The `getBuildDate()` method has been replaced with `buildTime()`, which returns
`java.time.Instant` instead of `java.util.Date`.

**Before (3.x):**

```java
import java.util.Date;

Metadata metadata = reader.getMetadata();
Date buildDate = metadata.getBuildDate();
```

**After (4.0.0):**

```java
import java.time.Instant;

Metadata metadata = reader.getMetadata();
Instant buildTime = metadata.buildTime();
```

If you need a `Date` object, you can convert:

```java
Date buildDate = Date.from(metadata.buildTime());
```

## DatabaseRecord Constructor Change

If you are manually constructing `DatabaseRecord` instances, the legacy
constructor has been removed.

**Before (3.x):**

```java
DatabaseRecord<MyData> record = new DatabaseRecord<>(data, ipAddress, prefixLength);
```

**After (4.0.0):**

```java
Network network = new Network(ipAddress, prefixLength);
DatabaseRecord<MyData> record = new DatabaseRecord<>(data, network);
```

## Deserialization Improvements

### Automatic Constructor Selection

If your classes use records or have a single public constructor, you may no
longer need to annotate them with `@MaxMindDbConstructor`.

**Before (3.x) - Required annotations:**

```java
public record Location(
    @MaxMindDbParameter(name = "latitude") Double latitude,
    @MaxMindDbParameter(name = "longitude") Double longitude
) {}
```

**After (4.0.0) - Annotations optional when names match:**

```java
public record Location(
    Double latitude,
    Double longitude
) {}
```

The canonical record constructor is used automatically, and component names
match the database field names.

### Parameter Name Support

For non-record classes, if you compile with the `-parameters` flag, parameter
names can be used instead of requiring `@MaxMindDbParameter` annotations.

**Before (3.x):**

```java
@MaxMindDbConstructor
public Location(
    @MaxMindDbParameter(name = "latitude") Double latitude,
    @MaxMindDbParameter(name = "longitude") Double longitude
) {
    this.latitude = latitude;
    this.longitude = longitude;
}
```

**After (4.0.0) - With `-parameters` flag:**

```java
@MaxMindDbConstructor
public Location(Double latitude, Double longitude) {
    this.latitude = latitude;
    this.longitude = longitude;
}
```

### New Injection Annotations

4.0.0 introduces `@MaxMindDbIpAddress` and `@MaxMindDbNetwork` annotations for
injecting lookup context into your constructors.

**Example:**

```java
public record EnrichedData(
    String city,
    @MaxMindDbIpAddress InetAddress lookupIp,
    @MaxMindDbNetwork Network network
) {}
```

## Large Database Support (>2GB)

Version 4.0.0 adds support for MaxMind DB files larger than 2GB. This change is
transparent to most users:

- Files under 2GB continue to use a single `ByteBuffer` for optimal performance
- Files 2GB and larger automatically use a multi-buffer implementation
- No code changes required in your application

## Migration Checklist

1. ✅ Upgrade to Java 17 or later
2. ✅ Update Maven/Gradle dependency to 4.0.0
3. ✅ Replace `record.getData()` with `record.data()`
4. ✅ Replace `record.getNetwork()` with `record.network()`
5. ✅ Replace `network.getNetworkAddress()` with `network.networkAddress()`
6. ✅ Replace `network.getPrefixLength()` with `network.prefixLength()`
7. ✅ Update all `Metadata` getter methods (remove `get` prefix, use camelCase)
8. ✅ Replace `metadata.getBuildDate()` with `metadata.buildTime()` and change
   type from `Date` to `Instant`
9. ✅ Update any manual `DatabaseRecord` constructions to use `Network`
   parameter
10. ✅ (Optional) Remove unnecessary `@MaxMindDbParameter` annotations if using
    records or `-parameters` flag

## Finding Issues

To help identify code that needs updating, you can search your codebase for:

- `.getData()`
- `.getNetwork()`
- `.getNetworkAddress()`
- `.getPrefixLength()`
- `.getBuildDate()`
- `.getBinaryFormatMajorVersion()` (and similar Metadata getters)
- `new DatabaseRecord<>` (check for three-parameter constructor)

## Support

If you encounter issues during the upgrade, please check:

- [GitHub Issues](https://github.com/maxmind/MaxMind-DB-Reader-java/issues)
- [MaxMind Support](https://www.maxmind.com/en/support)
