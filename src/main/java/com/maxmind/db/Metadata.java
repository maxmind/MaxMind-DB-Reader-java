package com.maxmind.db;

import java.math.BigInteger;
import java.util.Date;
import java.util.List;
import java.util.Map;

/**
 * {@code Metadata} holds data associated with the database itself.
 *
 * @param binaryFormatMajorVersion The major version number for the database's
 *                                 binary format.
 * @param binaryFormatMinorVersion The minor version number for the database's
 *                                 binary format.
 * @param buildEpoch               The date of the database build.
 * @param databaseType             A string that indicates the structure of each
 *                                 data record associated with an IP address.
 *                                 The actual definition of these structures is
 *                                 left up to the database creator.
 * @param languages                List of languages supported by the database.
 * @param description              Map from language code to description in that
 *                                 language.
 * @param ipVersion                Whether the database contains IPv4 or IPv6
 *                                 address data. The only possible values are 4
 *                                 and 6.
 * @param nodeCount                The number of nodes in the search tree.
 * @param recordSize               The number of bits in a record in the search
 *                                 tree. Note that each node consists of two
 *                                 records.
 */
public record Metadata(
        @MaxMindDbParameter(name = "binary_format_major_version") int binaryFormatMajorVersion,
        @MaxMindDbParameter(name = "binary_format_minor_version") int binaryFormatMinorVersion,
        @MaxMindDbParameter(name = "build_epoch") BigInteger buildEpoch,
        @MaxMindDbParameter(name = "database_type") String databaseType,
        @MaxMindDbParameter(name = "languages") List<String> languages,
        @MaxMindDbParameter(name = "description") Map<String, String> description,
        @MaxMindDbParameter(name = "ip_version") int ipVersion,
        @MaxMindDbParameter(name = "node_count") long nodeCount,
        @MaxMindDbParameter(name = "record_size") int recordSize
) {
    /**
     * Compact constructor for the Metadata record.
     */
    @MaxMindDbConstructor
    public Metadata {}

    /**
     * @return the date of the database build.
     */
    public Date buildDate() {
        return new Date(buildEpoch.longValue() * 1000);
    }

    /**
     * @return the nodeByteSize
     */
    int nodeByteSize() {
        return recordSize / 4;
    }

    /**
     * @return the searchTreeSize
     */
    long searchTreeSize() {
        return nodeCount * nodeByteSize();
    }
}
