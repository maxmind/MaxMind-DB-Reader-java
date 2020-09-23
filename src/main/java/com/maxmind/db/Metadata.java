package com.maxmind.db;

import java.math.BigInteger;
import java.util.Date;
import java.util.List;
import java.util.Map;

public final class Metadata {
    private final int binaryFormatMajorVersion;
    private final int binaryFormatMinorVersion;

    private final BigInteger buildEpoch;

    private final String databaseType;

    private final Map<String, String> description;

    private final int ipVersion;

    private final List<String> languages;

    private final int nodeByteSize;

    private final int nodeCount;

    private final int recordSize;

    private final int searchTreeSize;

    @MaxMindDbConstructor
    public Metadata(
            @MaxMindDbParameter(name="binary_format_major_version")
            int binaryFormatMajorVersion,
            @MaxMindDbParameter(name="binary_format_minor_version")
            int binaryFormatMinorVersion,
            @MaxMindDbParameter(name="build_epoch")
            BigInteger buildEpoch,
            @MaxMindDbParameter(name="database_type")
            String databaseType,
            @MaxMindDbParameter(name="languages")
            List<String> languages,
            @MaxMindDbParameter(name="description")
            Map<String, String> description,
            @MaxMindDbParameter(name="ip_version")
            int ipVersion,
            @MaxMindDbParameter(name="node_count")
            long nodeCount,
            @MaxMindDbParameter(name="record_size")
            int recordSize
    ) {
        this.binaryFormatMajorVersion = binaryFormatMajorVersion;
        this.binaryFormatMinorVersion = binaryFormatMinorVersion;
        this.buildEpoch = buildEpoch;
        this.databaseType = databaseType;
        this.languages = languages;
        this.description = description;
        this.ipVersion = ipVersion;
        this.nodeCount = (int) nodeCount;
        this.recordSize = recordSize;

        this.nodeByteSize = this.recordSize / 4;
        this.searchTreeSize = this.nodeCount * this.nodeByteSize;
    }

    /**
     * @return the major version number for the database's binary format.
     */
    public int getBinaryFormatMajorVersion() {
        return this.binaryFormatMajorVersion;
    }

    /**
     * @return the minor version number for the database's binary format.
     */
    public int getBinaryFormatMinorVersion() {
        return this.binaryFormatMinorVersion;
    }

    /**
     * @return the date of the database build.
     */
    public Date getBuildDate() {
        return new Date(this.buildEpoch.longValue() * 1000);
    }

    /**
     * @return a string that indicates the structure of each data record
     * associated with an IP address. The actual definition of these
     * structures is left up to the database creator.
     */
    public String getDatabaseType() {
        return this.databaseType;
    }

    /**
     * @return map from language code to description in that language.
     */
    public Map<String, String> getDescription() {
        return this.description;
    }

    /**
     * @return whether the database contains IPv4 or IPv6 address data. The only
     * possible values are 4 and 6.
     */
    public int getIpVersion() {
        return this.ipVersion;
    }

    /**
     * @return list of languages supported by the database.
     */
    public List<String> getLanguages() {
        return this.languages;
    }

    /**
     * @return the nodeByteSize
     */
    int getNodeByteSize() {
        return this.nodeByteSize;
    }

    /**
     * @return the number of nodes in the search tree.
     */
    int getNodeCount() {
        return this.nodeCount;
    }

    /**
     * @return the number of bits in a record in the search tree. Note that each
     * node consists of two records.
     */
    int getRecordSize() {
        return this.recordSize;
    }

    /**
     * @return the searchTreeSize
     */
    int getSearchTreeSize() {
        return this.searchTreeSize;
    }

    /*
     * (non-Javadoc)
     *
     * @see java.lang.Object#toString()
     */
    @Override
    public String toString() {
        return "Metadata [binaryFormatMajorVersion="
                + this.binaryFormatMajorVersion + ", binaryFormatMinorVersion="
                + this.binaryFormatMinorVersion + ", buildEpoch="
                + this.buildEpoch + ", databaseType=" + this.databaseType
                + ", description=" + this.description + ", ipVersion="
                + this.ipVersion + ", nodeCount=" + this.nodeCount
                + ", recordSize=" + this.recordSize + "]";
    }
}
