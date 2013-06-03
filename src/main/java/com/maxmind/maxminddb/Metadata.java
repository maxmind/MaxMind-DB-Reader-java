package com.maxmind.maxminddb;

import java.math.BigInteger;
import java.util.Map;

// XXX - if we make this public, add getters
class Metadata {
    Integer binaryFormatMajorVersion;
    Integer binaryFormatMinorVersion;
    BigInteger buildEpoch;
    String databaseType;
    Map<String, Object> description;
    Integer ipVersion;
    Long nodeCount;
    Long recordSize;

    // XXX - think about how I want to construct this. Maybe look at how JSON
    // parsers deal with types
    public Metadata(Map<String, Object> metadata) {
        this.binaryFormatMajorVersion = (Integer) metadata
                .get("binary_format_major_version");
        this.binaryFormatMinorVersion = (Integer) metadata
                .get("binary_format_minor_version");
        this.buildEpoch = (BigInteger) metadata.get("build_epoch");
        this.databaseType = (String) metadata.get("database_type");
        this.description = (Map<String, Object>) metadata.get("description");
        this.ipVersion = (Integer) metadata.get("ip_version");
        this.nodeCount = (Long) metadata.get("node_count");
        this.recordSize = (Long) metadata.get("record_size");
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
