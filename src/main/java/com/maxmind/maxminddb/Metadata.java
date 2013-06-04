package com.maxmind.maxminddb;

import java.math.BigInteger;

import com.fasterxml.jackson.databind.JsonNode;

// XXX - if we make this public, add getters
class Metadata {
    Integer binaryFormatMajorVersion;
    Integer binaryFormatMinorVersion;
    BigInteger buildEpoch;
    String databaseType;
    JsonNode description;
    Integer ipVersion;
    Long nodeCount;
    Long recordSize;
    int nodeByteSize;
    long searchTreeSize;
    JsonNode languages;

    // XXX - think about how I want to construct this. Maybe look at how JSON
    // parsers deal with types
    public Metadata(JsonNode metadata) {
        this.binaryFormatMajorVersion = metadata.get(
                "binary_format_major_version").asInt();
        this.binaryFormatMinorVersion = metadata.get(
                "binary_format_minor_version").asInt();
        this.buildEpoch = metadata.get("build_epoch").bigIntegerValue();
        this.databaseType = metadata.get("database_type").asText();
        this.languages = metadata.get("languages");
        this.description = metadata.get("description");
        this.ipVersion = metadata.get("ip_version").asInt();
        this.nodeCount = metadata.get("node_count").asLong();
        this.recordSize = metadata.get("record_size").asLong();
        this.nodeByteSize = (int) (this.recordSize / 4);
        this.searchTreeSize = this.nodeCount * this.nodeByteSize;

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
