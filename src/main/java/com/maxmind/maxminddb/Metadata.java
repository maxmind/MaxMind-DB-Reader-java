package com.maxmind.maxminddb;

import java.math.BigInteger;

import com.fasterxml.jackson.databind.JsonNode;

// XXX - if we make this public, add getters
final class Metadata {
    final int binaryFormatMajorVersion;
    final int binaryFormatMinorVersion;
    private final BigInteger buildEpoch;
    final String databaseType;
    final JsonNode description;
    final int ipVersion;
    final int nodeCount;
    final int recordSize;
    final int nodeByteSize;
    final int searchTreeSize;
    final JsonNode languages;

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
        this.nodeCount = metadata.get("node_count").asInt();
        this.recordSize = metadata.get("record_size").asInt();
        this.nodeByteSize = this.recordSize / 4;
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
