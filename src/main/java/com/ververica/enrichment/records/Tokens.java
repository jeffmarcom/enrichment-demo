package com.ververica.enrichment.records;

import java.io.Serializable;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.annotation.JsonIgnoreProperties;

@JsonIgnoreProperties(ignoreUnknown = true)
public class Tokens implements Serializable {
    private String ggsLastUpdDate;
    private String sourceSys;

    // Constructor
    public Tokens(String ggsLastUpdDate, String sourceSys) {
        this.ggsLastUpdDate = ggsLastUpdDate;
        this.sourceSys = sourceSys;
    }

    // Default constructor
    public Tokens() {
    }

    // Getters and setters
    public String getGgsLastUpdDate() {
        return ggsLastUpdDate;
    }

    public void setGgsLastUpdDate(String ggsLastUpdDate) {
        this.ggsLastUpdDate = ggsLastUpdDate;
    }

    public String getSourceSys() {
        return sourceSys;
    }

    public void setSourceSys(String sourceSys) {
        this.sourceSys = sourceSys;
    }
}
