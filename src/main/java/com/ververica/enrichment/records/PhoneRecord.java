package com.ververica.enrichment.records;

import java.io.Serializable;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.annotation.JsonProperty;

@JsonIgnoreProperties(ignoreUnknown = true)
public class PhoneRecord implements Serializable {
    @JsonProperty("event_id")
    private String eventId;

    @JsonProperty("tokens")
    private Tokens tokens;

    @JsonProperty("op_table")
    private String opTable;

    @JsonProperty("op_type")
    private String opType;

    @JsonProperty("op_ts")
    private String opTs;

    @JsonProperty("after")
    private PhoneData after;

    // Constructor
    public PhoneRecord(String eventId, Tokens tokens, String opTable,
                       String opType, String opTs, PhoneData after) {
        this.eventId = eventId;
        this.tokens = tokens;
        this.opTable = opTable;
        this.opType = opType;
        this.opTs = opTs;
        this.after = after;
    }

    // Default constructor
    public PhoneRecord() {
    }

    // Getters and setters
    public String getEventId() {
        return eventId;
    }

    public void setEventId(String eventId) {
        this.eventId = eventId;
    }

    public Tokens getTokens() {
        return tokens;
    }

    public void setTokens(Tokens tokens) {
        this.tokens = tokens;
    }

    public String getOpTable() {
        return opTable;
    }

    public void setOpTable(String opTable) {
        this.opTable = opTable;
    }

    public String getOpType() {
        return opType;
    }

    public void setOpType(String opType) {
        this.opType = opType;
    }

    public String getOpTs() {
        return opTs;
    }

    public void setOpTs(String opTs) {
        this.opTs = opTs;
    }

    public PhoneData getAfter() {
        return after;
    }

    public void setAfter(PhoneData after) {
        this.after = after;
    }

    public static class PhoneData implements Serializable {
        @JsonProperty("ADDRESS_ID")
        private Long addressId;

        @JsonProperty("CONTACT_NAME")
        private String contactName;

        @JsonProperty("TELEPHONE_NUMBER")
        private String telephoneNumber;

        @JsonProperty("TELEPHONE_TYPE")
        private String telephoneType;

        // Constructor
        public PhoneData(Long addressId, String contactName,
                         String telephoneNumber, String telephoneType) {
            this.addressId = addressId;
            this.contactName = contactName;
            this.telephoneNumber = telephoneNumber;
            this.telephoneType = telephoneType;
        }

        // Default constructor
        public PhoneData() {
        }

        // Getters and setters
        public Long getAddressId() {
            return addressId;
        }

        public void setAddressId(Long addressId) {
            this.addressId = addressId;
        }

        public String getContactName() {
            return contactName;
        }

        public void setContactName(String contactName) {
            this.contactName = contactName;
        }

        public String getTelephoneNumber() {
            return telephoneNumber;
        }

        public void setTelephoneNumber(String telephoneNumber) {
            this.telephoneNumber = telephoneNumber;
        }

        public String getTelephoneType() {
            return telephoneType;
        }

        public void setTelephoneType(String telephoneType) {
            this.telephoneType = telephoneType;
        }
    }
}