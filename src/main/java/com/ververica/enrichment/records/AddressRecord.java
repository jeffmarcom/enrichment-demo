package com.ververica.enrichment.records;

import java.io.Serializable;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.annotation.JsonProperty;

@JsonIgnoreProperties(ignoreUnknown = true)
public class AddressRecord implements Serializable {
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
    private AddressData after;

    // Constructor
    public AddressRecord(String eventId, Tokens tokens, String opTable,
                         String opType, String opTs, AddressData after) {
        this.eventId = eventId;
        this.tokens = tokens;
        this.opTable = opTable;
        this.opType = opType;
        this.opTs = opTs;
        this.after = after;
    }

    // Default constructor
    public AddressRecord() {
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

    public AddressData getAfter() {
        return after;
    }

    public void setAfter(AddressData after) {
        this.after = after;
    }

    public static class AddressData implements Serializable {
        @JsonProperty("ADDRESS_ID")
        private Long addressId;

        @JsonProperty("CUSTOMER_NUMBER")
        private Long customerNumber;

        @JsonProperty("ADDRESS1")
        private String address1;

        @JsonProperty("ADDRESS2")
        private String address2;

        @JsonProperty("CITY")
        private String city;

        @JsonProperty("STATE")
        private String state;

        @JsonProperty("ZIP")
        private Long zip;

        @JsonProperty("LOCATION_NUMBER")
        private Integer locationNumber;

        // Constructor
        public AddressData(Long addressId, Long customerNumber,
                           String address1, String address2, String city,
                           String state, Long zip, Integer locationNumber) {
            this.addressId = addressId;
            this.customerNumber = customerNumber;
            this.address1 = address1;
            this.address2 = address2;
            this.city = city;
            this.state = state;
            this.zip = zip;
            this.locationNumber = locationNumber;
        }

        // Default constructor
        public AddressData() {
        }

        // Getters and setters
        public Long getAddressId() {
            return addressId;
        }

        public void setAddressId(Long addressId) {
            this.addressId = addressId;
        }

        public Long getCustomerNumber() {
            return customerNumber;
        }

        public void setCustomerNumber(Long customerNumber) {
            this.customerNumber = customerNumber;
        }

        public String getAddress1() {
            return address1;
        }

        public void setAddress1(String address1) {
            this.address1 = address1;
        }

        public String getAddress2() {
            return address2;
        }

        public void setAddress2(String address2) {
            this.address2 = address2;
        }

        public String getCity() {
            return city;
        }

        public void setCity(String city) {
            this.city = city;
        }

        public String getState() {
            return state;
        }

        public void setState(String state) {
            this.state = state;
        }

        public Long getZip() {
            return zip;
        }

        public void setZip(Long zip) {
            this.zip = zip;
        }

        public Integer getLocationNumber() {
            return locationNumber;
        }

        public void setLocationNumber(Integer locationNumber) {
            this.locationNumber = locationNumber;
        }
    }
}