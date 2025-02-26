package com.ververica.enrichment.records;

import java.io.Serializable;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.annotation.JsonProperty;

@JsonIgnoreProperties(ignoreUnknown = true)
public class CustomerRecord implements Serializable {
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
    private CustomerData after;

    // Constructor
    public CustomerRecord(String eventId, Tokens tokens, String opTable,
                          String opType, String opTs, CustomerData after) {
        this.eventId = eventId;
        this.tokens = tokens;
        this.opTable = opTable;
        this.opType = opType;
        this.opTs = opTs;
        this.after = after;
    }

    // Default constructor required for deserialization
    public CustomerRecord() {
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

    public CustomerData getAfter() {
        return after;
    }

    public void setAfter(CustomerData after) {
        this.after = after;
    }

    public static class CustomerData implements Serializable {
        @JsonProperty("CUSTOMER_NUMBER")
        private Long customerNumber;

        @JsonProperty("CUSTOMER_NAME")
        private String customerName;

        @JsonProperty("CUSTOMER_ADD_DATE")
        private String customerAddDate;

        @JsonProperty("TYPE_OF_CUSTOMER")
        private String typeOfCustomer;

        @JsonProperty("CUSTOMER_TIER_CODE")
        private String customerTierCode;

        @JsonProperty("DISTRICT")
        private Integer district;

        @JsonProperty("SALES_ROUTE")
        private Integer salesRoute;

        @JsonProperty("DEPT_IND")
        private String deptInd;

        @JsonProperty("CUTOFF_ID")
        private Integer cutoffId;

        @JsonProperty("CUSTOMER_PRODUCT_LIST_ID")
        private String customerProductListId;

        @JsonProperty("DELIVERY_INSTR")
        private String deliveryInstr;

        @JsonProperty("PRICE_BASIS")
        private String priceBasis;

        @JsonProperty("ORDER_ENTRY_FORMAT")
        private String orderEntryFormat;

        // Constructor
        public CustomerData(Long customerNumber, String customerName,
                            String customerAddDate, String typeOfCustomer,
                            String customerTierCode, Integer district,
                            Integer salesRoute, String deptInd, Integer cutoffId,
                            String customerProductListId, String deliveryInstr,
                            String priceBasis, String orderEntryFormat) {
            this.customerNumber = customerNumber;
            this.customerName = customerName;
            this.customerAddDate = customerAddDate;
            this.typeOfCustomer = typeOfCustomer;
            this.customerTierCode = customerTierCode;
            this.district = district;
            this.salesRoute = salesRoute;
            this.deptInd = deptInd;
            this.cutoffId = cutoffId;
            this.customerProductListId = customerProductListId;
            this.deliveryInstr = deliveryInstr;
            this.priceBasis = priceBasis;
            this.orderEntryFormat = orderEntryFormat;
        }

        // Default constructor
        public CustomerData() {
        }

        // Getters and setters
        public Long getCustomerNumber() {
            return customerNumber;
        }

        public void setCustomerNumber(Long customerNumber) {
            this.customerNumber = customerNumber;
        }

        public String getCustomerName() {
            return customerName;
        }

        public void setCustomerName(String customerName) {
            this.customerName = customerName;
        }

        public String getCustomerAddDate() {
            return customerAddDate;
        }

        public void setCustomerAddDate(String customerAddDate) {
            this.customerAddDate = customerAddDate;
        }

        public String getTypeOfCustomer() {
            return typeOfCustomer;
        }

        public void setTypeOfCustomer(String typeOfCustomer) {
            this.typeOfCustomer = typeOfCustomer;
        }

        public String getCustomerTierCode() {
            return customerTierCode;
        }

        public void setCustomerTierCode(String customerTierCode) {
            this.customerTierCode = customerTierCode;
        }

        public Integer getDistrict() {
            return district;
        }

        public void setDistrict(Integer district) {
            this.district = district;
        }

        public Integer getSalesRoute() {
            return salesRoute;
        }

        public void setSalesRoute(Integer salesRoute) {
            this.salesRoute = salesRoute;
        }

        public String getDeptInd() {
            return deptInd;
        }

        public void setDeptInd(String deptInd) {
            this.deptInd = deptInd;
        }

        public Integer getCutoffId() {
            return cutoffId;
        }

        public void setCutoffId(Integer cutoffId) {
            this.cutoffId = cutoffId;
        }

        public String getCustomerProductListId() {
            return customerProductListId;
        }

        public void setCustomerProductListId(String customerProductListId) {
            this.customerProductListId = customerProductListId;
        }

        public String getDeliveryInstr() {
            return deliveryInstr;
        }

        public void setDeliveryInstr(String deliveryInstr) {
            this.deliveryInstr = deliveryInstr;
        }

        public String getPriceBasis() {
            return priceBasis;
        }

        public void setPriceBasis(String priceBasis) {
            this.priceBasis = priceBasis;
        }

        public String getOrderEntryFormat() {
            return orderEntryFormat;
        }

        public void setOrderEntryFormat(String orderEntryFormat) {
            this.orderEntryFormat = orderEntryFormat;
        }
    }
}