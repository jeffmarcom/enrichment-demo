package com.ververica.enrichment.records;

import java.io.Serializable;
import java.util.List;

public class CustomerAddressData implements Serializable {
    private AddressRecord addressRecord;
    private List<PhoneRecord> phoneList;

    // Constructor
    public CustomerAddressData(AddressRecord addressRecord, List<PhoneRecord> phoneList) {
        this.addressRecord = addressRecord;
        this.phoneList = phoneList;
    }

    // Default constructor
    public CustomerAddressData() {
    }

    // Getters and setters
    public AddressRecord getAddressRecord() {
        return addressRecord;
    }

    public void setAddressRecord(AddressRecord addressRecord) {
        this.addressRecord = addressRecord;
    }

    public List<PhoneRecord> getPhoneList() {
        return phoneList;
    }

    public void setPhoneList(List<PhoneRecord> phoneList) {
        this.phoneList = phoneList;
    }
}
