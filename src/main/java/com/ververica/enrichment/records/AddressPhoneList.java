package com.ververica.enrichment.records;

import java.io.Serializable;
import java.util.List;

public class AddressPhoneList implements Serializable {
    private Long addressId;
    private List<PhoneRecord> phoneList;

    public AddressPhoneList(Long addressId, List<PhoneRecord> phoneList) {
        this.addressId = addressId;
        this.phoneList = phoneList;
    }

    public AddressPhoneList() {
    }

    public Long getAddressId() {
        return addressId;
    }

    public void setAddressId(Long addressId) {
        this.addressId = addressId;
    }

    public List<PhoneRecord> getPhoneList() {
        return phoneList;
    }

    public void setPhoneList(List<PhoneRecord> phoneList) {
        this.phoneList = phoneList;
    }
}
