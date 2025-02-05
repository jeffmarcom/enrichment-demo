package com.ververica.enrichment.records;

import java.io.Serializable;
import java.util.List;

public class CustomerAddressList implements Serializable {
    private Long customerNumber;
    private List<CustomerAddressData> addresses;

    public CustomerAddressList(Long customerNumber, List<CustomerAddressData> addresses) {
        this.customerNumber = customerNumber;
        this.addresses = addresses;
    }

    public CustomerAddressList() {}

    public Long getCustomerNumber() {
        return customerNumber;
    }

    public void setCustomerNumber(Long customerNumber) {
        this.customerNumber = customerNumber;
    }

    public List<CustomerAddressData> getAddresses() {
        return addresses;
    }

    public void setAddresses(List<CustomerAddressData> addresses) {
        this.addresses = addresses;
    }
}