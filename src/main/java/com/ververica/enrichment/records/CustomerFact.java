package com.ververica.enrichment.records;

import java.io.Serializable;
import java.util.List;

public class CustomerFact implements Serializable {
    private CustomerRecord customer;
    private List<CustomerAddressData> addresses;

    public CustomerFact(CustomerRecord customer, List<CustomerAddressData> addresses) {
        this.customer = customer;
        this.addresses = addresses;
    }

    public CustomerFact() {}

    public CustomerRecord getCustomer() {
        return customer;
    }

    public void setCustomer(CustomerRecord customer) {
        this.customer = customer;
    }

    public List<CustomerAddressData> getAddresses() {
        return addresses;
    }

    public void setAddresses(List<CustomerAddressData> addresses) {
        this.addresses = addresses;
    }
}