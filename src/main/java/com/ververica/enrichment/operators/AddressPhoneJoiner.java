package com.ververica.enrichment.operators;

import com.ververica.enrichment.records.*;
import org.apache.flink.api.common.state.MapState;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.typeutils.ResultTypeQueryable;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.co.KeyedCoProcessFunction;
import org.apache.flink.util.Collector;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.StreamSupport;

public class AddressPhoneJoiner
        extends KeyedCoProcessFunction<Long, AddressRecord, AddressPhoneList, CustomerAddressList>
        implements ResultTypeQueryable<CustomerAddressList> {

    private static final Logger LOG = LoggerFactory.getLogger(AddressPhoneJoiner.class);
    private MapState<Long, AddressRecord> addressState;
    private MapState<Long, List<PhoneRecord>> phoneListState;

    @Override
    public TypeInformation<CustomerAddressList> getProducedType() {
        return TypeInformation.of(CustomerAddressList.class);
    }

    @Override
    public void open(Configuration config) {
        addressState = getRuntimeContext().getMapState(
                new MapStateDescriptor<>("address-state",
                        TypeInformation.of(Long.class),
                        TypeInformation.of(AddressRecord.class)));

        phoneListState = getRuntimeContext().getMapState(
                new MapStateDescriptor<>("phone-list-state",
                        TypeInformation.of(Long.class),
                        TypeInformation.of(new TypeHint<List<PhoneRecord>>() {}))
        );
    }

    @Override
    public void processElement1(AddressRecord address, Context ctx, Collector<CustomerAddressList> out) throws Exception {
        Long addressId = address.getAfter().getAddressId();
        Long customerNumber = address.getAfter().getCustomerNumber();

        LOG.info("Processing address record - addressId: {}, customerNumber: {}", addressId, customerNumber);

        addressState.put(addressId, address);
        List<PhoneRecord> phones = phoneListState.get(addressId);

        if (phones != null) {
            LOG.info("Found {} phone records for address {}", phones.size(), addressId);
        } else {
            LOG.info("No phone records found for address {}", addressId);
        }

        emitCustomerAddressList(customerNumber, out);
    }

    @Override
    public void processElement2(AddressPhoneList phoneList, Context ctx, Collector<CustomerAddressList> out) throws Exception {
        Long addressId = phoneList.getAddressId();
        LOG.info("processElement2 - Received phone list for addressId: {} with {} phones",
                addressId, phoneList.getPhoneList().size());

        AddressRecord address = addressState.get(addressId);
        if (address != null) {
            LOG.info("Found matching address record for addressId: {}, customerNumber: {}",
                    addressId, address.getAfter().getCustomerNumber());
            phoneListState.put(addressId, phoneList.getPhoneList());
            emitCustomerAddressList(address.getAfter().getCustomerNumber(), out);
        } else {
            LOG.info("ERROR: No matching address record found for addressId: {}", addressId);
            // Debug state
            LOG.info("Current addresses in state: {}",
                    StreamSupport.stream(addressState.entries().spliterator(), false)
                            .map(entry -> String.format("addr_%d -> cust_%d",
                                    entry.getKey(),
                                    entry.getValue().getAfter().getCustomerNumber()))
                            .collect(Collectors.joining(", ")));
        }
    }

    private void emitCustomerAddressList(Long customerNumber, Collector<CustomerAddressList> out) throws Exception {
        List<CustomerAddressData> addressDataList = new ArrayList<>();
        int totalPhones = 0;

        for (AddressRecord address : addressState.values()) {
            if (address.getAfter().getCustomerNumber().equals(customerNumber)) {
                Long addressId = address.getAfter().getAddressId();
                List<PhoneRecord> phones = phoneListState.get(addressId);

                if (phones != null) {
                    totalPhones += phones.size();
                }

                addressDataList.add(new CustomerAddressData(address,
                        phones != null ? phones : new ArrayList<>()));
            }
        }

        if (!addressDataList.isEmpty()) {
            LOG.info("Emitting CustomerAddressList for customer {} with {} addresses and {} total phones",
                    customerNumber, addressDataList.size(), totalPhones);
            CustomerAddressList result = new CustomerAddressList(customerNumber, addressDataList);
            out.collect(result);
        } else {
            LOG.info("No addresses found for customer {}", customerNumber);
        }
    }
}