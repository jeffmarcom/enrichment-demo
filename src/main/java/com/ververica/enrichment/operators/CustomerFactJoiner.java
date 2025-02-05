package com.ververica.enrichment.operators;

import com.ververica.enrichment.records.*;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.typeutils.ResultTypeQueryable;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.co.KeyedCoProcessFunction;
import org.apache.flink.util.Collector;

public class CustomerFactJoiner
        extends KeyedCoProcessFunction<Long, CustomerRecord, CustomerAddressList, CustomerFact>
        implements ResultTypeQueryable<CustomerFact> {

    private ValueState<CustomerRecord> customerState;
    private ValueState<CustomerAddressList> addressListState;

    @Override
    public TypeInformation<CustomerFact> getProducedType() {
        return TypeInformation.of(CustomerFact.class);
    }

    @Override
    public void open(Configuration config) {
        customerState = getRuntimeContext().getState(
                new ValueStateDescriptor<>("customer-state", CustomerRecord.class));

        addressListState = getRuntimeContext().getState(
                new ValueStateDescriptor<>("address-list-state",
                        CustomerAddressList.class));
    }

    @Override
    public void processElement1(CustomerRecord customer, Context ctx, Collector<CustomerFact> out) throws Exception {
        customerState.update(customer);
        CustomerAddressList addressList = addressListState.value();

        if (addressList != null) {
            CustomerFact fact = new CustomerFact(customer, addressList.getAddresses());
            out.collect(fact);
        }
    }

    @Override
    public void processElement2(CustomerAddressList addressList, Context ctx, Collector<CustomerFact> out) throws Exception {
        addressListState.update(addressList);
        CustomerRecord customer = customerState.value();

        if (customer != null) {
            CustomerFact fact = new CustomerFact(customer, addressList.getAddresses());
            out.collect(fact);
        }
    }
}