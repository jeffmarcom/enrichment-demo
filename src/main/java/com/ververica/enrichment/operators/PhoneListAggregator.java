package com.ververica.enrichment.operators;

import com.ververica.enrichment.records.AddressPhoneList;
import com.ververica.enrichment.records.PhoneRecord;
import org.apache.flink.api.common.state.MapState;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.typeutils.ResultTypeQueryable;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.util.Collector;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;

public class PhoneListAggregator
        extends KeyedProcessFunction<Long, PhoneRecord, AddressPhoneList>
        implements ResultTypeQueryable<AddressPhoneList> {

    private static final Logger LOG = LoggerFactory.getLogger(PhoneListAggregator.class);
    private MapState<Long, List<PhoneRecord>> phoneListState;

    @Override
    public TypeInformation<AddressPhoneList> getProducedType() {
        return TypeInformation.of(AddressPhoneList.class);
    }

    @Override
    public void open(Configuration config) {
        phoneListState = getRuntimeContext().getMapState(
                new MapStateDescriptor<>("phone-list",
                        TypeInformation.of(Long.class),
                        TypeInformation.of(new TypeHint<List<PhoneRecord>>() {}))
        );
    }

    @Override
    public void processElement(PhoneRecord value, Context ctx, Collector<AddressPhoneList> out) throws Exception {
        Long addressId = value.getAfter().getAddressId();
        LOG.info("Processing phone record for addressId: {}", addressId);

        List<PhoneRecord> phones = phoneListState.get(addressId);
        if (phones == null) {
            LOG.info("Creating new phone list for addressId: {}", addressId);
            phones = new ArrayList<>();
        }

        phones.add(value);
        phoneListState.put(addressId, phones);
        LOG.info("Address {} now has {} phone records", addressId, phones.size());

        out.collect(new AddressPhoneList(addressId, phones));
    }
}