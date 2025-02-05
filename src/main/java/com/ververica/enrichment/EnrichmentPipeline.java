package com.ververica.enrichment;

import com.ververica.enrichment.config.JobConfig;
import com.ververica.enrichment.operators.*;
import com.ververica.enrichment.records.*;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.windowing.assigners.SlidingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.util.OutputTag;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;

public class EnrichmentPipeline {
    private static final Logger LOG = LoggerFactory.getLogger(EnrichmentPipeline.class);

    private final JobConfig config;
    private final OutputTag<CustomerFact> discardedUpdates =
            new OutputTag<CustomerFact>("discarded-updates"){};

    public EnrichmentPipeline(JobConfig config) {
        this.config = config;
    }

    public ProcessedStreams build(
            DataStream<CustomerRecord> customerStream,
            DataStream<AddressRecord> addressStream,
            DataStream<PhoneRecord> phoneStream) {

        // Kafka source will assign timestamps
        WatermarkStrategy<CustomerFact> watermarkStrategy = WatermarkStrategy
                .<CustomerFact>forBoundedOutOfOrderness(Duration.ofSeconds(config.getMaxOutOfOrderness()))
                .withIdleness(Duration.ofMinutes(1));

        // Aggregate phone records by address
        DataStream<AddressPhoneList> phoneListStream = phoneStream
                .keyBy((KeySelector<PhoneRecord, Long>) value -> value.getAfter().getAddressId(),
                        TypeInformation.of(Long.class))
                .process(new PhoneListAggregator())
                .uid("phone-aggregator")
                .name("Phone List Aggregation")
                .map(list -> {
                    LOG.info("Phone list before join - addressId: {}, phoneCount: {}",
                            list.getAddressId(), list.getPhoneList().size());
                    return list;
                });

        // Join addresses with phones using addressId
        DataStream<CustomerAddressList> addressListStream = addressStream
                .keyBy((KeySelector<AddressRecord, Long>) value -> value.getAfter().getAddressId(),
                        TypeInformation.of(Long.class))
                .connect(phoneListStream.keyBy(
                        (KeySelector<AddressPhoneList, Long>) value -> value.getAddressId(),
                        TypeInformation.of(Long.class)))
                .process(new AddressPhoneJoiner())
                .uid("address-phone-joiner")
                .name("Address Phone Joining");

        // Create enriched customer facts - join on customer number
        DataStream<CustomerFact> customerFactStream = customerStream
                .keyBy((KeySelector<CustomerRecord, Long>) value -> value.getAfter().getCustomerNumber(),
                        TypeInformation.of(Long.class))
                .connect(addressListStream.keyBy(
                        (KeySelector<CustomerAddressList, Long>) value -> value.getCustomerNumber(),
                        TypeInformation.of(Long.class)))
                .process(new CustomerFactJoiner())
                .uid("customer-fact-joiner")
                .name("Customer Fact Creation");

        // Apply window-based update limiting with watermarks
        SingleOutputStreamOperator<CustomerFact> limitedUpdates = customerFactStream
                .assignTimestampsAndWatermarks(watermarkStrategy)
                .keyBy(fact -> fact.getCustomer().getAfter().getCustomerNumber())
                .window(SlidingEventTimeWindows.of(
                        Time.seconds(config.getWindowSizeSeconds()),
                        Time.seconds(config.getWindowSlideSeconds())))
                .allowedLateness(Time.seconds(config.getAllowedLateness()))
                .process(new CustomerFactWindowLimiter(
                        config.getMaxUpdatesPerWindow(),
                        discardedUpdates))
                .uid("customer-fact-limiter")
                .name("Update Limiting");

        return new ProcessedStreams(
                limitedUpdates,                                  // Main output
                limitedUpdates.getSideOutput(discardedUpdates)  // Discarded updates
        );
    }

    public static class ProcessedStreams {
        private final DataStream<CustomerFact> mainStream;
        private final DataStream<CustomerFact> discardedStream;

        public ProcessedStreams(
                DataStream<CustomerFact> mainStream,
                DataStream<CustomerFact> discardedStream) {
            this.mainStream = mainStream;
            this.discardedStream = discardedStream;
        }

        public DataStream<CustomerFact> getMainStream() { return mainStream; }
        public DataStream<CustomerFact> getDiscardedStream() { return discardedStream; }
    }
}