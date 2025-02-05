package com.ververica.enrichment;

import com.ververica.enrichment.config.JobConfig;
import com.ververica.enrichment.factories.KafkaFactory;
import com.ververica.enrichment.records.*;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.ObjectMapper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class CustomerEnrichment {
    private static final Logger LOG = LoggerFactory.getLogger(CustomerEnrichment.class);

    public static void main(String[] args) throws Exception {
        JobConfig config = JobConfig.fromArgs(args);

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.enableCheckpointing(60000);
        env.setParallelism(config.getParallelism());

        // Create source streams
        DataStream<CustomerRecord> customerStream = KafkaFactory.createSource(
                env, config.getBootstrapServers(), config.getSourceCustomerTopic(), CustomerRecord.class);

        DataStream<AddressRecord> addressStream = KafkaFactory.createSource(
                env, config.getBootstrapServers(), config.getSourceAddressTopic(), AddressRecord.class);

        DataStream<PhoneRecord> phoneStream = KafkaFactory.createSource(
                env, config.getBootstrapServers(), config.getSourcePhoneTopic(), PhoneRecord.class);

        LOG.info("Starting pipeline build with window size: {}s, slide: {}s, max updates: {}",
                config.getWindowSizeSeconds(),
                config.getWindowSlideSeconds(),
                config.getMaxUpdatesPerWindow());

        // Build the enrichment pipeline
        EnrichmentPipeline pipeline = new EnrichmentPipeline(config);
        EnrichmentPipeline.ProcessedStreams streams = pipeline.build(customerStream, addressStream, phoneStream);

        // Add debug prints before sinks
        streams.getMainStream()
                .map(fact -> {
                    LOG.info("Main sink received fact for customer: {}",
                            fact.getCustomer().getAfter().getCustomerNumber());
                    return fact;
                });

        streams.getDiscardedStream()
                .map(fact -> {
                    LOG.info("Discard sink received fact for customer: {}",
                            fact.getCustomer().getAfter().getCustomerNumber());
                    return fact;
                });

        // Set up sinks
        ObjectMapper mapper = new ObjectMapper();

        // Main output sink
        streams.getMainStream()
                .map(fact -> mapper.writeValueAsString(fact))
                .returns(TypeInformation.of(String.class))
                .sinkTo(KafkaFactory.createSink(config.getBootstrapServers(), config.getSinkTopic()))
                .uid("customer-fact-sink")
                .name("Main Output Sink");

        // Discarded updates sink
        streams.getDiscardedStream()
                .map(fact -> mapper.writeValueAsString(fact))
                .returns(TypeInformation.of(String.class))
                .sinkTo(KafkaFactory.createSink(config.getBootstrapServers(), config.getDiscardTopic()))
                .uid("discarded-updates-sink")
                .name("Discarded Updates Sink");

        env.execute("Customer Enrichment Job");
    }
}