package com.ververica.enrichment.operators;

import com.ververica.enrichment.records.CustomerFact;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.metrics.Counter;
import org.apache.flink.metrics.MetricGroup;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;

public class CustomerFactWindowLimiter extends ProcessWindowFunction<CustomerFact, CustomerFact, Long, TimeWindow> {
    private static final Logger LOG = LoggerFactory.getLogger(CustomerFactWindowLimiter.class);
    private static final String OPERATOR_NAME = "customer-fact-limiter";

    private final int maxUpdatesPerWindow;
    private final OutputTag<CustomerFact> discardedUpdates;
    private transient Counter recordsEmitted;
    private transient Counter recordsDiscarded;

    public CustomerFactWindowLimiter(
            int maxUpdatesPerWindow,
            OutputTag<CustomerFact> discardedUpdates) {
        this.maxUpdatesPerWindow = maxUpdatesPerWindow;
        this.discardedUpdates = discardedUpdates;
    }

    @Override
    public void open(Configuration parameters) {
        MetricGroup metricGroup = getRuntimeContext().getMetricGroup().addGroup(OPERATOR_NAME);
        // These metric names match exactly with the Grafana dashboard queries
        recordsEmitted = metricGroup.counter("records_emitted");
        recordsDiscarded = metricGroup.counter("records_discarded");
    }

    @Override
    public void process(
            Long customerNumber,
            Context context,
            Iterable<CustomerFact> elements,
            Collector<CustomerFact> out) {

        List<CustomerFact> updates = new ArrayList<>();
        elements.forEach(updates::add);

        LOG.info("Processing window for customer {} with {} updates (max: {})",
                customerNumber, updates.size(), maxUpdatesPerWindow);

        // Emit allowed updates
        int emitCount = Math.min(maxUpdatesPerWindow, updates.size());
        LOG.info("Emitting {} updates for customer {}", emitCount, customerNumber);

        for (int i = 0; i < emitCount; i++) {
            out.collect(updates.get(i));
            recordsEmitted.inc();
        }

        // Send excess updates to side output
        if (updates.size() > maxUpdatesPerWindow) {
            int discardCount = updates.size() - maxUpdatesPerWindow;
            LOG.info("Discarding {} excess updates for customer {}", discardCount, customerNumber);

            for (int i = maxUpdatesPerWindow; i < updates.size(); i++) {
                context.output(discardedUpdates, updates.get(i));
                recordsDiscarded.inc();
            }
        }
    }
}