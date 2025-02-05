package com.ververica.enrichment.metrics;

import org.apache.flink.metrics.Counter;
import org.apache.flink.metrics.Gauge;
import org.apache.flink.metrics.MetricGroup;

import java.io.Serializable;

public class ThrottlingMetrics implements Serializable {
    private static final long serialVersionUID = 1L;

    private transient Counter recordsEmitted;
    private transient Counter recordsInBuffer;
    private transient Counter emergencyFlushCount;
    private transient Counter emergencyFlushedRecords;
    private transient Gauge<Double> bufferUtilization;

    public void initializeMetrics(MetricGroup metricGroup, int maxBufferSize) {
        recordsEmitted = metricGroup.counter("records_emitted");
        recordsInBuffer = metricGroup.counter("records_in_buffer");
        emergencyFlushCount = metricGroup.counter("emergency_flush_count");
        emergencyFlushedRecords = metricGroup.counter("emergency_flushed_records");
        bufferUtilization = metricGroup.gauge("buffer_utilization",
                () -> (double) recordsInBuffer.getCount() / maxBufferSize);
    }

    public Counter getRecordsEmitted() { return recordsEmitted; }
    public Counter getRecordsInBuffer() { return recordsInBuffer; }
    public Counter getEmergencyFlushCount() { return emergencyFlushCount; }
    public Counter getEmergencyFlushedRecords() { return emergencyFlushedRecords; }
    public Gauge<Double> getBufferUtilization() { return bufferUtilization; }
}