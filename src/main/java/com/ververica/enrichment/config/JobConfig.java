package com.ververica.enrichment.config;

import org.apache.flink.api.java.utils.ParameterTool;

public class JobConfig {
    private final String bootstrapServers;
    private final String sourceCustomerTopic;
    private final String sourceAddressTopic;
    private final String sourcePhoneTopic;
    private final String sinkTopic;
    private final String discardTopic;
    private final int windowSizeSeconds;
    private final int windowSlideSeconds;
    private final int maxUpdatesPerWindow;
    private final int maxBufferSize;
    private final int allowedLateness;
    private final int maxOutOfOrderness;
    private final int parallelism;

    private JobConfig(ParameterTool params) {
        this.bootstrapServers = params.get("bootstrap-servers", "localhost:9092");
        this.sourceCustomerTopic = params.get("source-customer-topic", "data-customer-01");
        this.sourceAddressTopic = params.get("source-address-topic", "data-address-01");
        this.sourcePhoneTopic = params.get("source-phone-topic", "data-phone-01");
        this.sinkTopic = params.get("sink-topic", "data-fct-customer-01");
        this.discardTopic = params.get("discard-topic", "data-fct-customer-discarded-01");

        // Window configuration
        this.windowSizeSeconds = params.getInt("window-size-seconds", 300);  // 5 minutes default
        this.windowSlideSeconds = params.getInt("window-slide-seconds", 60);  // 1 minute default
        this.maxUpdatesPerWindow = params.getInt("max-updates-window", 10);
        this.maxBufferSize = params.getInt("max-buffer-size", 1000);

        // Event time configuration
        this.allowedLateness = params.getInt("allowed-lateness", 60);  // 1 minute default
        this.maxOutOfOrderness = params.getInt("max-out-of-orderness", 30);

        this.parallelism = params.getInt("parallelism", 1);
    }

    public static JobConfig fromArgs(String[] args) {
        final ParameterTool params = ParameterTool.fromArgs(args);
        if (params.has("-h") || params.has("--help")) {
            printHelp();
            System.exit(0);
        }
        return new JobConfig(params);
    }

    private static void printHelp() {
        System.err.println(
                "Usage: CustomerEnrichment\n" +
                        "  Required:\n" +
                        "    --bootstrap-servers <servers>        Kafka bootstrap servers\n" +
                        "    --source-customer-topic <topic>      Source topic for customer data\n" +
                        "    --source-address-topic <topic>       Source topic for address data\n" +
                        "    --source-phone-topic <topic>         Source topic for phone data\n" +
                        "    --sink-topic <topic>                 Main destination topic\n" +
                        "\n" +
                        "  Optional:\n" +
                        "    --discard-topic <topic>              Topic for discarded updates\n" +
                        "    --window-size-seconds <seconds>      Window size (default: 300)\n" +
                        "    --window-slide-seconds <seconds>     Window slide interval (default: 60)\n" +
                        "    --max-updates-window <count>         Max updates per window (default: 10)\n" +
                        "    --max-buffer-size <size>             Max buffer size (default: 1000)\n" +
                        "    --allowed-lateness <seconds>         Max allowed lateness (default: 60)\n" +
                        "    --max-out-of-orderness <seconds>     Max out of orderness (default: 30)\n" +
                        "    --parallelism <num>                  Job parallelism (default: 1)\n"
        );
    }

    // Getters
    public String getBootstrapServers() { return bootstrapServers; }
    public String getSourceCustomerTopic() { return sourceCustomerTopic; }
    public String getSourceAddressTopic() { return sourceAddressTopic; }
    public String getSourcePhoneTopic() { return sourcePhoneTopic; }
    public String getSinkTopic() { return sinkTopic; }
    public String getDiscardTopic() { return discardTopic; }
    public int getWindowSizeSeconds() { return windowSizeSeconds; }
    public int getWindowSlideSeconds() { return windowSlideSeconds; }
    public int getMaxUpdatesPerWindow() { return maxUpdatesPerWindow; }
    public int getMaxBufferSize() { return maxBufferSize; }
    public int getAllowedLateness() { return allowedLateness; }
    public int getMaxOutOfOrderness() { return maxOutOfOrderness; }
    public int getParallelism() { return parallelism; }
}