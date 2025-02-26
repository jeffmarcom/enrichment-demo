package com.ververica.enrichment.producers;

import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.TableEnvironment;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class AddressDataIngest {
    private static final Logger LOG = LoggerFactory.getLogger(AddressDataIngest.class);

    public static void main(String[] args) throws Exception {
        // Parse command line arguments
        final ParameterTool params = ParameterTool.fromArgs(args);

        if (params.has("-h") || params.has("--help")) {
            System.err.println("Usage: AddressDataIngest --bootstrap-servers <servers> " +
                    "--source-address-topic <topic> [--records-per-second <num>]");
            return;
        }

        String bootstrapServers = params.get("bootstrap-servers", "localhost:9092");
        String sourceAddressTopic = params.get("source-address-topic", "data-address-01");
        int recordsPerSecond = params.getInt("records-per-second", 10);
        int numberOfRows = params.getInt("num-of-rows", 1000);  // Reduced from 10000

        // Set up the Table environment
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        EnvironmentSettings settings = EnvironmentSettings.newInstance().inStreamingMode().build();
        TableEnvironment tableEnv = StreamTableEnvironment.create(env, settings);

        // Create DataGen source table
        createAddressSourceTable(tableEnv, recordsPerSecond, numberOfRows);

        // Create Kafka sink table
        createAddressSinkTable(tableEnv, sourceAddressTopic, bootstrapServers);

        // Execute INSERT statement
        tableEnv.executeSql("INSERT INTO address_sink SELECT * FROM address_source");
    }

    private static void createAddressSourceTable(TableEnvironment tableEnv, int recordsPerSecond, int numberOfRows) {
        tableEnv.executeSql(String.format("""
        CREATE TABLE address_source (
            event_id STRING,
            tokens ROW<
                LAST_UPD_DATE STRING,
                SOURCE_SYS STRING
            >,
            op_table STRING,
            op_type STRING,
            op_ts STRING,
            after ROW<
                ADDRESS_ID BIGINT,
                CUSTOMER_NUMBER BIGINT,
                ADDRESS1 STRING,
                ADDRESS2 STRING,
                CITY STRING,
                STATE STRING,
                ZIP BIGINT,
                LOCATION_NUMBER INT
            >,
            PRIMARY KEY (event_id) NOT ENFORCED
        ) WITH (
            'connector' = 'datagen',
            'rows-per-second' = '%d',
            'number-of-rows' = '%d',
            
            'fields.event_id.kind' = 'random',
            'fields.event_id.length' = '32',
            
            'fields.tokens.LAST_UPD_DATE.length' = '19',
            'fields.tokens.LAST_UPD_DATE.var-len' = 'false',
            'fields.tokens.SOURCE_SYS.length' = '3',
            'fields.tokens.SOURCE_SYS.var-len' = 'false',
            
            'fields.op_table.kind' = 'random',
            'fields.op_table.length' = '10',
            'fields.op_type.kind' = 'random',
            'fields.op_type.length' = '6',
            'fields.op_ts.length' = '19',
            'fields.op_ts.var-len' = 'false',
            
            'fields.after.ADDRESS_ID.min' = '1000',
            'fields.after.ADDRESS_ID.max' = '1100',  // Narrower range for testing
            
            'fields.after.CUSTOMER_NUMBER.min' = '10000',
            'fields.after.CUSTOMER_NUMBER.max' = '10100',
            
            'fields.after.ADDRESS1.length' = '40',
            'fields.after.ADDRESS1.var-len' = 'true',
            'fields.after.ADDRESS2.length' = '40',
            'fields.after.ADDRESS2.var-len' = 'true',
            'fields.after.CITY.length' = '25',
            'fields.after.CITY.var-len' = 'true',
            'fields.after.STATE.length' = '2',
            'fields.after.STATE.var-len' = 'false',
            'fields.after.ZIP.min' = '10000',
            'fields.after.ZIP.max' = '99999',
            'fields.after.LOCATION_NUMBER.min' = '1',
            'fields.after.LOCATION_NUMBER.max' = '99'
        )
    """, recordsPerSecond, numberOfRows));
    }

    private static void createAddressSinkTable(TableEnvironment tableEnv, String addressTopic, String bootstrapServers) {
        tableEnv.executeSql(String.format("""
            CREATE TABLE address_sink (
                event_id STRING,
                tokens ROW<
                    LAST_UPD_DATE STRING,
                    SOURCE_SYS STRING
                >,
                op_table STRING,
                op_type STRING,
                op_ts STRING,
                after ROW<
                    ADDRESS_ID BIGINT,
                    CUSTOMER_NUMBER BIGINT,
                    ADDRESS1 STRING,
                    ADDRESS2 STRING,
                    CITY STRING,
                    STATE STRING,
                    ZIP BIGINT,
                    LOCATION_NUMBER INT
                >
            ) WITH (
                'connector' = 'kafka',
                'topic' = '%s',
                'properties.bootstrap.servers' = '%s',
                'format' = 'json',
                'key.format' = 'raw',
                'key.fields' = 'event_id',
                'value.format' = 'json'
            )
        """, addressTopic, bootstrapServers));
    }
}