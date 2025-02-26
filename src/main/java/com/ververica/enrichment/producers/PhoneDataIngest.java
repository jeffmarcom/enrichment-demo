package com.ververica.enrichment.producers;

import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.TableEnvironment;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class PhoneDataIngest {
    private static final Logger LOG = LoggerFactory.getLogger(PhoneDataIngest.class);

    public static void main(String[] args) throws Exception {
        // Parse command line arguments
        final ParameterTool params = ParameterTool.fromArgs(args);

        if (params.has("-h") || params.has("--help")) {
            System.err.println("Usage: PhoneDataIngest --bootstrap-servers <servers> " +
                    "--source-phone-topic <topic> [--records-per-second <num>]");
            return;
        }

        String bootstrapServers = params.get("bootstrap-servers", "localhost:9092");
        String sourcePhoneTopic = params.get("source-phone-topic", "data-phone-01");
        int recordsPerSecond = params.getInt("records-per-second", 10);
        int numberOfRows = params.getInt("num-of-rows", 1000);  // Reduced from 10000

        // Set up the Table environment
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        EnvironmentSettings settings = EnvironmentSettings.newInstance().inStreamingMode().build();
        TableEnvironment tableEnv = StreamTableEnvironment.create(env, settings);

        // Create DataGen source table
        createPhoneSourceTable(tableEnv, recordsPerSecond, numberOfRows);

        // Create Kafka sink table
        createPhoneSinkTable(tableEnv, sourcePhoneTopic, bootstrapServers);

        // Execute INSERT statement
        tableEnv.executeSql("INSERT INTO phone_sink SELECT * FROM phone_source");
    }

    private static void createPhoneSourceTable(TableEnvironment tableEnv, int recordsPerSecond, int numberOfRows) {
        tableEnv.executeSql(String.format("""
                    CREATE TABLE phone_source (
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
                            CONTACT_NAME STRING,
                            TELEPHONE_NUMBER STRING,
                            TELEPHONE_TYPE STRING
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
                        
                        'fields.after.CONTACT_NAME.length' = '30',
                        'fields.after.CONTACT_NAME.var-len' = 'true',
                        'fields.after.TELEPHONE_NUMBER.length' = '14',
                        'fields.after.TELEPHONE_NUMBER.var-len' = 'false',
                        'fields.after.TELEPHONE_TYPE.length' = '6',
                        'fields.after.TELEPHONE_TYPE.var-len' = 'false'
                    )
                """, recordsPerSecond, numberOfRows));
    }

    private static void createPhoneSinkTable(TableEnvironment tableEnv, String phoneTopic, String bootstrapServers) {
        tableEnv.executeSql(String.format("""
            CREATE TABLE phone_sink (
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
                    CONTACT_NAME STRING,
                    TELEPHONE_NUMBER STRING,
                    TELEPHONE_TYPE STRING
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
        """, phoneTopic, bootstrapServers));
    }
}