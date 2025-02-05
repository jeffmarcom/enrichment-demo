package com.ververica.enrichment.producers;

import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.TableEnvironment;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class CustomerDataIngest {
    private static final Logger LOG = LoggerFactory.getLogger(CustomerDataIngest.class);

    public static void main(String[] args) throws Exception {
        // Parse command line arguments
        final ParameterTool params = ParameterTool.fromArgs(args);

        if (params.has("-h") || params.has("--help")) {
            System.err.println("Usage: CustomerDataIngest --bootstrap-servers <servers> " +
                    "--source-customer-topic <topic> [--records-per-second <num>]");
            return;
        }


        String bootstrapServers = params.get("bootstrap-servers", "localhost:9092");
        String sourceCustomerTopic = params.get("source-customer-topic", "data-customer-01");
        int recordsPerSecond = params.getInt("records-per-second", 100);
        int numberOfRows = params.getInt("num-of-rows", 10000);

        // Set up the Table environment
        EnvironmentSettings settings = EnvironmentSettings.newInstance().build();
        TableEnvironment tableEnv = TableEnvironment.create(settings);

        // Create DataGen source table
        createCustomerSourceTable(tableEnv, recordsPerSecond, numberOfRows);

        // Create Kafka sink table
        createCustomerSinkTable(tableEnv, sourceCustomerTopic, bootstrapServers);

        // Execute INSERT statement
        tableEnv.executeSql("INSERT INTO customer_sink SELECT * FROM customer_source");
    }

    private static void createCustomerSourceTable(TableEnvironment tableEnv, int recordsPerSecond, int numberOfRows) {
        tableEnv.executeSql(String.format("""
        CREATE TABLE customer_source (
            event_id STRING,
            tokens ROW<
                LAST_UPD_DATE STRING,
                SOURCE_SYS STRING
            >,
            op_table STRING,
            op_type STRING,
            op_ts STRING,
            after ROW<
                CUSTOMER_NUMBER BIGINT,
                CUSTOMER_NAME STRING,
                CUSTOMER_ADD_DATE STRING,
                TYPE_OF_CUSTOMER STRING,
                CUSTOMER_TIER_CODE STRING,
                DISTRICT INT,
                SALES_ROUTE INT,
                DEPT_IND STRING,
                CUTOFF_ID INT,
                CUSTOMER_PRODUCT_LIST_ID STRING,
                DELIVERY_INSTR STRING,
                PRICE_BASIS STRING,
                ORDER_ENTRY_FORMAT STRING
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
            
            'fields.after.CUSTOMER_NUMBER.min' = '10000',
            'fields.after.CUSTOMER_NUMBER.max' = '10100',
            'fields.after.CUSTOMER_NAME.length' = '50',
            'fields.after.CUSTOMER_NAME.var-len' = 'true',
            'fields.after.CUSTOMER_ADD_DATE.length' = '19',
            'fields.after.CUSTOMER_ADD_DATE.var-len' = 'false',
            'fields.after.TYPE_OF_CUSTOMER.length' = '10',
            'fields.after.TYPE_OF_CUSTOMER.var-len' = 'false',
            'fields.after.CUSTOMER_TIER_CODE.length' = '3',
            'fields.after.CUSTOMER_TIER_CODE.var-len' = 'false',
            'fields.after.DISTRICT.min' = '1',
            'fields.after.DISTRICT.max' = '99',
            'fields.after.SALES_ROUTE.min' = '100',
            'fields.after.SALES_ROUTE.max' = '999',
            'fields.after.DEPT_IND.length' = '1',
            'fields.after.DEPT_IND.var-len' = 'false',
            'fields.after.CUTOFF_ID.min' = '1',
            'fields.after.CUTOFF_ID.max' = '99',
            'fields.after.CUSTOMER_PRODUCT_LIST_ID.length' = '10',
            'fields.after.CUSTOMER_PRODUCT_LIST_ID.var-len' = 'false',
            'fields.after.DELIVERY_INSTR.length' = '100',
            'fields.after.DELIVERY_INSTR.var-len' = 'true',
            'fields.after.PRICE_BASIS.length' = '10',
            'fields.after.PRICE_BASIS.var-len' = 'false',
            'fields.after.ORDER_ENTRY_FORMAT.length' = '10',
            'fields.after.ORDER_ENTRY_FORMAT.var-len' = 'false'
        )
    """, recordsPerSecond, numberOfRows));
    }

    private static void createCustomerSinkTable(TableEnvironment tableEnv, String customerTopic, String bootstrapServers) {
        tableEnv.executeSql(String.format("""
            CREATE TABLE customer_sink (
                event_id STRING,
                tokens ROW<
                    LAST_UPD_DATE STRING,
                    SOURCE_SYS STRING
                >,
                op_table STRING,
                op_type STRING,
                op_ts STRING,
                after ROW<
                    CUSTOMER_NUMBER BIGINT,
                    CUSTOMER_NAME STRING,
                    CUSTOMER_ADD_DATE STRING,
                    TYPE_OF_CUSTOMER STRING,
                    CUSTOMER_TIER_CODE STRING,
                    DISTRICT INT,
                    SALES_ROUTE INT,
                    DEPT_IND STRING,
                    CUTOFF_ID INT,
                    CUSTOMER_PRODUCT_LIST_ID STRING,
                    DELIVERY_INSTR STRING,
                    PRICE_BASIS STRING,
                    ORDER_ENTRY_FORMAT STRING
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
        """, customerTopic, bootstrapServers));
    }
}