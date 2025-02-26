# Customer Data Enrichment Pipeline w/ Window-Based Update Limiting

A Flink application that enriches customer data by combining information from multiple Kafka topics and applying window-based update limiting to prevent downstream system overload.

## Overview

This project processes customer data through several stages:
1. Ingests customer, address, and phone data from separate Kafka topics
2. Aggregates phone records by address
3. Joins addresses with their associated phone numbers
4. Creates enriched customer facts by combining all the information
5. Applies window-based update limiting per customer
6. Outputs to three Kafka topics:
    - Main topic: Accepted updates within window limits
    - Discard topic: Updates exceeding window limits
   
## Architecture

```
┌──────────────┐     ┌──────────────┐     ┌──────────────┐
│ Customer     │     │ Address      │     │ Phone        │
│ Topic        │     │ Topic        │     │ Topic        │
└──────┬───────┘     └──────┬───────┘     └──────┬───────┘
       │                    │                    │
       │                    │                    │
       ▼                    ▼                    ▼
┌──────────────────────────────────────────────────────┐
│                                                      │
│               Enrichment Pipeline                    │
│                                                      │
│  ┌────────────┐     ┌─────────────┐                  │
│  │   Phone    │     │  Address    │                  │
│  │Aggregation │────▶│    Join     │                  │
│  └────────────┘     └──────┬──────┘                  │
│                            │                         │
│                            ▼                         │
│                    ┌──────────────┐                  │
│                    │  Customer    │                  │
│                    │    Fact      │                  │
│                    └──────┬───────┘                  │
│                           │                          │
│                    ┌──────▼───────┐                  │
│                    │   Window     │                  │
│                    │   Limiter    │                  │
│                    └──┬───────┬───┘                  │
│                       │       │                      │
└───────────────────────┼───────┼──────────────────────┘
                        │       │
                        ▼       ▼
           ┌────────────────┐  ┌────────────────┐
           │ Main Topic     │  │ Discard Topic  │
           └────────────────┘  └────────────────┘

```

## Window-Based Update Limiting Strategy

The pipeline uses Flink's built-in windowing capabilities to limit updates:

- **Window Configuration**
    - Sliding event-time windows
    - Configurable window size and slide interval
    - Watermark strategy for handling out-of-order events

- **Update Limiting**
    - Each customer is limited to N updates per window
    - Updates are processed in timestamp order within windows
    - Excess updates are redirected to a discard topic

### Configuration
Configuration is handled through command-line arguments:
```
--bootstrap-servers        Kafka bootstrap servers
--source-customer-topic    Source topic for customer data
--source-address-topic     Source topic for address data
--source-phone-topic       Source topic for phone data
--sink-topic               Main destination topic for enriched data
--discard-topic            Topic for updates exceeding window limit
--late-data-topic          Topic for late-arriving updates
--window-size-seconds      Window size for update limiting
--window-slide-seconds     Window slide interval
--max-updates-window       Maximum updates per customer per window
--max-out-of-orderness     Maximum out of orderness for watermarks
--parallelism              Job parallelism
```

### Monitoring
The application exposes several metrics through Flink's metrics system:
- Updates per window (accepted vs discarded)
- Window utilization
- Updates per subtask
- Total discarded updates

A Grafana dashboard is provided in `src/dashboards/grafana-windowed-enrichment.json`

## Usage

### Build
```bash
mvn clean package
```

### Run
```bash
flink run -c com.ververica.enrichment.CustomerEnrichment target/customer-enrichment-1.0-SNAPSHOT.jar \
  --bootstrap-servers localhost:9092 \
  --source-customer-topic data-customer-01 \
  --source-address-topic data-address-01 \
  --source-phone-topic data-phone-01 \
  --sink-topic data-fct-customer-01 \
  --discard-topic data-fct-customer-discarded-01 \
  --late-data-topic data-fct-customer-late-01 \
  --window-size-seconds 300 \
  --window-slide-seconds 60 \
  --max-updates-window 10 \
  --max-out-of-orderness 30
```

## Development

### Project Structure
```
com.ververica.enrichment/
├── CustomerEnrichment.java     # Main application entry point
├── EnrichmentPipeline.java     # Pipeline assembly
├── config/                     # Configuration handling
├── factories/                  # Kafka source/sink creation
├── metrics/                    # Monitoring metrics
├── operators/                  # Stream processing operators
├── producers/                  # Test data producers
└── records/                    # Data model classes
```