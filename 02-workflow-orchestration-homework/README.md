# NYC Taxi Data Pipeline: 2021 Data Extension

This guide provides the steps to extend our existing data pipeline to include NYC Taxi data for the year **2021** (January through July).

## 📋 Prerequisites

Before running the controller method, you **must** update the input validation in your core ingestion flow. Kestra enforces strict input rules, and by default, the course flows only allow 2019 and 2020.

**Update 04_postgres_taxi**
Find the `year` input and update the `values` list to include 2021:

```yaml
inputs:
  - id: year
    type: SELECT
    displayName: Select year
    values: ["2019", "2020", "2021"] # Added 2021
    defaults: "2019"
```
This method uses a "Controller" flow. Instead of manual clicking, we use nested loops to programmatically trigger 14 separate executions (2 taxi types × 7 months).
## The Orchestrator YAML
Create a new flow named ``04_taxi_2021_backfill_orchestrator`` in the  `zoomcamp` namespace:

```yaml
id: 04_taxi_2021_backfill_orchestrator
namespace: zoomcamp
description: |
  Automated orchestrator to loop over taxi types and 
  months for the 2021 backfill.

tasks:
  - id: taxi_loop
    type: io.kestra.plugin.core.flow.ForEach
    values: ["yellow", "green"]
    tasks:
      - id: month_loop
        type: io.kestra.plugin.core.flow.ForEach
        values: ["01", "02", "03", "04", "05", "06", "07"]
        tasks:
          - id: call_ingestion_subflow
            type: io.kestra.plugin.core.flow.Subflow
            flowId: 04_postgres_taxi
            namespace: zoomcamp
            inputs:
              taxi: "{{ parent.taskrun.value }}"
              year: "2021"
              month: "{{ taskrun.value }}"
            wait: true 
            transmitFailed: true
```
**Key Logic Explained**:
- `ForEach` Tasks: We nest the month loop inside the taxi loop.
- **Variable Scoping**:  `{{ parent.taskrun.value }}`: Correctly identifies the taxi color from the outer loop.
    - `{{ taskrun.value }}`: Identifies the specific month from the inner loop.
- `Subflow` Task: This acts as a bridge, sending these dynamic values as inputs to our main ingestion flow.

# Quiz Questions
Q1. Within the execution for Yellow Taxi data for the year 2020 and month 12: what is the uncompressed file size (i.e. the output file yellow_tripdata_2020-12.csv of the extract task)?
A: 128.3 MiB

Q2. What is the rendered value of the variable file when the inputs taxi is set to green, year is set to 2020, and month is set to 04 during execution?
A: green_tripdata_2020-04.csv

Q3. How many rows are there for the Yellow Taxi data for all CSV files in the year 2020?
A: 24,648,499

Q4. How many rows are there for the Green Taxi data for all CSV files in the year 2020?
A: 1,734,051

Q5. How many rows are there for the Yellow Taxi data for the March 2021 CSV file?
A: 1,925,152

Q6. How would you configure the timezone to New York in a Schedule trigger?
A: Add a timezone property set to America/New_York in the Schedule trigger configuration