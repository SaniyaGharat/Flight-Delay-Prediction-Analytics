# PowerBI Star-Schema Data Model & Architecture Documentation

## 1. Dimensional Architecture & Star Schema Design

The PowerBI analytical dataset is structured as a conformed dimensional star schema. To support interactive exploration of 5.82 million flights with sub-second response times in PowerBI Desktop and Service, the raw record-level data has been pre-aggregated into additive, atomic fact tables aligned along conformed business dimensions.

### Core Design Principles
1. **Strictly Additive Measures**: Fact tables store base integer and double sums (`total_flights`, `flown_flights`, `delayed_arr_15`, `sum_arr_delay_min`). Precomputed ratios are intentionally excluded from facts to ensure correct mathematical rollups across dynamic slicer contexts.
2. **Safe DIVIDE DAX Ratios**: All rates, shares, and averages are defined via DAX measures using `DIVIDE(numerator, denominator, 0)`.
3. **Zero Orphan Foreign Keys**: Airport codes without mappings in October are standardized to `'UNMAPPED'`, which maps to a dedicated dimension entity in `dim_airport`.
4. **Bi-Format Export**: All tables are dual-published as Snappy-compressed Parquet in HDFS (`processed/powerbi/`) and single-file UTF-8 CSVs in `output/powerbi/`.

---

## 2. Table Relationships & Role-Playing Dimensions

| From (Fact Table) | Foreign Key | To (Dimension Table) | Primary Key | Cardinality | Active / Inactive | Operational Relationship |
| :--- | :--- | :--- | :--- | :--- | :--- | :--- |
| `fact_daily_airline` | `FLIGHT_DATE` | `dim_date` | `DATE` | Many-to-One (*:1) | **Active** | Flight operation date |
| `fact_daily_airline` | `AIRLINE` | `dim_airline` | `code` | Many-to-One (*:1) | **Active** | Operating air carrier |
| `fact_daily_origin` | `FLIGHT_DATE` | `dim_date` | `DATE` | Many-to-One (*:1) | **Active** | Flight departure date |
| `fact_daily_origin` | `ORIGIN_AIRPORT` | `dim_airport` | `code` | Many-to-One (*:1) | **Active** | Origin departure airport |
| `fact_daily_dest` | `FLIGHT_DATE` | `dim_date` | `DATE` | Many-to-One (*:1) | **Active** | Flight arrival date |
| `fact_daily_dest` | `DEST_AIRPORT` | `dim_airport` | `code` | Many-to-One (*:1) | *Inactive* | Role-playing destination airport (activated via `USERELATIONSHIP`) |
| `fact_airline_dow_hour` | `AIRLINE` | `dim_airline` | `code` | Many-to-One (*:1) | **Active** | Carrier schedule aggregation |
| `fact_route_month` | `ORIGIN` | `dim_airport` | `code` | Many-to-One (*:1) | **Active** | Route origin endpoint |
| `fact_route_month` | `DEST` | `dim_airport` | `code` | Many-to-One (*:1) | *Inactive* | Route destination endpoint |
| `fact_cause_daily_airline` | `FLIGHT_DATE` | `dim_date` | `DATE` | Many-to-One (*:1) | **Active** | Date of delay occurrence |
| `fact_cause_daily_airline` | `AIRLINE` | `dim_airline` | `code` | Many-to-One (*:1) | **Active** | Attributed carrier |
| `fact_cancel_daily_airline_reason` | `FLIGHT_DATE` | `dim_date` | `DATE` | Many-to-One (*:1) | **Active** | Date of cancellation |
| `fact_cancel_daily_airline_reason` | `AIRLINE` | `dim_airline` | `code` | Many-to-One (*:1) | **Active** | Carrier cancelling flight |

### Role-Playing Dimension Pattern for `dim_airport`
Because flights have two airport endpoints (Origin and Destination), `dim_airport` functions as a **Role-Playing Dimension**:
- **Origin Airport**: The relationship between `fact_daily_origin[ORIGIN_AIRPORT]` and `dim_airport[code]` is **Active** by default.
- **Destination Airport**: The relationship between `fact_daily_dest[DEST_AIRPORT]` and `dim_airport[code]` is configured as **Inactive** in the model. In DAX measures focusing on arrival airports, the relationship is programmatically activated using `USERELATIONSHIP`:
  ```dax
  Flights by Destination Airport = 
  CALCULATE(
      SUM(fact_daily_dest[total_flights]),
      USERELATIONSHIP(fact_daily_dest[DEST_AIRPORT], dim_airport[code])
  )
  ```

---

## 3. Data Dictionary (Generated from Actual Schemas)

### Table: `dim_date`
- **Description**: Dimension: Calendar Date
- **Row Count**: 365
- **Parquet Storage**: `hdfs://localhost:9000/user/saniya/flight_delay/processed/powerbi/dim_date`
- **Local CSV File**: `output/powerbi/dim_date.csv`

| Column Name | Data Type | Nullable | Description / Business Meaning |
| :--- | :--- | :--- | :--- |
| `DATE` | `Date` | YES | ISO calendar date (YYYY-MM-DD), primary date join key |
| `YEAR` | `Integer` | YES | Dimensional key or attribute |
| `MONTH` | `Integer` | YES | Calendar month integer (1 to 12) |
| `MONTH_NAME` | `String` | YES | Dimensional key or attribute |
| `DAY` | `Integer` | YES | Dimensional key or attribute |
| `DAY_OF_WEEK` | `Integer` | YES | Day of week integer (1=Monday to 7=Sunday) |
| `DAY_OF_WEEK_NAME` | `String` | YES | Dimensional key or attribute |
| `WEEK_OF_YEAR` | `Integer` | YES | Dimensional key or attribute |
| `IS_WEEKEND` | `Integer` | YES | Dimensional key or attribute |
| `IS_HOLIDAY_WINDOW` | `Integer` | YES | Dimensional key or attribute |

### Table: `dim_airline`
- **Description**: Dimension: Commercial Air Carrier
- **Row Count**: 14
- **Parquet Storage**: `hdfs://localhost:9000/user/saniya/flight_delay/processed/powerbi/dim_airline`
- **Local CSV File**: `output/powerbi/dim_airline.csv`

| Column Name | Data Type | Nullable | Description / Business Meaning |
| :--- | :--- | :--- | :--- |
| `code` | `String` | YES | Standard IATA code or dimension primary key |
| `name` | `String` | YES | Full business name of carrier or airport |

### Table: `dim_airport`
- **Description**: Dimension: Commercial Airport (Role-Playing)
- **Row Count**: 323
- **Parquet Storage**: `hdfs://localhost:9000/user/saniya/flight_delay/processed/powerbi/dim_airport`
- **Local CSV File**: `output/powerbi/dim_airport.csv`

| Column Name | Data Type | Nullable | Description / Business Meaning |
| :--- | :--- | :--- | :--- |
| `code` | `String` | YES | Standard IATA code or dimension primary key |
| `name` | `String` | YES | Full business name of carrier or airport |
| `city` | `String` | YES | Dimensional key or attribute |
| `state` | `String` | YES | Dimensional key or attribute |
| `lat` | `Double` | YES | Dimensional key or attribute |
| `lon` | `Double` | YES | Dimensional key or attribute |
| `HAS_COORDS` | `Integer` | YES | Dimensional key or attribute |

### Table: `fact_daily_airline`
- **Description**: Fact: Daily Carrier Performance
- **Row Count**: 4,926
- **Parquet Storage**: `hdfs://localhost:9000/user/saniya/flight_delay/processed/powerbi/fact_daily_airline`
- **Local CSV File**: `output/powerbi/fact_daily_airline.csv`

| Column Name | Data Type | Nullable | Description / Business Meaning |
| :--- | :--- | :--- | :--- |
| `FLIGHT_DATE` | `Date` | YES | ISO calendar date (YYYY-MM-DD), primary date join key |
| `AIRLINE` | `String` | YES | Standard IATA code or dimension primary key |
| `total_flights` | `Long` | YES | Total scheduled flight operations count |
| `flown_flights` | `Long` | YES | Successfully completed flight count (cancelled=0 and diverted=0) |
| `cancelled_flights` | `Long` | YES | Cancelled flight count |
| `diverted_flights` | `Long` | YES | Diverted flight count |
| `delayed_arr_15` | `Long` | YES | Flights arriving 15+ minutes behind schedule |
| `delayed_dep_15` | `Long` | YES | Flights departing 15+ minutes behind schedule |
| `sum_arr_delay_min` | `Double` | YES | Sum of arrival delay minutes across flown flights |
| `sum_dep_delay_min` | `Double` | YES | Sum of departure delay minutes across scheduled flights |
| `sum_delay_minutes_pos` | `Double` | YES | Sum of positive arrival delay minutes (for severity metrics) |
| `sum_cause_air_system` | `Double` | YES | Dimensional key or attribute |
| `sum_cause_security` | `Double` | YES | Dimensional key or attribute |
| `sum_cause_airline` | `Double` | YES | Dimensional key or attribute |
| `sum_cause_late_aircraft` | `Double` | YES | Dimensional key or attribute |
| `sum_cause_weather` | `Double` | YES | Dimensional key or attribute |

### Table: `fact_daily_origin`
- **Description**: Fact: Daily Origin Airport Operations
- **Row Count**: 110,891
- **Parquet Storage**: `hdfs://localhost:9000/user/saniya/flight_delay/processed/powerbi/fact_daily_origin`
- **Local CSV File**: `output/powerbi/fact_daily_origin.csv`

| Column Name | Data Type | Nullable | Description / Business Meaning |
| :--- | :--- | :--- | :--- |
| `FLIGHT_DATE` | `Date` | YES | ISO calendar date (YYYY-MM-DD), primary date join key |
| `ORIGIN_AIRPORT` | `String` | YES | Standard IATA code or dimension primary key |
| `total_flights` | `Long` | YES | Total scheduled flight operations count |
| `flown_flights` | `Long` | YES | Successfully completed flight count (cancelled=0 and diverted=0) |
| `cancelled_flights` | `Long` | YES | Cancelled flight count |
| `diverted_flights` | `Long` | YES | Diverted flight count |
| `delayed_arr_15` | `Long` | YES | Flights arriving 15+ minutes behind schedule |
| `delayed_dep_15` | `Long` | YES | Flights departing 15+ minutes behind schedule |
| `sum_arr_delay_min` | `Double` | YES | Sum of arrival delay minutes across flown flights |
| `sum_dep_delay_min` | `Double` | YES | Sum of departure delay minutes across scheduled flights |
| `sum_delay_minutes_pos` | `Double` | YES | Sum of positive arrival delay minutes (for severity metrics) |
| `sum_cause_air_system` | `Double` | YES | Dimensional key or attribute |
| `sum_cause_security` | `Double` | YES | Dimensional key or attribute |
| `sum_cause_airline` | `Double` | YES | Dimensional key or attribute |
| `sum_cause_late_aircraft` | `Double` | YES | Dimensional key or attribute |
| `sum_cause_weather` | `Double` | YES | Dimensional key or attribute |

### Table: `fact_daily_dest`
- **Description**: Fact: Daily Destination Airport Arrivals
- **Row Count**: 110,908
- **Parquet Storage**: `hdfs://localhost:9000/user/saniya/flight_delay/processed/powerbi/fact_daily_dest`
- **Local CSV File**: `output/powerbi/fact_daily_dest.csv`

| Column Name | Data Type | Nullable | Description / Business Meaning |
| :--- | :--- | :--- | :--- |
| `FLIGHT_DATE` | `Date` | YES | ISO calendar date (YYYY-MM-DD), primary date join key |
| `DEST_AIRPORT` | `String` | YES | Standard IATA code or dimension primary key |
| `total_flights` | `Long` | YES | Total scheduled flight operations count |
| `flown_flights` | `Long` | YES | Successfully completed flight count (cancelled=0 and diverted=0) |
| `cancelled_flights` | `Long` | YES | Cancelled flight count |
| `diverted_flights` | `Long` | YES | Diverted flight count |
| `delayed_arr_15` | `Long` | YES | Flights arriving 15+ minutes behind schedule |
| `delayed_dep_15` | `Long` | YES | Flights departing 15+ minutes behind schedule |
| `sum_arr_delay_min` | `Double` | YES | Sum of arrival delay minutes across flown flights |
| `sum_dep_delay_min` | `Double` | YES | Sum of departure delay minutes across scheduled flights |
| `sum_delay_minutes_pos` | `Double` | YES | Sum of positive arrival delay minutes (for severity metrics) |
| `sum_cause_air_system` | `Double` | YES | Dimensional key or attribute |
| `sum_cause_security` | `Double` | YES | Dimensional key or attribute |
| `sum_cause_airline` | `Double` | YES | Dimensional key or attribute |
| `sum_cause_late_aircraft` | `Double` | YES | Dimensional key or attribute |
| `sum_cause_weather` | `Double` | YES | Dimensional key or attribute |

### Table: `fact_airline_dow_hour`
- **Description**: Fact: Airline Schedule Profile by DOW & Hour
- **Row Count**: 2,069
- **Parquet Storage**: `hdfs://localhost:9000/user/saniya/flight_delay/processed/powerbi/fact_airline_dow_hour`
- **Local CSV File**: `output/powerbi/fact_airline_dow_hour.csv`

| Column Name | Data Type | Nullable | Description / Business Meaning |
| :--- | :--- | :--- | :--- |
| `AIRLINE` | `String` | YES | Standard IATA code or dimension primary key |
| `DAY_OF_WEEK` | `Integer` | YES | Day of week integer (1=Monday to 7=Sunday) |
| `SCHED_DEP_HOUR` | `Integer` | YES | Scheduled departure clock hour (0 to 23) |
| `total_flights` | `Long` | YES | Total scheduled flight operations count |
| `flown_flights` | `Long` | YES | Successfully completed flight count (cancelled=0 and diverted=0) |
| `cancelled_flights` | `Long` | YES | Cancelled flight count |
| `diverted_flights` | `Long` | YES | Diverted flight count |
| `delayed_arr_15` | `Long` | YES | Flights arriving 15+ minutes behind schedule |
| `delayed_dep_15` | `Long` | YES | Flights departing 15+ minutes behind schedule |
| `sum_arr_delay_min` | `Double` | YES | Sum of arrival delay minutes across flown flights |
| `sum_dep_delay_min` | `Double` | YES | Sum of departure delay minutes across scheduled flights |
| `sum_delay_minutes_pos` | `Double` | YES | Sum of positive arrival delay minutes (for severity metrics) |

### Table: `fact_route_month`
- **Description**: Fact: Monthly Route Delay & Volume Profile
- **Row Count**: 48,782
- **Parquet Storage**: `hdfs://localhost:9000/user/saniya/flight_delay/processed/powerbi/fact_route_month`
- **Local CSV File**: `output/powerbi/fact_route_month.csv`

| Column Name | Data Type | Nullable | Description / Business Meaning |
| :--- | :--- | :--- | :--- |
| `ROUTE` | `String` | YES | Airport pair identifier formatted as ORIGIN-DEST (e.g., ORD-LAX) |
| `ORIGIN` | `String` | YES | Standard IATA code or dimension primary key |
| `DEST` | `String` | YES | Standard IATA code or dimension primary key |
| `MONTH` | `Integer` | YES | Calendar month integer (1 to 12) |
| `total_flights` | `Long` | YES | Total scheduled flight operations count |
| `flown_flights` | `Long` | YES | Successfully completed flight count (cancelled=0 and diverted=0) |
| `cancelled_flights` | `Long` | YES | Cancelled flight count |
| `diverted_flights` | `Long` | YES | Diverted flight count |
| `delayed_arr_15` | `Long` | YES | Flights arriving 15+ minutes behind schedule |
| `delayed_dep_15` | `Long` | YES | Flights departing 15+ minutes behind schedule |
| `sum_arr_delay_min` | `Double` | YES | Sum of arrival delay minutes across flown flights |
| `sum_dep_delay_min` | `Double` | YES | Sum of departure delay minutes across scheduled flights |
| `sum_delay_minutes_pos` | `Double` | YES | Sum of positive arrival delay minutes (for severity metrics) |

### Table: `fact_cause_daily_airline`
- **Description**: Fact: Daily Carrier Delay Cause Breakdown (Long)
- **Row Count**: 20,024
- **Parquet Storage**: `hdfs://localhost:9000/user/saniya/flight_delay/processed/powerbi/fact_cause_daily_airline`
- **Local CSV File**: `output/powerbi/fact_cause_daily_airline.csv`

| Column Name | Data Type | Nullable | Description / Business Meaning |
| :--- | :--- | :--- | :--- |
| `FLIGHT_DATE` | `Date` | YES | ISO calendar date (YYYY-MM-DD), primary date join key |
| `AIRLINE` | `String` | YES | Standard IATA code or dimension primary key |
| `CAUSE` | `String` | YES | FAA delay cause category (Air System, Security, Airline, Late Aircraft, Weather) |
| `MINUTES` | `Double` | YES | Total attributed delay minutes for specified cause |
| `FLIGHTS` | `Long` | YES | Count of flights experiencing delay in specified cause |

### Table: `fact_cancel_daily_airline_reason`
- **Description**: Fact: Daily Carrier Cancellation Reasons (Long)
- **Row Count**: 6,339
- **Parquet Storage**: `hdfs://localhost:9000/user/saniya/flight_delay/processed/powerbi/fact_cancel_daily_airline_reason`
- **Local CSV File**: `output/powerbi/fact_cancel_daily_airline_reason.csv`

| Column Name | Data Type | Nullable | Description / Business Meaning |
| :--- | :--- | :--- | :--- |
| `FLIGHT_DATE` | `Date` | YES | ISO calendar date (YYYY-MM-DD), primary date join key |
| `AIRLINE` | `String` | YES | Standard IATA code or dimension primary key |
| `CANCELLATION_REASON` | `String` | YES | FAA cancellation reason code (A=Carrier, B=Weather, C=NAS, D=Security) |
| `CANCELLED_FLIGHTS` | `Long` | YES | Count of cancelled flights under specified cancellation reason |

