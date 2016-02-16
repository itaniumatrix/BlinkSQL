# SQL on S3
Data Engineering project comparing distributed SQL processing frameworks

Provides timings for basic SQL queries (aggregations, filters) and a self-join on the Reddit comments data set (converted to Parquet from JSON).

## Requirements
* Python 2.7
* Java 8 (for Presto)

Tested with:
Presto 0.136  
Spark 1.5.2 
Drill 1.4  
(and Hive 1.2.1 for the smallest queries up to 5 GB)
