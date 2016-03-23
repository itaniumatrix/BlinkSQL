# SQL on S3
Data Engineering project comparing distributed SQL processing frameworks

[Presentation Link] (https://docs.google.com/presentation/d/1UdsrCil9fzDRCrzpe-F4MXPa2FBXWUoxsn_8mTEh7Nk/pub?start=false&loop=false&delayms=3000&slide=id.p)

Provides timings for basic SQL queries (aggregations, filters) and a self-join on the Reddit comments data set (converted to Parquet from JSON).

## Requirements
* Python 2.7+
* Java 8 (for Presto)

Tested with:  
Presto 0.136  
Spark 1.5.2  
Drill 1.4  
(optional Hive 1.2.1)
