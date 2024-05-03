# README 

These datasets are based on [Open Travel Data datasets](https://github.com/opentraveldata/opentraveldata) (or OPTD for short).

## File optd_por_public_filtered.csv

This dataset contains airports from all over the world. 

The schema is as follows: 

- id: replica of the iata_code, is the primary key
- iata_code: unique identifier of an airport (for instance, SFO for San Francisco)
- envelope_id: the version (as in OPTD) taken for the airport in this dataset
- name: name of the airport
- latitude
- longitude
- date_from: start validity of the record
- date_until: end validity of the record
- comment
- country_code
- country_name
- continent_name
- timezone
- wiki_link

The CSV has been generated from [this csv on OPTD](https://raw.githubusercontent.com/opentraveldata/opentraveldata/master/opentraveldata/optd_por_public.csv) using the following snippet:

```scala
// This scripts simply prepares the input dataset, filtering the airports with non null IATA code and location type A.
// Only the most recent entry for each IATA code is kept.
import org.apache.spark.sql.functions.col
val input = ??? // path to optd_por_public.csv from open
val outputPath = ???
val rawCsv = spark.read.option("delimiter","^").option("header","true").csv(input)
val projected = rawCsv.withColumn("id", col("iata_code")).select("id", "iata_code", "envelope_id", "name", "latitude", "longitude", "date_from", "date_until", "comment", "country_code", "country_name", "continent_name", "timezone", "wiki_link")
projected.where(col("location_type")==="A" and col("iata_code").isNotNull).createOrReplaceTempView("table")
val airports = spark.sql("SELECT row_number() OVER (PARTITION BY iata_code ORDER BY envelope_id, date_from DESC) as r, * FROM table").where(col("r") === 1).drop("r")
airports.coalesce(1).write.mode("overwrite").option("delimiter","^").option("header", "true").csv(outputPath)
```
