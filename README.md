Healthdirect NHSD coding challenge
----------------------------------

## Instructions

Complete the missing pieces of the Scala/Spark application, identified by ??? (three question marks).
Feel free to also change other parts of the code if you see fit.

The goal of the app is to load/validate/transform raw input data and produce clean de-normalised
views. The raw input data is json encoded events containing healthcare services, organisations and
locations (see data/events.jsonl).

The output should be three json files:
  1. De-normalised pharmacies - output/pharmacies.jsonl
  2. De-normalised GP's - output/gps.jsonl
  3. Errors - output/errors.jsonl

The app already writes these files based off typed Datasets where the types are also already defined.

## Recommendation

Since we are evaluating both spark and Scala skills, we highly encourage the solution to leverage
typed Dataset API's.

You can use any json serialisation lib, but spark already has json4s-jackson as a dependency so it
might be easier to use that.

Please think of and handle different types or errors that could happen at each step in the
application.

## Setup

Maven: https://maven.apache.org/install.html

Spark: https://spark.apache.org/downloads.html (try 3.1.3 with pre-built hadoop 3.2 and later)

After the above is installed make sure you have `SPARK_HOME` env var pointed to where spark is
installed.

## Run

From the project root:

```
mvn package

$SPARK_HOME/bin/spark-submit \
  --class "au.org.healthdirect.nhsd.codingchallenge.App" \
  --master 'local[2]' \
  target/coding-challenge_2.12-1.0-SNAPSHOT.jar \
  <path to the events.jsonl file>
```
