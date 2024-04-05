# README

This module allows to launch snippets on Spark 3, where each contains:

- a scala snippet
- the dependencies
- the spark settings

## Getting started

### a. Initialize the environment

The very first time you use the project you need to install it. 

1. Add the following to your `.bashrc` file: 

```bash
export SSCE_PATH=<this-path>
export PATH=$PATH:<spark-shell-directory>
source $SSCE_PATH/source.sh # to define the aliases
```

2. Then set up some sample datasets:

```
spark-init-datasets-local
spark-init-datasets-databricks
```

3. Make sure `spark-shell` is in your `PATH`

### b. Launch a snippet

```bash
spark-run-local <snippet.sc>
```

Some existing snippets:

- Spill
- Thread Contention
- Partition Pruning
- File Pruning
- Z-Order
- Dynamic File Pruning
- Deletion vectors
- ...

### c. Write your snippet

#### IDE setup

To write a snippet we encourage you to create a `build.sbt` file, and load the project as a scala project:

```scala
val sparkVersion = "3.4.1"
val deltaVersion = "2.4.0"
val deps = Seq(
  "org.apache.spark" %% "spark-core" % sparkVersion,
  "org.apache.spark" %% "spark-sql" % sparkVersion,
  "org.apache.spark" %% "spark-avro" % sparkVersion,
  "io.delta" %% "delta-core" % deltaVersion
)
lazy val root = (project in file("."))
  .settings(
    name := "ssce-spark-sandbox",
    scalaVersion := "2.12.13",
    libraryDependencies := deps
  )
```

#### Snippet structure

Each snippet is supposed to have the same structure of the following example:

```scala
// Spark: <version of spark this snippet is intended to be used with>
// Local: <spark shell extra options when used locally, e.g. --executor-memory 1G --driver-memory 1G --executor-cores 1 --master local[2] >
// Databricks: <placeholder, unused for now>

// COMMAND ----------

/*
<brief description of problem and solution high level>

# Symptom
<problem symptoms>

# Explanation
<explanation of the potential solution, the analysis or any other detail about how to address the problem>
*/

// COMMAND ----------

... scala code corresponding to the snippet ...

```
