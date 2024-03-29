# README

This module allows to launch 'boundles' of snippets on Spark 3, where each boundle contains:

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

```bash
mkdir -p $SSCE_PATH/datasets/
curl https://raw.githubusercontent.com/opentraveldata/opentraveldata/master/opentraveldata/optd_por_public_all.csv > datasets/optd_por_public_all.csv
qspark spark3/dataset-gen.sc # this will generate .parquet, .delta, ... datasets
```

3. Make sure `spark-shell` is in your `PATH`

### b. Launch a bundle

```bash
qspark <bundle.sc> # this will launch the snippet with the settings defined after '// Arguments: ' prefixed line
```

### c. Write your own bundle

A very simple bundle looks like this:

```scala
// Arguments: --executor-memory 1G --driver-memory 1G --executor-cores 1 --master local[6] --conf spark.sql.adaptive.enabled=false

val df = spark.read.option("delimiter","^").option("header","true").csv("datasets/optd_por_public_all.csv")

```

Each bundle contains a `// Arguments: ` section with the parameters passed to Spark shell to run the snippet.

## Use cases

Basic:
- Spill
- TODO Simple merge
- Simple merge in streaming query

Read Amplification:
- Z-Order
- DFP
- 

Write Amplification:
- ...

Misc:
- Thread Contention
