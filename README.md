# Performance Hikes for Apache Spark

[![License](http://img.shields.io/:license-Apache%202-brightgreen.svg)](http://www.apache.org/licenses/LICENSE-2.0.txt)

This project contains a collection of code snippets designed for hands-on exploration of Apache Spark functionalities, with a strong focus on performance optimization.

Some problems addressed:

- Spill
- Thread Contention
- Read amplification (addressed with partition pruning, DFP, z-ordering, FP, ...)
- Write amplification (addressed with deletion vectors, ...)
- Spark UI retention limits
- ...

Each snippet is pedagogic, self-contained, executable, and focuses on one performance problem providing at least one possible solution.

Thanks to this, the user learns about problems/features by:
- exploring and reading the snippets
- running them
- following them on Spark UI
- monitoring them
- modifying them
- ...

A typical snippet has the folowing structure:

```scala
// Spark: <version of spark this snippet is intended to be used with>
// Local: <spark shell extra options when used locally, e.g. --master local[2] --driver-memory 1G >
// Databricks: <details about runtime used to test the snipped, unused for now>

// COMMAND ----------

/*
<brief description of problem and a possible solution high level>

# Symptom
<problem symptoms>

# Explanation
<explanation of the potential solution, the analysis or any other detail about how to address the problem>

# What to aim for concretely
<concrete steps to get to the metrics / indicator that the solution proposed is working properly>
*/

// COMMAND ----------

spark.sparkContext.setJobDescription("JOB")
val path = ???
val airports = spark.read.option("delimiter","^").option("header","true").csv(path)
/// ... more scala code ...
```

## Getting started

### Setup the environment

The very first time you use the project you need to do some setup. 

Add the following to your `~/.bashrc` (`~/.zshrc` for MacOS): 

```bash
export PERF_HIKES_PATH=<this-path>
source $PERF_HIKES_PATH/source.sh # to define the aliases
```

You can use the project: 
- locally using **spark-shell** or 
- remotely using your own **Databricks** workspace

#### Setup the environment based on **spark-shell**

1. Install spark locally and make sure `spark-shell` is in your `PATH`

You can install Apache Spark and the Spark Shell by following the instructions on the [Apache Spark website](https://spark.apache.org/downloads.html).
If you want to reproduce as close as possible the expected behavior of each snippet,
you should install the same version of Spark as the one mentioned in the snippet (at least same major.minor).

2. Append the following to your `~/.bashrc` (`~/.zshrc` for MacOS): 

```bash
export PATH=$PATH:<spark-shell-directory>
```

3. Then set up some sample datasets running in your shell:

```
spark-init-datasets-local 
```

#### Setup the environment based on **Databricks**

1. Configure the Databricks cli on your machine.

   Be sure to:
   - install Databricks cli versions 0.205 and above (cf. https://docs.databricks.com/en/dev-tools/cli/tutorial.html)
   - define the Databricks profile (containing name, url and credentials of your Databricks workspace) in your `~/.databrickscfg` file.

2. Then set up some sample datasets:

```
spark-init-datasets-databricks <databricks-profile-name>
```

### Launch a snippet

#### Launch a snippet locally

```bash
spark-run-local <snippet.sc>
```

#### Launch a snippet _on Databricks_

First copy the snippet you want to your Databricks workspace:

```
spark-import-databricks <snippet.sc> <databricks-profile-name> <destination-folder> <extra-args: e.g., --overwrite>
```

Then run it as a notebook.

## Contributing

### IDE setup

First you need to setup the environment as described above.

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
    name := "perf-hikes",
    scalaVersion := "2.12.13",
    libraryDependencies := deps
  )
```

### Write the snippet

A new snippet must honor the description and the structure provided above.

The naming convention is: <problem-description>.<solution-description>.sc

### Create a Pull Request

Please follow [the Apache Software Foundation Code of Conduct](https://www.apache.org/foundation/policies/conduct.html).

All contributions must go through a Pull Request review before being accepted. 

Before opening a Pull Request, please consider the following questions:

- Is the change ready enough to ask the community to spend time reviewing?
- Is the change being proposed clearly explained and motivated?

When you contribute code, you affirm that the contribution is your original work and that you
license the work to the project under the project's open source license. Whether or not you
state this explicitly, by submitting any copyrighted material via pull request, email, or
other means you agree to license the material under the project's open source license and
warrant that you have the legal authority to do so.
