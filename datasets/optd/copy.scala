%scala
// This script helps copying the input dataset to the right location in Databricks

// 1. Create new git repo from using URL: https://github.com/AmadeusITGroup/spark-perf-hikes.git
// 2. Run this script
val username = dbutils.notebook.getContext.userName.get
val src = s"file:/Workspace/Users/${username}/spark-perf-hikes/datasets/optd/optd_por_public_filtered.csv"
val dst = "dbfs:/tmp/perf-hikes/datasets/optd_por_public_filtered.csv"
dbutils.fs.cp(src, dst)
