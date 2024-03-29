SPARK_DEFAULT_PACKAGES=io.delta:delta-core_2.12:2.1.0,org.apache.spark:spark-avro_2.12:3.3.2
SPARK_DEFAULT_CONFIGS="--conf spark.sql.extensions=io.delta.sql.DeltaSparkSessionExtension --conf spark.sql.catalog.spark_catalog=org.apache.spark.sql.delta.catalog.DeltaCatalog"

function qspark-with-packages() {
  qspark-init
  local packs_coords_list=${1:-"com.beachape:enumeratum_2.12:1.5.15"}
  spark-shell --packages "$packs_coords_list"
}

function qspark-init() {
  cd $SSCE_PATH
}

function qspark() {
  local script="$1"
  qspark-init
  if [ -z "$script" ] 
  then
    spark-shell --packages $SPARK_DEFAULT_PACKAGES $SPARK_DEFAULT_CONFIGS $args
  else
    local args=$(cat $script | grep '// Arguments: ' | sed 's#// Arguments: ##g')
    cat $script - | spark-shell --packages $SPARK_DEFAULT_PACKAGES $SPARK_DEFAULT_CONFIGS $args
  fi
}

