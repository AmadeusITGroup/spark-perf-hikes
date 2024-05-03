#!/bin/bash

ph_spark_sandbox_tmp_path=/tmp/perf-hikes/
ph_green='\033[0;32m'
ph_yellow='\033[1;33m'
ph_red='\033[0;31m'
ph_nc='\033[0m' # No Color

# Log information
function ph_log_info() {
    echo -e "${ph_green}[INFO] $1${ph_nc}"
}

# Log warning
function ph_log_warn() {
    echo -e "${ph_yellow}[WARN] $1${ph_nc}"
}

# Log error
function ph_log_error() {
    echo -e "${ph_red}[ERROR] $1${ph_nc}" >&2
}

function spark-init-datasets-local() {
  if [ ! -f ${ph_spark_sandbox_tmp_path}datasets/optd_por_public_filtered.csv ]; then
    mkdir -p $ph_spark_sandbox_tmp_path/datasets
    cp misc/optd_por_public_filtered.csv $ph_spark_sandbox_tmp_path/datasets/optd_por_public_filtered.csv
  else
    ph_log_info "Datasets already present in ${ph_spark_sandbox_tmp_path}datasets/"
  fi
}

function spark-local-check-version() {
  local expected_version=$1
  # shellcheck disable=SC2046
  local readlink_cmd="readlink"
  if command -v greadlink >/dev/null 2>&1; then
    local readlink_cmd="greadlink"
  fi
  local spark_path=$($readlink_cmd -e $(which spark-shell))
  if [[ "${spark_path}" == *"$expected_version"* ]] ;then
    ph_log_info "Version of spark seems OK ($spark_path vs required $expected_version)"
  else
    ph_log_warn "WARNING: version of spark ($spark_path) seems not to match required version $expected_version!!!!"
  fi
}

function spark-run-local() {
  local script="$1"
  local args=$(cat $script | sed -n 's#// Local: ##gp')
  local version=$(cat $script | sed -n 's#// Spark: ##gp')
  spark-init-datasets-local
  spark-local-check-version $version
  ph_log_info "script: $script, spark-shell ${args}"
  cat $script - | eval spark-shell ${args}
}

function spark-import-databricks() {
  local src=$1
  local filename=$(basename $src)
  local profile=$2
  local dst=$3/$filename
  local extraargs=$4
  local args=$(cat $src | sed -n 's#// Cluster: ##p') # unused for now
  ph_log_info "Uploading: $src to ($profile) $dst ..."
  databricks workspace import  $dst --profile $profile --language SCALA --file $src $extraargs 
}

function spark-init-datasets-databricks() {
  local profile=$1
  spark-init-datasets-local
  local src=$ph_spark_sandbox_tmp_path/datasets/optd_por_public_filtered.csv
  databricks fs mkdir dbfs:/$(dirname $src) --profile $profile
  databricks fs cp $src dbfs:/$src --profile $profile
}
