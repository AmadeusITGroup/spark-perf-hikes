#!/bin/bash

spark_sandbox_tmp_path=/tmp/amadeus-spark-lab/

SSCE_GREEN='\033[0;32m'
SSCE_YELLOW='\033[1;33m'
SSCE_RED='\033[0;31m'
SSCE_NC='\033[0m' # No Color

# Log information
function ssce_log_info() {
    echo -e "${SSCE_GREEN}[INFO] $1${SSCE_NC}"
}

# Log warning
function ssce_log_warn() {
    echo -e "${SSCE_YELLOW}[WARN] $1${SSCE_NC}"
}

# Log error
function ssce_log_error() {
    echo -e "${SSCE_RED}[ERROR] $1${SSCE_NC}" >&2
}


function spark-init-datasets-local() {
  if [ ! -f ${spark_sandbox_tmp_path}datasets/optd_por_public.csv ]; then
    ssce_log_info "Downloading datasets in ${spark_sandbox_tmp_path}datasets/"
    mkdir -p $spark_sandbox_tmp_path/datasets
    curl https://raw.githubusercontent.com/opentraveldata/opentraveldata/master/opentraveldata/optd_por_public.csv > $spark_sandbox_tmp_path/datasets/optd_por_public.csv
  else
    ssce_log_info "Datasets already downloaded in ${spark_sandbox_tmp_path}datasets/"
  fi
}

function spark-init-datasets-local-tmp() {
  if [ ! -f ${spark_sandbox_tmp_path}datasets/optd_por_public_filtered.csv ]; then
    mkdir -p $spark_sandbox_tmp_path/datasets
    cp misc/optd_por_public_filtered.csv $spark_sandbox_tmp_path/datasets/optd_por_public_filtered.csv
  else
    ssce_log_info "Datasets already present in ${spark_sandbox_tmp_path}datasets/"
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
    ssce_log_info "Version of spark seems OK ($spark_path vs required $expected_version)"
  else
    ssce_log_warn "WARNING: version of spark ($spark_path) seems not to match required version $expected_version!!!!"
  fi
}

function spark-run-local() {
  local script="$1"
  local args=$(cat $script | sed -n 's#// Local: ##gp')
  local version=$(cat $script | sed -n 's#// Spark: ##gp')
  spark-init-datasets-local-tmp
  spark-local-check-version $version
  ssce_log_info "script: $script, spark-shell ${args}"
  cat $script - | eval spark-shell ${args}
}



function spark-import-databricks() {
  local src=$1
  local filename=$(basename $src)
  local profile=$2
  local dst=$3/$filename
  local extraargs=$4
  local args=$(cat $src | sed -n 's#// Cluster: ##p') # unused for now
  ssce_log_info "Uploading: $src to ($profile) $dst ..."
  databricks workspace import  $dst --profile $profile --language SCALA --file $src $extraargs 
}

function spark-init-datasets-databricks() {
  local profile=$1
  spark-init-datasets-local
  local src=$spark_sandbox_tmp_path/datasets/optd_por_public.csv
  databricks fs mkdir dbfs:/$(dirname $src) --profile $profile
  databricks fs cp $src dbfs:/$src --profile $profile
}

function spark-init-datasets-databricks-tmp() {
  local profile=$1
  spark-init-datasets-local
  local src=$spark_sandbox_tmp_path/datasets/optd_por_public_filtered.csv
  databricks fs mkdir dbfs:/$(dirname $src) --profile $profile
  databricks fs cp $src dbfs:/$src --profile $profile
}
