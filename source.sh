spark_sandbox_tmp_path=/tmp/amadeus-spark-lab/

function spark-init-datasets-local() {
  if [ ! -f ${spark_sandbox_tmp_path}datasets/optd_por_public.csv ]; then
    echo "Downloading datasets in ${spark_sandbox_tmp_path}datasets/"
    mkdir -p $spark_sandbox_tmp_path/datasets
    curl https://raw.githubusercontent.com/opentraveldata/opentraveldata/master/opentraveldata/optd_por_public.csv > $spark_sandbox_tmp_path/datasets/optd_por_public.csv
  else
    echo "Datasets already downloaded in ${spark_sandbox_tmp_path}datasets/"
  fi
}

function spark-init-datasets-local-tmp() {
  if [ ! -f ${spark_sandbox_tmp_path}datasets/optd_por_public_filtered.csv ]; then
    mkdir -p $spark_sandbox_tmp_path/datasets
    cp misc/optd_por_public_filtered.csv $spark_sandbox_tmp_path/datasets/optd_por_public_filtered.csv
  else
    echo "Datasets already present in ${spark_sandbox_tmp_path}datasets/"
  fi
}

function spark-local-check-version() {
  local expected_version=$1
  local spark_path=$(readlink -e $(which spark-shell))
  if [[ "${spark_path}" == *"$expected_version"* ]] ;then
    echo "Version of spark seems OK ($spark_path vs required $expected_version)"
  else
    echo "WARNING: version of spark ($spark_path) seems not to match required version $expected_version!!!!"
  fi
}

function spark-run-local() {
  local script="$1"
  local args=$(cat $script | sed -n 's#// Local: ##gp')
  local version=$(cat $script | sed -n 's#// Spark: ##gp')
  spark-init-datasets-local-tmp
  spark-local-check-version $version
  cat $script - | spark-shell $args
}

function spark-import-databricks() {
  local src=$1
  local filename=$(basename $src)
  local profile=$2
  local dst=$3/$filename
  local extraargs=$4
  local args=$(cat $src | sed -n 's#// Cluster: ##p') # unused for now
  echo "Uploading: $src to ($profile) $dst ..."
  databricks workspace import  $dst --profile $profile --language SCALA --file $src $extraargs 
}

function spark-init-datasets-databricks() {
  local profile=$1
  spark-init-datasets-local
  local src=$spark_sandbox_tmp_path/datasets/optd_por_public.csv
  databricks fs cp $src dbfs:/$src --profile $profile
}

function spark-init-datasets-databricks-tmp() {
  local profile=$1
  spark-init-datasets-local
  local src=$spark_sandbox_tmp_path/datasets/optd_por_public_filtered.csv
  databricks fs cp $src dbfs:/$src --profile $profile
}
