function spark-run-local() {
  local script="$1"
  local args=$(cat $script | sed -n 's#// Local: ##gp')
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

