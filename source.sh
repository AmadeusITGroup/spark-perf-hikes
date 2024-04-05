function spark-run-local() {
  local script="$1"
  local args=$(cat $script | sed 's#// Arguments: ##gp')
  cat $script - | spark-shell $args
}

function spark-import-databricks() {
  local src=$1
  local profile=$2
  local dst=$3
  local args=$(cat $script | sed 's#// Cluster: ##gp') # unused for now
  echo "Uploading: $i to $profile:$dst ..."
  databricks workspace import --profile $profile --language SCALA --overwrite $scr $dst
}

