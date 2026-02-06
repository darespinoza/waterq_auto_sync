# Create and DAGSTER_HOME dir to current execution dir
DG_HOME="$(pwd)/dagster_home"
mkdir -p "${DG_HOME}"
export DAGSTER_HOME="${DG_HOME}"

# Run Dagster daemon using the
pkill -f dagster-daemon
dagster-daemon run -w "$(pwd)/src/waterq_auto_sync/workspace.yaml"
