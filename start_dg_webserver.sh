# Create and DAGSTER_HOME dir to current execution dir
DG_HOME="$(pwd)/dagster_home"
mkdir -p "${DG_HOME}"
export DAGSTER_HOME="${DG_HOME}"

# Start Dagster UI
pkill -f dagster-webserver
echo "Starting dagster-webserver on port 3000..."
./.venv/bin/dagster-webserver -h 0.0.0.0 -p 3000 -w "$(pwd)/src/waterq_auto_sync/workspace.yaml" > /dev/null 2>&1 &