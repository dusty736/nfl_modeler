#!/bin/bash

# Define variables for clarity and easier maintenance
RSCRIPT_PATH="/usr/local/bin/Rscript" 
R_SCRIPT_TO_RUN="./etl/R/process_scripts/db_refresh_cloud.R"
PYTHON_SCRIPT_TO_RUN="./modeling/Python/pregame_total_predgen.py" # Your Python prediction script
PYTHON_ARGS="--run-id latest --all --to-db"

PROXY_BINARY="./cloud-sql-proxy"
INSTANCE_CONNECTION_NAME="nfl-modeling:europe-west2:nfl-pg-01"

# Define the socket path that both R and Python will use
SOCKET_PATH="/tmp/${INSTANCE_CONNECTION_NAME}"

# --- Main Script Logic ---

# Check if the proxy binary exists and is executable
if [ ! -x "$PROXY_BINARY" ]; then
    echo "Error: Cloud SQL Proxy binary not found or is not executable at $PROXY_BINARY."
    exit 1
fi

# Start the Cloud SQL Auth Proxy in the background.
echo "Starting Cloud SQL Auth Proxy..."
# FIX: Use -u (or --unix-socket) followed by the directory /tmp as separate arguments.
"$PROXY_BINARY" -u /tmp "$INSTANCE_CONNECTION_NAME" &

# Store the Process ID (PID) of the proxy.
PROXY_PID=$!

# Use 'trap' to ensure the proxy is killed upon script exit, even if errors occur
trap "echo 'Terminating proxy...'; kill '$PROXY_PID' 2>/dev/null" EXIT

# Wait for the proxy to initialize and create the socket.
sleep 5

# Check if the proxy process is still running after a few seconds.
if ! kill -0 "$PROXY_PID" 2>/dev/null; then
    echo "Error: Proxy failed to start. Check your credentials or network."
    exit 1
fi

echo "Proxy started with PID: $PROXY_PID"
echo "---"

# 1. Run R Script (Data Refresh)
echo "Running R script to refresh data..."
"$RSCRIPT_PATH" "$R_SCRIPT_TO_RUN"

# 2. Run Python Script (Prediction Generation)
echo "Running Python prediction script..."
# Set the environment variable that the Python script uses for Unix socket connection
export CLOUDSQL_SOCKET_PATH="$SOCKET_PATH"

python3 "$PYTHON_SCRIPT_TO_RUN" $PYTHON_ARGS

echo "---"
echo "Script finished successfully."
# The EXIT trap automatically terminates the proxy process now.