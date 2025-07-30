#!/bin/bash

# Ensure Mermaid CLI is installed
if ! command -v mmdc &> /dev/null; then
  echo "Error: Mermaid CLI (mmdc) not found. Install with: npm install -g @mermaid-js/mermaid-cli"
  exit 1
fi

# Define paths
INPUT_FILE="sql/mermaid_db.mmd"
SVG_OUTPUT="sql/nfl_schema.svg"
PNG_OUTPUT="sql/nfl_schema.png"

# Create output directory if it doesn't exist
mkdir -p "$(dirname "$SVG_OUTPUT")"

echo "Rendering Mermaid diagram to SVG..."
mmdc -i "$INPUT_FILE" -o "$SVG_OUTPUT"

echo "Rendering Mermaid diagram to high-resolution PNG..."
mmdc -i "$INPUT_FILE" -o "$PNG_OUTPUT" --width 12000

echo "Done: SVG and high-res PNG saved to $(dirname "$SVG_OUTPUT")"
