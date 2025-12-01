#!/bin/bash

# Example script for using vector_data_gen

echo "Building the project..."
cargo build --release

echo ""
echo "=== Example 1: Generate a single 512MB file ==="
echo "Running: ./target/release/vector_data_gen --verbose"
./target/release/vector_data_gen --verbose

echo ""
echo "=== Example 2: Generate 3 files with 100MB each using Zstd compression ==="
echo "Running: ./target/release/vector_data_gen -n 3 -f 100MB -c zstd --verbose"
./target/release/vector_data_gen -n 3 -f 100MB -c zstd --verbose

echo ""
echo "=== Example 3: Generate files with custom parameters ==="
echo "Running: ./target/release/vector_data_gen --vector-dim 768 --scalar-len 64 --output-dir ./custom_data --verbose"
./target/release/vector_data_gen --vector-dim 768 --scalar-len 64 --output-dir ./custom_data --verbose

echo ""
echo "=== Checking generated files ==="
echo "Files in ./output directory:"
ls -lh ./output/*.parquet 2>/dev/null || echo "No files found in ./output"

echo ""
echo "Files in ./custom_data directory:"
ls -lh ./custom_data/*.parquet 2>/dev/null || echo "No files found in ./custom_data"

echo ""
echo "=== Cleanup ==="
echo "Removing generated directories..."
rm -rf ./output ./custom_data

echo "Done!"