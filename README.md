# Vector Data Generator

A Rust tool for generating test data with vector fields and scalar strings, outputting to compressed Parquet files.

## Features

- Generates 1024-dimensional vector fields (f32 values)
- Generates 32-byte scalar strings
- Outputs to compressed Parquet files (512MB per file by default)
- Supports multiple compression types: Snappy, Gzip, LZ4, Zstd
- Configurable parameters via command line
- Progress reporting with `indicatif`
- Reproducible data generation with seed support

## Installation

```bash
# Clone the repository
git clone <repository-url>
cd vector_data_gen

# Build the project
cargo build --release
```

## Usage

### Basic Usage

Generate a single 512MB file with default settings:

```bash
cargo run --release
```

### Command Line Options

```bash
vector_data_gen --help
```

```
Usage: vector_data_gen [OPTIONS]

Options:
  -o, --output-dir <OUTPUT_DIR>     Output directory for generated files [default: ./output]
  -n, --total-rows <TOTAL_ROWS>     Total number of rows to generate [default: 1000]
  -f, --file-size <FILE_SIZE>       Target file size per file [default: 512MB]
  -c, --compression <COMPRESSION>   Compression type to use [default: snappy] [possible values: snappy, gzip, lz4, zstd, uncompressed]
      --vector-dim <VECTOR_DIM>     Vector dimension [default: 1024]
      --scalar-len <SCALAR_LEN>     Scalar string length in bytes [default: 32]
      --seed <SEED>                 Random seed for reproducible data [default: 42]
  -b, --batch-size <BATCH_SIZE>     Batch size for data generation [default: 10000]
  -v, --verbose                     Enable verbose output
      --prefix <PREFIX>             Prefix for generated file names [default: vector_data]
  -h, --help                        Print help
  -V, --version                     Print version
```

### Examples

1. Generate 5 files with 1GB each using Zstd compression:

```bash
cargo run --release --  --total-rows 100000  --prefix wgcn1 --file-size 200mb  --compression zstd --verbose
```

2. Generate files with custom vector dimension and scalar length:

```bash
cargo run --release -- --vector-dim 768 --scalar-len 64 --output-dir ./my_data
```

3. Generate files with specific random seed for reproducibility:

```bash
cargo run --release -- --seed 12345 --num-files 3 --verbose
```

## Data Schema

Generated Parquet files contain two columns:

1. **vector**: Fixed-size list of 1024 f32 values (default)
   - Type: `FixedSizeList<Float32>`
   - Dimension: 1024 (configurable)

2. **scalar**: 32-byte string (default)
   - Type: `Utf8`
   - Length: 32 bytes (configurable)

## Project Structure

```
src/
├── lib.rs          # Core library with data generation and Parquet writing logic
└── main.rs         # CLI application with argument parsing and progress reporting
```

## Dependencies

- `parquet` / `arrow`: For Parquet file writing and Arrow data structures
- `rand`: For random data generation
- `clap`: For command line argument parsing
- `indicatif`: For progress bars
- `bytesize`: For human-readable file size parsing
- `anyhow`: For error handling

## Testing

Run the tests:

```bash
cargo test
```

## Performance

The tool is optimized for performance:
- Uses batch processing for data generation
- Configurable batch size for memory/performance trade-off
- Parallel data generation support via `rayon` (if needed)
- Efficient Parquet writing with compression

## License
Apache-2.0