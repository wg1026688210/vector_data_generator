use arrow::compute::min;
use clap::{Parser, ValueEnum};
use indicatif::{ProgressBar, ProgressStyle};
use std::path::PathBuf;
use std::str::FromStr;
use std::time::Instant;
use bytesize::ByteSize;
use vector_data_gen::{Config, CompressionType, DataGenerator, ParquetWriter};
use anyhow::{Result, Context};

/// Command line arguments
#[derive(Parser, Debug)]
#[command(author, version, about, long_about = None)]
struct Args {
    /// Output directory for generated files
    #[arg(short, long, default_value = "./output")]
    output_dir: PathBuf,

    /// Total number of rows to generate
    #[arg(short, long, default_value_t = 1000)]
    total_rows: usize,

    /// Target file size per file
    #[arg(short, long, default_value = "512MB")]
    file_size: String,

    /// Compression type to use
    #[arg(short, long, value_enum, default_value_t = Compression::Snappy)]
    compression: Compression,

    /// Vector dimension
    #[arg(long, default_value_t = 1024)]
    vector_dim: usize,

    /// Scalar string length in bytes
    #[arg(long, default_value_t = 32)]
    scalar_len: usize,

    /// Random seed for reproducible data
    #[arg(long, default_value_t = 42)]
    seed: u64,

    /// Batch size for data generation
    #[arg(short, long, default_value_t = 10000)]
    batch_size: usize,

    /// Enable verbose output
    #[arg(short, long)]
    verbose: bool,
}

/// Compression type enum for CLI
#[derive(ValueEnum, Clone, Debug)]
enum Compression {
    Snappy,
    Gzip,
    Lz4,
    Zstd,
    Uncompressed,
}

impl From<Compression> for CompressionType {
    fn from(value: Compression) -> Self {
        match value {
            Compression::Snappy => CompressionType::Snappy,
            Compression::Gzip => CompressionType::Gzip,
            Compression::Lz4 => CompressionType::Lz4,
            Compression::Zstd => CompressionType::Zstd,
            Compression::Uncompressed => CompressionType::Uncompressed,
        }
    }
}

fn parse_file_size(size_str: &str) -> Result<u64> {
    let size = ByteSize::from_str(size_str)
        .map_err(|e| anyhow::anyhow!("Invalid file size format '{}': {}", size_str, e))?;
    Ok(size.as_u64())
}

fn main() -> Result<()> {
    let args = Args::parse();
    // Parse file size
    let target_file_size = parse_file_size(&args.file_size)?;

    // Create output directory
    std::fs::create_dir_all(&args.output_dir)
        .with_context(|| format!("Failed to create output directory: {:?}", args.output_dir))?;

    // Create configuration
    let config = Config::new(
        args.vector_dim,
        args.scalar_len,
        target_file_size,
        args.compression.into(),
        args.seed,
    );

    if args.verbose {
        println!("Configuration:");
        println!("  Vector dimension: {}", config.vector_dim);
        println!("  Scalar length: {} bytes", config.scalar_len);
        println!("  Target file size: {}", ByteSize::b(target_file_size));
        println!("  Compression: {:?}", config.compression);
        println!("  Random seed: {}", config.seed);
        println!("  Prefix: {}", args.prefix);
        println!("  Output directory: {:?}", args.output_dir);
        println!("  Total rows to generate: {}", args.total_rows);
        println!("  Batch size: {}", args.batch_size);
        println!();
    }

    // Create data generator and estimate rows per file
    let generator = DataGenerator::new(config.clone());
    let rows_per_file = generator.estimate_rows_per_file();

    if args.verbose {
        println!("Estimated rows per file: {}", rows_per_file);
        println!("Starting data generation...");
        println!();
    }

    // Create progress bar
    let progress = ProgressBar::new(args.total_rows as u64);
    progress.set_style(
        ProgressStyle::default_bar()
            .template("{spinner:.green} [{elapsed_precise}] [{bar:40.cyan/blue}] {pos}/{len} files ({eta})")
            .unwrap()
            .progress_chars("#>-"),
    );

    let writer = ParquetWriter::new(config.clone());
    let total_start = Instant::now();

    let mut num_files = 0;
    let mut total_rows_written = 0;
    while true {
        let start_time = Instant::now();
        let file_seed = args.seed + num_files as u64;
        let mut file_generator = DataGenerator::new(Config::new(
            args.vector_dim,
            args.scalar_len,
            target_file_size,
            config.compression,
            file_seed,
        ));
        let file_name = format!("{}-{:08}.parquet", args.prefix, num_files);
        let file_path = args.output_dir.join(file_name);
        if args.verbose {
            println!("Generating file {}: {:?}", num_files + 1, file_path);
        }

        let remaining_rows = args.total_rows - total_rows_written;
        let num_rows_to_write = {
            if remaining_rows>rows_per_file {
            rows_per_file
        } else {
            remaining_rows
        }};
    

        let rows_written = writer.write_to_file(
            file_path.to_str().unwrap(),
            &mut file_generator,
            num_rows_to_write,
            args.batch_size,
        )?;
        total_rows_written += rows_written;
        if total_rows_written >= args.total_rows {
            break;
        }
        num_files += 1;

        let elapsed = start_time.elapsed();
        let file_size = std::fs::metadata(&file_path)?.len();

        if args.verbose {
            println!(
                "  Generated {} rows ({} bytes) in {:.2?} ({:.2} rows/sec)",
                rows_written,
                ByteSize::b(file_size),
                elapsed,
                rows_written as f64 / elapsed.as_secs_f64()
            );
        }
        progress.inc(rows_written as u64);
    }


    progress.finish_with_message("Data generation complete!");

    let total_elapsed = total_start.elapsed();
    println!("\nTotal time: {:.2?}", total_elapsed);
    println!("Generated {} files in {:?}", num_files, args.output_dir);

    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_parse_file_size() {
        // bytesize uses decimal units (MB = 1,000,000 bytes)
        assert_eq!(parse_file_size("512MB").unwrap(), 512_000_000);
        assert_eq!(parse_file_size("1GB").unwrap(), 1_000_000_000);
        assert_eq!(parse_file_size("100KB").unwrap(), 100_000);
    }

    #[test]
    fn test_parse_invalid_file_size() {
        assert!(parse_file_size("invalid").is_err());
        assert!(parse_file_size("123XYZ").is_err());
    }
}