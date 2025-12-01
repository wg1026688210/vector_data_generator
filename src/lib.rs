//! Vector data generation library for creating test Parquet files
//!
//! This library generates test data with:
//! - 1024-dimensional vector fields (f32)
//! - 32-byte scalar strings
//! - Outputs to compressed Parquet files (512MB per file)

use arrow::array::{ArrayRef, BinaryArray, StringArray};
use arrow::datatypes::{DataType, Field, Schema};
use arrow::record_batch::RecordBatch;
use parquet::arrow::ArrowWriter;
use parquet::basic::{Compression, GzipLevel, ZstdLevel};
use parquet::file::properties::WriterProperties;
use rand::distributions::{Distribution, Uniform, Alphanumeric};
use rand::SeedableRng;
use rand::rngs::StdRng;
use std::fs::File;
use std::sync::Arc;
use anyhow::{Result, Context};

/// Configuration for data generation
#[derive(Debug, Clone)]
pub struct Config {
    /// Vector dimension (default: 1024)
    pub vector_dim: usize,
    /// Scalar string length in bytes (default: 32)
    pub scalar_len: usize,
    /// Target file size in bytes (default: 512MB)
    pub target_file_size: u64,
    /// Compression type for Parquet files
    pub compression: CompressionType,
    /// Random seed for reproducible data
    pub seed: u64,
}

/// Compression types supported by Parquet
#[derive(Debug, Clone, Copy)]
pub enum CompressionType {
    Snappy,
    Gzip,
    Lz4,
    Zstd,
    Uncompressed,
}

impl Default for Config {
    fn default() -> Self {
        Self {
            vector_dim: 1024,
            scalar_len: 32,
            target_file_size: 512 * 1024 * 1024, // 512MB
            compression: CompressionType::Snappy,
            seed: 42,
        }
    }
}

impl Config {
    /// Create a new configuration with custom parameters
    pub fn new(
        vector_dim: usize,
        scalar_len: usize,
        target_file_size: u64,
        compression: CompressionType,
        seed: u64,
    ) -> Self {
        Self {
            vector_dim,
            scalar_len,
            target_file_size,
            compression,
            seed,
        }
    }
}

/// Data generator for creating test data
pub struct DataGenerator {
    config: Config,
    rng: StdRng,
    vector_field: Field,
    scalar_field: Field,
    schema: Schema,
}

impl DataGenerator {
    /// Create a new data generator with the given configuration
    pub fn new(config: Config) -> Self {
        let rng = StdRng::seed_from_u64(config.seed);

        // Define schema - using Binary for vector data (store as raw bytes)
        let vector_field = Field::new("vector", DataType::Binary, false);
        let scalar_field = Field::new("scalar", DataType::Utf8, false);

        let schema = Schema::new(vec![vector_field.clone(), scalar_field.clone()]);

        Self {
            config,
            rng,
            vector_field,
            scalar_field,
            schema,
        }
    }

    /// Generate a single vector (1024 f32 values) as bytes
    pub fn generate_vector(&mut self) -> Vec<u8> {
        let uniform = Uniform::new(-1.0, 1.0);
        let floats: Vec<f32> = (0..self.config.vector_dim)
            .map(|_| uniform.sample(&mut self.rng))
            .collect();

        // Convert to bytes (little-endian)
        let mut bytes = Vec::with_capacity(floats.len() * 4);
        for &f in &floats {
            bytes.extend_from_slice(&f.to_le_bytes());
        }
        bytes
    }

    /// Generate a single scalar string (32 bytes)
    pub fn generate_scalar(&mut self) -> String {
        let chars: Vec<char> = Alphanumeric
            .sample_iter(&mut self.rng)
            .take(self.config.scalar_len)
            .map(char::from)
            .collect();
        chars.into_iter().collect()
    }

    /// Generate a batch of data with the specified number of rows
    pub fn generate_batch(&mut self, batch_size: usize) -> Result<RecordBatch> {
        // Generate vectors as binary data
        let mut vector_data: Vec<Vec<u8>> = Vec::with_capacity(batch_size);
        let mut scalar_data = Vec::with_capacity(batch_size);

        for _ in 0..batch_size {
            vector_data.push(self.generate_vector());
            scalar_data.push(self.generate_scalar());
        }

        // Create arrays
        let vector_array = BinaryArray::from_iter_values(vector_data.iter().map(|v| v.as_slice()));
        let scalar_array = StringArray::from(scalar_data);

        let batch = RecordBatch::try_new(
            Arc::new(self.schema.clone()),
            vec![
                Arc::new(vector_array) as ArrayRef,
                Arc::new(scalar_array) as ArrayRef,
            ],
        )?;

        Ok(batch)
    }

    /// Get the Arrow schema
    pub fn schema(&self) -> &Schema {
        &self.schema
    }

    /// Estimate number of rows needed to reach target file size
    pub fn estimate_rows_per_file(&self) -> usize {
        // Rough estimation: each row has vector (1024 * 4 bytes) + scalar (32 bytes + overhead)
        // Binary data has some overhead for length encoding
        let bytes_per_row = (self.config.vector_dim * 4 + 8) + (self.config.scalar_len + 8);
        (self.config.target_file_size as usize / bytes_per_row).max(1)
    }
}

/// Writer for generating Parquet files
pub struct ParquetWriter {
    config: Config,
    writer_props: WriterProperties,
}

impl ParquetWriter {
    /// Create a new Parquet writer with the given configuration
    pub fn new(config: Config) -> Self {
        let builder = WriterProperties::builder();

        let builder = match config.compression {
            CompressionType::Snappy => builder.set_compression(Compression::SNAPPY),
            CompressionType::Gzip => builder.set_compression(Compression::GZIP(GzipLevel::default())),
            CompressionType::Lz4 => builder.set_compression(Compression::LZ4),
            CompressionType::Zstd => builder.set_compression(Compression::ZSTD(ZstdLevel::default())),
            CompressionType::Uncompressed => builder.set_compression(Compression::UNCOMPRESSED),
        };

        // Enable dictionary encoding for better compression
        let builder = builder.set_dictionary_enabled(true);

        // Set row group size to optimize for large files
        let builder = builder.set_max_row_group_size(100_000);

        Self {
            config,
            writer_props: builder.build(),
        }
    }

    /// Write data to a Parquet file
    pub fn write_to_file(
        &self,
        file_path: &str,
        data_generator: &mut DataGenerator,
        num_rows: usize,
        batch_size: usize,
    ) -> Result<usize> {
        let file = File::create(file_path)
            .with_context(|| format!("Failed to create file: {}", file_path))?;

        let schema = data_generator.schema().clone();
        let mut writer = ArrowWriter::try_new(
            file,
            Arc::new(schema),
            Some(self.writer_props.clone()),
        )?;

        let mut total_rows = 0;
        let mut remaining_rows = num_rows;

        while remaining_rows > 0 {
            let current_batch_size = batch_size.min(remaining_rows);
            let batch = data_generator.generate_batch(current_batch_size)?;

            let batch_rows = batch.num_rows();
            writer.write(&batch)?;

            total_rows += batch_rows;
            remaining_rows -= batch_rows;
        }

        writer.close()?;

        Ok(total_rows)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::NamedTempFile;

    #[test]
    fn test_data_generation() {
        let config = Config::default();
        let mut generator = DataGenerator::new(config);

        // Test vector generation (1024 f32 values = 4096 bytes)
        let vector = generator.generate_vector();
        assert_eq!(vector.len(), 1024 * 4); // 1024 f32 * 4 bytes each

        // Test scalar generation
        let scalar = generator.generate_scalar();
        assert_eq!(scalar.len(), 32);

        // Test batch generation
        let batch = generator.generate_batch(10).unwrap();
        assert_eq!(batch.num_rows(), 10);
        assert_eq!(batch.num_columns(), 2);
    }

    #[test]
    fn test_parquet_writing() {
        let config = Config::default();
        let mut generator = DataGenerator::new(config.clone());
        let writer = ParquetWriter::new(config);

        let temp_file = NamedTempFile::new().unwrap();
        let file_path = temp_file.path().to_str().unwrap();

        let rows_written = writer.write_to_file(
            file_path,
            &mut generator,
            100,
            10,
        ).unwrap();

        assert_eq!(rows_written, 100);

        // Verify file exists and has content
        let metadata = std::fs::metadata(file_path).unwrap();
        assert!(metadata.len() > 0);
    }

    #[test]
    fn test_estimate_rows() {
        let config = Config::default();
        let generator = DataGenerator::new(config);

        let estimated = generator.estimate_rows_per_file();
        assert!(estimated > 0);
    }
}