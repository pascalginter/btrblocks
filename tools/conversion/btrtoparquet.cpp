// ------------------------------------------------------------------------------
#include <iostream>
// ------------------------------------------------------------------------------
#include <arrow/io/api.h>
#include <gflags/gflags.h>
#include <parquet/arrow/writer.h>
// ------------------------------------------------------------------------------
#include "arrow/DirectoryReader.hpp"
#include "scheme/SchemePool.hpp"
// ------------------------------------------------------------------------------
DEFINE_string(btr, "btr", "Directory for btr input");
DEFINE_string(parquet, "parquet", "Parquet output file");
DEFINE_string(writer_properties, "SNAPPY", "Parquet writer properties");
// ------------------------------------------------------------------------------
std::shared_ptr<parquet::WriterProperties> getWriterProperties() {
  parquet::WriterProperties::Builder props;
  props.compression(arrow::Compression::SNAPPY);
  if (FLAGS_writer_properties == "SNAPPY") {
    return props.build();
  }
  props.compression(arrow::Compression::GZIP);
  if (FLAGS_writer_properties == "GZIP") {
    return props.build();
  }
  props.compression(arrow::Compression::BROTLI);
  if (FLAGS_writer_properties == "BROTLI") {
    return props.build();
  }
  props.compression(arrow::Compression::ZSTD);
  if (FLAGS_writer_properties == "ZSTD") {
    return props.build();
  }
  props.compression(arrow::Compression::LZ4);
  if (FLAGS_writer_properties == "LZ4") {
    return props.build();
  }

  props.compression(arrow::Compression::UNCOMPRESSED);
  if (FLAGS_writer_properties == "uncompressed") {
    return props.build();
  }
  props.disable_dictionary();
  props.encoding(parquet::Encoding::PLAIN);
  if (FLAGS_writer_properties == "btr") {
    return props.build();
  }
  props.max_row_group_length(65536);
  if (FLAGS_writer_properties == "btr_original_rowgroups") {
    return props.build();
  }
  assert(false && "unsupported writer properties");
}
// ------------------------------------------------------------------------------
int main(int argc, char** argv){
  gflags::ParseCommandLineFlags(&argc, &argv, true);
  btrblocks::SchemePool::refresh();
  btrblocks::arrow::DirectoryReader directoryReader(FLAGS_btr);
  std::shared_ptr<arrow::Table> table;
  auto status = directoryReader.ReadTable(&table);
  if (!status.ok()) {
    std::cout << status << "\n";
    exit(1);
  }
  std::cout << "Read btr data successfully" << "\n";

  std::shared_ptr<arrow::io::FileOutputStream> outfile =
    arrow::io::FileOutputStream::Open(FLAGS_parquet).ValueOrDie();
  auto writeProps = getWriterProperties();
  status = parquet::arrow::WriteTable(*table, arrow::default_memory_pool(), outfile,
    writeProps->max_row_group_length(), writeProps);
  if (!status.ok()) {
    std::cout << status << "\n";
    exit(1);
  }
  std::cout << "Wrote parquet data successfully " << FLAGS_parquet << std::endl;
  return 0;
}