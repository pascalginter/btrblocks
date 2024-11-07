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
DEFINE_string(writer_properties, "writer_properties", "Parquet writer properties");
// ------------------------------------------------------------------------------
std::shared_ptr<parquet::WriterProperties> getWriterProperties() {
  std::shared_ptr<parquet::WriterProperties> props =
   parquet::WriterProperties::Builder()
     .compression(parquet::Compression::SNAPPY)
     //->disable_dictionary()->encoding(parquet::Encoding::PLAIN)
     ->build();
  return props;
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
  status = parquet::arrow::WriteTable(*table, arrow::default_memory_pool(), outfile, 65536, getWriterProperties());
  if (!status.ok()) {
    std::cout << status << "\n";
    exit(1);
  }
  std::cout << "Wrote parquet data successfully\n";
  return 0;
}