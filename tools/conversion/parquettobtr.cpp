#include <arrow/api.h>
#include <arrow/io/api.h>

#include "gflags/gflags.h"

#include <parquet/arrow/reader.h>

#include <iostream>

#include "arrow/arrow.hpp"

#include "scheme/SchemePool.hpp"

#include "btrfiles.hpp"

DEFINE_string(parquet, "parquet", "Parquet input file");
DEFINE_string(btr, "btr", "Directory for btr output");

int main(int argc, char** argv){
  gflags::ParseCommandLineFlags(&argc, &argv, true);

  std::shared_ptr<arrow::io::RandomAccessFile> input =
      arrow::io::ReadableFile::Open(FLAGS_parquet).ValueOrDie();

  // Open Parquet file reader
  std::unique_ptr<parquet::arrow::FileReader> arrow_reader;
  auto status = parquet::arrow::OpenFile(input, arrow::default_memory_pool(), &arrow_reader);
   if (!status.ok()) {
     std::cerr << status << '\n';
     exit(1);
   }

  std::shared_ptr<arrow::Table> table;
  status = arrow_reader->ReadTable(&table);
  if (!status.ok()) {
    std::cerr << status << '\n';
    exit(1);
  }
  std::cout << "Read parquet data successfully" << "\n";

  btrblocks::SchemePool::refresh();

  btrblocks::Relation relation= btrblocks::arrow::parseArrowTable(table);
  btrblocks::files::writeDirectory(relation, FLAGS_btr + "/", "stats.txt", "compressionout.txt");
  std::cout << "Wrote btr data successfully\n";
  return 0;
}