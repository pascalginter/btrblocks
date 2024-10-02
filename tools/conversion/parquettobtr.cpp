#include <arrow/api.h>
#include <arrow/io/api.h>
#include <parquet/arrow/reader.h>

#include <iostream>

#include "arrow/arrow.hpp"

#include "scheme/SchemePool.hpp"

#include "btrfiles.hpp"

int main(){
  std::shared_ptr<arrow::io::RandomAccessFile> input =
      arrow::io::ReadableFile::Open("lineitem.parquet").ValueOrDie();

  // Open Parquet file reader
  std::unique_ptr<parquet::arrow::FileReader> arrow_reader;
  auto status = parquet::arrow::OpenFile(input, arrow::default_memory_pool(), &arrow_reader);
   if (!status.ok()) {
     std::cout << status << std::endl;
   }

  std::shared_ptr<arrow::Table> table;
  status = arrow_reader->ReadTable(&table);
  if (!status.ok()) {
    std::cout << status << std::endl;
  }
  std::cout << table->ToString() << "\n";

  btrblocks::SchemePool::refresh();

  btrblocks::Relation relation= btrblocks::arrow::parseArrowTable(table);
  btrblocks::files::writeDirectory(relation, "binary/", "stats.txt", "compressionout.txt");
  std::cout << "done" << std::endl;
}