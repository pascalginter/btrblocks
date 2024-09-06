#include <arrow/api.h>
#include <arrow/io/api.h>
#include <parquet/arrow/reader.h>

#include <iostream>

#include "arrow/arrow.hpp"

#include "scheme/SchemePool.hpp"

#include "btrfiles.hpp"

int main(){
  std::shared_ptr<arrow::io::RandomAccessFile> input =
      arrow::io::ReadableFile::Open("btr.parquet").ValueOrDie();

  // Open Parquet file reader
  std::unique_ptr<parquet::arrow::FileReader> arrow_reader;
  parquet::arrow::OpenFile(input, arrow::default_memory_pool(), &arrow_reader).ok();

  std::shared_ptr<arrow::Table> table;
  arrow_reader->ReadTable(&table).ok();
  std::cout << table->ToString() << "\n";

  btrblocks::SchemePool::refresh();

  btrblocks::Relation relation= btrblocks::arrow::parseArrowTable(table);
  btrblocks::files::writeDirectory(relation, "binary/", "stats.txt", "compressionout.txt");
  std::cout << "done" << std::endl;
}