#include <arrow/io/api.h>
#include <parquet/arrow/writer.h>

#include <iostream>

#include "arrow/DirectoryReader.hpp"
#include "scheme/SchemePool.hpp"

int main(){
  btrblocks::SchemePool::refresh();
  btrblocks::arrow::DirectoryReader directoryReader("./binary");
  std::shared_ptr<arrow::Table> table;
  if (!directoryReader.ReadTable(&table).ok()){
    std::cout << "Error reading btr directory\n";
    exit(1);
  }

  std::cout << table->ToString() << std::endl;

  std::shared_ptr<arrow::io::FileOutputStream> outfile =
    arrow::io::FileOutputStream::Open("btr.parquet").ValueOrDie();
  parquet::arrow::WriteTable(*table.get(), arrow::default_memory_pool(), outfile).ok();
  std::cout << "done" << std::endl;
}