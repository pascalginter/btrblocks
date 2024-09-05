#include <iostream>
#include "arrow/DirectoryReader.hpp"
#include "scheme/SchemePool.hpp"

int main(){
  btrblocks::SchemePool::refresh();
  btrblocks::arrow::DirectoryReader directoryReader("./binary");
  std::shared_ptr<arrow::Table> table;
  directoryReader.ReadTable(&table).Warn();
  std::cout << "done reading" << std::endl;
  std::cout << table->schema() << std::endl;
  std::cout << table->ToString() << std::endl;
}