#pragma once
#include <arrow/api.h>

#include "common/Units.hpp"
#include "compression/Datablock.hpp"

namespace btrblocks::arrow::util {
// -------------------------------------------------------------------------------------------------
inline ColumnType ConvertArrowTypeToType(const std::shared_ptr<::arrow::DataType>& arrow_type){
  if (arrow_type == ::arrow::int32()){
    return ColumnType::INTEGER;
  }else if (arrow_type == ::arrow::float64()){
    return ColumnType::DOUBLE;
  }else if (arrow_type == ::arrow::utf8()){
    return ColumnType::STRING;
  }else{
    return ColumnType::UNDEFINED;
  }
}
// -------------------------------------------------------------------------------------------------
std::shared_ptr<::arrow::DataType> ConvertTypeToArrowType(ColumnType type) {
  switch (type) {
    case ColumnType::INTEGER:
      return ::arrow::int32();
    case ColumnType::DOUBLE:
      return ::arrow::float64();
    case ColumnType::STRING:
      return ::arrow::utf8();
    default:
      std::cout << "Unknown type" << std::endl;
      exit(1);
  }
}
// -------------------------------------------------------------------------------------------------
std::shared_ptr<::arrow::Schema> CreateSchemaFromFileMetadata(const FileMetadata* file_metadata) {
  ::arrow::FieldVector field_vector(file_metadata->num_columns);
  for (u32 i = 0; i != file_metadata->num_columns; i++) {
    field_vector[i] = ::arrow::field(
      "c_" + std::to_string(i),
      ConvertTypeToArrowType(file_metadata->columns[i].type));
  }
  return schema(field_vector);
}
// -------------------------------------------------------------------------------------------------
} // namespace btrblocks::arrow
// -------------------------------------------------------------------------------------------------