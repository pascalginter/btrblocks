#include <arrow/api.h>

#include "common/Units.hpp"

namespace btrblocks {
inline namespace units {
// -------------------------------------------------------------------------------------
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
// -------------------------------------------------------------------------------------
} // namespace units
} // namespace btrblocks
// -------------------------------------------------------------------------------------