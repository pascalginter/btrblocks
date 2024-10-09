#pragma once
#include <arrow/api.h>

#include "common/Units.hpp"
#include "compression/Datablock.hpp"

// -------------------------------------------------------------------------------------------------
namespace btrblocks::arrow::util {
// -------------------------------------------------------------------------------------------------
inline ColumnType ConvertArrowTypeToType(const std::shared_ptr<::arrow::DataType>& arrow_type);
std::shared_ptr<::arrow::DataType> ConvertTypeToArrowType(ColumnType type);
std::shared_ptr<::arrow::Schema> CreateSchemaFromFileMetadata(const FileMetadata* file_metadata);
// -------------------------------------------------------------------------------------------------
} // namespace btrblocks::arrow
// -------------------------------------------------------------------------------------------------