#pragma once

#include <arrow/api.h>

#include "extern/RoaringBitmap.hpp"
#include "compression/BtrReader.hpp"
//--------------------------------------------------------------------------------------------------
namespace btrblocks::arrow {
//--------------------------------------------------------------------------------------------------
struct ChunkToArrowArrayConverter {
  // -----------------------------------------------------------------------------------------------
  template <typename T, typename U>
  static ::arrow::Result<std::shared_ptr<::arrow::Array>> convertNumericChunk(
      U* data, u32 num_elements, unsigned char* bitmap) {
    ::arrow::NumericBuilder<T> builder;
    builder.AppendValues(data, static_cast<int64_t>(num_elements), bitmap).ok();
    return builder.Finish();
  }
  //------------------------------------------------------------------------------------------------
  static ::arrow::Result<std::shared_ptr<::arrow::Array>> convertStringChunkNoCopy(
    std::shared_ptr<::arrow::Buffer>&& buffer, u32 num_elements, BitmapWrapper* bitmap);
  static ::arrow::Result<std::shared_ptr<::arrow::Array>> convertStringChunkCopy(
   std::shared_ptr<::arrow::Buffer>&& buffer, u32 num_elements, BitmapWrapper* bitmap);
  //------------------------------------------------------------------------------------------------
  static ::arrow::Result<std::shared_ptr<::arrow::Array>> convertStringChunk(
    const Vector<str>& vector, const Vector<BITMAP>& bitmap);
  //------------------------------------------------------------------------------------------------
};
//--------------------------------------------------------------------------------------------------
} // namespace btrblocks::arrow
//--------------------------------------------------------------------------------------------------