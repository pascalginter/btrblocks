#pragma once

#include <arrow/api.h>

#include "extern/RoaringBitmap.hpp"
#include "storage/StringArrayViewer.hpp"
#include "storage/StringPointerArrayViewer.hpp"
#include "compression/BtrReader.hpp"
//--------------------------------------------------------------------------------------------------
namespace btrblocks::arrow {
//--------------------------------------------------------------------------------------------------
struct ChunkToArrowArrayConverter {
  //------------------------------------------------------------------------------------------------
  template <typename T, typename U>
  static ::arrow::Result<std::shared_ptr<::arrow::Array>> convertNumericChunk(
    U* buffer, u32 tupleCount, const BITMAP* bitmap) {
    std::vector<u8> output_buffer(tupleCount * sizeof(U));
    ::arrow::NumericBuilder<T> builder;

    BtrReader reader(buffer);
    builder.AppendValues(reinterpret_cast<U*>(output_buffer.data()), static_cast<int64_t>(tupleCount), bitmap).ok();
    return builder.Finish();
  }
  //------------------------------------------------------------------------------------------------
  static ::arrow::Result<std::shared_ptr<::arrow::Array>> convertStringChunk(
    StringArrayViewer viewer, u32 num_elements, BitmapWrapper* bitmap);
  static ::arrow::Result<std::shared_ptr<::arrow::Array>> convertStringChunk(
   StringPointerArrayViewer viewer, u32 num_elements, BitmapWrapper* bitmap);
  //------------------------------------------------------------------------------------------------
  static ::arrow::Result<std::shared_ptr<::arrow::Array>> convertStringChunk(
    const Vector<str>& vector, const Vector<BITMAP>& bitmap);
  //------------------------------------------------------------------------------------------------
};
//--------------------------------------------------------------------------------------------------
} // namespace btrblocks::arrow
//--------------------------------------------------------------------------------------------------