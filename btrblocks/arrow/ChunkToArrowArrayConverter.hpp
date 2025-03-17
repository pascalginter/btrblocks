#pragma once

#include <arrow/api.h>

#include "extern/RoaringBitmap.hpp"
#include "compression/BtrReader.hpp"
//--------------------------------------------------------------------------------------------------
namespace btrblocks::arrow {
//--------------------------------------------------------------------------------------------------
struct ChunkToArrowArrayConverter {
  // -----------------------------------------------------------------------------------------------
  static const std::shared_ptr<::arrow::DataType>  dictionary_type;
  // -----------------------------------------------------------------------------------------------
  template <typename T>
  static ::arrow::Result<std::shared_ptr<::arrow::Array>> convertNumericChunk(
    std::shared_ptr<::arrow::Buffer>&& buffer, u32 num_elements, BitmapWrapper* bitmap) {
    auto null_bitmap = ::arrow::AllocateBitmap(num_elements, ::arrow::default_memory_pool()).ValueOrDie();
    bitmap->writeArrowBitmap(null_bitmap->mutable_data());
    auto array_data = ::arrow::ArrayData::Make(
      ::arrow::TypeTraits<T>::type_singleton(), num_elements,
      {null_bitmap, buffer}
    );
    return ::arrow::MakeArray(array_data);
  }

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