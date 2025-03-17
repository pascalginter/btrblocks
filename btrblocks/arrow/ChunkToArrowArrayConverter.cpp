#include <compression/BtrReader.hpp>

#include "arrow/ChunkToArrowArrayConverter.hpp"

#include <common/Log.hpp>
//--------------------------------------------------------------------------------------------------
namespace btrblocks::arrow {
//--------------------------------------------------------------------------------------------------
//--------------------------------------------------------------------------------------------------
::arrow::Result<std::shared_ptr<::arrow::Array>> ChunkToArrowArrayConverter::convertStringChunkNoCopy(
    std::shared_ptr<::arrow::Buffer>&& buffer, u32 tupleCount, BitmapWrapper* bitmap){
  StringArrayViewer viewer(buffer->data());
  auto null_bitmap = ::arrow::AllocateBitmap(tupleCount, ::arrow::default_memory_pool()).ValueOrDie();
  bitmap->writeArrowBitmap(null_bitmap->mutable_data());
  auto data_buffer = ::arrow::SliceMutableBuffer(buffer, viewer.data_offset(), viewer.data_size());
  auto offset_buffer = ::arrow::SliceMutableBuffer(buffer, 0, viewer.data_offset());
  const auto offsets = reinterpret_cast<u32*>(offset_buffer->mutable_data());
  assert(tupleCount == viewer.tuple_count());
  for (u32 i=1; i<=tupleCount; i++) {
    offsets[i] -= offsets[0];
  }
  offsets[0] = 0;
  auto array_data = ::arrow::ArrayData::Make(
    ::arrow::utf8(), tupleCount,
    {std::move(null_bitmap), std::move(offset_buffer), std::move(data_buffer)}
  );
  return ::arrow::MakeArray(array_data);
}
//--------------------------------------------------------------------------------------------------
::arrow::Result<std::shared_ptr<::arrow::Array>> ChunkToArrowArrayConverter::convertStringChunkCopy(
    std::shared_ptr<::arrow::Buffer>&& buffer, u32 tupleCount, BitmapWrapper* bitmap){
  StringPointerArrayViewer viewer(buffer->data(), tupleCount);
  u32 unique_tuple_count = viewer.viewer.tuple_count();
  u32 viewer_offset = viewer.viewer_offset();
  u32 data_offset = viewer.data_offset();
  auto char_buffer = ::arrow::SliceMutableBuffer(buffer, data_offset);
  auto index_buffer = ::arrow::SliceMutableBuffer(buffer, 0, (tupleCount) * sizeof(INTEGER));
  auto offsets_buffer = ::arrow::SliceMutableBuffer(buffer, viewer_offset,
    (unique_tuple_count + 1) * sizeof(INTEGER));
  auto* offsets = offsets_buffer->mutable_data_as<INTEGER>();
  for (int i=1; i<=unique_tuple_count; i++) {
    offsets[i] -= offsets[0];
  }
  offsets[0] = 0;

  auto null_bitmap = ::arrow::AllocateBitmap(tupleCount, ::arrow::default_memory_pool()).ValueOrDie();
  bitmap->writeArrowBitmap(null_bitmap->mutable_data());
  auto index_array_data = ::arrow::ArrayData::Make(
    ::arrow::int32(), tupleCount,
    {std::move(null_bitmap), std::move(index_buffer)}
  );
  auto index_array = ::arrow::MakeArray(index_array_data);

  auto dictionary_array_data = ::arrow::ArrayData::Make(
    ::arrow::utf8(), unique_tuple_count,
    {nullptr, std::move(offsets_buffer), std::move(char_buffer)}
  );
  auto dictionary_array = ::arrow::MakeArray(dictionary_array_data);
  return std::make_shared<::arrow::DictionaryArray>(dictionary_type, index_array, dictionary_array);
}
//--------------------------------------------------------------------------------------------------
::arrow::Result<std::shared_ptr<::arrow::Array>> ChunkToArrowArrayConverter::convertStringChunk(
   const Vector<str>& vector, const Vector<BITMAP>& bitmap) {
  ::arrow::StringBuilder builder;
  u32 tupleCount = vector.size();
  for (u32 i=0; i!=tupleCount; i++) {
    if (!bitmap[i]) {
      ARROW_RETURN_NOT_OK(builder.AppendNull());
    } else {
      ARROW_RETURN_NOT_OK(builder.Append(vector[i]));
    }
  }
  return builder.Finish();
}
//--------------------------------------------------------------------------------------------------
const std::shared_ptr<::arrow::DataType> ChunkToArrowArrayConverter::dictionary_type =
  ::arrow::dictionary(::arrow::int32(), ::arrow::utf8());
//--------------------------------------------------------------------------------------------------
} // namespace btrblocks::arrow
//--------------------------------------------------------------------------------------------------