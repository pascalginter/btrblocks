#include <compression/BtrReader.hpp>

#include "arrow/ChunkToArrowArrayConverter.hpp"
//--------------------------------------------------------------------------------------------------
namespace btrblocks::arrow {
//--------------------------------------------------------------------------------------------------
//--------------------------------------------------------------------------------------------------
::arrow::Result<std::shared_ptr<::arrow::Array>> ChunkToArrowArrayConverter::convertStringChunkNoCopy(
    std::shared_ptr<::arrow::Buffer>&& buffer, u32 tupleCount, BitmapWrapper* bitmap){
  StringArrayViewer viewer(buffer->data());
  auto null_bitmap = ::arrow::AllocateBitmap(tupleCount, ::arrow::default_memory_pool()).ValueOrDie();
  bitmap->writeArrowBitmap(null_bitmap->mutable_data());
  auto data_buffer = ::arrow::SliceBuffer(buffer, viewer.data_offset(), viewer.data_size());
  auto offset_buffer = ::arrow::SliceBuffer(buffer, 0, viewer.data_offset());
  const auto offsets = reinterpret_cast<u32*>(buffer->mutable_data());
  for (u32 i=0; i<=tupleCount; i++) {
    offsets[i] -= viewer.data_offset();
  }
  auto array_data = ::arrow::ArrayData::Make(
    ::arrow::utf8(), tupleCount,
    {null_bitmap, std::move(offset_buffer), std::move(data_buffer)}
  );
  return ::arrow::MakeArray(array_data);
}
//--------------------------------------------------------------------------------------------------
::arrow::Result<std::shared_ptr<::arrow::Array>> ChunkToArrowArrayConverter::convertStringChunkCopy(
    std::shared_ptr<::arrow::Buffer>&& buffer, u32 tupleCount, BitmapWrapper* bitmap){
  StringPointerArrayViewer viewer(buffer->data());
  auto null_bitmap = ::arrow::AllocateBitmap(tupleCount, ::arrow::default_memory_pool()).ValueOrDie();
  bitmap->writeArrowBitmap(null_bitmap->mutable_data());
  auto data_buffer = ::arrow::AllocateBuffer(viewer.copied_data_size(tupleCount), ::arrow::default_memory_pool()).ValueOrDie();
  auto* data = data_buffer->mutable_data();
  auto offset_buffer = ::arrow::AllocateBuffer(sizeof(u32) * (tupleCount+1), ::arrow::default_memory_pool()).ValueOrDie();
  const auto offsets = reinterpret_cast<u32*>(offset_buffer->mutable_data());

  u32 copied_data_offset = 0;
  for (u32 i=0; i!=tupleCount; i++) {
    const auto& view = viewer.views[i];
    offsets[i] = copied_data_offset;
    memcpy(data + copied_data_offset, viewer.data + view.offset, view.length);
    copied_data_offset += viewer.views[i].length;
  }
  offsets[tupleCount] = copied_data_offset;

  auto array_data = ::arrow::ArrayData::Make(
    ::arrow::utf8(), tupleCount,
    {null_bitmap, std::move(offset_buffer), std::move(data_buffer)}
  );
  return ::arrow::MakeArray(array_data);
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
} // namespace btrblocks::arrow
//--------------------------------------------------------------------------------------------------