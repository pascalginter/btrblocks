#include <compression/BtrReader.hpp>

#include "arrow/ChunkToArrowArrayConverter.hpp"
//--------------------------------------------------------------------------------------------------
namespace btrblocks::arrow {
//--------------------------------------------------------------------------------------------------
//--------------------------------------------------------------------------------------------------
::arrow::Result<std::shared_ptr<::arrow::Array>> ChunkToArrowArrayConverter::convertStringChunk(
    StringArrayViewer viewer, u32 tupleCount, BitmapWrapper* bitmap){
  auto null_bitmap = ::arrow::AllocateBitmap(tupleCount, ::arrow::default_memory_pool()).ValueOrDie();
  bitmap->writeArrowBitmap(null_bitmap->mutable_data());
  auto data_buffer = ::arrow::AllocateBuffer(viewer.data_size(), ::arrow::default_memory_pool()).ValueOrDie();
  memcpy(data_buffer->mutable_data(), viewer.data_ptr(), viewer.data_size());
  auto offset_buffer = ::arrow::AllocateBuffer(viewer.data_offset()).ValueOrDie();
  const auto offsets = reinterpret_cast<u32*>(offset_buffer->mutable_data());
  const auto* slots = reinterpret_cast<const u32*>(viewer.slots_ptr);
  for (u32 i=0; i<=tupleCount; i++) {
    offsets[i] = slots[i] - viewer.data_offset();
  }
  auto array_data = ::arrow::ArrayData::Make(
    ::arrow::utf8(), tupleCount,
    {null_bitmap, std::move(offset_buffer), std::move(data_buffer)},
    bitmap->get_bitset()->count()
  );
  return ::arrow::MakeArray(array_data);
}
//--------------------------------------------------------------------------------------------------
::arrow::Result<std::shared_ptr<::arrow::Array>> ChunkToArrowArrayConverter::convertStringChunk(
    StringPointerArrayViewer viewer, u32 tupleCount, BitmapWrapper* bitmap){
  auto null_bitmap = ::arrow::AllocateBitmap(tupleCount, ::arrow::default_memory_pool()).ValueOrDie();
  bitmap->writeArrowBitmap(null_bitmap->mutable_data());
  auto data_buffer = ::arrow::AllocateBuffer(viewer.copied_data_size(tupleCount), ::arrow::default_memory_pool()).ValueOrDie();
  auto offset_buffer = ::arrow::AllocateBuffer(sizeof(u32) * (tupleCount+1), ::arrow::default_memory_pool()).ValueOrDie();
  const auto offsets = reinterpret_cast<u32*>(offset_buffer->mutable_data());

  u32 copied_data_offset = 0;
  for (u32 i=0; i!=tupleCount; i++) {
    const auto& view = viewer.views[i];
    offsets[i] = copied_data_offset;
    memcpy(data_buffer->mutable_data() + copied_data_offset, viewer.data + view.offset, view.length);
    copied_data_offset += viewer.views[i].length;
  }
  offsets[tupleCount] = copied_data_offset;

  auto array_data = ::arrow::ArrayData::Make(
    ::arrow::utf8(), tupleCount,
    {null_bitmap, std::move(offset_buffer), std::move(data_buffer)},
    bitmap->get_bitset()->count()
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