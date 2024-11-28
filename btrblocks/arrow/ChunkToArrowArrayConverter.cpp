#include <compression/BtrReader.hpp>

#include "arrow/ChunkToArrowArrayConverter.hpp"
//--------------------------------------------------------------------------------------------------
namespace btrblocks::arrow {
//--------------------------------------------------------------------------------------------------
::arrow::Result<std::shared_ptr<::arrow::Array>> ChunkToArrowArrayConverter::convertStringChunkNoCopy(
    std::shared_ptr<::arrow::Buffer>&& buffer, u32 tupleCount, BitmapWrapper* bitmap){
  StringArrayViewer viewer(buffer->data());
  auto null_bitmap = ::arrow::AllocateBitmap(tupleCount, ::arrow::default_memory_pool()).ValueOrDie();
  bitmap->writeArrowBitmap(null_bitmap->mutable_data());
  auto data_buffer = ::arrow::SliceMutableBuffer(buffer, viewer.data_offset(), viewer.data_size());
  auto offset_buffer = ::arrow::SliceMutableBuffer(buffer, 0, viewer.data_offset());
  const auto offsets = reinterpret_cast<u32*>(buffer->mutable_data());
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
  std::vector<u32> map;
  StringPointerArrayViewer viewer(buffer->data());

  u32 data_offset = viewer.data_offset(tupleCount);
  auto char_buffer = ::arrow::SliceBuffer(buffer, data_offset);
  auto index_buffer = ::arrow::AllocateBuffer(tupleCount * sizeof(u32), ::arrow::default_memory_pool()).ValueOrDie();
  auto* indexes = index_buffer->mutable_data_as<u32>();
  for (u32 i=0; i!=tupleCount; i++) {
    if (std::find(map.cbegin(), map.cend(), viewer.views[i].offset + viewer.views[i].length) == map.cend()) {
      map.push_back(viewer.views[i].offset + viewer.views[i].length);
    }
    const auto it = std::find(map.cbegin(), map.cend(), viewer.views[i].offset + viewer.views[i].length);
    indexes[i] = it - map.cbegin();
  }

  auto offsets_buffer = ::arrow::AllocateBuffer((map.size() + 1) * sizeof(u32), ::arrow::default_memory_pool()).ValueOrDie();
  auto* offsets = offsets_buffer->mutable_data_as<u32>();
  offsets[0] = 0;
  int i=1;
  std::sort(map.begin(), map.end());
  for (auto el : map) {
    //std::cout << offsets[i-1] << " " <<  el.offset - data_offset << std::endl;
    // assert(offsets[i-1] == el - data_offset);
    offsets[i] = el - data_offset;
    i++;
  }

  auto null_bitmap = ::arrow::AllocateBitmap(tupleCount, ::arrow::default_memory_pool()).ValueOrDie();
  bitmap->writeArrowBitmap(null_bitmap->mutable_data());
  auto index_array_data = ::arrow::ArrayData::Make(
    ::arrow::int32(), tupleCount,
    {std::move(null_bitmap), std::move(index_buffer), nullptr}
  );
  auto index_array = ::arrow::MakeArray(index_array_data);

  auto dictionary_array_data = ::arrow::ArrayData::Make(
    ::arrow::utf8(), map.size(),
    {nullptr, std::move(offsets_buffer), std::move(char_buffer)}
  );
  auto dictionary_array = ::arrow::MakeArray(dictionary_array_data);
  return ::arrow::DictionaryArray::FromArrays(index_array, dictionary_array).ValueOrDie();
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