#include "ColumnReadState.hpp"
#include "ChunkToArrowArrayConverter.hpp"
#include "common/Utils.hpp"
//--------------------------------------------------------------------------------------------------
namespace btrblocks::arrow {
//--------------------------------------------------------------------------------------------------
ColumnReadState::ColumnReadState(
  const FileMetadata* file_metadata, int column_i, int chunk_i, const std::string& dir) :
    column_info(file_metadata->columns[column_i]),
    part_info(file_metadata->parts[column_info.part_offset]),
    metadata(file_metadata),
    path_prefix(dir + "/" + "column" + std::to_string(column_i) + "_part"){
  advance(chunk_i);
}
//--------------------------------------------------------------------------------------------------
template <typename T>
::arrow::Result<std::shared_ptr<::arrow::Array>> ColumnReadState::decompressNumericChunk(){
  auto outputBuffer = ::arrow::AllocateBuffer(reader->getDecompressedSize(chunk_i), ::arrow::default_memory_pool()).ValueOrDie();
  const bool requiresCopy = reader->readColumn(outputBuffer->mutable_data(), chunk_i);
  assert(!requiresCopy);
  const u32 tupleCount = reader->getTupleCount(chunk_i);
  auto* bitmap = reader->getBitmap(chunk_i);
  return ChunkToArrowArrayConverter::convertNumericChunk<T>(
    std::move(outputBuffer), tupleCount, bitmap);
}
//--------------------------------------------------------------------------------------------------
::arrow::Result<std::shared_ptr<::arrow::Array>> ColumnReadState::decompressStringChunk(){
  u32 tupleCount = reader->getTupleCount(chunk_i);
  const auto bitmap = reader->getBitmap(chunk_i);
  auto outputBuffer = ::arrow::AllocateBuffer(reader->getDecompressedSize(chunk_i), ::arrow::default_memory_pool()).ValueOrDie();
  const bool requiresCopy = reader->readColumn(outputBuffer->mutable_data(), chunk_i);
  if (requiresCopy) {
    return ChunkToArrowArrayConverter::convertStringChunkCopy(std::move(outputBuffer), tupleCount, bitmap);
  } else {
    return ChunkToArrowArrayConverter::convertStringChunkNoCopy(std::move(outputBuffer), tupleCount, bitmap);
  }
}
//--------------------------------------------------------------------------------------------------
::arrow::Result<std::shared_ptr<::arrow::Array>> ColumnReadState::decompressCurrentChunk() {
  switch (column_info.type) {
    case ColumnType::INTEGER:
      return decompressNumericChunk<::arrow::Int32Type>();
    case ColumnType::DOUBLE:
      return decompressNumericChunk<::arrow::DoubleType>();
    case ColumnType::STRING:
      return decompressStringChunk();
    default:
      return ::arrow::Status::Invalid("Type can not be converted");
  }
}
//--------------------------------------------------------------------------------------------------
void ColumnReadState::advance(int next_chunk_i) {
  bool part_i_changed = false;
  while (global_chunk_i != next_chunk_i) {
    assert(global_chunk_i < next_chunk_i);
    if (part_i == -1 || part_info.num_chunks == ++chunk_i) {
      part_info = metadata->parts[column_info.part_offset + ++part_i];
      part_i_changed = true;
      assert(part_i < column_info.num_parts);
      chunk_i = 0;
    }
    global_chunk_i++;
  }
  if (part_i_changed) {
    const std::string filename = path_prefix + std::to_string(part_i);
    Utils::readFileToMemory(filename, buffer);
    reader = BtrReader(buffer.data());
  }
}
//--------------------------------------------------------------------------------------------------
} // namespace btrblocks::arrow
//--------------------------------------------------------------------------------------------------