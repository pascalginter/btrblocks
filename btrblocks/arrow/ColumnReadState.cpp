#include <aws/s3/model/GetObjectRequest.h>
#include <aws/s3/model/HeadObjectRequest.h>
#include <aws/s3/S3Errors.h>

#include "ColumnReadState.hpp"
#include "ChunkToArrowArrayConverter.hpp"
#include "common/Utils.hpp"
//--------------------------------------------------------------------------------------------------
namespace btrblocks::arrow {
//--------------------------------------------------------------------------------------------------
const std::string ColumnReadState::bucket = "adl-tpch";
ColumnReadStateCache ColumnReadState::shared_state = {};
thread_local Aws::S3::S3Client ColumnReadState::client = {};
//--------------------------------------------------------------------------------------------------
char* ColumnReadState::fetchFile(std::string fileName) {
  Aws::S3::Model::GetObjectRequest request;
  request.SetBucket(bucket);
  request.SetKey(fileName);
  char* result;
  if (const auto outcome = client.GetObject(request); outcome.IsSuccess()) {
    const size_t length = outcome.GetResult().GetContentLength();
    result = static_cast<char*>(malloc(length));
    outcome.GetResult().GetBody().read(result, length);
    return result;
  } else {
    std::cout << outcome.GetError().GetMessage().c_str() << std::endl;
    exit(1);
  }
}
//--------------------------------------------------------------------------------------------------
ColumnReadState::ColumnReadState(
  const FileMetadata* file_metadata, int column_i, int chunk_i, const std::string& dir) :
    column_info(file_metadata->columns[column_i]),
    part_info(file_metadata->parts[column_info.part_offset]),
    metadata(file_metadata),
    path_prefix(dir + "/" + "column" + std::to_string(column_i) + "_part"){
  advance(chunk_i);
  if (shared_state.parts_cache.find(column_info) == shared_state.parts_cache.end()) {
    shared_state.parts_cache[column_info] = std::vector<char*>(column_info.num_parts, nullptr);
  }
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
    if (shared_state.parts_cache[column_info][part_i] == nullptr) {
      shared_state.parts_cache[column_info][part_i] = fetchFile(path_prefix + std::to_string(part_i));
    }
    reader = BtrReader(shared_state.parts_cache[column_info][part_i]);
  }
}
//--------------------------------------------------------------------------------------------------
void ColumnReadState::reset() {
  part_i = -1;
  chunk_i = -1;
  global_chunk_i = -1;
}

//--------------------------------------------------------------------------------------------------
} // namespace btrblocks::arrow
//--------------------------------------------------------------------------------------------------