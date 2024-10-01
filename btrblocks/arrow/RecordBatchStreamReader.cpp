//--------------------------------------------------------------------------------------------------
#include "arrow/RecordBatchStreamReader.hpp"

#include <common/Utils.hpp>
//--------------------------------------------------------------------------------------------------
namespace btrblocks::arrow {
//--------------------------------------------------------------------------------------------------
RecordBatchStreamReader::ColumnReadState::ColumnReadState(
  const FileMetadata* file_metadata, int column_i, int chunk_i, const std::string& dir) :
    column_info(file_metadata->columns[column_i]),
    part_info(file_metadata->parts[column_info.part_offset]),
    metadata(file_metadata),
    path_prefix(dir + "/" + "column" + std::to_string(column_i) + "_part"){
  advance(chunk_i);
}
//--------------------------------------------------------------------------------------------------
void RecordBatchStreamReader::ColumnReadState::advance(int chunk_i) {
  while (global_chunk_i != chunk_i) {
    if (part_i == -1 || part_info.num_chunks == ++chunk_i) {
      part_info = metadata->parts[column_info.part_offset + ++part_i];
      chunk_i = 0;
    }
    global_chunk_i++;
  }
  const std::string filename = path_prefix + std::to_string(part_i);
  Utils::readFileToMemory(filename, buffer);
}
//--------------------------------------------------------------------------------------------------
RecordBatchStreamReader::RecordBatchStreamReader(
  std::string directory,
  const FileMetadata* file_metadata,
  const std::vector<int>& row_group_indices,
  const std::vector<int>& column_indices) :
      dir(std::move(directory)), chunks(row_group_indices) {
  for (int column_index : column_indices) {
    read_states.emplace_back(file_metadata, column_index, row_group_indices[0], dir);
  }
}
//--------------------------------------------------------------------------------------------------
template <typename T, typename U>
::arrow::Result<std::shared_ptr<::arrow::Array>> RecordBatchStreamReader::decompressNumericChunk(
    BtrReader& reader,int chunk_i){
  std::vector<u8> output_buffer;
  ::arrow::NumericBuilder<T> builder;

  const u32 tupleCount = reader.getTupleCount(chunk_i);
  reader.readColumn(output_buffer, chunk_i);
  auto* bitmap = reader.getBitmap(chunk_i);
  std::vector<u8> validityBytes(tupleCount);
  bitmap->writeValidityBytes(validityBytes.data());
  builder.AppendValues(reinterpret_cast<U*>(output_buffer.data()),
                       static_cast<int64_t>(tupleCount), validityBytes.data()).ok();
  return builder.Finish();
}
//--------------------------------------------------------------------------------------------------
::arrow::Result<std::shared_ptr<::arrow::Array>> RecordBatchStreamReader::decompressStringChunk(
    BtrReader& reader, int chunk_i){
  ::arrow::StringBuilder builder;
  std::vector<u8> output_buffer;

  const bool requiresCopy = reader.readColumn(output_buffer, chunk_i);
  const auto bitmap = reader.getBitmap(chunk_i);
  for (u32 j=0; j!=reader.getTupleCount(chunk_i); j++){
    if (!bitmap->test(j)){
      ARROW_RETURN_NOT_OK(builder.AppendNull());
    }else if (requiresCopy){
      auto string_pointer_array_viewer = StringPointerArrayViewer(reinterpret_cast<const u8*>(
        output_buffer.data()));
      ARROW_RETURN_NOT_OK(builder.Append(string_pointer_array_viewer(j)));
    }else{
      auto string_array_viewer = StringArrayViewer(reinterpret_cast<const u8*>(
        output_buffer.data()));
      ARROW_RETURN_NOT_OK(builder.Append(string_array_viewer(j)));
    }
  }
  return builder.Finish();
}
//--------------------------------------------------------------------------------------------------
::arrow::Status RecordBatchStreamReader::ReadNext(
    std::shared_ptr<::arrow::RecordBatch>* record_batch) {
  current_chunk = chunks[++chunk_index];
  for (auto& read_state : read_states) read_state.advance(current_chunk);
  ::arrow::ArrayVector arrays;

  *record_batch = ::arrow::RecordBatch::Make(schema_, 5, arrays);
  return ::arrow::Status::OK();
}
//--------------------------------------------------------------------------------------------------
::arrow::Status RecordBatchStreamReader::Close() {
  return ::arrow::Status::OK();
}
//--------------------------------------------------------------------------------------------------
} // namespace btrblocks::arrow
//--------------------------------------------------------------------------------------------------
