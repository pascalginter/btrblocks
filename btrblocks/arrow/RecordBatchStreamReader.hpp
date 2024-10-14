#pragma once

#include <arrow/api.h>

#include <compression/Datablock.hpp>
#include <compression/BtrReader.hpp>
//--------------------------------------------------------------------------------------------------
namespace btrblocks::arrow {
//--------------------------------------------------------------------------------------------------
class RecordBatchStreamReader final : public ::arrow::RecordBatchReader {
  //------------------------------------------------------------------------------------------------
  class ColumnReadState {
    std::vector<char> buffer;

    int chunk_index = 0;
    int global_chunk_i = -1;
    int chunk_i = -1;
    int part_i = -1;

    const ColumnInfo& column_info;
    ColumnPartInfo& part_info;
    const FileMetadata* metadata;

    std::string path_prefix;

    template <typename T, typename U>
    ::arrow::Result<std::shared_ptr<::arrow::Array>> decompressNumericChunk();
    ::arrow::Result<std::shared_ptr<::arrow::Array>> decompressStringChunk();
  public:
    ColumnReadState(const FileMetadata* file_metadata, int column_i, int chunk_i, const std::string& dir);

    ::arrow::Result<std::shared_ptr<::arrow::Array>> decompressCurrentChunk();
    void advance(int chunk_i);
  };
  //--------------------------------------------------------------------------------------------------
  std::string dir;
  std::vector<int> chunks;
  std::shared_ptr<::arrow::Schema> schema_;
  std::vector<ColumnReadState> read_states;
  int current_chunk = -1;
  int chunk_index = -1;

public:
  RecordBatchStreamReader(std::string directory, const FileMetadata* file_metadata,
    const std::vector<int>& row_group_indices, const std::vector<int>& column_indices);
  ~RecordBatchStreamReader() override = default;
  [[nodiscard]] std::shared_ptr<::arrow::Schema> schema() const override { return schema_; };
  ::arrow::Status ReadNext(std::shared_ptr<::arrow::RecordBatch>* record_batch) override;
  inline ::arrow::Status Close() override;
};
//--------------------------------------------------------------------------------------------------
} // namespace btrblocks::arrow
//--------------------------------------------------------------------------------------------------