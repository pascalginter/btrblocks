 #pragma once
#include <arrow/api.h>
#include <aws/core/Aws.h>
#include <aws/s3/S3Client.h>
//--------------------------------------------------------------------------------------------------
#include <compression/Datablock.hpp>
#include <compression/BtrReader.hpp>
//--------------------------------------------------------------------------------------------------
namespace btrblocks::arrow {
//--------------------------------------------------------------------------------------------------
struct ColumnReadStateCache {
  std::map<ColumnInfo, std::vector<char*>> parts_cache;
};

class ColumnReadState {
    static ColumnReadStateCache shared_state;
    static const std::string bucket;
    static thread_local Aws::S3::S3Client client;

    int global_chunk_i = -1;
    int chunk_i = -1;
    int part_i = -1;

    const ColumnInfo& column_info;
    ColumnPartInfo part_info;
    const FileMetadata* metadata;
    std::optional<BtrReader> reader = std::nullopt;

    std::string path_prefix;

    template <typename T>
    ::arrow::Result<std::shared_ptr<::arrow::Array>> decompressNumericChunk();
    ::arrow::Result<std::shared_ptr<::arrow::Array>> decompressStringChunk();

    char* fetchFile(std::string fileName);
  public:
    ColumnReadState(const FileMetadata* file_metadata, int column_i, int chunk_i, const std::string& dir);

    ::arrow::Result<std::shared_ptr<::arrow::Array>> decompressCurrentChunk();
    void advance(int chunk_i);
    void reset();
  };
//--------------------------------------------------------------------------------------------------
} // namespace btrblocks::arrow
//--------------------------------------------------------------------------------------------------