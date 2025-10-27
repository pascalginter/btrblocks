#include "arrow/DirectoryReader.hpp"
#include "arrow/RecordBatchStreamReader.hpp"
#include "common/Utils.hpp"
#include "compression/BtrReader.hpp"
#include "compression/Datablock.hpp"

#include <aws/core/Aws.h>
#include <aws/s3/S3Client.h>
#include <aws/s3/model/GetObjectRequest.h>
#include <aws/s3/S3Errors.h>

#include <numeric>

namespace btrblocks::arrow {

DirectoryReader::DirectoryReader(std::string dir) : btr_dir{dir}{
  // Read the metadata
  {
    Aws::S3::S3Client client;
    Aws::S3::Model::GetObjectRequest request;
    request.SetBucket(dir);
    request.SetKey(btr_dir / "metadata");
    if (const auto outcome = client.GetObject(request); outcome.IsSuccess()) {
      const size_t length = outcome.GetResult().GetContentLength();
      char* result = static_cast<char*>(malloc(length));
      outcome.GetResult().GetBody().read(result, length);
      file_metadata = FileMetadata::fromMemory(result);
    } else {
      std::cout << outcome.GetError().GetMessage().c_str() << std::endl;
      exit(1);
    }
  }

  ::arrow::FieldVector fields;
  for (u32 i = 0; i != file_metadata->num_columns; i++){
    std::shared_ptr<::arrow::DataType> type;
    switch (file_metadata->columns[i].type){
      case ColumnType::INTEGER:
        type = ::arrow::int32();
        break;
      case ColumnType::DOUBLE:
        type = ::arrow::float64();
        break;
      case ColumnType::STRING:
        type = ::arrow::utf8();
        break;
      default:
        std::cout << i << " " << static_cast<int>(file_metadata->columns[i].type) << std::endl;
        exit(1);
    }
    fields.push_back(::arrow::field("c_" + std::to_string(i), type));
  }
  schema = ::arrow::schema(fields);
}

::arrow::Status DirectoryReader::GetSchema(std::shared_ptr<::arrow::Schema>* out) {
  *out = schema;
  return ::arrow::Status::OK();
}

std::vector<int> DirectoryReader::get_all_row_group_indices() const {
  std::vector<int> row_group_indices(num_row_groups());
  std::iota(row_group_indices.begin(), row_group_indices.end(), 0);
  return row_group_indices;
}

std::vector<int> DirectoryReader::get_all_column_indices() const {
  std::vector<int> column_indices;
  for (u32 i=0; i!=file_metadata->num_columns; i++){
    if (file_metadata->columns[i].type == ColumnType::INTEGER
        || file_metadata->columns[i].type == ColumnType::DOUBLE
        || file_metadata->columns[i].type == ColumnType::STRING){
      column_indices.push_back(i);
    }
  }
  return column_indices;
}

::arrow::Status DirectoryReader::GetRecordBatchReader(std::shared_ptr<::arrow::RecordBatchReader>* out) {
  return GetRecordBatchReader(get_all_row_group_indices(), out);
}

::arrow::Status DirectoryReader::GetRecordBatchReader(const std::vector<int>& row_group_indices,
                                     std::shared_ptr<::arrow::RecordBatchReader>* out){
  return GetRecordBatchReader(row_group_indices, get_all_column_indices(), out);
}

::arrow::Status DirectoryReader::GetRecordBatchReader(const std::vector<int>& row_group_indices,
                                     const std::vector<int>& column_indices,
                                     std::shared_ptr<::arrow::RecordBatchReader>* out){
  *out = std::make_shared<RecordBatchStreamReader>(
    std::string(btr_dir.c_str()), file_metadata, row_group_indices, column_indices);
  return ::arrow::Status::OK();
}

::arrow::Status DirectoryReader::ReadTable(std::shared_ptr<::arrow::Table>* out){
  return ReadTable(get_all_column_indices(), out);
}

::arrow::Status DirectoryReader::ReadTable(const std::vector<int>& column_indices,
                          std::shared_ptr<::arrow::Table>* out){
  std::shared_ptr<::arrow::RecordBatchReader> recordBatchReader;
  ARROW_RETURN_NOT_OK(GetRecordBatchReader(get_all_row_group_indices(), column_indices, &recordBatchReader));
  auto result = recordBatchReader->ToTable();
  if (result.ok()) *out = result.ValueOrDie();
  return result.status();
}

int DirectoryReader::num_row_groups() const {
  return file_metadata->num_chunks;
}

} // namespace btrblocks::arrow