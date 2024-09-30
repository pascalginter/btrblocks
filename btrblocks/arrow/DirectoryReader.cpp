#include "arrow/DirectoryReader.hpp"
#include "common/Utils.hpp"
#include "compression/BtrReader.hpp"
#include "compression/Datablock.hpp"

#include <numeric>

namespace btrblocks::arrow {

DirectoryReader::DirectoryReader(std::string dir) : btr_dir{dir}{
  // Read the metadata
  {
    auto metadata_path = btr_dir / "metadata";
    Utils::readFileToMemory(metadata_path.string(), raw_file_metadata);
    file_metadata = reinterpret_cast<const FileMetadata*>(raw_file_metadata.data());
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
      case ColumnType::SKIP:
        continue;
      default:
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

std::vector<int> DirectoryReader::get_all_rowgroup_indices() {
  std::vector<int> row_group_indices(num_row_groups());
  std::iota(row_group_indices.begin(), row_group_indices.end(), 0);
  return row_group_indices;
}

std::vector<int> DirectoryReader::get_all_column_indices() {
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

template <typename T, typename U>
::arrow::Status DirectoryReader::readNumericColumn(int i, std::shared_ptr<::arrow::ChunkedArray>* out){
  std::vector<char> buffer;
  std::vector<u8> output_buffer;
  ::arrow::NumericBuilder<T> builder;
  ::arrow::ArrayVector arrays;

  for (u32 part_i = 0; part_i < file_metadata->columns[i].num_parts; part_i++) {
    auto path = btr_dir / ("column" + std::to_string(i) + "_part" + std::to_string(part_i));
    Utils::readFileToMemory(path.string(), buffer);
    BtrReader reader(buffer.data());

    for (u32 chunk_i = 0; chunk_i != reader.getChunkCount(); chunk_i++){
      u32 tupleCount = reader.getTupleCount(chunk_i);
      reader.readColumn(output_buffer, chunk_i);
      auto* bitmap = reader.getBitmap(chunk_i);
      std::vector<u8> validityBytes(tupleCount);
      bitmap->writeValidityBytes(validityBytes.data());
      builder.AppendValues(reinterpret_cast<U*>(output_buffer.data()),
                           static_cast<int64_t>(tupleCount), validityBytes.data()).ok();
      arrays.push_back(builder.Finish().ValueOrDie());
    }
    *out = ::arrow::ChunkedArray::Make(arrays).ValueOrDie();
  }
  return ::arrow::Status::OK();
}

::arrow::Status DirectoryReader::readStringColumn(int i, std::shared_ptr<::arrow::ChunkedArray>* out){
  ::arrow::StringBuilder builder;
  std::vector<char> buffer;
  std::vector<u8> output_buffer;
  ::arrow::ArrayVector arrays;

  for (u32 part_i = 0; part_i < file_metadata->columns[i].num_parts; part_i++) {
    auto path = btr_dir / ("column" + std::to_string(i) + "_part" + std::to_string(part_i));
    Utils::readFileToMemory(path.string(), buffer);
    BtrReader reader(buffer.data());


    for (u32 chunk_i = 0; chunk_i != reader.getChunkCount(); chunk_i++){
      bool requiresCopy = reader.readColumn(output_buffer, chunk_i);
      auto bitmap = reader.getBitmap(chunk_i);
      for (u32 j=0; j!=reader.getTupleCount(chunk_i); j++){
        if (!bitmap->test(j)){
          ARROW_RETURN_NOT_OK(builder.AppendNull());
        }else if (requiresCopy){
          auto string_pointer_array_viewer = StringPointerArrayViewer(reinterpret_cast<const u8*>(output_buffer.data()));
          ARROW_RETURN_NOT_OK(builder.Append(string_pointer_array_viewer(j)));
        }else{
          auto string_array_viewer = StringArrayViewer(reinterpret_cast<const u8*>(output_buffer.data()));
          ARROW_RETURN_NOT_OK(builder.Append(string_array_viewer(j)));
        }
      }
      arrays.push_back(builder.Finish().ValueOrDie());
    }
    *out = ::arrow::ChunkedArray::Make(arrays).ValueOrDie();
  }
  return ::arrow::Status::OK();
}

::arrow::Status DirectoryReader::ReadColumn(int i, std::shared_ptr<::arrow::ChunkedArray>* out) {
  if (schema->field(i)->type() == ::arrow::int32()){
    return readNumericColumn<::arrow::Int32Type, int32_t>(i, out);
  }else if (schema->field(i)->type() == ::arrow::float64()) {
    return readNumericColumn<::arrow::DoubleType, double>(i, out);
  }else if (schema->field(i)->type() == ::arrow::utf8()){
    return readStringColumn(i, out);
  }else{
    std::cout << "Unknown type encountered while reading column" << std::endl;
    exit(1);
  }
}

::arrow::Status DirectoryReader::GetRecordBatchReader(std::shared_ptr<::arrow::RecordBatchReader>* out) {
  return GetRecordBatchReader(get_all_rowgroup_indices(), out);
}

::arrow::Status DirectoryReader::GetRecordBatchReader(const std::vector<int>& row_group_indices,
                                     std::shared_ptr<::arrow::RecordBatchReader>* out){
  return GetRecordBatchReader(row_group_indices, get_all_column_indices(), out);
}

::arrow::Status DirectoryReader::GetRecordBatchReader(const std::vector<int>& row_group_indices,
                                     const std::vector<int>& column_indices,
                                     std::shared_ptr<::arrow::RecordBatchReader>* out){
  return ::arrow::Status::NotImplemented("GetRecordBatchReader");
}

::arrow::Status DirectoryReader::ReadTable(std::shared_ptr<::arrow::Table>* out){
  return ReadTable(get_all_column_indices(), out);
}

::arrow::Status DirectoryReader::ReadTable(const std::vector<int>& column_indices,
                          std::shared_ptr<::arrow::Table>* out){
  std::vector<std::shared_ptr<::arrow::ChunkedArray>> columns(column_indices.size());
  int i = 0;
  for (int index : column_indices){
    ARROW_RETURN_NOT_OK(ReadColumn(index, &columns[i++]));
  }
  ::arrow::FieldVector relevantFields;
  for (auto index : column_indices) relevantFields.push_back(schema->field(index));
  *out = ::arrow::Table::Make(::arrow::schema(relevantFields), columns);
  return ::arrow::Status::OK();
}

int DirectoryReader::num_row_groups() const {
  return file_metadata->num_chunks;
}

} // namespace btrblocks::arrow