// ------------------------------------------------------------------------------
#include "storage/Relation.hpp"

#include "ChunkToArrowArrayConverter.hpp"
#include "Util.hpp"
#include "arrow.hpp"
#include "btrblocks.hpp"
#include "common/Units.hpp"
// ------------------------------------------------------------------------------
namespace btrblocks::arrow {
// ------------------------------------------------------------------------------
using Data = std::variant<Vector<INTEGER>, Vector<DOUBLE>, Vector<str>>;
// ------------------------------------------------------------------------------
template <typename T>
Column parseFixedSizeArrowColumn(const std::string& name,
                                 const std::shared_ptr<::arrow::ChunkedArray>& chunkedArr){
  Data column_data = Vector<T>(chunkedArr->length());
  Vector<u8> bitmap(chunkedArr->length());
  s64 offset = 0;
  for (const auto& arr : chunkedArr->chunks()) {
    memcpy(std::get<Vector<T>>(column_data).data + offset, arr->data()->buffers[1]->data(),
           arr->length() * sizeof(T));

    for (int i=0; i!=arr->length(); i++){
      bitmap[i] = arr->data()->buffers[0] == nullptr
                      ? 1
                      : (arr->data()->buffers[0]->data()[i / 8] & (1 << i % 8));
    }
    offset += arr->length();
  }

  return {name, std::move(column_data), std::move(bitmap)};
}

// TODO writing and reading should be completely unecessary but for prototyping ok
Column parseStringArrowColumn(const std::string& name,
                              const std::shared_ptr<::arrow::ChunkedArray>& chunkedArr){
  int64_t totalLength = 0;
  for (const auto& arr : chunkedArr->chunks()) {
    totalLength += arr->data()->buffers[2]->size();
  }
  Vector<str> column_data(chunkedArr->length(), totalLength);
  Vector<u8> bitmap(chunkedArr->length());
  int global_i = 0, offset = (2 * chunkedArr->length() + 1ull) * sizeof(uint64_t);
  for (const auto& arr : chunkedArr->chunks()){
    const auto* stringOffsets = reinterpret_cast<const int32_t*>(arr->data()->buffers[1]->data());
    for (int i=0; i!=arr->length(); i++, global_i++){
      int32_t stringBegin = stringOffsets[i];
      int32_t stringEnd = stringOffsets[i+1];
      int32_t stringLength = stringEnd - stringBegin;
      column_data.data->slot[global_i].offset = offset;
      column_data.data->slot[global_i].size = stringLength;
      memcpy(reinterpret_cast<u8*>(column_data.data) + offset, arr->data()->buffers[2]->data() + stringBegin, stringLength);
      offset += stringLength;
      bitmap[global_i] = arr->data()->buffers[0] == nullptr
                             ? 1
                             : (arr->data()->buffers[0]->data()[i / 8] & (1 << i % 8));
    }
  }
  return {name, std::move(column_data), std::move(bitmap)};
}

std::optional<Column> parseArrowColumn(const std::string& name,
                        const std::shared_ptr<::arrow::ChunkedArray>& chunkedArr){
    if (chunkedArr->type() == ::arrow::int32()) {
      return parseFixedSizeArrowColumn<INTEGER>(name, chunkedArr);
    }else if (chunkedArr->type() == ::arrow::float64()){
      return parseFixedSizeArrowColumn<DOUBLE>(name, chunkedArr);
    }else if (chunkedArr->type() == ::arrow::utf8()){
      return parseStringArrowColumn(name, chunkedArr);
    }else{
      return std::nullopt;
    }
}
// ------------------------------------------------------------------------------
Relation parseArrowTable(const std::shared_ptr<::arrow::Table>& table){
  Relation result;
  for (int column_i=0; column_i!=table->num_columns(); column_i++){
    std::string column_name = table->schema()->field(column_i)->name();
    if (auto potentiallyParsedColumn = parseArrowColumn(column_name, table->column(column_i))) {
      result.columns.push_back(std::move(potentiallyParsedColumn.value()));
    }
  }
  result.fixTupleCount();
  return result;
}
// ------------------------------------------------------------------------------
::arrow::Result<std::shared_ptr<::arrow::Table>> parseRelationToArrowTable(const Relation& relation) {
  ::arrow::FieldVector fields;
  ::arrow::ArrayVector columns;
  for (const auto& column : relation.columns) {
    fields.push_back(::arrow::field(column.name, util::ConvertTypeToArrowType(column.type)));
    ::arrow::Result<std::shared_ptr<::arrow::Array>> array;
    switch (column.type) {
      case ColumnType::INTEGER:
        array = ChunkToArrowArrayConverter::convertNumericChunk<::arrow::Int32Type, int32_t>(
          column.integers().data, column.size(), column.bitmaps().data);
        break;
      case ColumnType::DOUBLE:
        array = ChunkToArrowArrayConverter::convertNumericChunk<::arrow::DoubleType, double>(
          column.doubles().data, column.size(), column.bitmaps().data);
        break;
      case ColumnType::STRING:
        array = ChunkToArrowArrayConverter::convertStringChunk(column.strings(), column.bitmap);
      default:
        return ::arrow::Status::NotImplemented("Encountered unknown type");
    }
    ARROW_RETURN_NOT_OK(array);
  }
  return ::arrow::Table::Make(::arrow::schema(fields), columns);
}
// ------------------------------------------------------------------------------
} // namespace btrblocks::arrow
// ------------------------------------------------------------------------------