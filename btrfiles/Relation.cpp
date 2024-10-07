// ------------------------------------------------------------------------------
#include "storage/Relation.hpp"
#include "btrfiles.hpp"
#include "common/Utils.hpp"
#include "compression/BtrReader.hpp"
#include "compression/Datablock.hpp"
// ------------------------------------------------------------------------------
#include <oneapi/tbb/parallel_for.h>
#include <oneapi/tbb/global_control.h>
#include <yaml-cpp/yaml.h>
#include <filesystem>
#include <fstream>
// ------------------------------------------------------------------------------
namespace btrblocks::files {
void verify_or_die(const std::string& filename, const std::vector<InputChunk> &input_chunks) {
  // Verify that decompression works
  thread_local std::vector<char> compressed_data;
  Utils::readFileToMemory(filename, compressed_data);
  BtrReader reader(compressed_data.data());
  for (SIZE chunk_i = 0; chunk_i < reader.getChunkCount(); chunk_i++) {
    std::vector<u8> output(reader.getDecompressedSize(chunk_i));
    bool requires_copy = reader.readColumn(output, chunk_i);
    auto bm = reader.getBitmap(chunk_i)->writeBITMAP();
    if (!input_chunks[chunk_i].compareContents(output.data(), bm, reader.getTupleCount(chunk_i),
                                               requires_copy)) {
      throw Generic_Exception("Decompression yields different contents");
    }
  }
}
// ------------------------------------------------------------------------------
Relation readDirectory(const YAML::Node& schema, const string& columns_dir, const string& only_type) {
  die_if(columns_dir.back() == '/');
  Relation result;
  result.columns.reserve(schema["columns"].size());
  const auto& columns = schema["columns"];
  for (u32 column_i = 0; column_i < columns.size(); column_i++) {
    const auto& column = columns[column_i];
    const auto column_name = column["name"].as<string>();
    auto column_type = column["type"].as<string>();
    if (column_type == "smallint") {
      column_type = "integer";
    } else if (column_type == "float") {
      column_type = "double";
    }
    // -------------------------------------------------------------------------------------
    if (only_type != "" && column_type != only_type) { continue; }
    // -------------------------------------------------------------------------------------
    const string column_file_prefix =
        columns_dir + std::to_string(column_i + 1) + "_" + column_name;
    const string column_file_path = column_file_prefix + "." + column_type;
    if (column_type == "integer" || column_type == "double" || column_type == "string") {
      result.addColumn(column_file_path);
    }
  }
  return result;
}
// ------------------------------------------------------------------------------
void writeDirectory(const Relation& relation, std::string btr_dir, std::string stats_dir,
                    std::string compressionout_dir, ColumnType typefilter, int32_t column_filter,
                    int32_t chunk_filter, bool verify,  uint64_t  binary_creation_time){
  // Prepare datastructures for btr compression
  auto ranges = relation.getRanges(SplitStrategy::SEQUENTIAL, 9999);
  assert(ranges.size() > 0);
  Datablock datablockV2(relation);
  std::filesystem::create_directory(btr_dir);
  //    if (!std::filesystem::create_directory(btr_dir)) {
  //        throw Generic_Exception("Unable to create btr output directory");
  //    }

  // These counter are for statistics that match the harbook.
  std::vector<std::atomic_size_t> sizes_uncompressed(relation.columns.size());
  std::vector<std::atomic_size_t> sizes_compressed(relation.columns.size());
  std::vector<std::vector<u32>> part_counters(relation.columns.size(), std::vector<u32>(1, 0));
  std::vector<ColumnType> types(relation.columns.size());
  std::vector<ColumnChunkInfo> chunk_infos;

  // TODO run in parallel over individual columns and handle chunks inside
  // TODO collect statistics for overall metadata like
  //      - total tuple count
  //      - for every column: total number of parts
  //      - for every column: name, type
  // TODO chunk flag
  auto start_time = std::chrono::steady_clock::now();
  //oneapi::tbb::parallel_for(SIZE(0), relation.columns.size(), [&](SIZE column_i) {
  for (SIZE column_i = 0; column_i != relation.columns.size(); column_i++){
    types[column_i] = relation.columns[column_i].type;
    if (typefilter != ColumnType::UNDEFINED && typefilter != types[column_i]) {
      return;
    }
    if (column_filter != -1 && column_filter != column_i) {
      return;
    }
    std::vector<InputChunk> input_chunks;
    std::string path_prefix = btr_dir + "/" + "column" + std::to_string(column_i) + "_part";
    ColumnPart part;
    for (SIZE chunk_i = 0; chunk_i < ranges.size(); chunk_i++) {
      if (chunk_filter != -1 && chunk_filter != chunk_i) {
        continue;
      }

      // TODO pascalginter fix
      auto input_chunk = relation.getInputChunk(ranges[chunk_i], chunk_i, column_i);
      ColumnChunkInfo info{
        .uncompressedSize = input_chunk.size,
        .min_value = std::numeric_limits<u32>::min(),
        .max_value = std::numeric_limits<u32>::max(),
      };
      chunk_infos.push_back(info);
      std::vector<u8> data = Datablock::compress(input_chunk);
      sizes_uncompressed[column_i] += input_chunk.size;

      if (!part.canAdd(data.size())) {
        std::string filename = path_prefix + std::to_string(part_counters[column_i].size() - 1);
        sizes_compressed[column_i] += part.writeToDisk(filename);
        part_counters[column_i].push_back(0);
        if (verify) verify_or_die(filename, input_chunks);
        input_chunks.clear();
      }else {
        part_counters[column_i].back()++;
      }

      input_chunks.push_back(std::move(input_chunk));
      part.addCompressedChunk(std::move(data));
    }

    if (!part.chunks.empty()) {
      std::string filename = path_prefix + std::to_string(part_counters[column_i].size() - 1);
      sizes_compressed[column_i] += part.writeToDisk(filename);
      part_counters[column_i].push_back(0);
      if (verify) verify_or_die(filename, input_chunks);
      input_chunks.clear();
    }
  };

  Datablock::writeMetadata(btr_dir + "/metadata", types, part_counters, chunk_infos, ranges.size());
  std::ofstream stats_stream(stats_dir);
  size_t total_uncompressed = 0;
  size_t total_compressed = 0;
  stats_stream << "Column,uncompressed,compressed\n";
  for (SIZE col=0; col  < relation.columns.size(); col++) {
    total_uncompressed += sizes_uncompressed[col];
    total_compressed += sizes_compressed[col];

    stats_stream << relation.columns[col].name << "," << sizes_uncompressed[col] << "," << sizes_compressed[col] << "\n";
  }
  auto end_time = std::chrono::steady_clock::now();
  uint64_t btr_creation_time = std::chrono::duration_cast<std::chrono::microseconds>(end_time - start_time).count();

  stats_stream << "Total," << total_uncompressed << "," << total_compressed << std::endl;

  std::ofstream compressionout_stream(compressionout_dir);
  double binary_creation_time_seconds = static_cast<double>(binary_creation_time) / 1e6;
  double btr_creation_time_seconds = static_cast<double>(btr_creation_time) / 1e6;

  compressionout_stream << "binary: " << binary_creation_time_seconds
                        << " btr: " << btr_creation_time_seconds
                        << " total: " << (binary_creation_time_seconds + btr_creation_time_seconds)
                        << " verify: " <<  verify << std::endl;
}
// ------------------------------------------------------------------------------
}  // namespace btrblocks::files
// ------------------------------------------------------------------------------
