#pragma once
// -------------------------------------------------------------------------------------
#include "compression/Compressor.hpp"
#include "scheme/CompressionScheme.hpp"
#include "storage/Chunk.hpp"
// -------------------------------------------------------------------------------------
#include <unordered_map>
// -------------------------------------------------------------------------------------
namespace btrblocks {
// -------------------------------------------------------------------------------------
// Begin new chunking
struct ColumnChunkMeta {
  u8 compression_type;
  BitmapType nullmap_type;
  ColumnType type;
  // There is 1 unused Bytes here.
  u32 nullmap_offset = 0;
  u32 tuple_count;
  u8 data[];
};
static_assert(sizeof(ColumnChunkMeta) == 12);

struct ColumnChunkInfo {
  u64 uncompressedSize;
  u64 min_value;
  u64 max_value;
};

struct ColumnPartInfo {
  u32 chunk_offset;   // Offset into the flat chunk array
  u32 num_chunks;
};

struct ColumnInfo {
  ColumnType type;
  u32 part_offset;   // Offset into the flat parts array
  u32 num_parts;
};

struct FileMetadata {
  u32 num_columns;
  u32 num_chunks;
  u32 total_parts;

  // Flattened arrays
  ColumnInfo* columns;           // num_columns elements
  ColumnPartInfo* parts;         // num_parts elements
  ColumnChunkInfo* chunks;       // num_chunks elements

  static FileMetadata* fromMemory(char* data) {
    auto* metadata = reinterpret_cast<FileMetadata*>(data);
    metadata->columns = reinterpret_cast<ColumnInfo*>(metadata + 1);
    metadata->parts = reinterpret_cast<ColumnPartInfo*>(reinterpret_cast<u8*>(metadata->columns)
      + metadata->num_columns * sizeof(ColumnInfo));
    metadata->chunks = reinterpret_cast<ColumnChunkInfo*>(reinterpret_cast<u8*>(metadata->parts)
      + metadata->total_parts * sizeof(ColumnPartInfo));

    return metadata;
  }
};

// End new chunking
struct __attribute__((packed)) ColumnMeta {
  u8 compression_type;
  ColumnType column_type;
  BitmapType bitmap_type;
  u8 padding;
  u32 offset;
  s32 bias;
  u32 nullmap_offset = 0;
};
static_assert(sizeof(ColumnMeta) == 16);
// -------------------------------------------------------------------------------------
struct DatablockMeta {
  u32 count;
  u32 size;
  u32 column_count;
  u32 padding;
  ColumnMeta attributes_meta[];
};
static_assert(sizeof(DatablockMeta) == 16);
// -------------------------------------------------------------------------------------
class Datablock : public RelationCompressor {
 public:
  explicit Datablock(const Relation& relation);
  OutputBlockStats compress(const Chunk& input_chunk, BytesArray& output_block) override;
  Chunk decompress(const BytesArray& input_block) override;
  virtual void getCompressedColumn(const BytesArray& input_db, u32 col_i, u8*& ptr, u32& size);

  static bool decompress(const u8* data_in, BitmapWrapper** bitmap_out, u8* data_out);
  static vector<u8> compress(const InputChunk& input_chunk);
  static u32 writeMetadata(const std::string& path,
                           std::vector<ColumnType> types,
                           vector<vector<u32>> part_counters,
                           vector<ColumnChunkInfo> chunk_infos,
                           u32 num_chunks);

  static SIZE compress(const InputChunk& input_chunk, u8* output_buffer);
};
// -------------------------------------------------------------------------------------
}  // namespace btrblocks
// -------------------------------------------------------------------------------------
