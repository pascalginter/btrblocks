#pragma once
// -------------------------------------------------------------------------------------
#include "common/Units.hpp"
#include "stats/NumberStats.hpp"
#include "stats/StringStats.hpp"
// -------------------------------------------------------------------------------------
#include <cstring>
// -------------------------------------------------------------------------------------
namespace btrblocks {
// -------------------------------------------------------------------------------------
struct ChunkStats {
  u64 min;
  u64 max;
  s32 unique_tuple_count; // -1 means unknown
  u32 total_unique_length; // 0 if unknown


  static constexpr size_t NUM_STRING_BYTES = 8;

  ChunkStats() {
    min = std::numeric_limits<u64>::min();
    max = std::numeric_limits<u64>::max();
    unique_tuple_count = -1;
    total_unique_length = 0;
  }

  explicit ChunkStats(const NumberStats<INTEGER>& stats){
    min = stats.min;
    max = stats.max;
    unique_tuple_count = -1;
    total_unique_length = 0;
  }

  explicit ChunkStats(const NumberStats<DOUBLE>& stats){
    min = *reinterpret_cast<const u64*>(&stats.min);
    max = *reinterpret_cast<const u64*>(&stats.max);
    unique_tuple_count = -1;
    total_unique_length = 0;
  }

  explicit ChunkStats(const StringStats& stats){
    char buffer[NUM_STRING_BYTES] = {};
    memcpy(buffer, stats.min.data(), std::min(stats.min.size(), NUM_STRING_BYTES));
    min = *reinterpret_cast<u64*>(buffer);

    memset(buffer, 0, NUM_STRING_BYTES);
    memcpy(buffer, stats.max.data(), std::min(stats.max.size(), NUM_STRING_BYTES));
    max = *reinterpret_cast<u64*>(buffer);

    unique_tuple_count = stats.unique_count;
    total_unique_length = stats.total_unique_length;
   }
};
// -------------------------------------------------------------------------------------
}  // namespace btrblocks
// -------------------------------------------------------------------------------------
