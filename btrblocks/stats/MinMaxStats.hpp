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
struct MinMaxStats {
  u64 min;
  u64 max;

  static constexpr size_t NUM_STRING_BYTES = 8;

  MinMaxStats() {
    min = std::numeric_limits<u64>::min();
    max = std::numeric_limits<u64>::max();
  }

  explicit MinMaxStats(const NumberStats<INTEGER>& stats){
    min = stats.min;
    max = stats.max;
  }

  explicit MinMaxStats(const NumberStats<DOUBLE>& stats){
    min = *reinterpret_cast<const u64*>(&stats.min);
    max = *reinterpret_cast<const u64*>(&stats.max);
  }

  explicit MinMaxStats(const StringStats& stats){
    char buffer[NUM_STRING_BYTES] = {};
    memcpy(buffer, stats.min.data(), std::min(stats.min.size(), NUM_STRING_BYTES));
    min = *reinterpret_cast<u64*>(buffer);

    memset(buffer, 0, NUM_STRING_BYTES);
    memcpy(buffer, stats.max.data(), std::min(stats.max.size(), NUM_STRING_BYTES));
    max = *reinterpret_cast<u64*>(buffer);
   }
};
// -------------------------------------------------------------------------------------
}  // namespace btrblocks
// -------------------------------------------------------------------------------------
