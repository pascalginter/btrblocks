#include "StringStats.hpp"
// -------------------------------------------------------------------------------------
namespace btrblocks {
// -------------------------------------------------------------------------------------
StringStats StringStats::generateStats(const btrblocks::StringArrayViewer src,
                                       const BITMAP* nullmap,
                                       u32 tuple_count,
                                       SIZE column_data_size) {
  // -------------------------------------------------------------------------------------
  // Collect stats
  StringStats stats;
  // -------------------------------------------------------------------------------------
  stats.tuple_count = tuple_count;
  stats.total_size = column_data_size;
  stats.total_length = 0;
  stats.total_unique_length = 0;
  stats.null_count = 0;
  stats.min = src(0);
  stats.max = src(0);
  // -------------------------------------------------------------------------------------
  for (u64 row_i = 0; row_i < tuple_count; row_i++) {
    if (nullmap == nullptr || nullmap[row_i]) {
      auto current_value = src(row_i);
      if (stats.distinct_values.find(current_value) == stats.distinct_values.end()) {
        stats.distinct_values.insert(current_value);
        stats.total_unique_length += current_value.length();
        if (stats.min > current_value) stats.min = current_value;
        if (stats.max < current_value) stats.max = current_value;
      }
      stats.total_length += current_value.size();
    } else {
      stats.distinct_values.insert("");
      stats.null_count++;
      continue;
    }
  }
  // -------------------------------------------------------------------------------------
  stats.unique_count = stats.distinct_values.size();
  stats.set_count = stats.tuple_count - stats.null_count;
  return stats;
}
// -------------------------------------------------------------------------------------
}  // namespace btrblocks