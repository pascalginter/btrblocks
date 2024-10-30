#pragma once
// -------------------------------------------------------------------------------------
#include "common/Units.hpp"
// -------------------------------------------------------------------------------------
namespace btrblocks {
// -------------------------------------------------------------------------------------
/*
 * This supports a minimal interface. In theory we could unify this interface
 * and the interface of StringArrayViewer
 */
struct StringPointerArrayViewer {
  struct View {
    u32 length;
    u32 offset;
  };
  static_assert(sizeof(View) == 8);
  const View* views;
  const u8* data;

  explicit StringPointerArrayViewer(const u8* data_) : data(data_) {
    this->views = reinterpret_cast<const View*>(data_);
  }

  [[nodiscard]] inline u32 copied_data_size(u32 tuple_count) {
    u32 data_size = 0;
    for (u32 i=0; i!=tuple_count; i++) {
      data_size += views[i].length;
    }
    return data_size;
  }

  inline str operator()(u32 i) const {
    return {reinterpret_cast<const char*>(this->views) + views[i].offset, views[i].length};
  }
};
// -------------------------------------------------------------------------------------
}  // namespace btrblocks
