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
  size_t tupleCount;
  const INTEGER* indices;
  StringArrayViewer viewer;

  explicit StringPointerArrayViewer(const u8* data_, size_t tupleCount_)
    : tupleCount(tupleCount_), viewer(data_ + tupleCount * sizeof(INTEGER))  {
    this->indices = reinterpret_cast<const INTEGER*>(data_);
  }

  [[nodiscard]] inline u32 copied_data_size() {
    u32 data_size = 0;
    for (u32 i=0; i!=tupleCount; i++) {
      data_size += viewer.size(indices[i]);
    }
    return data_size;
  }

  [[nodiscard]] inline u32 data_size() {
    return viewer.data_size();
  }

  [[nodiscard]] inline u32 viewer_offset() {
    return tupleCount * sizeof(INTEGER);
  }

  [[nodiscard]] inline u32 data_offset() {
     return viewer_offset() + viewer.data_offset();
  }

  inline str operator()(u32 i) const {
    return viewer(indices[i]);
  }
};
// -------------------------------------------------------------------------------------
}  // namespace btrblocks
