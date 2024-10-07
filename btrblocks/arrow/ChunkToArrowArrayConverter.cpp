#include <compression/BtrReader.hpp>

#include "arrow/ChunkToArrowArrayConverter.hpp"
//--------------------------------------------------------------------------------------------------
namespace btrblocks::arrow {
//--------------------------------------------------------------------------------------------------
//--------------------------------------------------------------------------------------------------
::arrow::Result<std::shared_ptr<::arrow::Array>> ChunkToArrowArrayConverter::convertStringChunk(
    StringArrayViewer viewer, u32 tupleCount, BitmapWrapper* bitmap){
  ::arrow::StringBuilder builder;
  for (u32 i=0; i !=tupleCount; i++) {
    if (!bitmap->test(i)) {
      ARROW_RETURN_NOT_OK(builder.AppendNull());
    } else {
      ARROW_RETURN_NOT_OK(builder.Append(viewer(i)));
    }
  }
  return builder.Finish();
}
//--------------------------------------------------------------------------------------------------
::arrow::Result<std::shared_ptr<::arrow::Array>> ChunkToArrowArrayConverter::convertStringChunk(
    StringPointerArrayViewer viewer, u32 tupleCount, BitmapWrapper* bitmap){
  ::arrow::StringBuilder builder;
  for (u32 i=0; i !=tupleCount; i++) {
    if (!bitmap->test(i)) {
      ARROW_RETURN_NOT_OK(builder.AppendNull());
    } else {
      ARROW_RETURN_NOT_OK(builder.Append(viewer(i)));
    }
  }
  return builder.Finish();
}
//--------------------------------------------------------------------------------------------------
::arrow::Result<std::shared_ptr<::arrow::Array>> ChunkToArrowArrayConverter::convertStringChunk(
   const Vector<str>& vector, const Vector<BITMAP>& bitmap) {
  ::arrow::StringBuilder builder;
  u32 tupleCount = vector.size();
  for (u32 i=0; i!=tupleCount; i++) {
    if (!bitmap[i]) {
      ARROW_RETURN_NOT_OK(builder.AppendNull());
    } else {
      ARROW_RETURN_NOT_OK(builder.Append(vector[i]));
    }
  }
  return builder.Finish();
}

//--------------------------------------------------------------------------------------------------
} // namespace btrblocks::arrow
//--------------------------------------------------------------------------------------------------