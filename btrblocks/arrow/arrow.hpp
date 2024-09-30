#ifndef BTRARROW_H
#define BTRARROW_H
// ------------------------------------------------------------------------------
#include "storage/Relation.hpp"
// ------------------------------------------------------------------------------
#include <arrow/api.h>
// ------------------------------------------------------------------------------
namespace btrblocks::arrow {
// ------------------------------------------------------------------------------
Relation parseArrowTable(const std::shared_ptr<::arrow::Table>& table);
// ------------------------------------------------------------------------------
} // namespace btrblocks::arrow
// ------------------------------------------------------------------------------
#endif // BTRARROW_H