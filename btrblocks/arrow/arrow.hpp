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

::arrow::Result<std::shared_ptr<::arrow::Table>> parseRelationToArrowTable(const Relation& relation);
// ------------------------------------------------------------------------------
} // namespace btrblocks::arrow
// ------------------------------------------------------------------------------
#endif // BTRARROW_H