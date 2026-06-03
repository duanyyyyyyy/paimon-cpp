/*
 * Copyright 2024-present Alibaba Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

#include "paimon/core/operation/internal_read_context.h"

#include <utility>

#include "paimon/common/predicate/compound_predicate_impl.h"
#include "paimon/common/predicate/leaf_predicate_impl.h"
#include "paimon/common/predicate/predicate_validator.h"
#include "paimon/common/table/special_fields.h"
#include "paimon/common/types/data_field.h"
#include "paimon/core/schema/arrow_schema_validator.h"
#include "paimon/status.h"

namespace arrow {
class Schema;
}  // namespace arrow

namespace paimon {
namespace {
// Remap each leaf predicate's field index to the position of its field in
// `read_schema`. The field name is the stable identity used for lookup. The
// upstream (Java SDK / FE) constructs predicates against the latest table
// schema, so when a query projects a subset of columns the predicate's field
// index no longer matches the position in the projected `read_schema`. The
// downstream predicate-test code uses the field index directly, so leaving
// the mismatch in place would silently read the wrong column. Remap once at
// context construction so every subsequent reader sees a predicate already
// aligned with `read_schema`.
Result<std::shared_ptr<Predicate>> RemapPredicateFieldIndex(
    const arrow::Schema& read_schema, const std::shared_ptr<Predicate>& predicate) {
    if (auto leaf = std::dynamic_pointer_cast<LeafPredicateImpl>(predicate)) {
        int32_t new_index = read_schema.GetFieldIndex(leaf->FieldName());
        if (new_index == -1) {
            return Status::Invalid(
                fmt::format("field {} does not exist in schema", leaf->FieldName()));
        }
        if (new_index == leaf->FieldIndex()) {
            return predicate;
        }
        return std::static_pointer_cast<Predicate>(leaf->NewLeafPredicate(new_index));
    }
    if (auto compound = std::dynamic_pointer_cast<CompoundPredicateImpl>(predicate)) {
        std::vector<std::shared_ptr<Predicate>> remapped_children;
        remapped_children.reserve(compound->Children().size());
        bool any_changed = false;
        for (const auto& child : compound->Children()) {
            PAIMON_ASSIGN_OR_RAISE(auto remapped, RemapPredicateFieldIndex(read_schema, child));
            if (remapped != child) {
                any_changed = true;
            }
            remapped_children.push_back(std::move(remapped));
        }
        if (!any_changed) {
            return predicate;
        }
        return std::static_pointer_cast<Predicate>(
            compound->NewCompoundPredicate(remapped_children));
    }
    return predicate;
}
}  // namespace

Result<std::unique_ptr<InternalReadContext>> InternalReadContext::Create(
    const std::shared_ptr<ReadContext>& context, const std::shared_ptr<TableSchema>& table_schema,
    const std::map<std::string, std::string>& options) {
    PAIMON_ASSIGN_OR_RAISE(CoreOptions core_options,
                           CoreOptions::FromMap(options, context->GetSpecificFileSystem(),
                                                context->GetFileSystemSchemeToIdentifierMap()));
    // prepare read schema
    std::vector<DataField> read_data_fields;
    if (!context->GetReadFieldIds().empty()) {
        read_data_fields.reserve(context->GetReadFieldIds().size());
        for (const auto& field_id : context->GetReadFieldIds()) {
            // if enable row tracking or data evolution, check special fields
            if (core_options.RowTrackingEnabled() && field_id == SpecialFields::RowId().Id()) {
                read_data_fields.push_back(SpecialFields::RowId());
                continue;
            }
            if (core_options.RowTrackingEnabled() &&
                field_id == SpecialFields::SequenceNumber().Id()) {
                read_data_fields.push_back(SpecialFields::SequenceNumber());
                continue;
            }
            if (core_options.DataEvolutionEnabled() &&
                field_id == SpecialFields::IndexScore().Id()) {
                read_data_fields.push_back(SpecialFields::IndexScore());
                continue;
            }
            PAIMON_ASSIGN_OR_RAISE(DataField field, table_schema->GetField(field_id));
            read_data_fields.push_back(field);
        }
    } else if (!context->GetReadSchema().empty()) {
        read_data_fields.reserve(context->GetReadSchema().size());
        for (const auto& name : context->GetReadSchema()) {
            // if enable row tracking or data evolution, check special fields
            if (core_options.RowTrackingEnabled() && name == SpecialFields::RowId().Name()) {
                read_data_fields.push_back(SpecialFields::RowId());
                continue;
            }
            if (core_options.RowTrackingEnabled() &&
                name == SpecialFields::SequenceNumber().Name()) {
                read_data_fields.push_back(SpecialFields::SequenceNumber());
                continue;
            }
            if (core_options.DataEvolutionEnabled() && name == SpecialFields::IndexScore().Name()) {
                read_data_fields.push_back(SpecialFields::IndexScore());
                continue;
            }
            PAIMON_ASSIGN_OR_RAISE(DataField field, table_schema->GetField(name));
            read_data_fields.push_back(field);
        }
    } else {
        // if field names not set, read all fields
        read_data_fields = table_schema->Fields();
    }
    auto read_schema = DataField::ConvertDataFieldsToArrowSchema(read_data_fields);
    // validate read schema to avoid redundant fields
    PAIMON_RETURN_NOT_OK(ArrowSchemaValidator::ValidateSchemaWithFieldId(*read_schema));
    // remap and validate predicate
    std::shared_ptr<Predicate> remapped_predicate;
    if (context->GetPredicate()) {
        PAIMON_ASSIGN_OR_RAISE(
            remapped_predicate, RemapPredicateFieldIndex(*read_schema, context->GetPredicate()));
        PAIMON_RETURN_NOT_OK(PredicateValidator::ValidatePredicateWithSchema(
            *read_schema, remapped_predicate, /*validate_field_idx=*/true));
        PAIMON_RETURN_NOT_OK(
            PredicateValidator::ValidatePredicateWithLiterals(remapped_predicate));
    }

    return std::unique_ptr<InternalReadContext>(new InternalReadContext(
        context, table_schema, read_schema, core_options, std::move(remapped_predicate)));
}

InternalReadContext::InternalReadContext(const std::shared_ptr<ReadContext>& read_context,
                                         const std::shared_ptr<TableSchema>& table_schema,
                                         const std::shared_ptr<arrow::Schema>& read_schema,
                                         const CoreOptions& options,
                                         std::shared_ptr<Predicate> remapped_predicate)
    : read_context_(read_context),
      table_schema_(table_schema),
      read_schema_(read_schema),
      options_(options),
      remapped_predicate_(std::move(remapped_predicate)) {}

}  // namespace paimon
