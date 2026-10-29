/**
 * Copyright (c) 2025 OceanBase.
 *
 * Licensed under the Apache License, Version 2.0.
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

#define USING_LOG_PREFIX SHARE_SCHEMA

#include "share/schema/ob_ai_column_schema.h"
#include "lib/utility/ob_macro_utils.h"

using namespace oceanbase::common;

namespace oceanbase
{
namespace share
{
namespace schema
{

OB_SERIALIZE_MEMBER(ObAiColumnSchema,
                    table_id_,
                    column_id_,
                    state_column_id_,
                    request_column_id_,
                    result_spec_column_id_,
                    definition_version_,
                    max_retries_,
                    is_embedding_,
                    vector_dimension_,
                    column_name_,
                    normalized_expr_,
                    model_name_,
                    input_expr_,
                    sync_mode_,
                    dependency_column_ids_);

int ObAiColumnSchema::set_column_name(ObIAllocator &allocator, const ObString &value)
{
  return ob_write_string(allocator, value, column_name_);
}

int ObAiColumnSchema::set_normalized_expr(ObIAllocator &allocator, const ObString &value)
{
  return ob_write_string(allocator, value, normalized_expr_);
}

int ObAiColumnSchema::set_model_name(ObIAllocator &allocator, const ObString &value)
{
  return ob_write_string(allocator, value, model_name_);
}

int ObAiColumnSchema::set_input_expr(ObIAllocator &allocator, const ObString &value)
{
  return ob_write_string(allocator, value, input_expr_);
}

int ObAiColumnSchema::set_sync_mode(ObIAllocator &allocator, const ObString &value)
{
  return ob_write_string(allocator, value, sync_mode_);
}

int ObAiColumnSchema::assign(ObIAllocator &allocator, const ObAiColumnSchema &other)
{
  int ret = OB_SUCCESS;
  if (this != &other) {
    reset();
    table_id_ = other.table_id_;
    column_id_ = other.column_id_;
    state_column_id_ = other.state_column_id_;
    request_column_id_ = other.request_column_id_;
    result_spec_column_id_ = other.result_spec_column_id_;
    definition_version_ = other.definition_version_;
    max_retries_ = other.max_retries_;
    is_embedding_ = other.is_embedding_;
    vector_dimension_ = other.vector_dimension_;
    if (OB_FAIL(set_column_name(allocator, other.column_name_))) {
    } else if (OB_FAIL(set_normalized_expr(allocator, other.normalized_expr_))) {
    } else if (OB_FAIL(set_model_name(allocator, other.model_name_))) {
    } else if (OB_FAIL(set_input_expr(allocator, other.input_expr_))) {
    } else if (OB_FAIL(set_sync_mode(allocator, other.sync_mode_))) {
    } else if (OB_FAIL(dependency_column_ids_.assign(other.dependency_column_ids_))) {
      LOG_WARN("failed to assign ai column dependencies", K(ret));
    }
  }
  return ret;
}

bool ObAiColumnSchema::depends_on(uint64_t column_id) const
{
  bool found = false;
  for (int64_t i = 0; !found && i < dependency_column_ids_.count(); ++i) {
    found = dependency_column_ids_.at(i) == column_id;
  }
  return found;
}

int ObAiColumnSchema::remap_column_ids(const hash::ObHashMap<uint64_t, uint64_t> &id_map)
{
  int ret = OB_SUCCESS;
  uint64_t *ids[] = {&column_id_, &state_column_id_, &request_column_id_, &result_spec_column_id_};
  for (int64_t i = 0; OB_SUCC(ret) && i < static_cast<int64_t>(sizeof(ids) / sizeof(ids[0])); ++i) {
    uint64_t new_id = OB_INVALID_ID;
    if (OB_INVALID_ID == *ids[i]) {
    } else if (OB_FAIL(id_map.get_refactored(*ids[i], new_id))) {
      LOG_WARN("failed to remap AI column id", K(ret), K(*ids[i]));
    } else {
      *ids[i] = new_id;
    }
  }
  for (int64_t i = 0; OB_SUCC(ret) && i < dependency_column_ids_.count(); ++i) {
    uint64_t new_id = OB_INVALID_ID;
    if (OB_FAIL(id_map.get_refactored(dependency_column_ids_.at(i), new_id))) {
      LOG_WARN("failed to remap AI dependency id", K(ret), K(dependency_column_ids_.at(i)));
    } else {
      dependency_column_ids_.at(i) = new_id;
    }
  }
  return ret;
}

} // namespace schema
} // namespace share
} // namespace oceanbase
