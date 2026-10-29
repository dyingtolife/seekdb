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

#pragma once

#include "lib/string/ob_string.h"
#include "lib/container/ob_se_array.h"
#include "lib/hash/ob_hashmap.h"
#include "lib/utility/ob_unify_serialize.h"
#include "common/ob_common_types.h"

namespace oceanbase
{
namespace share
{
namespace schema
{

enum class ObAiColumnResultState : int64_t
{
  PENDING = 0,
  READY = 1,
  FAILED = 2,
};

// Per-column metadata for a user column declared with an AI COLUMN (...)
// clause. Persisted in __all_ai_column (table id 1131) and cached inside
// ObTableSchema::ai_columns_. The definition is versioned independently from
// the schema version; workers must validate both the request id and this
// version before publishing results into the hidden result column.
class ObAiColumnSchema final
{
  OB_UNIS_VERSION(1);
public:
  ObAiColumnSchema() { reset(); }
  ~ObAiColumnSchema() = default;

  void reset()
  {
    table_id_ = common::OB_INVALID_ID;
    column_id_ = common::OB_INVALID_ID;
    state_column_id_ = common::OB_INVALID_ID;
    request_column_id_ = common::OB_INVALID_ID;
    result_spec_column_id_ = common::OB_INVALID_ID;
    definition_version_ = 0;
    max_retries_ = 0;
    is_embedding_ = false;
    vector_dimension_ = 0;
    column_name_.reset();
    normalized_expr_.reset();
    sync_mode_.reset();
    dependency_column_ids_.reset();
  }

  int assign(common::ObIAllocator &allocator, const ObAiColumnSchema &other);
  int set_column_name(common::ObIAllocator &allocator, const common::ObString &value);
  int set_normalized_expr(common::ObIAllocator &allocator, const common::ObString &value);
  int set_sync_mode(common::ObIAllocator &allocator, const common::ObString &value);
  int remap_column_ids(const common::hash::ObHashMap<uint64_t, uint64_t> &id_map);
  bool depends_on(uint64_t column_id) const;

  int64_t get_convert_size() const
  {
    return column_name_.length() + normalized_expr_.length() + sync_mode_.length() + 3;
  }

  TO_STRING_KV(K_(table_id), K_(column_id), K_(column_name),
               K_(definition_version), K_(max_retries), K_(is_embedding),
               K_(vector_dimension), K_(state_column_id), K_(request_column_id),
               K_(result_spec_column_id), K_(sync_mode), K_(dependency_column_ids));

  // Identifiers. Stored public (plain struct style, like ObForeignKeyInfo).
  uint64_t table_id_;
  uint64_t column_id_;
  uint64_t state_column_id_;
  uint64_t request_column_id_;
  uint64_t result_spec_column_id_;
  uint64_t definition_version_;
  int64_t max_retries_;
  bool is_embedding_;
  int64_t vector_dimension_;
  common::ObString column_name_;
  common::ObString normalized_expr_;
  common::ObString sync_mode_;
  common::ObSEArray<uint64_t, 4> dependency_column_ids_;
};

} // namespace schema
} // namespace share
} // namespace oceanbase
