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

#include "lib/hash/ob_hashmap.h"
#include "lib/string/ob_string.h"
#include "share/schema/ob_table_schema.h"

namespace oceanbase
{
namespace sql
{

class ObStmt;

// Per-statement runtime state of the AI column DML linkage. Collected per
// changed row by the DML row processing hooks; flushed once at the end of
// the DML operator through the session inner connection, inside the user's
// transaction (task rows commit/rollback atomically with the data change).
struct ObAiColumnDMLCtx
{
  // One captured change of one row.
  struct RowChange
  {
    common::ObString rowkey_predicate_;  // e.g. "`id` = 2"; OLD key on PK update
    common::ObString old_rowkey_predicate_;  // NEW key predicate (PK update)
    common::ObString request_id_;        // per-row unique change id
    bool is_delete_;
    RowChange() : is_delete_(false) {}
  };

  bool has_ai_columns_;
  uint64_t table_id_;                    // data table id (user table)
  int64_t request_seq_;                  // per-statement sequence for request ids
  common::ObSEArray<RowChange, 8> row_changes_;
  // AI dependency column names of the table (collected at init_ctx): an
  // UPDATE only triggers tasks when it assigns one of these columns.
  common::ObSEArray<common::ObString, 8> dependency_names_;
  common::ObSEArray<common::ObString, 8> rowkey_names_;
  // AI result column ids assigned directly by this UPDATE (resolver fills;
  // executor rejects).
  common::ObSEArray<uint64_t, 4> assigned_ai_result_cols_;

  ObAiColumnDMLCtx() : has_ai_columns_(false), table_id_(common::OB_INVALID_ID), request_seq_(0) {}

  void reset()
  {
    has_ai_columns_ = false;
    table_id_ = common::OB_INVALID_ID;
    request_seq_ = 0;
    row_changes_.reset();
    dependency_names_.reset();
    rowkey_names_.reset();
    assigned_ai_result_cols_.reset();
  }

  bool is_empty() const { return row_changes_.empty() && assigned_ai_result_cols_.empty(); }
};

// Helper utilities called from the DML executor hooks.
class ObAiColumnDMLUtils
{
public:
  // Initialize the per-statement AI ctx from the table schema: records
  // whether the table has async AI columns. Called at inner_open.
  static int init_ctx(const share::schema::ObTableSchema &table_schema,
                      ObAiColumnDMLCtx &ai_ctx,
                      common::ObIAllocator *alloc_holder);

  // Capture one changed row: builds the rowkey predicate from the evaluated
  // row datums, generates the change request id and appends a RowChange.
  static int capture_row(ObEvalCtx &eval_ctx,
                         const ExprFixedArray &key_exprs,
                         ObAiColumnDMLCtx &ai_ctx,
                         bool is_delete,
                         const common::ObString *old_rowkey_predicate);

  // Flush the captured changes: hidden-column invalidation + task rows.
  // Uses the session inner connection so everything runs in the user txn.
  static int flush_changes(ObExecContext &exec_ctx,
                           const share::schema::ObTableSchema &table_schema,
                           ObAiColumnDMLCtx &ai_ctx);

  // Enforce "business SQL must not assign AI result columns": error if the
  // UPDATE set list touches an AI result column of the table.
  static int check_no_direct_assignment(
      const share::schema::ObTableSchema &table_schema,
      const ObAiColumnDMLCtx &ai_ctx);

  // Statement-level strict read: for each AI column referenced by the
  // resolved statement, error when any row of the column is not READY.
  // Called from ObSql::generate_physical_plan for DML statements when the
  // session variable ai_column_strict_read is ON.
  static int check_strict_read_for_stmt(
      ObStmt &basic_stmt,
      sql::ObSQLSessionInfo *session,
      share::schema::ObSchemaGetterGuard *schema_guard);

  // RETRY AI COLUMN: select matched rowkeys (status pseudo column maps to
  // the hidden state column), invalidate them and enqueue fresh tasks.
  static int retry_ai_column(
      ObExecContext &exec_ctx,
      const share::schema::ObTableSchema &table_schema,
      const common::ObString &column_name,
      const common::ObString &where_clause_text,
      int64_t &matched_rows);

  // Session strict-read gate: error 11120 when any AI column referenced by
  // the statement has not-ready rows and ai_column_strict_read is ON.
  static int check_strict_read(
      share::schema::ObSchemaGetterGuard &guard,
      const share::schema::ObTableSchema &table_schema,
      const common::ObIArray<uint64_t> &referenced_ai_column_ids,
      bool strict_read_enabled);

public:
  // Public wrapper so the update hook can build the NEW key predicate.
  static int build_rowkey_predicate_from_exprs(
      ObEvalCtx &eval_ctx,
      const ExprFixedArray &key_exprs,
      const common::ObIArray<common::ObString> &key_names,
      common::ObIAllocator &allocator,
      common::ObString &predicate)
  {
    return build_rowkey_predicate(eval_ctx, key_exprs, key_names, allocator, predicate);
  }

private:
  static int build_rowkey_predicate(
      ObEvalCtx &eval_ctx,
      const ExprFixedArray &key_exprs,
      const common::ObIArray<common::ObString> &key_names,
      common::ObIAllocator &allocator,
      common::ObString &predicate);
};

} // namespace sql
} // namespace oceanbase
