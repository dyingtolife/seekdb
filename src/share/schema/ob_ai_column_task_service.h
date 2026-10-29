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

#include "share/ob_dml_sql_splicer.h"
#include "lib/string/ob_sql_string.h"
#include "share/schema/ob_ai_column_schema.h"

namespace oceanbase
{
namespace share
{
namespace schema
{

// Durability states of a __all_ai_column_task row (`state` column).
// PENDING -> EXECUTING -> (RETRY_WAIT -> EXECUTING)* -> terminal (removed).
enum ObAiColumnTaskState
{
  AI_TASK_PENDING = 0,
  AI_TASK_EXECUTING = 1,
  AI_TASK_RETRY_WAIT = 3,
};

// Durable task queue over __all_ai_column_task (table id 1133).
//
// enqueue_* run INSIDE the user's DML transaction through the session inner
// connection, so task rows commit/rollback atomically with the data change
// (the task row key includes the source request id).
//
// claim/publish/fail run from the background AI worker on its own inner
// connection. publish performs a request-identity + definition-version check
// so a late result for an older request can never overwrite a newer one, and
// a result for a deleted row is dropped instead of resurrecting it.
class ObAiColumnTaskService final
{
public:
  explicit ObAiColumnTaskService(common::ObIAllocator &allocator)
    : allocator_(allocator)
  {}
  ~ObAiColumnTaskService() = default;

  // --- user-transaction side (session inner connection) ---
  // Insert one task row per AI column of the table for one changed row.
  // request_id identifies the (table, rowkey, change) event: all AI columns
  // of the same row change share it (grouped publish).
  static int enqueue_tasks(common::ObISQLClient &sql_client,
                           const ObTableSchema &table_schema,
                           const uint64_t tenant_id,
                           const common::ObString &rowkey_predicate,
                           const common::ObString &request_id);

  // Invalidate hidden columns of one changed row inside the user txn: set
  // <col>_result = NULL, <col>_state = 'PENDING', <col>_request = request_id
  // for every async AI column of the table. Called before enqueue so the
  // transaction sees the invalidation.
  static int invalidate_row(common::ObISQLClient &sql_client,
                            const ObTableSchema &table_schema,
                            const common::ObString &rowkey_predicate,
                            const common::ObString &request_id);

  // Remove all pending task rows of a table for one rowkey (DELETE of the
  // business row, or PK update invalidation of the old key).
  static int remove_tasks_for_row(common::ObISQLClient &sql_client,
                                  uint64_t table_id,
                                  const common::ObString &rowkey_predicate);

  // Remove all task rows of a table (DROP TABLE / TRUNCATE cleanup).
  static int remove_tasks_for_table(common::ObISQLClient &sql_client,
                                    uint64_t table_id);

  // --- worker side (worker-owned connection) ---
  struct ClaimedTask
  {
    uint64_t table_id_;
    uint64_t column_id_;
    int64_t task_id_;
    common::ObString rowkey_predicate_;
    common::ObString request_id_;
    int64_t definition_version_;
    int64_t retry_count_;
    ClaimedTask() { reset(); }
    void reset()
    {
      table_id_ = common::OB_INVALID_ID;
      column_id_ = common::OB_INVALID_ID;
      task_id_ = 0;
      definition_version_ = 0;
      retry_count_ = 0;
      rowkey_predicate_.reset();
      request_id_.reset();
    }
  };

  // Atomically claim up to max_batch PENDING tasks that are eligible
  // (eligible_ts <= now): one inner-SQL transaction per batch, single-writer
  // via UPDATE ... LIMIT-style claim loop (SELECT id, then CAS UPDATE
  // state=PENDING->EXECUTING; rows whose state changed are ours).
  int claim_tasks(common::ObISQLClient &sql_client,
                         int64_t max_batch,
                         common::ObIArray<ClaimedTask> &tasks);

  // Reset EXECUTING tasks back to PENDING (crash recovery on worker start:
  // a task left EXECUTING by a dead worker is re-claimable).
  int recover_stuck_tasks(common::ObISQLClient &sql_client);

  // Publish one result group: guarded by request_id + definition_version.
  // Sets <col>_result = value, <col>_state = READY for every entry of the
  // group whose hidden request column still equals request_id; task rows are
  // removed in the same transaction. Returns per-column success map.
  struct PublishEntry
  {
    uint64_t column_id_;
    common::ObString value_;   // SQL literal (already escaped)
    PublishEntry() : column_id_(common::OB_INVALID_ID) {}
  };
  int publish_group(common::ObISQLClient &sql_client,
                           const ObTableSchema &table_schema,
                           uint64_t tenant_id,
                           const common::ObString &rowkey_predicate,
                           const common::ObString &request_id,
                           const common::ObIArray<PublishEntry> &entries,
                           int64_t definition_version);

  // Handle a failed execution: bounded retry (retry_count < max_retries
  // from schema) re-schedules with backoff; exhausted tasks publish
  // state=FAILED and remove the task row.
  int fail_task(common::ObISQLClient &sql_client,
                       const ClaimedTask &task,
                       int64_t max_retries,
                       int64_t retry_delay_us);

  // Final-failure publish for a group where the model call failed
  // terminally: state=FAILED for columns of this request, task rows removed.
  int publish_failure_group(common::ObISQLClient &sql_client,
                                   const ObTableSchema &table_schema,
                                   uint64_t tenant_id,
                                   const common::ObString &rowkey_predicate,
                                   const common::ObString &request_id,
                                   const common::ObIArray<ClaimedTask> &tasks,
                                   int64_t definition_version);

  // RETRY AI COLUMN support: enqueue a fresh task row for one AI column and
  // invalidate the hidden result/state/request columns of the matched rows.
  static int retry_column_rows(common::ObISQLClient &sql_client,
                               const ObTableSchema &table_schema,
                               const ObAiColumnSchema &ai_column,
                               const common::ObString &extra_predicate,
                               bool use_status_pseudo,
                               const common::ObString &status_value,
                               int64_t &matched_rows);

private:
  common::ObIAllocator &allocator_;
  static int splice_task_insert(const uint64_t table_id,
                                const int64_t task_id,
                                uint64_t column_id,
                                const common::ObString &rowkey_predicate,
                                const common::ObString &request_id,
                                int64_t definition_version,
                                share::ObDMLSqlSplicer &dml);
  static int next_task_id(common::ObISQLClient &sql_client, int64_t &task_id);
  static int exec_write(common::ObISQLClient &sql_client,
                        const common::ObSqlString &sql,
                        int64_t &affected_rows);

  DISALLOW_COPY_AND_ASSIGN(ObAiColumnTaskService);
};

} // namespace schema
} // namespace share
} // namespace oceanbase
