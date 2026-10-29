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

#include "lib/thread/ob_reentrant_thread.h"
#include "common/mysqlclient/ob_mysql_proxy.h"
#include "share/schema/ob_ai_column_task_service.h"

namespace oceanbase
{
namespace observer
{

// Background AI column worker. Polls __all_ai_column_task, executes model
// calls for claimed tasks and publishes results. Runs on its own inner SQL
// connection; never blocks user DML.
class ObAiColumnWorker final : public share::ObReentrantThread
{
public:
  static ObAiColumnWorker &get_instance();

  int start_worker();
  void stop_worker();

  virtual void run2() override;
  virtual int before_blocking_run() override { return common::OB_SUCCESS; }
  virtual int blocking_run() override { BLOCKING_RUN_IMPLEMENT(); }

  // Poll interval in microseconds.
  static constexpr int64_t POLL_INTERVAL_US = 100 * 1000;

private:
  ObAiColumnWorker();
  ~ObAiColumnWorker() = default;
  int process_one_task(common::ObMySQLProxy &sql_proxy,
                       share::schema::ObAiColumnTaskService &task_svc,
                       const share::schema::ObAiColumnTaskService::ClaimedTask &task);
  int execute_task_group(common::ObMySQLProxy &sql_proxy,
                         const share::schema::ObAiColumnTaskService::ClaimedTask &task,
                         const share::schema::ObTableSchema &table_schema,
                         const common::ObString &db_name,
                         const share::schema::ObAiColumnSchema &ai_col,
                         common::ObString &result_literal);
  int build_result_literal(common::ObIAllocator &allocator,
                           const share::schema::ObAiColumnSchema &ai_col,
                           const common::ObString &raw_result,
                           common::ObString &literal);

  bool inited_;
  DISALLOW_COPY_AND_ASSIGN(ObAiColumnWorker);
};

} // namespace observer
} // namespace share
