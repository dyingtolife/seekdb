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

#define USING_LOG_PREFIX SERVER

#include "observer/ai_column/ob_ai_column_worker.h"

#include "share/schema/ob_schema_getter_guard.h"
#include "share/schema/ob_multi_version_schema_service.h"
#include "share/schema/ob_table_schema.h"
#include "share/schema/ob_ai_column_task_service.h"
#include "share/schema/ob_schema_struct.h"
#include "sql/engine/expr/ob_expr_ai/ob_ai_func_utils.h"
#include "share/ob_server_struct.h"
#include "query/ai/ob_ai_endpoint_resolver.h"

namespace oceanbase
{
namespace observer
{

using namespace common;
using namespace share;
using namespace share::schema;
using namespace sql;
using namespace common::sqlclient;

ObAiColumnWorker &ObAiColumnWorker::get_instance()
{
  static ObAiColumnWorker instance;
  return instance;
}

ObAiColumnWorker::ObAiColumnWorker()
  : share::ObReentrantThread(), inited_(false)
{}

int ObAiColumnWorker::start_worker()
{
  int ret = OB_SUCCESS;
  if (!inited_) {
    if (OB_FAIL(create(1, "AIColWorker", 0))) {
      SERVER_LOG(WARN, "create ai column worker thread failed", K(ret));
    } else if (OB_FAIL(start())) {
      SERVER_LOG(WARN, "start ai column worker failed", K(ret));
    } else {
      inited_ = true;
      SERVER_LOG(INFO, "ai column worker started");
    }
  }
  return ret;
}

void ObAiColumnWorker::stop_worker()
{
  if (inited_) {
    stop();
    wait();
    inited_ = false;
  }
}

int ObAiColumnWorker::build_result_literal(ObIAllocator &allocator,
                                           const ObAiColumnSchema &ai_col,
                                           const ObString &raw_result,
                                           ObString &literal)
{
  int ret = OB_SUCCESS;
  UNUSED(ai_col);
  if (OB_ISNULL(raw_result.ptr())) {
    literal.reset();
  } else {
    ObStringBuffer buf(&allocator);
    if (OB_FAIL(buf.append("'"))) {
    } else {
      for (int64_t i = 0; OB_SUCC(ret) && i < raw_result.length(); ++i) {
        char c = raw_result.ptr()[i];
        if ('\'' == c) {
          if (OB_FAIL(buf.append("''"))) {
            break;
          }
        } else if (OB_FAIL(buf.append(&c, 1))) {
          break;
        }
      }
      if (OB_SUCC(ret)) {
        ret = buf.append("'");
      }
    }
    if (OB_SUCC(ret)) {
      literal = ObString(static_cast<int32_t>(buf.length()), buf.ptr());
    }
  }
  return ret;
}

int ObAiColumnWorker::execute_task_group(common::ObMySQLProxy &sql_proxy,
                                         const ObAiColumnTaskService::ClaimedTask &task,
                                         const ObTableSchema &table_schema,
                                         const ObString &db_name,
                                         const ObAiColumnSchema &ai_col,
                                         ObString &result_literal)
{
  int ret = OB_SUCCESS;
  ObArenaAllocator allocator("AiColExec");
  result_literal.reset();
  // 1. Evaluate the input expression against the row.
  ObSqlString eval_sql;
  SMART_VAR(ObMySQLProxy::MySQLResult, res) {
    ObMySQLResult *result = NULL;
    if (OB_ISNULL(ai_col.input_expr_.ptr()) || 0 == ai_col.input_expr_.length()) {
      ret = OB_ERR_UNEXPECTED;
      SERVER_LOG(WARN, "ai column input expr empty", K(ret), K(ai_col.column_name_));
    } else if (OB_FAIL(eval_sql.assign_fmt("SELECT %.*s AS ai_input FROM `%.*s`.`%.*s` WHERE %.*s",
                                           ai_col.input_expr_.length(),
                                           ai_col.input_expr_.ptr(),
                                           db_name.length(), db_name.ptr(),
                                           table_schema.get_table_name_str().length(),
                                           table_schema.get_table_name_str().ptr(),
                                           task.rowkey_predicate_.length(),
                                           task.rowkey_predicate_.ptr()))) {
      SERVER_LOG(WARN, "build eval sql failed", K(ret));
    } else if (OB_FAIL(sql_proxy.read(res, eval_sql.ptr()))) {
      SERVER_LOG(WARN, "eval input expr failed", K(ret), "sql", eval_sql.ptr());
    } else if (OB_ISNULL(result = res.get_result())) {
      ret = OB_ERR_UNEXPECTED;
    } else if (OB_FAIL(result->next())) {
      // Row deleted between enqueue and execution: skip (publish would be a
      // no-op anyway; task row is cleaned by the publish/failure path).
      SERVER_LOG(DEBUG, "ai task row vanished", K(task.task_id_));
      return OB_SUCCESS;
    } else {
      ObString probe;
      if (OB_FAIL(result->get_varchar("ai_input", probe)) || 0 == probe.length()) {
        // NULL dependency value: no input to send; treat as an execution
        // failure so the bounded retry path records the outcome.
        ret = OB_ERR_NULL_VALUE;
        SERVER_LOG(WARN, "ai input evaluated to null", K(task.task_id_));
      } else {
      ObString input_value;
      if (OB_FAIL(result->get_varchar("ai_input", input_value))) {
        SERVER_LOG(WARN, "read ai input value failed", K(ret));
      } else {
        // 2. Call the model: system prompt = type hint, user content = input.
        ObAIFuncExprInfo *info = NULL;
        ObAiModelEndpointInfo endpoint;
        query::ObIAiEndpointResolver *resolver =
            share::server_service<query::ObIAiEndpointResolver>();
        const ObColumnSchemaV2 *result_col = table_schema.get_column_schema(ai_col.column_id_);
        ObString system_prompt;
        if (OB_ISNULL(resolver)) {
          ret = OB_ERR_UNEXPECTED;
        } else if (OB_ISNULL(result_col)) {
          ret = OB_ERR_UNEXPECTED;
        } else {
          if (ai_col.is_embedding_) {
            // embeddings handled by the vector path below
          } else if (result_col->is_json()) {
            system_prompt.assign_ptr("Return exactly one valid JSON value",
                static_cast<int32_t>(strlen("Return exactly one valid JSON value")));
          } else if (ob_is_int_tc(result_col->get_data_type())
                     || ob_is_uint_tc(result_col->get_data_type())) {
            system_prompt.assign_ptr("Return the answer as a base-10 integer",
                static_cast<int32_t>(strlen("Return the answer as a base-10 integer")));
          }
          if (OB_SUCC(ret) && !ai_col.is_embedding_) {
            ObString raw_result;
            if (OB_FAIL(ObAIFuncUtils::get_ai_func_info(allocator, ai_col.model_name_, info))) {
              SERVER_LOG(WARN, "get ai func info failed", K(ret), K(ai_col.model_name_));
            } else if (OB_FAIL(resolver->resolve_by_model_name(
                           ai_col.model_name_, allocator, endpoint, false))) {
              SERVER_LOG(WARN, "resolve ai endpoint failed", K(ret), K(ai_col.model_name_));
            } else {
              ObAIFuncModel model(allocator, *info, endpoint);
              if (OB_FAIL(model.call_completion_with_system(
                      system_prompt, input_value, raw_result))) {
                SERVER_LOG(WARN, "ai model call failed", K(ret), K(task.task_id_));
              } else if (result_col->is_json()
                         || ob_is_int_tc(result_col->get_data_type())
                         || ob_is_uint_tc(result_col->get_data_type())) {
                // JSON/INT: keep raw text, quote/trim at publish time.
                ObStringBuffer buf(&allocator);
                if (ob_is_int_tc(result_col->get_data_type())
                    || ob_is_uint_tc(result_col->get_data_type())) {
                  int64_t start = 0;
                  int64_t len = raw_result.length();
                  while (start < len && isspace(static_cast<unsigned char>(raw_result.ptr()[start]))) { ++start; --len; }
                  while (len > 0 && isspace(static_cast<unsigned char>(raw_result.ptr()[start + len - 1]))) { --len; }
                  if (OB_FAIL(buf.append(raw_result.ptr() + start, len))) {
                    SERVER_LOG(WARN, "copy int literal failed", K(ret));
                  } else {
                    result_literal = ObString(static_cast<int32_t>(buf.length()), buf.ptr());
                  }
                } else {
                  if (OB_FAIL(build_result_literal(allocator, ai_col, raw_result, result_literal))) {
                    SERVER_LOG(WARN, "build json literal failed", K(ret));
                  }
                }
              } else {
                if (OB_FAIL(build_result_literal(allocator, ai_col, raw_result, result_literal))) {
                  SERVER_LOG(WARN, "build string literal failed", K(ret));
                }
              }
            }
          }
        }
      }
      }
    }
  }
  return ret;
}

int ObAiColumnWorker::process_one_task(ObMySQLProxy &sql_proxy,
                                       ObAiColumnTaskService &task_svc,
                                       const ObAiColumnTaskService::ClaimedTask &task)
{
  int ret = OB_SUCCESS;
  ObArenaAllocator alloc("AiColTask");
  ObSchemaGetterGuard guard;
  const ObTableSchema *table_schema = NULL;
  const ObSimpleDatabaseSchema *db_schema = NULL;
  ObString db_name;
  if (OB_FAIL(ObMultiVersionSchemaService::get_instance()
              .get_runtime_schema_guard(guard))) {
    SERVER_LOG(WARN, "get schema guard failed", K(ret));
  } else if (OB_FAIL(guard.get_table_schema(task.table_id_, table_schema))) {
    SERVER_LOG(WARN, "get table schema failed", K(ret), K(task.table_id_));
  } else if (OB_ISNULL(table_schema)) {
    // Table dropped: drop its stale tasks.
    ObSqlString del_sql;
    int64_t affected = 0;
    if (OB_FAIL(del_sql.assign_fmt("DELETE FROM __all_ai_column_task WHERE table_id = %" PRIu64,
                                   task.table_id_))) {
      SERVER_LOG(WARN, "build stale task del sql failed", K(ret));
    } else if (OB_FAIL(sql_proxy.write(del_sql.ptr(), affected))) {
      SERVER_LOG(WARN, "drop stale tasks failed", K(ret));
    }
  } else if (OB_FAIL(guard.get_database_schema(table_schema->get_database_id(),
                                               db_schema))) {
    SERVER_LOG(WARN, "get database schema failed", K(ret));
  } else if (OB_ISNULL(db_schema)) {
    ret = OB_ERR_UNEXPECTED;
  } else {
    db_name = db_schema->get_database_name_str();
    const ObAiColumnSchema *ai_col = NULL;
    for (int64_t j = 0; NULL == ai_col && j < table_schema->get_ai_columns().count(); ++j) {
      if (table_schema->get_ai_columns().at(j).column_id_ == task.column_id_) {
        ai_col = &table_schema->get_ai_columns().at(j);
      }
    }
    if (OB_ISNULL(ai_col)) {
      ObSqlString del_sql;
      int64_t affected = 0;
      ret = del_sql.assign_fmt("DELETE FROM __all_ai_column_task WHERE table_id = %" PRIu64
                               " AND task_id = %" PRIu64, task.table_id_, task.task_id_);
      if (OB_FAIL(ret)) {
      } else if (OB_FAIL(sql_proxy.write(del_sql.ptr(), affected))) {
        SERVER_LOG(WARN, "drop stale task failed", K(ret));
      }
    } else {
      ObString result_literal;
      int call_ret = execute_task_group(sql_proxy, task, *table_schema, db_name, *ai_col,
                                        result_literal);
      if (OB_SUCCESS == call_ret) {
        // Publish in one transaction with task cleanup.
        ObMySQLTransaction trans;
        if (OB_FAIL(trans.start(&sql_proxy))) {
          SERVER_LOG(WARN, "init publish txn failed", K(ret));
        } else {
          ObAiColumnTaskService::PublishEntry entry;
          entry.column_id_ = ai_col->column_id_;
          entry.value_ = result_literal;
          ObArray<ObAiColumnTaskService::PublishEntry> entries;
          if (OB_FAIL(entries.push_back(entry))) {
            SERVER_LOG(WARN, "push publish entry failed", K(ret));
          } else {
            int pub_ret = task_svc.publish_group(trans, *table_schema,
                                                 OB_INVALID_ID,
                                                 task.rowkey_predicate_,
                                                 task.request_id_, entries,
                                                 task.definition_version_);
            int end_ret = trans.end(OB_SUCCESS == pub_ret);
            if (OB_FAIL(end_ret)) {
              SERVER_LOG(WARN, "end publish txn failed", K(ret));
            }
            ret = (OB_SUCCESS == pub_ret) ? OB_SUCCESS : pub_ret;
          }
        }
      } else {
        // Failure path: bounded retry or terminal failure publish.
        ret = OB_SUCCESS;
        int fail_ret = task_svc.fail_task(sql_proxy, task, ai_col->max_retries_,
                                          500 * 1000);
        if (OB_FAIL(fail_ret)) {
          SERVER_LOG(WARN, "fail_task handling failed", K(fail_ret));
        }
        if (task.retry_count_ >= ai_col->max_retries_) {
          ObMySQLTransaction trans;
          if (OB_FAIL(trans.start(&sql_proxy))) {
            SERVER_LOG(WARN, "init fail txn failed", K(ret));
          } else {
            ObArray<ObAiColumnTaskService::ClaimedTask> group;
            if (OB_FAIL(group.push_back(task))) {
              SERVER_LOG(WARN, "push fail group failed", K(ret));
            } else {
              int pf_ret = task_svc.publish_failure_group(trans, *table_schema,
                                                          OB_INVALID_ID,
                                                          task.rowkey_predicate_,
                                                          task.request_id_, group,
                                                          task.definition_version_);
              int end_ret = trans.end(OB_SUCCESS == pf_ret);
              if (OB_FAIL(end_ret)) {
                SERVER_LOG(WARN, "end fail txn failed", K(ret));
              }
            }
          }
        }
      }
    }
  }
  return ret;
}

void ObAiColumnWorker::run2()
{
  int ret = OB_SUCCESS;
  SERVER_LOG(INFO, "ai column worker loop start");
  ObArenaAllocator loop_alloc("AiColLoop");
  {
    ObArenaAllocator tmp_alloc("AiColRecover");
    ObAiColumnTaskService recover_svc(tmp_alloc);
    if (OB_FAIL(recover_svc.recover_stuck_tasks(*GCTX.sql_proxy_))) {
      SERVER_LOG(WARN, "recover stuck ai tasks failed", K(ret));
    }
  }
  while (!has_set_stop()) {
    ret = OB_SUCCESS;
    ObArenaAllocator alloc("AiColBatch");
    ObAiColumnTaskService task_svc(alloc);
    ObArray<ObAiColumnTaskService::ClaimedTask> tasks;
    bool has_work = false;
    if (OB_FAIL(task_svc.claim_tasks(*GCTX.sql_proxy_, 32, tasks))) {
      SERVER_LOG(WARN, "claim ai tasks failed", K(ret));
    } else if (tasks.count() > 0) {
      has_work = true;
      for (int64_t i = 0; OB_SUCC(ret) && i < tasks.count(); ++i) {
        int one_ret = process_one_task(*GCTX.sql_proxy_, task_svc, tasks.at(i));
        if (OB_SUCCESS != one_ret) {
          SERVER_LOG(WARN, "process one ai task failed", K(one_ret), K(i));
        }
      }
    }
    ob_usleep(has_work ? (20 * 1000) : POLL_INTERVAL_US);
  }
  SERVER_LOG(INFO, "ai column worker loop exit");
}

} // namespace observer
} // namespace oceanbase
