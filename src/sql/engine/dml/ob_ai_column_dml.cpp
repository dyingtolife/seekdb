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

#define USING_LOG_PREFIX SQL_ENG

#include "sql/engine/dml/ob_ai_column_dml.h"

#include "sql/session/ob_sql_session_info.h"
#include "sql/engine/ob_exec_context.h"
#include "share/schema/ob_ai_column_task_service.h"
#include "share/inner_table/ob_inner_table_schema.h"
#include "query/session/ob_inner_sql_connection_access.h"

namespace oceanbase
{
namespace sql
{
// Minimal ObISQLClient view over a session ObISQLConnection so the task
// service (ObISQLClient based) can run through the session inner connection.
class ObAiInnerConnClient : public ObISQLClient
{
public:
  explicit ObAiInnerConnClient(ObISQLConnection *conn) : conn_(conn) {}
  virtual ~ObAiInnerConnClient() = default;
  virtual int escape(const char *from, const int64_t from_size,
                     char *to, const int64_t to_size, int64_t &out_size) override
  {
    // Copy-through escape: every AI-column SQL value is already passed
    // through ObHexEscapeSqlStr upstream, so no further escaping is needed.
    int ret = common::OB_SUCCESS;
    if (to_size < from_size) {
      ret = common::OB_BUF_NOT_ENOUGH;
    } else if (NULL != from && from_size > 0) {
      MEMCPY(to, from, from_size);
      out_size = from_size;
    } else {
      out_size = 0;
    }
    return ret;
  }
  virtual int read(ReadResult &res, const char *sql, const int32_t group_id) override
  {
    UNUSED(group_id);
    return NULL == conn_ ? OB_NOT_INIT : conn_->execute_read(ObString(sql), res);
  }
  virtual int write(const char *sql, const int32_t group_id, int64_t &affected_rows) override
  {
    UNUSED(group_id);
    return NULL == conn_ ? OB_NOT_INIT : conn_->execute_write(ObString(sql), affected_rows);
  }
  virtual sqlclient::ObISQLConnection *get_connection() override { return conn_; }
private:
  ObISQLConnection *conn_;
};
}  // namespace sql
}
#include "sql/resolver/dml/ob_dml_stmt.h"
#include "sql/resolver/expr/ob_raw_expr.h"

namespace oceanbase
{
namespace sql
{

using namespace common;
using namespace share;
using namespace share::schema;

int ObAiColumnDMLUtils::init_ctx(const ObTableSchema &table_schema,
                                 ObAiColumnDMLCtx &ai_ctx,
                                 ObIAllocator *alloc_holder)
{
  int ret = OB_SUCCESS;
  ObIAllocator *alloc_holder_ = alloc_holder;
  ai_ctx.reset();
  const ObIArray<ObAiColumnSchema> &ai_columns = table_schema.get_ai_columns();
  bool has_async = false;
  for (int64_t i = 0; !has_async && i < ai_columns.count(); ++i) {
    has_async = (0 == ai_columns.at(i).sync_mode_.case_compare("async"));
  }
  ai_ctx.has_ai_columns_ = has_async;
  ai_ctx.table_id_ = table_schema.get_table_id();
  // Rowkey column names (for PK-update detection).
  ObArray<share::schema::ObColDesc> rk_ids;
  if (OB_FAIL(table_schema.get_rowkey_column_ids(rk_ids))) {
    SQL_ENG_LOG(WARN, "get rowkey column ids failed", K(ret));
  } else {
    for (int64_t i = 0; OB_SUCC(ret) && i < rk_ids.count(); ++i) {
      const ObColumnSchemaV2 *rk_col =
          table_schema.get_column_schema(rk_ids.at(i).col_id_);
      if (OB_NOT_NULL(rk_col)) {
        ObString name_copy;
        ObIAllocator &alloc = *alloc_holder_;
        if (OB_FAIL(ob_write_string(alloc, rk_col->get_column_name_str(), name_copy))) {
          SQL_ENG_LOG(WARN, "copy rowkey name failed", K(ret));
        } else if (OB_FAIL(ai_ctx.rowkey_names_.push_back(name_copy))) {
          SQL_ENG_LOG(WARN, "push rowkey name failed", K(ret));
        }
      }
    }
  }
  // Collect dependency column names across all AI columns of the table.
  for (int64_t i = 0; OB_SUCC(ret) && i < ai_columns.count(); ++i) {
    const ObAiColumnSchema &ai_col = ai_columns.at(i);
    for (int64_t j = 0; OB_SUCC(ret) && j < ai_col.dependency_column_ids_.count(); ++j) {
      const ObColumnSchemaV2 *dep_col =
          table_schema.get_column_schema(ai_col.dependency_column_ids_.at(j));
      if (OB_ISNULL(dep_col)) {
        ret = OB_ERR_UNEXPECTED;
        SQL_ENG_LOG(WARN, "ai dependency column missing", K(ret),
                    K(ai_col.dependency_column_ids_.at(j)));
      } else {
        ObString name_copy;
        ObIAllocator &alloc = *alloc_holder_;
        if (OB_FAIL(ob_write_string(alloc, dep_col->get_column_name_str(), name_copy))) {
          SQL_ENG_LOG(WARN, "copy dependency name failed", K(ret));
        } else if (OB_FAIL(ai_ctx.dependency_names_.push_back(name_copy))) {
          SQL_ENG_LOG(WARN, "push dependency name failed", K(ret));
        }
      }
    }
  }
  // ROUND 14 FIX (A): the v1 AI column DDL does not persist
  // dependency_column_ids_ (see ObAiColumnSqlService::gen_column_dml), so the
  // schema-side dependency list is always empty and no UPDATE could ever be
  // matched against it. Derive the dependency column names from the AI
  // column's input expression instead: scan the expression text, drop string
  // literals, and keep only identifiers that resolve to a real column of this
  // table, skipping the AI bookkeeping columns (result/state/request/spec).
  if (OB_SUCC(ret) && ai_ctx.dependency_names_.empty() && !ai_columns.empty()) {
    for (int64_t i = 0; OB_SUCC(ret) && i < ai_columns.count(); ++i) {
      const ObAiColumnSchema &ai_col = ai_columns.at(i);
      const ObString &expr = ai_col.input_expr_.length() > 0
          ? ai_col.input_expr_ : ai_col.normalized_expr_;
      const char *p = expr.ptr();
      const int64_t len = expr.length();
      int64_t k = 0;
      while (OB_SUCC(ret) && OB_NOT_NULL(p) && k < len) {
        const char c = p[k];
        if ('\'' == c || '"' == c) {  // string literal: never a column name
          const char quote = c;
          ++k;
          while (k < len && p[k] != quote) { ++k; }
          ++k;
        } else if (!isalpha(static_cast<unsigned char>(c)) && '_' != c) {
          ++k;
        } else {
          const int64_t start = k;
          while (k < len && (isalnum(static_cast<unsigned char>(p[k])) || '_' == p[k])) {
            ++k;
          }
          ObString word(static_cast<int32_t>(k - start), p + start);
          const ObColumnSchemaV2 *dep_col = table_schema.get_column_schema(word);
          if (OB_NOT_NULL(dep_col)) {
            bool is_bookkeeping = false;
            for (int64_t a = 0; !is_bookkeeping && a < ai_columns.count(); ++a) {
              const ObAiColumnSchema &other = ai_columns.at(a);
              is_bookkeeping = (dep_col->get_column_id() == other.column_id_
                                || dep_col->get_column_id() == other.state_column_id_
                                || dep_col->get_column_id() == other.request_column_id_
                                || dep_col->get_column_id() == other.result_spec_column_id_);
            }
            bool dup = false;
            for (int64_t d = 0; !dup && d < ai_ctx.dependency_names_.count(); ++d) {
              dup = (0 == ai_ctx.dependency_names_.at(d).case_compare(word));
            }
            if (!is_bookkeeping && !dup) {
              ObString name_copy;
              if (OB_SUCCESS != ob_write_string(*alloc_holder_, word, name_copy)) {
                // best effort: a missing dependency only means a missed trigger
              } else if (OB_SUCCESS != ai_ctx.dependency_names_.push_back(name_copy)) {
                // best effort
              } else {
                SQL_ENG_LOG(ERROR, "ai column dep derived", K(word),
                            "ai_col", ai_col.column_name_);
              }
            }
          }
        }
      }
    }
  }
  ai_ctx.inited_ = (OB_SUCCESS == ret);
  return ret;
}

int ObAiColumnDMLUtils::ensure_ctx(ObExecContext &exec_ctx,
                                   uint64_t ref_table_id,
                                   ObAiColumnDMLCtx &ai_ctx,
                                   ObIAllocator *alloc_holder)
{
  int ret = OB_SUCCESS;
  if (ai_ctx.inited_) {
    // already resolved for this statement
  } else {
    ObSchemaGetterGuard *guard = NULL;
    const ObTableSchema *table_schema = NULL;
    if (OB_ISNULL(exec_ctx.get_sql_ctx())
        || OB_ISNULL(guard = exec_ctx.get_sql_ctx()->schema_guard_)) {
      ret = OB_ERR_UNEXPECTED;
      SQL_ENG_LOG(ERROR, "ai column lazy init: no schema guard", K(ret),
                  K(ref_table_id));
    } else if (OB_FAIL(guard->get_table_schema(ref_table_id, table_schema))) {
      SQL_ENG_LOG(ERROR, "ai column lazy init: get table schema failed", K(ret),
                  K(ref_table_id));
    } else if (OB_ISNULL(table_schema)) {
      ret = OB_ERR_UNEXPECTED;
      SQL_ENG_LOG(ERROR, "ai column lazy init: table schema missing", K(ret),
                  K(ref_table_id));
    } else if (OB_FAIL(init_ctx(*table_schema, ai_ctx, alloc_holder))) {
      SQL_ENG_LOG(ERROR, "ai column lazy init: init ctx failed", K(ret),
                  K(ref_table_id));
    } else {
      SQL_ENG_LOG(ERROR, "ai column lazy init done", K(ref_table_id),
                  "has_ai_columns", ai_ctx.has_ai_columns_,
                  "ai_column_cnt", table_schema->get_ai_column_count(),
                  "table_name", table_schema->get_table_name_str());
    }
  }
  return ret;
}

int ObAiColumnDMLUtils::build_rowkey_predicate(
    ObEvalCtx &eval_ctx,
    const ObIArray<ObExpr *> &key_exprs,
    const ObIArray<ObString> &key_names,
    ObIAllocator &allocator,
    ObString &predicate)
{
  int ret = OB_SUCCESS;
  ObStringBuffer buf(&allocator);
  // Set when a rowkey column type cannot be rendered as a SQL literal: the
  // predicate is then left empty and capture_row skips the row rather than
  // emitting a corrupt WHERE clause.
  bool unsupported_type = false;
  for (int64_t i = 0; OB_SUCC(ret) && !unsupported_type && i < key_exprs.count(); ++i) {
    const char *name_ptr = "";
    int32_t name_len = 0;
    if (i < key_names.count()) {
      name_ptr = key_names.at(i).ptr();
      name_len = key_names.at(i).length();
    }
    ObExpr *expr = key_exprs.at(i);
    ObDatum *datum = NULL;
    if (i > 0 && OB_FAIL(buf.append(" AND "))) {
      break;
    } else if (OB_ISNULL(expr)) {
      ret = OB_ERR_UNEXPECTED;
    } else if (OB_FAIL(buf.append("`"))) {
      break;
    } else if (OB_FAIL(buf.append(name_ptr, name_len))) {
      break;
    } else if (OB_FAIL(buf.append("` = "))) {
      break;
    } else if (OB_FAIL(expr->eval(eval_ctx, datum))) {
      SQL_ENG_LOG(WARN, "eval key expr failed", K(ret), K(i));
    } else {
        // backticked column names are not available here; the predicate is
        // rebuilt against column ids by the flush phase. Store the VALUE
        // only: `id` = <v> shapes are produced at flush time from schema.
      if (datum->is_null()) {
        // NULL rowkey is impossible; defensive: never matches anything.
        if (OB_FAIL(buf.append("NULL"))) {
          break;
        }
      } else if (ob_is_string_type(expr->datum_meta_.type_)) {
        ObString val = datum->get_string();
        if (OB_FAIL(buf.append("'"))) {
          break;
        }
        for (int64_t j = 0; OB_SUCC(ret) && j < val.length(); ++j) {
          char c = val.ptr()[j];
          if ('\'' == c) {
            if (OB_FAIL(buf.append("''"))) { break; }
          } else if (OB_FAIL(buf.append(&c, 1))) {
            break;
          }
        }
        if (OB_SUCC(ret)) {
          ret = buf.append("'");
        }
      } else if (ob_is_integer_type(expr->datum_meta_.type_)) {
        // Render the literal from the datum value. ObDatum::to_string() prints
        // the internal debug form ("{len: 8, flag: 0, ...}"), which produced
        // unparseable predicates (OB_ERR_PARSE) before this fix.
        char tmp[64];
        int64_t tmp_pos = 0;
        if (ob_is_unsigned_type(expr->datum_meta_.type_)) {
          ret = databuff_printf(tmp, sizeof(tmp), tmp_pos, "%lu",
                                static_cast<unsigned long>(datum->get_uint64()));
        } else {
          ret = databuff_printf(tmp, sizeof(tmp), tmp_pos, "%ld",
                                static_cast<long>(datum->get_int()));
        }
        if (OB_FAIL(ret)) {
          SQL_ENG_LOG(ERROR, "print key value failed", K(ret), K(i));
        } else if (OB_FAIL(buf.append(tmp, static_cast<int32_t>(tmp_pos)))) {
          SQL_ENG_LOG(ERROR, "append key value failed", K(ret));
        }
      } else if (ObFloatType == expr->datum_meta_.type_
                 || ObDoubleType == expr->datum_meta_.type_) {
        char tmp[64];
        int64_t tmp_pos = 0;
        ret = databuff_printf(tmp, sizeof(tmp), tmp_pos, "%.17g",
                             datum->get_double());
        if (OB_FAIL(ret)) {
          SQL_ENG_LOG(ERROR, "print key value failed", K(ret), K(i));
        } else if (OB_FAIL(buf.append(tmp, static_cast<int32_t>(tmp_pos)))) {
          SQL_ENG_LOG(ERROR, "append key value failed", K(ret));
        }
      } else {
        unsupported_type = true;
        SQL_ENG_LOG(ERROR, "ai rowkey column type not renderable, row not tracked",
                    "obj_type", expr->datum_meta_.type_, K(i));
      }
    }
  }
  if (OB_SUCC(ret) && !unsupported_type) {
    predicate = ObString(static_cast<int32_t>(buf.length()), buf.ptr());
  } else if (OB_SUCC(ret)) {
    predicate.reset();
  }
  return ret;
}

int ObAiColumnDMLUtils::capture_row(ObEvalCtx &eval_ctx,
                                    const ObIArray<ObExpr *> &key_exprs,
                                    ObAiColumnDMLCtx &ai_ctx,
                                    bool is_delete,
                                    const ObString *old_rowkey_predicate)
{
  int ret = OB_SUCCESS;
  if (!ai_ctx.has_ai_columns_) {
    // fast path: table has no AI columns
  } else {
    SQL_ENG_LOG(ERROR, "ai capture_row entry", K(ai_ctx.table_id_), K(is_delete),
                "key_cnt", key_exprs.count(),
                "rowkey_name_cnt", ai_ctx.rowkey_names_.count(),
                "changes", ai_ctx.row_changes_.count());
    ObArenaAllocator *alloc_ptr = NULL;
    // use the eval ctx's allocator for the strings (statement lifetime)
    ObIAllocator &alloc = eval_ctx.exec_ctx_.get_allocator();
    ObAiColumnDMLCtx::RowChange change;
    change.is_delete_ = is_delete;
    bool skip_row = false;
    if (OB_FAIL(build_rowkey_predicate(eval_ctx, key_exprs, ai_ctx.rowkey_names_, alloc,
                                       change.rowkey_predicate_))) {
      SQL_ENG_LOG(ERROR, "build ai rowkey predicate failed", K(ret));
    } else if (change.rowkey_predicate_.empty()) {
      // No rowkey expression reached us (a table without a primary key, or a
      // call site that passed an empty key list). An empty WHERE would turn
      // the invalidation UPDATE into a full-table write, so the row is left
      // untracked instead. Never expected for a table with a primary key.
      skip_row = true;
      SQL_ENG_LOG(ERROR, "ai capture_row: empty rowkey predicate, row not tracked",
                  K(ai_ctx.table_id_), K(is_delete), "key_cnt", key_exprs.count(),
                  "rowkey_name_cnt", ai_ctx.rowkey_names_.count());
    } else if (NULL != old_rowkey_predicate) {
      change.old_rowkey_predicate_ = *old_rowkey_predicate;
    } else {
      // request id: per statement + per row counter, unique per row change.
      char req_buf[64];
      int64_t pos = 0;
      uint64_t sess_id = eval_ctx.exec_ctx_.get_my_session()->get_sid();
      int64_t stmt_id = eval_ctx.exec_ctx_.get_execution_id();
      int64_t seq = ++ai_ctx.request_seq_;
      if (OB_FAIL(databuff_printf(req_buf, sizeof(req_buf), pos,
                                  "%lu-%ld-%ld", sess_id, stmt_id, seq))) {
        SQL_ENG_LOG(WARN, "build request id failed", K(ret));
      } else if (OB_FAIL(ob_write_string(alloc, ObString(static_cast<int32_t>(pos), req_buf),
                                         change.request_id_))) {
        SQL_ENG_LOG(WARN, "copy request id failed", K(ret));
      }
    }
    if (OB_SUCC(ret) && !skip_row) {
      if (OB_FAIL(ai_ctx.row_changes_.push_back(change))) {
        SQL_ENG_LOG(ERROR, "append row change failed", K(ret));
      }
    }
    ObString last_pred;
    if (!ai_ctx.row_changes_.empty()) {
      last_pred = ai_ctx.row_changes_.at(ai_ctx.row_changes_.count() - 1)
                      .rowkey_predicate_;
    }
    SQL_ENG_LOG(ERROR, "ai capture_row done", K(ai_ctx.table_id_), K(ret),
                "changes", ai_ctx.row_changes_.count(), K(last_pred));
  }
  return ret;
}

int ObAiColumnDMLUtils::flush_changes(ObExecContext &exec_ctx,
                                      const ObTableSchema &table_schema,
                                      ObAiColumnDMLCtx &ai_ctx)
{
  int ret = OB_SUCCESS;
  if (!ai_ctx.has_ai_columns_ || ai_ctx.row_changes_.empty()) {
    // nothing to do
  } else {
    ObSQLSessionInfo *session = exec_ctx.get_my_session();
    const int64_t saved_nested_count = OB_ISNULL(session) ? 0 : session->get_nested_count();
    SQL_ENG_LOG(ERROR, "ai flush_changes entry", K(ai_ctx.table_id_),
                "changes", ai_ctx.row_changes_.count(),
                "ai_cols", table_schema.get_ai_columns().count(),
                "table", table_schema.get_table_name_str());
    if (OB_ISNULL(session)) {
      ret = OB_ERR_UNEXPECTED;
      SQL_ENG_LOG(ERROR, "ai flush_changes: no session", K(ret));
    } else {
      // Session inner connection: joins the user's current transaction, so
      // task rows and hidden-column updates commit/rollback with the data.
      common::sqlclient::ObISQLConnectionGuard conn_guard;
      if (OB_FAIL(query::ObInnerSQLConnectionAccess::create_connection_with_external_session(
              session, conn_guard))) {
        SQL_ENG_LOG(ERROR, "create session inner connection failed", K(ret));
      } else {
        SQL_ENG_LOG(ERROR, "ai flush: session inner connection ready", K(ret));
        ObArenaAllocator alloc("AiColFlush");
        ObAiColumnTaskService task_svc(alloc);
        ObAiInnerConnClient client_wrapper(conn_guard.get_ptr());
        for (int64_t i = 0; OB_SUCC(ret) && i < ai_ctx.row_changes_.count(); ++i) {
          ObAiColumnDMLCtx::RowChange &change = ai_ctx.row_changes_.at(i);
          if (change.is_delete_) {
            // DELETE: remove pending tasks for the old rowkey.
            if (OB_FAIL(task_svc.remove_tasks_for_row(client_wrapper, ai_ctx.table_id_,
                                                      change.rowkey_predicate_))) {
              SQL_ENG_LOG(ERROR, "ai flush: remove tasks on delete failed", K(ret),
                          K(ai_ctx.table_id_), "pred", change.rowkey_predicate_);
            }
          } else if (!change.old_rowkey_predicate_.empty()) {
            // PK update: rowkey_predicate_ is the OLD key,
            // old_rowkey_predicate_ is the NEW key. Remove old-key tasks,
            // then invalidate+enqueue for the new key.
            if (OB_FAIL(task_svc.remove_tasks_for_row(client_wrapper, ai_ctx.table_id_,
                                                      change.rowkey_predicate_))) {
              SQL_ENG_LOG(ERROR, "ai flush: remove old key tasks failed", K(ret));
            } else if (OB_FAIL(task_svc.invalidate_row(client_wrapper, table_schema,
                                                       change.old_rowkey_predicate_,
                                                       change.request_id_))) {
              SQL_ENG_LOG(ERROR, "ai flush: invalidate new key failed", K(ret),
                          "pred", change.old_rowkey_predicate_);
            } else if (OB_FAIL(task_svc.enqueue_tasks(client_wrapper, table_schema,
                                                       OB_INVALID_ID,
                                                       change.old_rowkey_predicate_,
                                                       change.request_id_))) {
              SQL_ENG_LOG(ERROR, "ai flush: enqueue new key tasks failed", K(ret),
                          "pred", change.old_rowkey_predicate_);
            }
          } else {
            // INSERT or dependency UPDATE: invalidate the hidden columns
            // (result=NULL, state=PENDING, request=rid) then enqueue tasks.
            if (OB_FAIL(task_svc.invalidate_row(client_wrapper, table_schema,
                                                change.rowkey_predicate_,
                                                change.request_id_))) {
              SQL_ENG_LOG(ERROR, "ai flush: invalidate row failed", K(ret),
                          K(ai_ctx.table_id_), "pred", change.rowkey_predicate_);
            } else if (OB_FAIL(task_svc.enqueue_tasks(client_wrapper, table_schema,
                                                       OB_INVALID_ID,
                                                       change.rowkey_predicate_,
                                                       change.request_id_))) {
              SQL_ENG_LOG(ERROR, "ai flush: enqueue tasks failed", K(ret),
                          K(ai_ctx.table_id_), "pred", change.rowkey_predicate_);
            } else {
              SQL_ENG_LOG(ERROR, "ai flush: row retasked", K(ai_ctx.table_id_),
                          "pred", change.rowkey_predicate_);
            }
          }
        }
        if (OB_SUCC(ret)) {
          // reset the ctx so a rescan does not double-flush
          ai_ctx.row_changes_.reset();
        }
      }
      // The inner SQL above runs on the user's own session, so it shares the
      // outer statement's session bookkeeping. Restore it: if the inner SQL
      // path leaves the nested-statement counter changed, the outer
      // statement's end_stmt() fails with OB_ERR_UNEXPECTED (-4016) even
      // though every write of the statement already succeeded.
      SQL_ENG_LOG(ERROR, "ai flush: nested session count", K(ai_ctx.table_id_),
                  "before", saved_nested_count,
                  "after", session->get_nested_count());
      session->set_nested_count(saved_nested_count);
    }
  }
  return ret;
}

int ObAiColumnDMLUtils::check_no_direct_assignment(
    const ObTableSchema &table_schema,
    const ObAiColumnDMLCtx &ai_ctx)
{
  int ret = OB_SUCCESS;
  for (int64_t i = 0; OB_SUCC(ret) && i < ai_ctx.assigned_ai_result_cols_.count(); ++i) {
    uint64_t col_id = ai_ctx.assigned_ai_result_cols_.at(i);
    const ObColumnSchemaV2 *col = table_schema.get_column_schema(col_id);
    if (OB_ISNULL(col)) {
      ret = OB_ERR_UNEXPECTED;
    } else {
      ret = OB_NOT_SUPPORTED;
      LOG_USER_ERROR(OB_NOT_SUPPORTED, "direct update of an AI result column");
      SQL_ENG_LOG(WARN, "direct assignment to ai result column rejected", K(ret),
                  K(col_id), K(col->get_column_name_str()));
    }
  }
  return ret;
}

int ObAiColumnDMLUtils::check_strict_read_for_stmt(
    ObStmt &basic_stmt,
    sql::ObSQLSessionInfo *session,
    share::schema::ObSchemaGetterGuard *schema_guard)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(session)) {
    ret = OB_ERR_UNEXPECTED;
  } else {
    // ROUND 14 FIX (B): the sys_vars_cache_ entry is not refreshed by a SET
    // SESSION (the sysvar store is, hence @@session reports the new value),
    // so read the authoritative store value and fall back to the cache.
    int64_t strict = 0;
    int strict_ret = session->get_sys_variable(
        share::SYS_VAR_AI_COLUMN_STRICT_READ, strict);
    if (OB_SUCCESS != strict_ret) {
      strict = session->get_ai_column_strict_read();
    }
    SQL_ENG_LOG(ERROR, "ai strict gate: entry", K(strict), K(strict_ret),
                "stmt_type", basic_stmt.get_stmt_type(),
                "is_dml", basic_stmt.is_dml_stmt(),
                "sess_id", session->get_sid());
    if (0 == strict) {
      // strict read off: no check
    } else if (!basic_stmt.is_dml_stmt()) {
      // only SELECT/UPDATE/DELETE (+their EXPLAIN) go through the check
    } else {
      ObDMLStmt *dml_stmt = static_cast<ObDMLStmt *>(&basic_stmt);
      ObSchemaGetterGuard *guard = schema_guard;
      // Collect distinct table ids referenced by the statement's column
      // expressions.
      ObArray<ObColumnRefRawExpr *> col_exprs;
      if (OB_ISNULL(guard)) {
        ret = OB_ERR_UNEXPECTED;
        SQL_ENG_LOG(WARN, "no schema guard for strict read", K(ret));
      } else if (OB_FAIL(dml_stmt->get_column_exprs(col_exprs))) {
        SQL_ENG_LOG(WARN, "collect column exprs failed", K(ret));
      } else {
        ObArray<uint64_t> checked;  // checked table ids
        for (int64_t i = 0; OB_SUCC(ret) && i < col_exprs.count(); ++i) {
          ObColumnRefRawExpr *col_expr = col_exprs.at(i);
          uint64_t tid = col_expr->get_table_id();
          SQL_ENG_LOG(ERROR, "ai strict gate: col expr", K(tid),
                      "col_id", col_expr->get_column_id(),
                      "col_name", col_expr->get_column_name());
          const ObTableSchema *table_schema = NULL;
          const ObAiColumnSchema *ai_col = NULL;
          bool already = false;
          for (int64_t j = 0; !already && j < checked.count(); ++j) {
            already = (checked.at(j) == tid);
          }
          if (already || OB_INVALID_ID == tid) {
            continue;
          }
          // look up the table schema and AI columns
          if (OB_FAIL(ret)) {
          } else if (OB_FAIL(guard->get_table_schema(tid, table_schema))) {
            SQL_ENG_LOG(WARN, "get table schema failed", K(ret), K(tid));
          } else if (OB_ISNULL(table_schema)) {
            // not a real table
          } else if (table_schema->get_ai_column_count() <= 0) {
            // no AI columns
          } else {
            // find the AI column matching this column expr id
            for (int64_t a = 0; OB_SUCC(ret) && a < table_schema->get_ai_columns().count(); ++a) {
              const ObAiColumnSchema &cand = table_schema->get_ai_columns().at(a);
              if (cand.column_id_ == col_expr->get_column_id()) {
                ai_col = &cand;
                break;
              }
            }
            SQL_ENG_LOG(ERROR, "ai strict gate: col match", K(tid),
                        "col_id", col_expr->get_column_id(),
                        "is_ai", OB_NOT_NULL(ai_col),
                        "ai_col_id", OB_ISNULL(ai_col) ? OB_INVALID_ID : ai_col->column_id_,
                        "ai_col_cnt", table_schema->get_ai_columns().count());
            if (OB_ISNULL(ai_col)) {
              // column is not an AI column: unaffected
              if (OB_FAIL(checked.push_back(tid))) {
              }
            } else {
              // column-level check: any row of this AI column not READY?
              const ObColumnSchemaV2 *state_col =
                  table_schema->get_column_schema(ai_col->state_column_id_);
              const ObDatabaseSchema *db_schema = NULL;
              if (OB_ISNULL(state_col)) {
                ret = OB_ERR_UNEXPECTED;
              } else if (OB_FAIL(guard->get_database_schema(
                             table_schema->get_database_id(), db_schema))) {
                SQL_ENG_LOG(WARN, "get db schema failed", K(ret));
              } else if (OB_ISNULL(db_schema)) {
                ret = OB_ERR_UNEXPECTED;
              } else {
                ObSqlString sql;
                SMART_VAR(ObMySQLProxy::MySQLResult, res) {
                  ObMySQLResult *result = NULL;
                  if (OB_FAIL(sql.assign_fmt("SELECT 1 FROM `%.*s`.`%.*s` WHERE %.*s <> 'READY' LIMIT 1",
                                             db_schema->get_database_name_str().length(),
                                             db_schema->get_database_name_str().ptr(),
                                             table_schema->get_table_name_str().length(),
                                             table_schema->get_table_name_str().ptr(),
                                             state_col->get_column_name_str().length(),
                                             state_col->get_column_name_str().ptr()))) {
                    SQL_ENG_LOG(WARN, "build strict check sql failed", K(ret));
                  } else if (OB_FAIL(GCTX.sql_proxy_->read(res, sql.ptr()))) {
                    SQL_ENG_LOG(WARN, "strict check read failed", K(ret), "sql", sql.ptr());
                  } else if (OB_ISNULL(result = res.get_result())) {
                    ret = OB_ERR_UNEXPECTED;
                  } else if (OB_SUCCESS == result->next()) {
                    // Not-ready row exists: error the statement (column-wide).
                    SQL_ENG_LOG(ERROR, "ai strict gate: not-ready row found",
                                K(tid), "ai_col_id", ai_col->column_id_,
                                "sql", sql.ptr());
                    ret = OB_ERR_AI_COLUMN_STRICT_READ_NOT_READY;
                    LOG_USER_ERROR(OB_ERR_AI_COLUMN_STRICT_READ_NOT_READY,
                                   static_cast<uint64_t>(table_schema->get_table_id()),
                                   static_cast<uint64_t>(ai_col->column_id_));
                  } else if (OB_ITER_END != ret) {
                    SQL_ENG_LOG(ERROR, "ai strict gate: probe next failed", K(ret),
                                "sql", sql.ptr());
                  } else {
                    SQL_ENG_LOG(ERROR, "ai strict gate: no not-ready row",
                                K(tid), "ai_col_id", ai_col->column_id_,
                                "sql", sql.ptr());
                    ret = OB_SUCCESS;
                  }
                }
              }
              if (OB_SUCC(ret) && OB_FAIL(checked.push_back(tid))) {
              }
              break;  // one AI column per table enough for the expr set
            }
          }
        }
      }
    }
  }
  return ret;
}

int ObAiColumnDMLUtils::retry_ai_column(
    ObExecContext &exec_ctx,
    const ObTableSchema &table_schema,
    const ObString &column_name,
    const ObString &where_clause_text,
    int64_t &matched_rows)
{
  int ret = OB_SUCCESS;
  matched_rows = 0;
  SQL_ENG_LOG(ERROR, "ai retry gate: entry", K(column_name),
              "where", where_clause_text,
              "ai_col_cnt", table_schema.get_ai_columns().count(),
              "table", table_schema.get_table_name_str());
  const ObIArray<ObAiColumnSchema> &ai_columns = table_schema.get_ai_columns();
  const ObAiColumnSchema *ai_col = NULL;
  for (int64_t i = 0; NULL == ai_col && i < ai_columns.count(); ++i) {
    if (0 == ai_columns.at(i).column_name_.case_compare(column_name)) {
      ai_col = &ai_columns.at(i);
    }
  }
  ObSQLSessionInfo *session = exec_ctx.get_my_session();
  if (OB_ISNULL(ai_col)) {
    SQL_ENG_LOG(ERROR, "ai retry gate: column not found", K(column_name));
    ret = OB_ERR_AI_COLUMN_NOT_FOUND;
    LOG_USER_ERROR(OB_ERR_AI_COLUMN_NOT_FOUND,
                   static_cast<int32_t>(column_name.length()), column_name.ptr());
  } else if (OB_ISNULL(session)) {
    ret = OB_ERR_UNEXPECTED;
  } else {
    const ObColumnSchemaV2 *state_col = table_schema.get_column_schema(ai_col->state_column_id_);
    const ObColumnSchemaV2 *result_col = table_schema.get_column_schema(ai_col->column_id_);
    const ObColumnSchemaV2 *request_col = table_schema.get_column_schema(ai_col->request_column_id_);
    if (OB_ISNULL(state_col) || OB_ISNULL(result_col) || OB_ISNULL(request_col)) {
      SQL_ENG_LOG(ERROR, "ai retry gate: hidden col missing", K(ret),
                  "ai_col_id", ai_col->column_id_,
                  "state_col_id", ai_col->state_column_id_,
                  "request_col_id", ai_col->request_column_id_);
      ret = OB_ERR_UNEXPECTED;
    } else {
      // The resolver stored the raw WHERE text. `status` in the text was
      // rewritten by the resolver to the hidden state column name unless
      // backticked. Build the SELECT of rowkeys.
      ObStringBuffer where_buf(&exec_ctx.get_allocator());
      // status rewrite: replace bare word status (not backticked) with the
      // state column name; backticked `status` stays as the physical column.
      const char *w = where_clause_text.ptr();
      int64_t wlen = where_clause_text.length();
      int64_t i = 0;
      ObString status_word("status");
      ObString state_name = state_col->get_column_name_str();
      while (OB_SUCC(ret) && i < wlen) {
        // copy non-word chars
        if (!isalpha(static_cast<unsigned char>(w[i])) && w[i] != '_') {
          if (OB_FAIL(where_buf.append(w + i, 1))) { break; }
          ++i;
          continue;
        }
        int64_t start = i;
        while (i < wlen && (isalnum(static_cast<unsigned char>(w[i])) || w[i] == '_')) { ++i; }
        ObString word(static_cast<int32_t>(i - start), w + start);
        // check for backtick right before start
        bool backticked = (start > 0 && w[start - 1] == '`');
        if (!backticked && 0 == word.case_compare(status_word)) {
          if (OB_FAIL(where_buf.append(state_name))) { break; }
        } else {
          if (OB_FAIL(where_buf.append(word))) { break; }
        }
      }
      if (OB_SUCC(ret)) {
        // Build the rowkey select list: backticked rowkey column names.
        ObStringBuffer rk_list(&exec_ctx.get_allocator());
        ObArray<share::schema::ObColDesc> rk_ids;
        if (OB_FAIL(table_schema.get_rowkey_column_ids(rk_ids))) {
          SQL_ENG_LOG(ERROR, "get rowkey column ids failed", K(ret));
        } else {
          for (int64_t k = 0; OB_SUCC(ret) && k < rk_ids.count(); ++k) {
            const ObColumnSchemaV2 *rk =
                table_schema.get_column_schema(rk_ids.at(k).col_id_);
            if (OB_ISNULL(rk)) {
              ret = OB_ERR_UNEXPECTED;
              break;
            }
            if (k > 0 && OB_FAIL(rk_list.append(","))) { break; }
            ObString rk_name = rk->get_column_name_str();
            if (OB_FAIL(rk_list.append("`"))) { break; }
            else if (OB_FAIL(rk_list.append(rk_name))) { break; }
            else if (OB_FAIL(rk_list.append("`"))) { break; }
          }
        }
        if (OB_SUCC(ret)) {
          ObString db_name;
          ObSchemaGetterGuard *guard_ptr = exec_ctx.get_sql_ctx()->schema_guard_;
          const ObDatabaseSchema *db_schema = NULL;
          if (OB_ISNULL(guard_ptr)) {
            ret = OB_ERR_UNEXPECTED;
          } else if (OB_FAIL(guard_ptr->get_database_schema(table_schema.get_database_id(), db_schema))) {
            SQL_ENG_LOG(ERROR, "get db schema failed", K(ret));
          } else if (OB_ISNULL(db_schema)) {
            ret = OB_ERR_UNEXPECTED;
          } else {
            db_name = db_schema->get_database_name_str();
            ObSqlString sel_sql;
            SMART_VAR(ObMySQLProxy::MySQLResult, sel_res) {
              ObMySQLResult *sel_result = NULL;
              if (OB_FAIL(sel_sql.assign_fmt("SELECT %.*s AS rk FROM `%.*s`.`%.*s`%.*s",
                                             rk_list.length(), rk_list.ptr(),
                                             db_name.length(), db_name.ptr(),
                                             table_schema.get_table_name_str().length(),
                                             table_schema.get_table_name_str().ptr(),
                                             where_buf.length() > 0 ? " WHERE " : "",
                                             where_buf.length(),
                                             where_buf.length() ? where_buf.ptr() : ""))) {
                SQL_ENG_LOG(ERROR, "build retry select failed", K(ret));
              } else if (OB_FAIL(GCTX.sql_proxy_->read(sel_res, sel_sql.ptr()))) {
                SQL_ENG_LOG(ERROR, "retry select failed", K(ret), "sql", sel_sql.ptr());
              } else if (OB_ISNULL(sel_result = sel_res.get_result())) {
                ret = OB_ERR_UNEXPECTED;
              } else {
                ObArray<ObString> rowkeys;
                while (OB_SUCC(ret) && OB_SUCC(sel_result->next())) {
                  ObString rk;
                  if (OB_FAIL(sel_result->get_varchar("rk", rk))) {
                    SQL_ENG_LOG(ERROR, "read rowkey failed", K(ret));
                  } else {
                    ObString copy;
                    if (OB_FAIL(ob_write_string(exec_ctx.get_allocator(), rk, copy))) {
                      SQL_ENG_LOG(ERROR, "copy rowkey failed", K(ret));
                    } else if (OB_FAIL(rowkeys.push_back(copy))) {
                      SQL_ENG_LOG(ERROR, "push rowkey failed", K(ret));
                    }
                  }
                }
                if (OB_ITER_END == ret) { ret = OB_SUCCESS; }
                if (OB_SUCC(ret)) {
                  // Invalidate + enqueue per matched row in this session txn.
                  common::sqlclient::ObISQLConnectionGuard conn_guard;
                  if (OB_FAIL(query::ObInnerSQLConnectionAccess::create_connection_with_external_session(
                          session, conn_guard))) {
                    SQL_ENG_LOG(ERROR, "create inner conn failed", K(ret));
                  } else {
                    ObAiInnerConnClient conn_client(conn_guard.get_ptr());
                    ObISQLClient *client = &conn_client;
                    ObArenaAllocator alloc("AiColRetry");
                    ObAiColumnTaskService task_svc(alloc);
                    for (int64_t r = 0; OB_SUCC(ret) && r < rowkeys.count(); ++r) {
                      char req_buf[64];
                      int64_t pos = 0;
                      uint64_t sess_id = session->get_sid();
                      int64_t stmt_id = exec_ctx.get_execution_id();
                      if (OB_FAIL(databuff_printf(req_buf, sizeof(req_buf), pos,
                                                  "%lu-%ld-%ld", sess_id, stmt_id, r))) {
                        SQL_ENG_LOG(ERROR, "build request id failed", K(ret));
                      } else {
                        ObString req_id(static_cast<int32_t>(pos), req_buf);
                        if (OB_FAIL(task_svc.invalidate_row(*client, table_schema,
                                                            rowkeys.at(r), req_id))) {
                          SQL_ENG_LOG(ERROR, "retry invalidate failed", K(ret));
                        } else if (OB_FAIL(task_svc.enqueue_tasks(*client, table_schema,
                                                                  OB_INVALID_ID,
                                                                  rowkeys.at(r), req_id))) {
                          SQL_ENG_LOG(ERROR, "retry enqueue failed", K(ret));
                        }
                      }
                    }
                    SQL_ENG_LOG(ERROR, "ai retry gate: rows done", K(ret),
                                "rowkeys", rowkeys.count());
                    if (OB_SUCC(ret)) {
                      matched_rows = rowkeys.count();
                    }
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

int ObAiColumnDMLUtils::check_strict_read(
    ObSchemaGetterGuard &guard,
    const ObTableSchema &table_schema,
    const ObIArray<uint64_t> &referenced_ai_column_ids,
    bool strict_read_enabled)
{
  int ret = OB_SUCCESS;
  UNUSED(guard);
  if (!strict_read_enabled || referenced_ai_column_ids.empty()) {
    // no check needed
  } else {
    // Column-level check: the referenced AI column must have no rows whose
    // hidden state column is not READY. Presence of any non-READY row in
    // that column errors the whole statement (column-wide strictness).
    // Implemented by the statement-level hook (see ob_sql.cpp):
    // here we only validate the referenced column ids are AI columns.
    for (int64_t i = 0; OB_SUCC(ret) && i < referenced_ai_column_ids.count(); ++i) {
      const ObColumnSchemaV2 *col =
          table_schema.get_column_schema(referenced_ai_column_ids.at(i));
      if (OB_ISNULL(col)) {
        ret = OB_ERR_UNEXPECTED;
        break;
      }
    }
  }
  return ret;
}

} // namespace sql
} // namespace oceanbase
