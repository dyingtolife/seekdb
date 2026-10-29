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

#include "share/schema/ob_ai_column_sql_service.h"
#include "share/ob_dml_sql_splicer.h"
#include "share/inner_table/ob_inner_table_schema.h"
#include "share/schema/ob_ai_column_schema.h"
#include "share/schema/ob_table_schema.h"

namespace oceanbase
{
namespace share
{
namespace schema
{

// __all_ai_column rowkey: (table_id, column_id);
// __all_ai_column_history rowkey: (table_id, column_id, schema_version).
int ObAiColumnSqlService::splice_ai_column_insert(const ObAiColumnSchema &ai_column,
                                                  const bool is_history,
                                                  const int64_t schema_version,
                                                  share::ObDMLSqlSplicer &dml)
{
  int ret = OB_SUCCESS;
  dml.reset();
  if (OB_FAIL(dml.add_uint64_pk_column("table_id", ai_column.table_id_))) {
  } else if (OB_FAIL(dml.add_uint64_pk_column("column_id", ai_column.column_id_))) {
  } else if (is_history && OB_FAIL(dml.add_pk_column("schema_version", schema_version))) {
  } else if (OB_FAIL(dml.add_column("column_name", ObHexEscapeSqlStr(ai_column.column_name_)))) {
  } else if (OB_FAIL(dml.add_column("definition_version",
                                    static_cast<int64_t>(ai_column.definition_version_)))) {
  } else if (OB_FAIL(dml.add_column("sync_mode", ObHexEscapeSqlStr(ai_column.sync_mode_)))) {
  } else if (OB_FAIL(dml.add_column("max_retries", ai_column.max_retries_))) {
  } else if (OB_FAIL(dml.add_column("is_embedding",
                                    static_cast<int64_t>(ai_column.is_embedding_)))) {
  } else if (OB_FAIL(dml.add_column("vector_dimension", ai_column.vector_dimension_))) {
  } else if (OB_FAIL(dml.add_uint64_column("state_column_id", ai_column.state_column_id_))) {
  } else if (OB_FAIL(dml.add_uint64_column("request_column_id", ai_column.request_column_id_))) {
  } else if (OB_FAIL(dml.add_uint64_column("result_spec_column_id",
                                           ai_column.result_spec_column_id_))) {
  } else if (OB_FAIL(dml.add_column("normalized_expr",
                                    ObHexEscapeSqlStr(ai_column.normalized_expr_)))) {
  } else if (is_history && OB_FAIL(dml.add_column("is_deleted", 0))) {
  } else {
    // dependency_column_ids_ is not persisted as a separate v1 column; the
    // worker re-derives dependencies from the normalized expression text.
  }
  return ret;
}

int ObAiColumnSqlService::exec_insert(const char *tname,
                                      share::ObDMLSqlSplicer &dml,
                                      common::ObISQLClient &sql_client)
{
  int ret = OB_SUCCESS;
  ObSqlString buffer;
  int64_t affected_rows = 0;
  if (OB_FAIL(dml.splice_insert_sql(tname, buffer))) {
    SHARE_SCHEMA_LOG(WARN, "failed to splice ai column insert sql", K(ret), K(tname));
  } else if (OB_FAIL(sql_client.write(buffer.ptr(), affected_rows))) {
    SHARE_SCHEMA_LOG(WARN, "failed to execute ai column insert", K(ret), K(tname),
                     "sql", buffer.ptr());
  } else if (!is_single_row(affected_rows)) {
    ret = OB_ERR_UNEXPECTED;
    SHARE_SCHEMA_LOG(WARN, "ai column insert affected unexpected rows", K(ret),
                     K(affected_rows), K(tname));
  }
  return ret;
}

int ObAiColumnSqlService::create_ai_column(const ObAiColumnSchema &ai_column,
                                           const uint64_t table_id,
                                           const int64_t table_schema_version,
                                           const common::ObString &ddl_stmt,
                                           common::ObISQLClient &sql_client)
{
  int ret = OB_SUCCESS;
  ObDMLSqlSplicer dml;
  if (common::OB_INVALID_ID == ai_column.table_id_
      || common::OB_INVALID_ID == ai_column.column_id_) {
    ret = OB_INVALID_ARGUMENT;
    SHARE_SCHEMA_LOG(WARN, "invalid ai column for persistence", K(ret), K(ai_column));
  }
  if (OB_SUCC(ret) && 0 == ai_column.definition_version_) {
    // First definition of this AI column.
    const_cast<ObAiColumnSchema &>(ai_column).definition_version_ = 1;
  }
  if (OB_FAIL(ret)) {
  } else if (OB_FAIL(splice_ai_column_insert(ai_column, false /*is_history*/,
                                             0 /*unused*/, dml))) {
    SHARE_SCHEMA_LOG(WARN, "failed to splice ai column row", K(ret));
  } else if (OB_FAIL(exec_insert(OB_ALL_AI_COLUMN_TNAME, dml, sql_client))) {
    SHARE_SCHEMA_LOG(WARN, "failed to insert ai column row", K(ret));
  } else if (OB_FAIL(splice_ai_column_insert(ai_column, true /*is_history*/,
                                             table_schema_version, dml))) {
    SHARE_SCHEMA_LOG(WARN, "failed to splice ai column history row", K(ret));
  } else if (OB_FAIL(exec_insert(OB_ALL_AI_COLUMN_HISTORY_TNAME, dml, sql_client))) {
    SHARE_SCHEMA_LOG(WARN, "failed to insert ai column history row", K(ret));
  } else {
    ObSchemaOperation opt;
    opt.table_id_ = table_id;
    opt.column_id_ = ai_column.column_id_;
    opt.op_type_ = OB_DDL_CREATE_AI_COLUMN;
    opt.schema_version_ = table_schema_version;
    opt.ddl_stmt_str_ = ddl_stmt;
    if (OB_FAIL(log_operation(opt, sql_client))) {
      SHARE_SCHEMA_LOG(WARN, "failed to log create ai column operation", K(ret));
    }
  }
  return ret;
}

int ObAiColumnSqlService::create_ai_columns(const ObTableSchema &table,
                                            common::ObISQLClient &sql_client)
{
  int ret = OB_SUCCESS;
  const common::ObIArray<ObAiColumnSchema> &ai_columns = table.get_ai_columns();
  if (ai_columns.count() <= 0) {
    // nothing to persist
  } else {
    const ObString ddl_stmt;
    for (int64_t i = 0; OB_SUCC(ret) && i < ai_columns.count(); ++i) {
      const ObAiColumnSchema &ai_column = ai_columns.at(i);
      if (OB_FAIL(create_ai_column(ai_column,
                                   table.get_table_id(),
                                   table.get_schema_version(),
                                   ddl_stmt,
                                   sql_client))) {
        SHARE_SCHEMA_LOG(WARN, "failed to persist ai column", K(ret),
                         K(i), K(table.get_table_id()));
      }
    }
  }
  return ret;
}

int ObAiColumnSqlService::drop_ai_columns(const uint64_t table_id,
                                          const int64_t new_schema_version,
                                          const common::ObString &ddl_stmt,
                                          common::ObISQLClient &sql_client)
{
  int ret = OB_SUCCESS;
  UNUSED(new_schema_version);
  UNUSED(ddl_stmt);
  ObSqlString buffer;
  int64_t affected_rows = 0;
  ObDMLSqlSplicer dml;
  if (common::OB_INVALID_ID == table_id) {
    ret = OB_INVALID_ARGUMENT;
  } else if (OB_FAIL(dml.add_uint64_pk_column("table_id", table_id))) {
  } else if (OB_FAIL(dml.splice_delete_sql(OB_ALL_AI_COLUMN_TNAME, buffer))) {
    SHARE_SCHEMA_LOG(WARN, "failed to splice ai column delete sql", K(ret));
  } else if (OB_FAIL(sql_client.write(buffer.ptr(), affected_rows))) {
    SHARE_SCHEMA_LOG(WARN, "failed to delete ai column rows", K(ret), K(table_id));
  }
  return ret;
}

} // namespace schema
} // namespace share
} // namespace oceanbase
