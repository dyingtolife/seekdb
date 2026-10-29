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

#include "share/schema/ob_ddl_sql_service.h"

namespace oceanbase
{
namespace share
{
namespace schema
{

class ObTableSchema;
class ObAiColumnSchema;

// Persistence helper for AI column metadata (__all_ai_column / history).
// Mirrors ObAiModelSqlService: ObDMLSqlSplicer based insert/delete plus a
// schema operation log entry. Called from the create-table helper and the
// alter-table add-column service path, inside the DDL transaction, so the
// metadata rows commit atomically with the table/column schema rows.
class ObAiColumnSqlService final : public ObDDLSqlService
{
public:
  ObAiColumnSqlService(ObSchemaService &schema_service)
    : ObDDLSqlService(schema_service)
  {}
  virtual ~ObAiColumnSqlService() = default;

  // Insert one __all_ai_column row (+ history row) per AI column of the
  // table. definition_version_ starts at 1. The schema_version used for the
  // history rowkey is read from the table schema.
  int create_ai_columns(const ObTableSchema &table,
                        common::ObISQLClient &sql_client);

  // Insert one __all_ai_column row for a single AI column entry (ALTER TABLE
  // ... ADD COLUMN with an AI COLUMN clause). table_schema_version is the new
  // schema version produced by the alter operation.
  int create_ai_column(const ObAiColumnSchema &ai_column,
                       const uint64_t table_id,
                       const int64_t table_schema_version,
                       const common::ObString &ddl_stmt,
                       common::ObISQLClient &sql_client);

  // Soft-delete all __all_ai_column rows of a table (DROP TABLE).
  int drop_ai_columns(const uint64_t table_id,
                      const int64_t new_schema_version,
                      const common::ObString &ddl_stmt,
                      common::ObISQLClient &sql_client);

private:
  int splice_ai_column_insert(const ObAiColumnSchema &ai_column,
                              const bool is_history,
                              const int64_t schema_version,
                              share::ObDMLSqlSplicer &dml);
  int exec_insert(const char *tname,
                  share::ObDMLSqlSplicer &dml,
                  common::ObISQLClient &sql_client);

  DISALLOW_COPY_AND_ASSIGN(ObAiColumnSqlService);
};

} // namespace schema
} // namespace share
} // namespace oceanbase
