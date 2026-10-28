#pragma once
/*
 * Copyright (c) 2025 OceanBase.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

#include "lib/allocator/ob_allocator.h"
#include "lib/container/ob_array.h"
#include "lib/string/ob_string.h"

namespace oceanbase
{
namespace sql
{

// Column data type supported by the deterministic file schema inference.
enum class ObFileColumnType : int8_t
{
  NIL = 0,      // every sampled value is NULL
  BOOLEAN,
  BIGINT,
  DOUBLE,
  VARCHAR,
  DATE,
  DATETIME,
  UNKNOWN,
};

const char *file_column_type_str(ObFileColumnType t);

// One row of raw (untyped) cell text.
struct ObFileRow
{
  common::ObArray<common::ObString> cells_;
};

// Result of schema inference over sampled rows.
struct ObFileSchema
{
  struct Column
  {
    common::ObString name_;
    ObFileColumnType type_ = ObFileColumnType::NIL;
    bool nullable_ = false;
  };
  common::ObArray<Column> columns_;
};

// Parsed CSV cell reader. Supports RFC4180 quoting, embedded newlines,
// escaped quotes and CRLF; a header row may be consumed separately.
class ObCsvCursor
{
public:
  ObCsvCursor(const char *buf, int64_t len, bool has_header);
  // Advances to the next record. Returns OB_ITER_END at EOF.
  int next_row(ObFileRow &row);
  // Header names (only valid after construction when has_header is set).
  const common::ObArray<common::ObString> &header() const { return header_; }
  // Diagnostic info for a bad row.
  int64_t current_line() const { return line_no_; }
private:
  int parse_quoted_field(ObFileRow &row);
  const char *buf_;
  int64_t len_;
  int64_t pos_ = 0;
  int64_t line_no_ = 1;
  common::ObArray<common::ObString> header_;
};

// JSONL (newline-delimited JSON) cursor. Only flat objects are accepted;
// nested objects/arrays are rejected per the competition spec. Cell text is
// copied onto `alloc` so views outlive the cursor.
class ObJsonlCursor
{
public:
  ObJsonlCursor(const char *buf, int64_t len, common::ObIAllocator &alloc);
  int next_row(ObFileRow &row);
private:
  const char *buf_;
  int64_t len_;
  int64_t pos_ = 0;
  int64_t line_no_ = 1;
  common::ObIAllocator &alloc_;
};

// Deterministic type inference over sampled rows.
int infer_file_schema(const common::ObArray<common::ObString> &header,
                      const common::ObArray<ObFileRow> &rows,
                      common::ObIAllocator &allocator,
                      ObFileSchema &schema);

// Render a typed cell as printable SQL text (for debugging / COPY export).
int file_cell_to_text(ObFileColumnType type, const common::ObString &raw,
                      bool is_null, common::ObIAllocator &allocator,
                      common::ObString &out);

}  // namespace sql
}  // namespace oceanbase
