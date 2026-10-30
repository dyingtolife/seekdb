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

// Parsed CSV cell reader over a memory buffer. Supports RFC4180 quoting,
// embedded newlines, escaped quotes and CRLF; a header row may be consumed
// separately. (Retained for unit tests and inference tooling.)
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

// ---------------------------------------------------------------------------
// Streaming file-scan layer (file-as-database execution)
// ---------------------------------------------------------------------------

// Source formats supported by the file-table execution path.
enum class ObFileSourceFormat : int8_t
{
  CSV = 0,
  JSONL,
  PARQUET,
  GLOB,
  INVALID,
};

// Named parameters accepted by the read_*() table functions. Only the fields
// relevant to a given format are honored; unknown names are rejected by the
// resolver.
struct ObFileParamSpec
{
  bool has_header_ = true;
  int64_t sample_size_ = -1;           // competition spec: only -1 supported
  bool header_set_ = false;
  bool sample_size_set_ = false;
  common::ObString delimiter_;         // CSV, default ","
  common::ObString quote_;             // CSV, default "\""
  common::ObString escape_;            // CSV, default "\""
  common::ObString nullstr_;           // CSV NULL text; empty = only empty cells
  common::ObString format_;            // read_json: "newline_delimited"
  int64_t max_file_size_ = 256LL * 1024 * 1024;  // resource cap
};

// File identity used for change detection between resolve / open / rescan.
struct ObFileSignature
{
  int64_t size_ = -1;
  int64_t mtime_sec_ = 0;
  int64_t mtime_nsec_ = 0;
  bool operator==(const ObFileSignature &o) const
  {
    return size_ == o.size_ && mtime_sec_ == o.mtime_sec_ && mtime_nsec_ == o.mtime_nsec_;
  }
  bool operator!=(const ObFileSignature &o) const { return !(*this == o); }
  bool is_valid() const { return size_ >= 0; }
  TO_STRING_KV(K_(size), K_(mtime_sec), K_(mtime_nsec));
};

// Stat a local file's signature. Returns OB_FILE_NOT_EXIST when missing.
int stat_file_signature(const common::ObString &path, ObFileSignature &sig);

// Row cursor abstraction. next_row() returns OB_ITER_END at end of input and
// OB_INVALID_DATA (with current_line() valid) for bad rows. Cell strings
// returned in `row` are views valid only until the following next_row() call.
class IFileRowCursor
{
public:
  virtual ~IFileRowCursor() = default;
  virtual int open() = 0;
  virtual int next_row(ObFileRow &row) = 0;
  virtual void close() = 0;
  virtual int64_t current_line() const = 0;
};

// Streaming RFC4180 CSV reader over a stdio FILE handle. The whole file is
// never materialized: a sliding window buffer is refilled as records are
// consumed; a single record may grow the window up to kMaxRecordBytes.
class ObCsvFileCursor : public IFileRowCursor
{
public:
  static constexpr int64_t kWindowBytes = 256 * 1024;
  static constexpr int64_t kMaxRecordBytes = 8 * 1024 * 1024;
  ObCsvFileCursor(const common::ObString &path, const ObFileParamSpec &params,
                  common::ObIAllocator &alloc);
  virtual ~ObCsvFileCursor() { close(); }
  virtual int open() override;
  virtual int next_row(ObFileRow &row) override;
  virtual void close() override;
  virtual int64_t current_line() const override { return line_no_; }
  const common::ObArray<common::ObString> &header() const { return header_; }
private:
  int ensure_window(int64_t need_from, int64_t &available);
  int parse_record(ObFileRow &row);
  int parse_quoted(ObFileRow &row, int64_t &end_pos);
  int read_header();
  common::ObString path_;
  const ObFileParamSpec &params_;
  common::ObIAllocator &alloc_;
  void *fp_ = nullptr;                 // FILE*
  char *buf_ = nullptr;
  int64_t buf_cap_ = 0;
  int64_t data_len_ = 0;               // valid bytes in buf_
  int64_t pos_ = 0;                    // parse offset within buf_
  bool eof_ = false;
  bool header_done_ = false;
  int64_t line_no_ = 1;
  char delim_ = ',';
  char quote_ = '"';
  char escape_ = '"';
  common::ObArray<common::ObString> header_;
};

// Streaming JSONL reader: one flat JSON object per line; field union is the
// caller's responsibility (rows carry key/value pairs).
class ObJsonlFileCursor : public IFileRowCursor
{
public:
  static constexpr int64_t kWindowBytes = 256 * 1024;
  static constexpr int64_t kMaxLineBytes = 8 * 1024 * 1024;
  ObJsonlFileCursor(const common::ObString &path, const ObFileParamSpec &params,
                    common::ObIAllocator &alloc);
  virtual ~ObJsonlFileCursor() { close(); }
  virtual int open() override;
  virtual int next_row(ObFileRow &row) override;
  virtual void close() override;
  virtual int64_t current_line() const override { return line_no_; }
private:
  int refill(bool &got_data);
  common::ObString path_;
  const ObFileParamSpec &params_;
  common::ObIAllocator &alloc_;
  void *fp_ = nullptr;
  char *buf_ = nullptr;
  int64_t buf_cap_ = 0;
  int64_t data_len_ = 0;
  int64_t pos_ = 0;
  bool eof_ = false;
  int64_t line_no_ = 1;
};

// Streaming glob: expands a pattern directory lazily, sorted by path, capped
// by kMaxMatches. Yields one cell (the matched path) per row.
class ObGlobCursor : public IFileRowCursor
{
public:
  static constexpr int64_t kMaxMatches = 100000;
  ObGlobCursor(const common::ObString &pattern, common::ObIAllocator &alloc);
  virtual ~ObGlobCursor() { close(); }
  virtual int open() override;
  virtual int next_row(ObFileRow &row) override;
  virtual void close() override;
  virtual int64_t current_line() const override { return 1; }
private:
  static int collect_recursive(const common::ObString &pattern, int64_t &count,
                               common::ObIAllocator &alloc,
                               common::ObArray<common::ObString> &out);
  common::ObString pattern_;
  common::ObIAllocator &alloc_;
  common::ObArray<common::ObString> matches_;
  int64_t idx_ = 0;
};

// Parquet cursor: reads the file into memory (footer + chunk random access
// require it; bounded by the resource cap), decodes row-group by row-group
// with column pruning (only `needed` columns are decoded).
class ObParquetFileCursor : public IFileRowCursor
{
public:
  ObParquetFileCursor(const common::ObString &path, const ObFileParamSpec &params,
                      const common::ObIArray<bool> &needed_cols,
                      common::ObIAllocator &alloc);
  virtual ~ObParquetFileCursor() { close(); }
  virtual int open() override;
  // next_row emits one cell per column of the file schema; pruned columns
  // yield empty cells (they must not appear in the projected schema).
  virtual int next_row(ObFileRow &row) override;
  virtual void close() override;
  virtual int64_t current_line() const override { return 1; }
  const common::ObArray<common::ObString> &column_names() const { return col_names_; }
private:
  int decode_group(int64_t group_idx);
  common::ObString path_;
  const ObFileParamSpec &params_;
  common::ObIAllocator &alloc_;
  common::ObArray<bool> needed_;
  char *file_buf_ = nullptr;
  int64_t file_len_ = 0;
  common::ObArray<common::ObString> col_names_;
  // decoded current row group: per column, value count + text forms
  struct GroupCols
  {
    common::ObArray<common::ObString> cells_;
  };
  common::ObArray<GroupCols> groups_;  // lazily decoded one at a time
  common::ObArray<GroupCols *> decoded_; // placeholder to keep layout simple
  int64_t cur_group_ = -1;
  int64_t cur_row_in_group_ = 0;
  int64_t group_row_cnt_ = 0;
  GroupCols cur_cells_;
  bool group_done_ = true;
};

// Incremental deterministic schema inference. Feed rows in file order; the
// widening ladder is: BOOLEAN < BIGINT < DOUBLE, DATE < DATETIME, and any
// conflict or unrecognized text widens to VARCHAR. Cells equal to the NULL
// text ("NULL", "\\N" or the configured nullstr) or empty mark nullability.
class ObFileSchemaInferer
{
public:
  void init(int64_t col_cnt);
  int feed_row(const ObFileRow &row, const ObFileParamSpec &params);
  int finalize(common::ObIAllocator &allocator, ObFileSchema &schema);
  // JSONL has no header: the column list is the union of the object fields in
  // first-seen order, so it can only be built while streaming the whole input.
  static int infer_jsonl_schema(IFileRowCursor &cursor,
                                const ObFileParamSpec &params,
                                common::ObIAllocator &allocator,
                                ObFileSchema &schema,
                                int64_t &row_cnt);
  static ObFileColumnType classify(const common::ObString &cell);
  static ObFileColumnType widen(ObFileColumnType cur, ObFileColumnType cand);
private:
  static bool is_bool_text(const common::ObString &s);
  static bool is_bigint_text(const common::ObString &s);
  static bool is_double_text(const common::ObString &s);
  static bool is_date_text(const common::ObString &s);
  static bool is_datetime_text(const common::ObString &s);
  common::ObArray<ObFileColumnType> types_;
  common::ObArray<bool> nullables_;
  int64_t col_cnt_ = 0;
};

// Row-batch cell -> typed ObObj conversion shared by resolver and operator.
int file_cell_to_obj(ObFileColumnType type, const common::ObString &raw,
                     bool is_null, common::ObIAllocator &allocator,
                     common::ObObjParam &out);

// Factory: build the streaming cursor for a format. `needed_cols` is used by
// the parquet format only. The returned cursor is owned by the caller and
// allocated from `allocator` (never freed by the factory).
int make_file_row_cursor(ObFileSourceFormat fmt,
                         const common::ObString &path,
                         const ObFileParamSpec &params,
                         const common::ObIArray<bool> &needed_cols,
                         common::ObIAllocator &allocator,
                         IFileRowCursor *&cursor);

}  // namespace sql
}  // namespace oceanbase
