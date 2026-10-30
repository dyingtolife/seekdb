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

#define USING_LOG_PREFIX SQL_ENG

#include "ob_file_scan_utils.h"
#include <algorithm>
#include <cstdio>
#include <cstring>
#include <cstdlib>
#include <dirent.h>
#include <sys/stat.h>
#include <sys/types.h>
#include "lib/string/ob_string_buffer.h"
#include "lib/string/ob_sql_string.h"
#include "common/object/ob_object.h"
#include "common/json_type/ob_json_common.h"
#include "common/json_type/ob_json_base.h"
#include "common/json_type/ob_json_parse.h"
#include "common/json_type/ob_json_tree.h"
#include "lib/utility/ob_mod_define.h"
#include "lib/allocator/ob_malloc.h"
#include "lib/utility/ob_sort.h"
#include "share/ob_define.h"
#include "common/timezone/ob_time_convert.h"
#include "sql/engine/file/ob_parquet_reader.h"

namespace oceanbase
{
namespace sql
{

const char *file_column_type_str(ObFileColumnType t)
{
  switch (t) {
    case ObFileColumnType::BOOLEAN: return "BOOLEAN";
    case ObFileColumnType::BIGINT: return "BIGINT";
    case ObFileColumnType::DOUBLE: return "DOUBLE";
    case ObFileColumnType::VARCHAR: return "VARCHAR";
    case ObFileColumnType::DATE: return "DATE";
    case ObFileColumnType::DATETIME: return "DATETIME";
    default: return "NULL";
  }
}

// ---------------- CSV ----------------

ObCsvCursor::ObCsvCursor(const char *buf, int64_t len, bool has_header)
    : buf_(buf), len_(len)
{
  if (has_header && buf != nullptr && len > 0) {
    ObFileRow header_row;
    if (OB_SUCCESS == next_row(header_row)) {
      for (int64_t i = 0; i < header_row.cells_.count(); ++i) {
        header_.push_back(header_row.cells_.at(i));
      }
    }
  }
}

int ObCsvCursor::parse_quoted_field(ObFileRow &row)
{
  int ret = OB_SUCCESS;
  // buf_[pos_] points at the opening quote.
  ++pos_;
  common::ObStringBuffer sb;
  bool closed = false;
  while (pos_ < len_ && OB_SUCC(ret)) {
    const char c = buf_[pos_];
    if (c == '"') {
      if (pos_ + 1 < len_ && buf_[pos_ + 1] == '"') {
        // escaped quote
        if (OB_FAIL(sb.append("\"", 1))) {
        } else {
          pos_ += 2;
        }
      } else {
        ++pos_;
        closed = true;
        break;
      }
    } else {
      if (c == '\n') {
        ++line_no_;
      }
      if (OB_FAIL(sb.append(&c, 1))) {
      } else {
        ++pos_;
      }
    }
  }
  if (OB_FAIL(ret)) {
  } else if (!closed) {
    ret = OB_ERR_UNEXPECTED;  // unterminated quote
  } else {
    // consume separator
    if (pos_ < len_ && buf_[pos_] == ',') {
      ++pos_;
    }
    common::ObString cell(static_cast<int32_t>(sb.length()), sb.ptr());
    ret = row.cells_.push_back(cell);
  }
  return ret;
}

int ObCsvCursor::next_row(ObFileRow &row)
{
  int ret = OB_SUCCESS;
  row.cells_.reset();
  if (pos_ >= len_) {
    return OB_ITER_END;
  }
  // skip leading CR/LF of empty lines
  while (pos_ < len_ && (buf_[pos_] == '\n' || buf_[pos_] == '\r')) {
    if (buf_[pos_] == '\n') { ++line_no_; }
    ++pos_;
  }
  if (pos_ >= len_) {
    return OB_ITER_END;
  }
  bool row_done = false;
  while (!row_done && OB_SUCC(ret)) {
    if (pos_ < len_ && buf_[pos_] == '"') {
      if (OB_FAIL(parse_quoted_field(row))) {
      }
    } else {
      int64_t start = pos_;
      while (pos_ < len_ && buf_[pos_] != ',' && buf_[pos_] != '\n') {
        ++pos_;
      }
      int64_t end = pos_;
      // trim trailing CR
      if (end > start && buf_[end - 1] == '\r') {
        --end;
      }
      common::ObString cell(static_cast<int32_t>(end - start), buf_ + start);
      if (OB_FAIL(row.cells_.push_back(cell))) {
      }
      if (pos_ < len_ && buf_[pos_] == ',') {
        ++pos_;
      }
    }
    // a field ended at '\n' (or EOF) terminates the record
    if (pos_ >= len_ || buf_[pos_] == '\n') {
      if (pos_ < len_ && buf_[pos_] == '\n') {
        ++pos_;
        ++line_no_;
      }
      row_done = true;
    }
  }
  return ret;
}

// ---------------- JSONL ----------------

ObJsonlCursor::ObJsonlCursor(const char *buf, int64_t len, common::ObIAllocator &alloc)
    : buf_(buf), len_(len), alloc_(alloc) {}

int ObJsonlCursor::next_row(ObFileRow &row)
{
  int ret = OB_SUCCESS;
  row.cells_.reset();
  // skip blank lines
  while (pos_ < len_) {
    while (pos_ < len_ && (buf_[pos_] == '\n' || buf_[pos_] == '\r')) {
      if (buf_[pos_] == '\n') { ++line_no_; }
      ++pos_;
    }
    if (pos_ < len_ && (buf_[pos_] == ' ' || buf_[pos_] == '\t')) {
      ++pos_;
      continue;
    }
    break;
  }
  if (pos_ >= len_) {
    return OB_ITER_END;
  }
  // one JSON object per line
  int64_t start = pos_;
  while (pos_ < len_ && buf_[pos_] != '\n') { ++pos_; }
  int64_t end = pos_;
  if (end > start && buf_[end - 1] == '\r') { --end; }
  common::ObString line(static_cast<int32_t>(end - start), buf_ + start);
  if (line.empty()) {
    // recursively read the next non-empty line
    return next_row(row);
  }
  // parse to flat object and serialize each top-level member textually
  ObArenaAllocator tmp_alloc("JsonlRow");
  ObJsonNode *j_tree = nullptr;
  if (OB_FAIL(ObJsonParser::get_tree(&tmp_alloc, line, j_tree))) {
  } else if (OB_ISNULL(j_tree) || j_tree->json_type() != ObJsonNodeType::J_OBJECT) {
    ret = OB_INVALID_DATA;  // only flat objects are supported
  } else {
    ObJsonObject *obj = static_cast<ObJsonObject *>(j_tree);
    for (int64_t i = 0; OB_SUCC(ret) && i < obj->element_count(); ++i) {
      common::ObString key;
      ObIJsonBase *value = nullptr;
      if (OB_FAIL(obj->get_object_value(static_cast<uint64_t>(i), key, value))) {
      } else if (value->json_type() == ObJsonNodeType::J_OBJECT
                 || value->json_type() == ObJsonNodeType::J_ARRAY) {
        ret = OB_INVALID_DATA;  // nested structures are rejected
      } else {
        // copy key and value text onto the caller allocator
        const int64_t klen = key.length();
        const common::ObString vtext(value->get_data_length(), value->get_data());
        char *kbuf = static_cast<char *>(alloc_.alloc(klen + 1));
        char *vbuf = static_cast<char *>(alloc_.alloc(vtext.length() + 1));
        if (OB_ISNULL(kbuf) || OB_ISNULL(vbuf)) {
          ret = OB_ALLOCATE_MEMORY_FAILED;
        } else {
          MEMCPY(kbuf, key.ptr(), klen); kbuf[klen] = '\0';
          MEMCPY(vbuf, vtext.ptr(), vtext.length()); vbuf[vtext.length()] = '\0';
          common::ObString kview(static_cast<int32_t>(klen), kbuf);
          common::ObString vview(static_cast<int32_t>(vtext.length()), vbuf);
          if (OB_FAIL(row.cells_.push_back(kview))) {
          } else if (OB_FAIL(row.cells_.push_back(vview))) {
          }
        }
      }
    }
  }
  return ret;
}

// ---------------- type inference ----------------

static bool is_bool_text(const common::ObString &s)
{
  return s.case_compare("true") == 0 || s.case_compare("false") == 0;
}

static bool is_bigint_text(const common::ObString &s)
{
  if (s.empty()) return false;
  int64_t i = (s[0] == '-' || s[0] == '+') ? 1 : 0;
  if (i >= s.length()) return false;
  for (; i < s.length(); ++i) {
    if (s[i] < '0' || s[i] > '9') return false;
  }
  return true;
}

static bool is_double_text(const common::ObString &s)
{
  if (s.empty()) return false;
  int64_t i = 0;
  if (s[0] == '-' || s[0] == '+') i = 1;
  bool dot = false, digit = false;
  for (; i < s.length(); ++i) {
    const char c = s[i];
    if (c >= '0' && c <= '9') { digit = true; }
    else if (c == '.' && !dot) { dot = true; }
    else if ((c == 'e' || c == 'E') && digit) {
      // optional sign then digits
      ++i;
      if (i < s.length() && (s[i] == '-' || s[i] == '+')) ++i;
      bool ed = false;
      for (; i < s.length(); ++i) {
        if (s[i] < '0' || s[i] > '9') return false;
        ed = true;
      }
      return ed;
    } else { return false; }
  }
  return digit && dot;
}

// strict ISO date: YYYY-MM-DD
static bool is_date_text(const common::ObString &s)
{
  if (s.length() != 10 || s[4] != '-' || s[7] != '-') return false;
  for (int64_t i = 0; i < 10; ++i) {
    if (i == 4 || i == 7) continue;
    if (s[i] < '0' || s[i] > '9') return false;
  }
  const int mon = (s[5] - '0') * 10 + (s[6] - '0');
  const int day = (s[8] - '0') * 10 + (s[9] - '0');
  return mon >= 1 && mon <= 12 && day >= 1 && day <= 31;
}

// ISO datetime: YYYY-MM-DD[ T]HH:MM:SS[.frac]
static bool is_datetime_text(const common::ObString &s)
{
  if (s.length() < 19) return false;
  common::ObString date_part(10, s.ptr());
  const char sep = s[10];
  if (sep != ' ' && sep != 'T') return false;
  const common::ObString time_part(static_cast<int32_t>(s.length() - 11), s.ptr() + 11);
  if (!is_date_text(date_part)) return false;
  // HH:MM:SS
  if (time_part.length() < 8) return false;
  if (time_part[2] != ':' || time_part[5] != ':') return false;
  for (int64_t i = 0; i < 8; ++i) {
    if (i == 2 || i == 5) continue;
    if (time_part[i] < '0' || time_part[i] > '9') return false;
  }
  if (time_part.length() > 8 && time_part[8] != '.') return false;
  for (int64_t i = 9; i < time_part.length(); ++i) {
    if (time_part[i] < '0' || time_part[i] > '9') return false;
  }
  return true;
}

int infer_file_schema(const common::ObArray<common::ObString> &header,
                      const common::ObArray<ObFileRow> &rows,
                      common::ObIAllocator &allocator,
                      ObFileSchema &schema)
{
  int ret = OB_SUCCESS;
  schema.columns_.reset();
  const int64_t col_cnt = header.count();
  // track per-column candidate type; widen as we scan
  ObArray<ObFileColumnType> types;
  ObArray<bool> nullables;
  for (int64_t c = 0; OB_SUCC(ret) && c < col_cnt; ++c) {
    types.push_back(ObFileColumnType::NIL);
    nullables.push_back(false);
  }
  for (int64_t r = 0; OB_SUCC(ret) && r < rows.count(); ++r) {
    const ObFileRow &row = rows.at(r);
    for (int64_t c = 0; OB_SUCC(ret) && c < col_cnt && c < row.cells_.count(); ++c) {
      const common::ObString &cell = row.cells_.at(c);
      if (cell.empty()) {
        nullables.at(c) = true;
        continue;
      }
      const ObFileColumnType cur = types.at(c);
      if (cell == "NULL" || cell == "\\N") {
        nullables.at(c) = true;
        continue;
      }
      if (cur == ObFileColumnType::VARCHAR) {
        continue;  // widest already
      }
      ObFileColumnType cand;
      if (is_bool_text(cell)) cand = ObFileColumnType::BOOLEAN;
      else if (is_bigint_text(cell)) cand = ObFileColumnType::BIGINT;
      else if (is_double_text(cell)) cand = ObFileColumnType::DOUBLE;
      else if (is_datetime_text(cell)) cand = ObFileColumnType::DATETIME;
      else if (is_date_text(cell)) cand = ObFileColumnType::DATE;
      else cand = ObFileColumnType::VARCHAR;
      // widen: NIL takes candidate; otherwise only stronger-or-equal fits
      ObFileColumnType merged = cur;
      if (cur == ObFileColumnType::NIL) {
        merged = cand;
      } else if (cur != cand) {
        // widening ladder: bool->bigint->double; date loses to datetime and to varchar
        if (cur == ObFileColumnType::BOOLEAN && cand == ObFileColumnType::BIGINT) {
          merged = ObFileColumnType::BIGINT;
        } else if (cur == ObFileColumnType::BOOLEAN && cand == ObFileColumnType::DOUBLE) {
          merged = ObFileColumnType::DOUBLE;
        } else if (cur == ObFileColumnType::BIGINT
                   && (cand == ObFileColumnType::BOOLEAN || cand == ObFileColumnType::DOUBLE)) {
          merged = (cand == ObFileColumnType::BOOLEAN) ? ObFileColumnType::BIGINT : ObFileColumnType::DOUBLE;
        } else if (cur == ObFileColumnType::DATE && cand == ObFileColumnType::DATETIME) {
          merged = ObFileColumnType::DATETIME;
        } else {
          merged = ObFileColumnType::VARCHAR;
        }
      }
      types.at(c) = merged;
    }
    // cells beyond header imply a bad row; keep them ignored here
  }
  for (int64_t c = 0; OB_SUCC(ret) && c < col_cnt; ++c) {
    ObFileSchema::Column col;
    common::ObString name = header.at(c);
    char *name_buf = static_cast<char *>(allocator.alloc(name.length() + 1));
    if (OB_ISNULL(name_buf)) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
    } else {
      MEMCPY(name_buf, name.ptr(), name.length());
      name_buf[name.length()] = '\0';
      col.name_.assign_ptr(name_buf, static_cast<int32_t>(name.length()));
      col.type_ = types.at(c);
      col.nullable_ = nullables.at(c);
      ret = schema.columns_.push_back(col);
    }
  }
  return ret;
}

int file_cell_to_text(ObFileColumnType type, const common::ObString &raw,
                      bool is_null, common::ObIAllocator &allocator,
                      common::ObString &out)
{
  int ret = OB_SUCCESS;
  if (is_null || raw.empty()) {
    out.assign_ptr(const_cast<char *>("NULL"), 4);
    return ret;
  }
  // only BOOLEAN needs text normalization ("true" -> 1)
  if (type == ObFileColumnType::BOOLEAN) {
    const char *v = (raw.case_compare("true") == 0) ? "1" : "0";
    out.assign_ptr(const_cast<char *>(v), 1);
    return ret;
  }
  if (type == ObFileColumnType::DATE && raw.length() == 10) {
    char *buf = static_cast<char *>(allocator.alloc(10));
    if (OB_ISNULL(buf)) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
    } else {
      MEMCPY(buf, raw.ptr(), 10);
      out.assign_ptr(buf, 10);
    }
    return ret;
  }
  if (type == ObFileColumnType::DATETIME && raw.length() >= 19 && raw[10] == 'T') {
    char *buf = static_cast<char *>(allocator.alloc(raw.length()));
    if (OB_ISNULL(buf)) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
    } else {
      MEMCPY(buf, raw.ptr(), raw.length());
      buf[10] = ' ';  // normalize ISO 'T' separator to space
      out.assign_ptr(buf, static_cast<int32_t>(raw.length()));
    }
    return ret;
  }
  char *buf = static_cast<char *>(allocator.alloc(raw.length() + 1));
  if (OB_ISNULL(buf)) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
  } else {
    MEMCPY(buf, raw.ptr(), raw.length());
    buf[raw.length()] = '\0';
    out.assign_ptr(buf, static_cast<int32_t>(raw.length()));
  }
  return ret;
}

// ---------------------------------------------------------------------------
// Streaming file-scan layer
// ---------------------------------------------------------------------------

int stat_file_signature(const common::ObString &path, ObFileSignature &sig)
{
  int ret = OB_SUCCESS;
  char path_buf[OB_MAX_FILE_NAME_LENGTH];
  if (path.empty() || path.length() >= static_cast<int32_t>(sizeof(path_buf))) {
    ret = OB_INVALID_ARGUMENT;
  } else {
    MEMCPY(path_buf, path.ptr(), path.length());
    path_buf[path.length()] = '\0';
    struct stat st;
    if (0 != ::stat(path_buf, &st)) {
      ret = OB_FILE_NOT_EXIST;
    } else if (!S_ISREG(st.st_mode)) {
      ret = OB_INVALID_ARGUMENT;
    } else {
      sig.size_ = static_cast<int64_t>(st.st_size);
      sig.mtime_sec_ = static_cast<int64_t>(st.st_mtim.tv_sec);
      sig.mtime_nsec_ = static_cast<int64_t>(st.st_mtim.tv_nsec);
    }
  }
  return ret;
}

// ---------------- CSV streaming ----------------

ObCsvFileCursor::ObCsvFileCursor(const common::ObString &path, const ObFileParamSpec &params,
                                 common::ObIAllocator &alloc)
    : path_(path), params_(params), alloc_(alloc)
{
  delim_ = params.delimiter_.empty() ? ',' : params.delimiter_.ptr()[0];
  quote_ = params.quote_.empty() ? '"' : params.quote_.ptr()[0];
  escape_ = params.escape_.empty() ? '"' : params.escape_.ptr()[0];
}

int ObCsvFileCursor::open()
{
  int ret = OB_SUCCESS;
  char path_buf[OB_MAX_FILE_NAME_LENGTH];
  if (NULL != fp_) {
    // already open
  } else if (path_.empty() || path_.length() >= static_cast<int32_t>(sizeof(path_buf))) {
    ret = OB_INVALID_ARGUMENT;
  } else {
    MEMCPY(path_buf, path_.ptr(), path_.length());
    path_buf[path_.length()] = '\0';
    fp_ = fopen(path_buf, "rb");
    if (OB_ISNULL(fp_)) {
      ret = OB_INVALID_ARGUMENT;
    } else if (OB_ISNULL(buf_ = static_cast<char *>(ob_malloc(kWindowBytes,
                   common::ObModIds::OB_SQL_CHUNK_ROW_STORE)))) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
    } else {
      buf_cap_ = kWindowBytes;
      data_len_ = 0;
      pos_ = 0;
      eof_ = false;
      header_done_ = true;  // next_row() guards on this; read_header() marks the first record as header below
      line_no_ = 1;
      if (OB_FAIL(read_header())) {
      }
    }
  }
  return ret;
}

void ObCsvFileCursor::close()
{
  if (NULL != fp_) {
    fclose(static_cast<FILE *>(fp_));
    fp_ = NULL;
  }
  if (NULL != buf_) {
    ob_free(buf_);
    buf_ = NULL;
  }
  buf_cap_ = 0;
  data_len_ = 0;
  pos_ = 0;
  eof_ = false;
}

int ObCsvFileCursor::read_header()
{
  int ret = OB_SUCCESS;
  ObFileRow header_row;
  int gr = next_row(header_row);
  if (OB_ITER_END == gr) {
    // empty file
    ret = OB_SUCCESS;
  } else if (OB_SUCCESS != gr) {
    ret = (OB_INVALID_DATA == gr) ? OB_INVALID_DATA : gr;
  } else {
    for (int64_t i = 0; OB_SUCC(ret) && i < header_row.cells_.count(); ++i) {
      const ObString &h = header_row.cells_.at(i);
      char *nb = static_cast<char *>(alloc_.alloc(h.length() + 1));
      if (OB_ISNULL(nb)) {
        ret = OB_ALLOCATE_MEMORY_FAILED;
      } else if (h.length() > 0) {
        MEMCPY(nb, h.ptr(), h.length());
      }
      if (OB_SUCC(ret)) {
        nb[h.length()] = '\0';
        ObString name(static_cast<int32_t>(h.length()), nb);
        if (OB_FAIL(header_.push_back(name))) {}
      }
    }
  }
  return ret;
}

// Ensure at least one record is fully inside the window starting at pos_.
// Grows the buffer when a single record exceeds the current capacity.
int ObCsvFileCursor::ensure_window(int64_t need_from, int64_t &available)
{
  int ret = OB_SUCCESS;
  available = 0;
  // scan for record end: returns true when a complete record ends at out
  auto record_complete = [&](int64_t from, int64_t &end, bool &complete) -> int {
    int r = OB_SUCCESS;
    int64_t i = from;
    bool in_quotes = false;
    complete = false;
    end = from;
    while (i < data_len_) {
      const char c = buf_[i];
      if (in_quotes) {
        if (c == escape_ && escape_ != quote_ && i + 1 < data_len_) {
          i += 2;  // escaped char (incl. newline) stays in record
          continue;
        }
        if (c == quote_) {
          if (i + 1 < data_len_ && buf_[i + 1] == quote_) {
            i += 2;
            continue;
          }
          in_quotes = false;
          ++i;
          continue;
        }
        ++i;
      } else {
        if (c == quote_) {
          in_quotes = true;
          ++i;
        } else if (c == '\n') {
          end = i + 1;
          complete = true;
          break;
        } else {
          ++i;
        }
      }
    }
    if (!complete && i >= data_len_ && eof_) {
      end = data_len_;
      complete = true;  // final unterminated record (tolerated at EOF)
    }
    return r;
  };

  int64_t end = 0;
  bool complete = false;
  while (OB_SUCC(ret)) {
    if (OB_FAIL(record_complete(pos_, end, complete))) {
    } else if (complete) {
      available = end;
      break;
    } else if (eof_) {
      available = data_len_;
      break;
    } else {
      // need more data: compact + refill
      if (pos_ > 0) {
        MEMMOVE(buf_, buf_ + pos_, data_len_ - pos_);
        data_len_ -= pos_;
        pos_ = 0;
      }
      if (data_len_ + 1 >= buf_cap_) {
        if (buf_cap_ >= kMaxRecordBytes) {
          ret = OB_SIZE_OVERFLOW;
          LOG_USER_ERROR(OB_SIZE_OVERFLOW, "csv record exceeds size limit");
          break;
        }
        const int64_t new_cap = std::min(buf_cap_ * 2, kMaxRecordBytes);
        char *nb = static_cast<char *>(ob_malloc(new_cap,
            common::ObModIds::OB_SQL_CHUNK_ROW_STORE));
        if (OB_ISNULL(nb)) {
          ret = OB_ALLOCATE_MEMORY_FAILED;
          break;
        }
        MEMCPY(nb, buf_, data_len_);
        ob_free(buf_);
        buf_ = nb;
        buf_cap_ = new_cap;
      }
      const size_t got = fread(buf_ + data_len_, 1, buf_cap_ - data_len_ - 1,
                               static_cast<FILE *>(fp_));
      if (got == 0) {
        if (feof(static_cast<FILE *>(fp_))) {
          eof_ = true;
        } else {
          ret = OB_IO_ERROR;
          break;
        }
      } else {
        data_len_ += static_cast<int64_t>(got);
      }
    }
  }
  return ret;
}

int ObCsvFileCursor::parse_quoted(ObFileRow &row, int64_t &end_pos)
{
  int ret = OB_SUCCESS;
  // buf_[pos_] is the opening quote
  ++pos_;
  // The decoded text is built into an arena-backed buffer. ObStringBuffer's
  // default constructor leaves allocator_ NULL, and every append() then fails
  // with OB_ERR_NULL_VALUE (-4152): the allocator must be supplied explicitly.
  common::ObStringBuffer sb(&alloc_);
  bool closed = false;
  while (pos_ < data_len_ && OB_SUCC(ret)) {
    const char c = buf_[pos_];
    if (c == escape_ && escape_ != quote_ && pos_ + 1 < data_len_) {
      if (OB_FAIL(sb.append(buf_ + pos_ + 1, 1))) {}
      else { pos_ += 2; }
    } else if (c == quote_) {
      if (pos_ + 1 < data_len_ && buf_[pos_ + 1] == quote_) {
        if (OB_FAIL(sb.append("\"", 1))) {}
        else { pos_ += 2; }
      } else {
        ++pos_;
        closed = true;
        break;
      }
    } else if (c == '\r' && pos_ + 1 < data_len_ && buf_[pos_ + 1] == '\n') {
      // CRLF inside a quoted field is a line break: normalize it to LF so the
      // value does not depend on the writer's line-ending convention.
      ++pos_;
    } else {
      if (c == '\n') { ++line_no_; }
      if (OB_FAIL(sb.append(&c, 1))) {}
      else { ++pos_; }
    }
  }
  common::ObString decoded;
  if (OB_FAIL(ret)) {
  } else if (!closed) {
    ret = OB_INVALID_DATA;  // unterminated quote
  } else if (OB_FAIL(sb.get_result_string(decoded))) {
    // transfers the arena block to `decoded` (kept alive by alloc_)
  } else if (OB_FAIL(row.cells_.push_back(common::ObString(
                 static_cast<int32_t>(decoded.length()), decoded.ptr())))) {
  }
  // A delimiter following the closing quote is consumed by parse_record, so
  // that a trailing empty field is emitted in exactly one place.
  end_pos = pos_;
  return ret;
}

int ObCsvFileCursor::parse_record(ObFileRow &row)
{
  int ret = OB_SUCCESS;
  row.cells_.reset();
  bool row_done = false;
  while (!row_done && OB_SUCC(ret)) {
    // set when the field just parsed was terminated by the delimiter: the
    // record then still owns one more (possibly empty) trailing field
    bool after_delim = false;
    if (pos_ < data_len_ && buf_[pos_] == quote_) {
      int64_t end_pos = 0;
      if (OB_FAIL(parse_quoted(row, end_pos))) {
      } else {
        pos_ = end_pos;
        if (pos_ < data_len_ && buf_[pos_] == delim_) {
          ++pos_;
          after_delim = true;
        }
      }
    } else {
      int64_t start = pos_;
      while (pos_ < data_len_ && buf_[pos_] != delim_ && buf_[pos_] != '\n') {
        ++pos_;
      }
      int64_t end = pos_;
      // trim trailing CR (CRLF handling)
      if (end > start && buf_[end - 1] == '\r') { --end; }
      if (OB_FAIL(row.cells_.push_back(common::ObString(
              static_cast<int32_t>(end - start), buf_ + start)))) {}
      else if (pos_ < data_len_ && buf_[pos_] == delim_) {
        ++pos_;
        after_delim = true;
      }
    }
    // end of record: end of window (last record without newline), LF, or CRLF
    bool at_eol = false;
    if (OB_FAIL(ret)) {
    } else if (pos_ >= data_len_) {
      at_eol = true;
    } else if (buf_[pos_] == '\n') {
      at_eol = true;
    } else if (buf_[pos_] == '\r'
               && (pos_ + 1 >= data_len_ || buf_[pos_ + 1] == '\n')) {
      at_eol = true;
    }
    if (at_eol) {
      // "a,b," is a three-field record: the delimiter opened a final empty field
      if (after_delim
          && OB_FAIL(row.cells_.push_back(common::ObString(
                 0, buf_ + pos_)))) {
      } else {
        if (pos_ < data_len_ && buf_[pos_] == '\r') { ++pos_; }
        if (pos_ < data_len_ && buf_[pos_] == '\n') {
          ++pos_;
          ++line_no_;
        }
        row_done = true;
      }
    }
  }
  return ret;
}

int ObCsvFileCursor::next_row(ObFileRow &row)
{
  int ret = OB_SUCCESS;
  int64_t available = 0;
  if (NULL == fp_) {
    ret = OB_NOT_INIT;
  } else if (!header_done_) {
    // header handled in open(); should not happen
    ret = OB_ERR_UNEXPECTED;
  } else if (OB_FAIL(ensure_window(pos_, available))) {
  } else {
    // skip leading CR/LF of blank lines
    while (pos_ < available && (buf_[pos_] == '\n' || buf_[pos_] == '\r')) {
      if (buf_[pos_] == '\n') { ++line_no_; }
      ++pos_;
    }
    if (pos_ >= available && eof_) {
      ret = OB_ITER_END;
    } else if (pos_ >= available) {
      ret = OB_ITER_END;  // incomplete tail without newline and not at EOF:
                          // treat as end (defensive; ensure_window handles it)
    } else if (OB_FAIL(parse_record(row))) {
      if (OB_INVALID_DATA == ret) {
        LOG_USER_ERROR(OB_INVALID_DATA, "bad csv row, at line %ld", line_no_);
      }
    } else {
      // normalize NULL text
      const ObString &ns = params_.nullstr_;
      for (int64_t i = 0; i < row.cells_.count(); ++i) {
        ObString &c = row.cells_.at(i);
        if (c.length() == 4 && c.case_compare("NULL") == 0 && ns.empty()) {
          c.assign_ptr("", 0);
        } else if (c.length() == 2 && c == "\\N" && ns.empty()) {
          c.assign_ptr("", 0);
        } else if (!ns.empty() && c.length() == ns.length()
                   && c.case_compare(ns) == 0) {
          c.assign_ptr("", 0);
        }
      }
    }
  }
  return ret;
}

// ---------------- JSONL streaming ----------------

ObJsonlFileCursor::ObJsonlFileCursor(const common::ObString &path, const ObFileParamSpec &params,
                                     common::ObIAllocator &alloc)
    : path_(path), params_(params), alloc_(alloc) {}

int ObJsonlFileCursor::open()
{
  int ret = OB_SUCCESS;
  char path_buf[OB_MAX_FILE_NAME_LENGTH];
  if (NULL != fp_) {
    // already open
  } else if (path_.empty() || path_.length() >= static_cast<int32_t>(sizeof(path_buf))) {
    ret = OB_INVALID_ARGUMENT;
  } else {
    MEMCPY(path_buf, path_.ptr(), path_.length());
    path_buf[path_.length()] = '\0';
    fp_ = fopen(path_buf, "rb");
    if (OB_ISNULL(fp_)) {
      ret = OB_INVALID_ARGUMENT;
    } else if (OB_ISNULL(buf_ = static_cast<char *>(ob_malloc(kWindowBytes,
                   common::ObModIds::OB_SQL_CHUNK_ROW_STORE)))) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
    } else {
      buf_cap_ = kWindowBytes;
      data_len_ = 0;
      pos_ = 0;
      eof_ = false;
      line_no_ = 1;
    }
  }
  return ret;
}

void ObJsonlFileCursor::close()
{
  if (NULL != fp_) {
    fclose(static_cast<FILE *>(fp_));
    fp_ = NULL;
  }
  if (NULL != buf_) {
    ob_free(buf_);
    buf_ = NULL;
  }
  buf_cap_ = 0;
  data_len_ = 0;
  pos_ = 0;
  eof_ = false;
}

int ObJsonlFileCursor::refill(bool &got_data)
{
  int ret = OB_SUCCESS;
  got_data = false;
  if (pos_ > 0) {
    MEMMOVE(buf_, buf_ + pos_, data_len_ - pos_);
    data_len_ -= pos_;
    pos_ = 0;
  }
  if (data_len_ + 1 >= buf_cap_) {
    if (buf_cap_ >= kMaxLineBytes) {
      ret = OB_SIZE_OVERFLOW;
      LOG_USER_ERROR(OB_SIZE_OVERFLOW, "jsonl line exceeds size limit");
    } else {
      const int64_t new_cap = std::min(buf_cap_ * 2, kMaxLineBytes);
      char *nb = static_cast<char *>(ob_malloc(new_cap,
          common::ObModIds::OB_SQL_CHUNK_ROW_STORE));
      if (OB_ISNULL(nb)) {
        ret = OB_ALLOCATE_MEMORY_FAILED;
      } else {
        MEMCPY(nb, buf_, data_len_);
        ob_free(buf_);
        buf_ = nb;
        buf_cap_ = new_cap;
      }
    }
  }
  if (OB_SUCC(ret) && !eof_) {
    const size_t got = fread(buf_ + data_len_, 1, buf_cap_ - data_len_ - 1,
                             static_cast<FILE *>(fp_));
    if (got == 0) {
      if (feof(static_cast<FILE *>(fp_))) {
        eof_ = true;
      } else {
        ret = OB_IO_ERROR;
      }
    } else {
      data_len_ += static_cast<int64_t>(got);
      got_data = true;
    }
  }
  return ret;
}

int ObJsonlFileCursor::next_row(ObFileRow &row)
{
  int ret = OB_SUCCESS;
  row.cells_.reset();
  if (NULL == fp_) {
    ret = OB_NOT_INIT;
  }
  // locate the next '\n' (refilling as needed)
  int64_t nl = -1;
  while (OB_SUCC(ret) && nl < 0) {
    for (int64_t i = pos_; i < data_len_; ++i) {
      if (buf_[i] == '\n') { nl = i; break; }
    }
    if (nl < 0) {
      if (eof_) {
        if (pos_ < data_len_) {
          nl = data_len_;  // final unterminated line
        } else {
          return OB_ITER_END;
        }
      } else {
        bool got = false;
        if (OB_FAIL(refill(got))) {
        } else if (!got && !eof_) {
          ret = OB_IO_ERROR;
        }
      }
    }
  }
  if (OB_SUCC(ret)) {
    int64_t start = pos_;
    int64_t end = nl;
    pos_ = nl < data_len_ ? nl + 1 : data_len_;
    ++line_no_;
    if (end > start && buf_[end - 1] == '\r') { --end; }
    common::ObString line(static_cast<int32_t>(end - start), buf_ + start);
    // skip blank lines: recurse by tail call semantics
    bool blank = true;
    for (int64_t i = 0; blank && i < line.length(); ++i) {
      if (line[i] != ' ' && line[i] != '\t' && line[i] != '\r') { blank = false; }
    }
    if (blank) {
      if (pos_ >= data_len_ && eof_ && nl >= data_len_) {
        return OB_ITER_END;
      }
      return next_row(row);
    }
    // parse the flat object
    ObArenaAllocator tmp_alloc("JsonlRow");
    ObJsonNode *j_tree = nullptr;
    if (OB_FAIL(ObJsonParser::get_tree(&tmp_alloc, line, j_tree))) {
      LOG_USER_ERROR(OB_INVALID_DATA, "invalid json line, at line %ld", line_no_);
      ret = OB_INVALID_DATA;
    } else if (OB_ISNULL(j_tree) || j_tree->json_type() != ObJsonNodeType::J_OBJECT) {
      ret = OB_INVALID_DATA;
      LOG_USER_ERROR(OB_INVALID_DATA, "only flat json objects are supported, at line %ld",
                     line_no_);
    } else {
      ObJsonObject *obj = static_cast<ObJsonObject *>(j_tree);
      for (int64_t i = 0; OB_SUCC(ret) && i < obj->element_count(); ++i) {
        common::ObString key;
        ObIJsonBase *value = nullptr;
        if (OB_FAIL(obj->get_object_value(static_cast<uint64_t>(i), key, value))) {
        } else if (value->json_type() == ObJsonNodeType::J_OBJECT
                   || value->json_type() == ObJsonNodeType::J_ARRAY) {
          ret = OB_INVALID_DATA;
          LOG_USER_ERROR(OB_INVALID_DATA, "nested json structures are rejected, at line %ld",
                         line_no_);
        } else {
          // Copy key and value text onto alloc_ so the views outlive the
          // parser arena. get_data()/get_data_length() only describe the
          // payload of string nodes -- for numbers and booleans they point at
          // internal storage, not at text -- so every scalar is rendered
          // through print(is_quoted = false), which also gives strings their
          // unquoted text. A JSON null becomes an empty (NULL) cell.
          const int64_t klen = key.length();
          common::ObJsonBuffer vtext_buf(&alloc_);
          common::ObString vtext;
          char *kbuf = nullptr;
          if (OB_FAIL(value->print(vtext_buf, false))) {
          } else if (OB_FAIL(vtext_buf.get_result_string(vtext))) {
          } else if (ObJsonNodeType::J_NULL == value->json_type()) {
            vtext.assign_ptr("", 0);
          }
          if (OB_FAIL(ret)) {
          } else if (OB_ISNULL(kbuf = static_cast<char *>(
                         alloc_.alloc(klen > 0 ? klen : 1)))) {
            ret = OB_ALLOCATE_MEMORY_FAILED;
          } else if (klen > 0 && OB_NOT_NULL(key.ptr())) {
            MEMCPY(kbuf, key.ptr(), klen);
          }
          if (OB_FAIL(ret)) {
          } else if (OB_FAIL(row.cells_.push_back(common::ObString(
                         static_cast<int32_t>(klen), kbuf)))) {
          } else if (OB_FAIL(row.cells_.push_back(common::ObString(
                         static_cast<int32_t>(vtext.length()), vtext.ptr())))) {
          }
        }
      }
    }
  }
  return ret;
}

// ---------------- glob streaming ----------------

static bool glob_match_one(const char *pat, int64_t plen, const char *str, int64_t slen)
{
  int64_t p = 0, s = 0, star_p = -1, star_s = -1;
  while (s < slen) {
    if (p < plen && (pat[p] == '?' || tolower(static_cast<unsigned char>(pat[p])) == tolower(static_cast<unsigned char>(str[s])))) { ++p; ++s; }
    else if (p < plen && pat[p] == '*') { star_p = p++; star_s = s; }
    else if (star_p != -1) { p = star_p + 1; s = ++star_s; }
    else { return false; }
  }
  while (p < plen && pat[p] == '*') { ++p; }
  return p == plen;
}

ObGlobCursor::ObGlobCursor(const common::ObString &pattern, common::ObIAllocator &alloc)
    : pattern_(pattern), alloc_(alloc) {}

int ObGlobCursor::collect_recursive(const common::ObString &pattern, int64_t &count,
                                    common::ObIAllocator &alloc,
                                    common::ObArray<common::ObString> &out)
{
  int ret = OB_SUCCESS;
  int64_t last_sep = -1;
  for (int64_t i = pattern.length() - 1; i >= 0; --i) {
    if (pattern.ptr()[i] == '/') { last_sep = i; break; }
  }
  ObString dir = last_sep < 0 ? ObString(".") :
      ObString(static_cast<int32_t>(last_sep), pattern.ptr());
  ObString pat = last_sep < 0 ? pattern :
      ObString(static_cast<int32_t>(pattern.length() - last_sep - 1),
               pattern.ptr() + last_sep + 1);
  char dir_buf[OB_MAX_FILE_NAME_LENGTH];
  if (dir.length() >= static_cast<int32_t>(sizeof(dir_buf))) {
    ret = OB_INVALID_ARGUMENT;
  } else {
    MEMCPY(dir_buf, dir.ptr(), dir.length());
    dir_buf[dir.length()] = '\0';
    bool dir_has_magic = false;
    for (int64_t mi = 0; !dir_has_magic && mi < dir.length(); ++mi) {
      const char mc = dir.ptr()[mi];
      dir_has_magic = (mc == '?' || mc == '*' || mc == '[');
    }
    if (dir_has_magic) {
      // wildcard inside the directory part: expand one level and recurse
      ObString parent, sub;
      int64_t sep2 = -1;
      for (int64_t i = dir.length() - 1; i >= 0; --i) {
        if (dir.ptr()[i] == '/') { sep2 = i; break; }
      }
      parent = sep2 < 0 ? ObString(".") : ObString(static_cast<int32_t>(sep2), dir.ptr());
      sub = sep2 < 0 ? dir :
          ObString(static_cast<int32_t>(dir.length() - sep2 - 1), dir.ptr() + sep2 + 1);
      char parent_buf[OB_MAX_FILE_NAME_LENGTH];
      if (parent.length() >= static_cast<int32_t>(sizeof(parent_buf))) {
        ret = OB_INVALID_ARGUMENT;
      } else {
        MEMCPY(parent_buf, parent.ptr(), parent.length());
        parent_buf[parent.length()] = '\0';
        DIR *dp = opendir(parent_buf);
        if (OB_ISNULL(dp)) {
          ret = OB_INVALID_ARGUMENT;
        } else {
          struct dirent *ent;
          while (OB_SUCC(ret) && NULL != (ent = readdir(dp))) {
            const char *name = ent->d_name;
            if (name[0] == '.') { continue; }
            if (!glob_match_one(sub.ptr(), sub.length(), name, strlen(name))) { continue; }
            ObSqlString subpat;
            if (OB_FAIL(subpat.append(parent)) || OB_FAIL(subpat.append("/")) ||
                OB_FAIL(subpat.append(name)) || OB_FAIL(subpat.append("/")) ||
                OB_FAIL(subpat.append(pat))) {
            } else if (OB_FAIL(collect_recursive(subpat.string(), count, alloc, out))) {
            }
          }
          closedir(dp);
        }
      }
    } else {
      DIR *dp = opendir(dir_buf);
      if (OB_ISNULL(dp)) {
        ret = OB_INVALID_ARGUMENT;
      } else {
        struct dirent *ent;
        while (OB_SUCC(ret) && NULL != (ent = readdir(dp))) {
          const char *name = ent->d_name;
          if (name[0] == '.') { continue; }
          if (!glob_match_one(pat.ptr(), pat.length(), name, strlen(name))) { continue; }
          if (++count > kMaxMatches) {
            ret = OB_SIZE_OVERFLOW;
            LOG_USER_ERROR(OB_SIZE_OVERFLOW, "too many glob matches");
            break;
          }
          ObSqlString full;
          if (OB_FAIL(full.append(dir)) || OB_FAIL(full.append("/")) ||
              OB_FAIL(full.append(name))) {
          } else {
            char *pb = static_cast<char *>(alloc.alloc(full.length()));
            if (OB_ISNULL(pb)) {
              ret = OB_ALLOCATE_MEMORY_FAILED;
            } else {
              MEMCPY(pb, full.ptr(), full.length());
              if (OB_FAIL(out.push_back(
                      ObString(static_cast<int32_t>(full.length()), pb)))) {}
            }
          }
        }
        closedir(dp);
      }
    }
  }
  return ret;
}

int ObGlobCursor::open()
{
  int ret = OB_SUCCESS;
  if (!matches_.empty()) {
    // already open
  } else {
    int64_t count = 0;
    if (OB_FAIL(collect_recursive(pattern_, count, alloc_, matches_))) {
    } else {
      // deterministic output order
      struct StrLess
      {
        bool operator()(const common::ObString &a, const common::ObString &b) const
        { return a.compare(b) < 0; }
      };
      std::sort(matches_.begin(), matches_.end(), StrLess());
      idx_ = 0;
    }
  }
  return ret;
}

int ObGlobCursor::next_row(ObFileRow &row)
{
  int ret = OB_SUCCESS;
  row.cells_.reset();
  if (idx_ >= matches_.count()) {
    ret = OB_ITER_END;
  } else {
    const common::ObString &p = matches_.at(idx_++);
    if (OB_FAIL(row.cells_.push_back(p))) {}
  }
  return ret;
}

void ObGlobCursor::close()
{
  matches_.reset();
  idx_ = 0;
}

// ---------------- parquet streaming ----------------

ObParquetFileCursor::ObParquetFileCursor(const common::ObString &path,
                                         const ObFileParamSpec &params,
                                         const common::ObIArray<bool> &needed_cols,
                                         common::ObIAllocator &alloc)
    : path_(path), params_(params), alloc_(alloc)
{
  for (int64_t i = 0; i < needed_cols.count(); ++i) {
    needed_.push_back(needed_cols.at(i));
  }
}

int ObParquetFileCursor::open()
{
  int ret = OB_SUCCESS;
  char path_buf[OB_MAX_FILE_NAME_LENGTH];
  if (NULL != file_buf_) {
    // already open
  } else if (path_.empty() || path_.length() >= static_cast<int32_t>(sizeof(path_buf))) {
    ret = OB_INVALID_ARGUMENT;
  } else {
    MEMCPY(path_buf, path_.ptr(), path_.length());
    path_buf[path_.length()] = '\0';
    FILE *fp = fopen(path_buf, "rb");
    if (OB_ISNULL(fp)) {
      ret = OB_INVALID_ARGUMENT;
    } else {
      if (0 != fseek(fp, 0, SEEK_END)) {
        ret = OB_INVALID_ARGUMENT;
      } else {
        const long sz = ftell(fp);
        if (sz < 0 || sz > params_.max_file_size_) {
          ret = OB_SIZE_OVERFLOW;
          LOG_USER_ERROR(OB_SIZE_OVERFLOW, "parquet file exceeds size limit");
        } else if (0 != fseek(fp, 0, SEEK_SET)) {
          ret = OB_INVALID_ARGUMENT;
        } else {
          file_len_ = sz;
          file_buf_ = static_cast<char *>(ob_malloc(file_len_ + 1,
              common::ObModIds::OB_SQL_CHUNK_ROW_STORE));
          if (OB_ISNULL(file_buf_)) {
            ret = OB_ALLOCATE_MEMORY_FAILED;
          } else if (file_len_ > 0 &&
                     fread(file_buf_, 1, file_len_, fp) != static_cast<size_t>(file_len_)) {
            ret = OB_IO_ERROR;
          } else {
            ObMiniParquetReader::ReadResult rr;
            if (OB_FAIL(ObMiniParquetReader::read_file(file_buf_, file_len_, rr))) {
              LOG_USER_ERROR(OB_INVALID_ARGUMENT, "invalid parquet file");
            } else {
              for (int64_t c = 0; OB_SUCC(ret) && c < rr.column_names_.count(); ++c) {
                col_names_.push_back(rr.column_names_.at(c));
              }
            }
          }
        }
      }
      fclose(fp);
    }
    cur_group_ = -1;
    cur_row_in_group_ = 0;
    group_row_cnt_ = 0;
    group_done_ = true;
  }
  return ret;
}

void ObParquetFileCursor::close()
{
  if (NULL != file_buf_) {
    ob_free(file_buf_);
    file_buf_ = NULL;
  }
  file_len_ = 0;
  cur_group_ = -1;
  cur_row_in_group_ = 0;
  group_row_cnt_ = 0;
  group_done_ = true;
}

int ObParquetFileCursor::decode_group(int64_t group_idx)
{
  int ret = OB_SUCCESS;
  cur_cells_.cells_.reset();
  if (group_idx < 0) {
    ret = OB_ERR_UNEXPECTED;
  } else {
    int64_t group_cnt = 0;
    // convert only the needed columns of this row group into text cells
    if (OB_FAIL(ObMiniParquetReader::read_group_as_rows(file_buf_, file_len_, group_idx,
                                                        group_cnt, needed_, alloc_,
                                                        cur_cells_.cells_))) {
      if (OB_ITER_END == ret) {
        // beyond the last row group
      } else {
        LOG_USER_ERROR(OB_INVALID_ARGUMENT, "invalid parquet row group");
      }
    }
  }
  return ret;
}

int ObParquetFileCursor::next_row(ObFileRow &row)
{
  int ret = OB_SUCCESS;
  row.cells_.reset();
  while (OB_SUCC(ret) && group_done_) {
    ++cur_group_;
    int gr = decode_group(cur_group_);
    if (OB_ITER_END == gr) {
      return OB_ITER_END;
    } else if (OB_SUCCESS != gr) {
      ret = gr;
    } else {
      const int64_t col_cnt = col_names_.count() == 0 ? 1 : col_names_.count();
      group_row_cnt_ = cur_cells_.cells_.count() / col_cnt;
      cur_row_in_group_ = 0;
      group_done_ = group_row_cnt_ == 0;
    }
  }
  if (OB_SUCC(ret)) {
    const int64_t col_cnt = col_names_.count();
    for (int64_t c = 0; OB_SUCC(ret) && c < group_row_cnt_ && c == cur_row_in_group_; ++c) {
      // emit cells of this row (column-major storage: cell = c * col_cnt + col)
      for (int64_t col = 0; OB_SUCC(ret) && col < col_cnt; ++col) {
        const int64_t cell_idx = cur_row_in_group_ * col_cnt + col;
        if (col < needed_.count() && !needed_.at(col)) {
          if (OB_FAIL(row.cells_.push_back(ObString()))) {}
        } else if (cell_idx < cur_cells_.cells_.count()) {
          if (OB_FAIL(row.cells_.push_back(cur_cells_.cells_.at(cell_idx)))) {}
        } else {
          ret = OB_ERR_UNEXPECTED;
        }
      }
      if (OB_SUCC(ret)) {
        ++cur_row_in_group_;
        if (cur_row_in_group_ >= group_row_cnt_) {
          group_done_ = true;
        }
      }
    }
  }
  return ret;
}

// ---------------- schema inferer ----------------

void ObFileSchemaInferer::init(int64_t col_cnt)
{
  types_.reset();
  nullables_.reset();
  col_cnt_ = col_cnt;
  for (int64_t i = 0; i < col_cnt; ++i) {
    types_.push_back(ObFileColumnType::NIL);
    nullables_.push_back(false);
  }
}

bool ObFileSchemaInferer::is_bool_text(const common::ObString &s)
{
  return s.case_compare("true") == 0 || s.case_compare("false") == 0;
}

bool ObFileSchemaInferer::is_bigint_text(const common::ObString &s)
{
  if (s.empty()) return false;
  int64_t i = (s[0] == '-' || s[0] == '+') ? 1 : 0;
  if (i >= s.length()) return false;
  for (; i < s.length(); ++i) {
    if (s[i] < '0' || s[i] > '9') return false;
  }
  return true;
}

bool ObFileSchemaInferer::is_double_text(const common::ObString &s)
{
  if (s.empty()) return false;
  int64_t i = 0;
  if (s[0] == '-' || s[0] == '+') i = 1;
  bool dot = false, digit = false;
  for (; i < s.length(); ++i) {
    const char c = s[i];
    if (c >= '0' && c <= '9') { digit = true; }
    else if (c == '.' && !dot) { dot = true; }
    else if ((c == 'e' || c == 'E') && digit) {
      ++i;
      if (i < s.length() && (s[i] == '-' || s[i] == '+')) ++i;
      bool ed = false;
      for (; i < s.length(); ++i) {
        if (s[i] < '0' || s[i] > '9') return false;
        ed = true;
      }
      return ed;
    } else { return false; }
  }
  return digit && dot;
}

bool ObFileSchemaInferer::is_date_text(const common::ObString &s)
{
  if (s.length() != 10 || s[4] != '-' || s[7] != '-') return false;
  for (int64_t i = 0; i < 10; ++i) {
    if (i == 4 || i == 7) continue;
    if (s[i] < '0' || s[i] > '9') return false;
  }
  const int mon = (s[5] - '0') * 10 + (s[6] - '0');
  const int day = (s[8] - '0') * 10 + (s[9] - '0');
  return mon >= 1 && mon <= 12 && day >= 1 && day <= 31;
}

bool ObFileSchemaInferer::is_datetime_text(const common::ObString &s)
{
  if (s.length() < 19) return false;
  common::ObString date_part(10, s.ptr());
  const char sep = s[10];
  if (sep != ' ' && sep != 'T') return false;
  const common::ObString time_part(static_cast<int32_t>(s.length() - 11), s.ptr() + 11);
  if (!is_date_text(date_part)) return false;
  if (time_part.length() < 8) return false;
  if (time_part[2] != ':' || time_part[5] != ':') return false;
  for (int64_t i = 0; i < 8; ++i) {
    if (i == 2 || i == 5) continue;
    if (time_part[i] < '0' || time_part[i] > '9') return false;
  }
  if (time_part.length() > 8 && time_part[8] != '.') return false;
  for (int64_t i = 9; i < time_part.length(); ++i) {
    if (time_part[i] < '0' || time_part[i] > '9') return false;
  }
  return true;
}

ObFileColumnType ObFileSchemaInferer::classify(const common::ObString &cell)
{
  ObFileColumnType cand;
  if (is_bool_text(cell)) cand = ObFileColumnType::BOOLEAN;
  else if (is_bigint_text(cell)) cand = ObFileColumnType::BIGINT;
  else if (is_double_text(cell)) cand = ObFileColumnType::DOUBLE;
  else if (is_datetime_text(cell)) cand = ObFileColumnType::DATETIME;
  else if (is_date_text(cell)) cand = ObFileColumnType::DATE;
  else cand = ObFileColumnType::VARCHAR;
  return cand;
}

ObFileColumnType ObFileSchemaInferer::widen(ObFileColumnType cur, ObFileColumnType cand)
{
  ObFileColumnType merged = cur;
  if (cur == ObFileColumnType::NIL) {
    merged = cand;
  } else if (cur != cand) {
    if (cur == ObFileColumnType::BOOLEAN && cand == ObFileColumnType::BIGINT) {
      merged = ObFileColumnType::BIGINT;
    } else if (cur == ObFileColumnType::BOOLEAN && cand == ObFileColumnType::DOUBLE) {
      merged = ObFileColumnType::DOUBLE;
    } else if (cur == ObFileColumnType::BIGINT && cand == ObFileColumnType::BOOLEAN) {
      merged = ObFileColumnType::BIGINT;
    } else if (cur == ObFileColumnType::BIGINT && cand == ObFileColumnType::DOUBLE) {
      merged = ObFileColumnType::DOUBLE;
    } else if (cur == ObFileColumnType::DATE && cand == ObFileColumnType::DATETIME) {
      merged = ObFileColumnType::DATETIME;
    } else {
      merged = ObFileColumnType::VARCHAR;
    }
  }
  return merged;
}

int ObFileSchemaInferer::feed_row(const ObFileRow &row, const ObFileParamSpec &params)
{
  int ret = OB_SUCCESS;
  for (int64_t c = 0; OB_SUCC(ret) && c < col_cnt_ && c < row.cells_.count(); ++c) {
    const common::ObString &cell = row.cells_.at(c);
    bool is_null = cell.empty();
    if (!is_null && cell.length() == 4 && cell.case_compare("NULL") == 0
        && params.nullstr_.empty()) {
      is_null = true;
    } else if (!is_null && cell.length() == 2 && cell == "\\N"
               && params.nullstr_.empty()) {
      is_null = true;
    } else if (!is_null && !params.nullstr_.empty()
               && cell.length() == params.nullstr_.length()
               && cell.case_compare(params.nullstr_) == 0) {
      is_null = true;
    }
    if (is_null) {
      nullables_.at(c) = true;
      continue;
    }
    if (types_.at(c) == ObFileColumnType::VARCHAR) {
      continue;  // widest already
    }
    types_.at(c) = widen(types_.at(c), classify(cell));
  }
  return ret;
}

// JSONL: field union in first-seen order with the same widening rules as CSV.
int ObFileSchemaInferer::infer_jsonl_schema(IFileRowCursor &cursor,
                                            const ObFileParamSpec &params,
                                            common::ObIAllocator &allocator,
                                            ObFileSchema &schema,
                                            int64_t &row_cnt)
{
  int ret = OB_SUCCESS;
  row_cnt = 0;
  common::ObArray<common::ObString> keys;
  common::ObArray<ObFileColumnType> types;
  common::ObArray<bool> nullables;
  ObFileRow row;
  while (OB_SUCC(ret)) {
    int gr = cursor.next_row(row);
    if (OB_ITER_END == gr) {
      break;
    } else if (OB_INVALID_DATA == gr) {
      ret = OB_INVALID_DATA;
    } else if (OB_SUCCESS != gr) {
      ret = gr;
    } else {
      ++row_cnt;
      // JSONL rows carry [key, value, key, value, ...] pairs
      for (int64_t i = 0; OB_SUCC(ret) && i + 1 < row.cells_.count(); i += 2) {
        const common::ObString &key = row.cells_.at(i);
        const common::ObString &val = row.cells_.at(i + 1);
        const bool is_null = val.empty()
            || (!params.nullstr_.empty() && val.length() == params.nullstr_.length()
                && val.case_compare(params.nullstr_) == 0);
        int64_t idx = -1;
        for (int64_t k = 0; k < keys.count(); ++k) {
          if (keys.at(k).case_compare(key) == 0) { idx = k; break; }
        }
        if (idx < 0) {
          common::ObString key_copy;
          if (OB_UNLIKELY(key.empty())) {
            ret = OB_INVALID_DATA;
          } else {
            char *buf = static_cast<char *>(allocator.alloc(key.length()));
            if (OB_ISNULL(buf)) {
              ret = OB_ALLOCATE_MEMORY_FAILED;
            } else {
              MEMCPY(buf, key.ptr(), key.length());
              key_copy.assign_ptr(buf, key.length());
            }
          }
          if (OB_SUCC(ret)) {
            if (OB_FAIL(keys.push_back(key_copy))
                || OB_FAIL(types.push_back(is_null ? ObFileColumnType::NIL : classify(val)))
                || OB_FAIL(nullables.push_back(is_null))) {
            }
          }
        } else if (is_null) {
          nullables.at(idx) = true;
        } else if (ObFileColumnType::VARCHAR != types.at(idx)) {
          types.at(idx) = widen(types.at(idx), classify(val));
        }
      }
    }
  }
  if (OB_FAIL(ret)) {
  } else {
    for (int64_t k = 0; OB_SUCC(ret) && k < keys.count(); ++k) {
      ObFileSchema::Column col;
      col.name_ = keys.at(k);
      col.type_ = types.at(k);
      col.nullable_ = nullables.at(k);
      if (OB_FAIL(schema.columns_.push_back(col))) {}
    }
  }
  return ret;
}

int ObFileSchemaInferer::finalize(common::ObIAllocator &allocator, ObFileSchema &schema)
{
  int ret = OB_SUCCESS;
  if (schema.columns_.count() != col_cnt_) {
    ret = OB_ERR_UNEXPECTED;
  } else {
    for (int64_t c = 0; OB_SUCC(ret) && c < col_cnt_; ++c) {
      schema.columns_.at(c).type_ = types_.at(c);
      schema.columns_.at(c).nullable_ = nullables_.at(c);
    }
  }
  return ret;
}

// strict signed decimal parse used by file_cell_to_obj
static int parse_int64_text_strict_impl(const common::ObString &s, int64_t &v)
{
  int ret = OB_SUCCESS;
  if (s.empty()) {
    ret = OB_INVALID_ARGUMENT;
  } else {
    int64_t i = (s.ptr()[0] == '-' || s.ptr()[0] == '+') ? 1 : 0;
    if (i >= s.length()) {
      ret = OB_INVALID_ARGUMENT;
    } else {
      int64_t acc = 0;
      for (; OB_SUCC(ret) && i < s.length(); ++i) {
        const char c = s.ptr()[i];
        if (c < '0' || c > '9') { ret = OB_INVALID_ARGUMENT; }
        else { acc = acc * 10 + (c - '0'); }
      }
      if (OB_SUCC(ret)) {
        v = (s.ptr()[0] == '-') ? -acc : acc;
      }
    }
  }
  return ret;
}

// ---------------- cell -> ObObj ----------------

int file_cell_to_obj(ObFileColumnType type, const common::ObString &raw,
                     bool is_null, common::ObIAllocator &allocator,
                     common::ObObjParam &out)
{
  int ret = OB_SUCCESS;
  if (is_null || raw.empty()) {
    out.set_null();
    return ret;
  }
  ObString text;
  if (OB_FAIL(file_cell_to_text(type, raw, false, allocator, text))) {
  } else {
    switch (type) {
      case ObFileColumnType::BOOLEAN: {
        out.set_tinyint(text == "1" ? 1 : 0);
        break;
      }
      case ObFileColumnType::BIGINT: {
        int64_t v = 0;
        if (OB_FAIL(parse_int64_text_strict_impl(text, v))) {
          out.set_null();
        } else {
          out.set_int(v);
        }
        break;
      }
      case ObFileColumnType::DOUBLE: {
        char *ntext = static_cast<char *>(allocator.alloc(text.length() + 1));
        if (OB_ISNULL(ntext)) {
          ret = OB_ALLOCATE_MEMORY_FAILED;
        } else {
          MEMCPY(ntext, text.ptr(), text.length());
          ntext[text.length()] = '\0';
          out.set_double(strtod(ntext, NULL));
        }
        break;
      }
      case ObFileColumnType::DATE: {
        int32_t v = 0;
        if (OB_FAIL(ObTimeConverter::str_to_date(text, v))) {
          out.set_null();
        } else {
          out.set_date(v);
        }
        break;
      }
      case ObFileColumnType::DATETIME: {
        int64_t v = 0;
        ObTimeConvertCtx cvrt_ctx(NULL, false, false);
        if (OB_FAIL(ObTimeConverter::str_to_datetime(text, cvrt_ctx, v))) {
          out.set_null();
        } else {
          out.set_datetime(v);
        }
        break;
      }
      default: {
        char *tbuf = static_cast<char *>(allocator.alloc(text.length()));
        if (OB_ISNULL(tbuf)) {
          ret = OB_ALLOCATE_MEMORY_FAILED;
        } else {
          if (text.length() > 0) { MEMCPY(tbuf, text.ptr(), text.length()); }
          out.set_varchar(ObString(static_cast<int32_t>(text.length()), tbuf));
          out.set_collation_type(CS_TYPE_UTF8MB4_BIN);
        }
        break;
      }
    }
  }
  return ret;
}

// ---------------- factory ----------------

int make_file_row_cursor(ObFileSourceFormat fmt,
                         const common::ObString &path,
                         const ObFileParamSpec &params,
                         const common::ObIArray<bool> &needed_cols,
                         common::ObIAllocator &allocator,
                         IFileRowCursor *&cursor)
{
  int ret = OB_SUCCESS;
  cursor = NULL;
  void *mem = NULL;
  switch (fmt) {
    case ObFileSourceFormat::CSV: {
      if (OB_ISNULL(mem = allocator.alloc(sizeof(ObCsvFileCursor)))) {
        ret = OB_ALLOCATE_MEMORY_FAILED;
      } else {
        cursor = new (mem) ObCsvFileCursor(path, params, allocator);
      }
      break;
    }
    case ObFileSourceFormat::JSONL: {
      if (OB_ISNULL(mem = allocator.alloc(sizeof(ObJsonlFileCursor)))) {
        ret = OB_ALLOCATE_MEMORY_FAILED;
      } else {
        cursor = new (mem) ObJsonlFileCursor(path, params, allocator);
      }
      break;
    }
    case ObFileSourceFormat::GLOB: {
      if (OB_ISNULL(mem = allocator.alloc(sizeof(ObGlobCursor)))) {
        ret = OB_ALLOCATE_MEMORY_FAILED;
      } else {
        cursor = new (mem) ObGlobCursor(path, allocator);
      }
      break;
    }
    case ObFileSourceFormat::PARQUET: {
      if (OB_ISNULL(mem = allocator.alloc(sizeof(ObParquetFileCursor)))) {
        ret = OB_ALLOCATE_MEMORY_FAILED;
      } else {
        cursor = new (mem) ObParquetFileCursor(path, params, needed_cols, allocator);
      }
      break;
    }
    default:
      ret = OB_ERR_UNEXPECTED;
      break;
  }
  return ret;
}

}  // namespace sql
}  // namespace oceanbase
