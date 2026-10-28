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
#include "lib/string/ob_string_buffer.h"
#include "common/json_type/ob_json_common.h"
#include "lib/utility/ob_mod_define.h"

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

}  // namespace sql
}  // namespace oceanbase
