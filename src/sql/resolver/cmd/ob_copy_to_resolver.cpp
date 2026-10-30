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

#define USING_LOG_PREFIX SQL_RESV

#include <sys/stat.h>
#include <unistd.h>

#include "lib/string/ob_sql_string.h"
#include "sql/resolver/cmd/ob_copy_to_resolver.h"
#include "sql/resolver/ob_resolver.h"
#include "sql/resolver/ob_resolver_utils.h"
#include "sql/resolver/dml/ob_select_stmt.h"
#include "share/schema/ob_schema_struct.h"

using namespace oceanbase;
using namespace oceanbase::common;
using namespace oceanbase::sql;

namespace oceanbase
{
namespace sql
{

namespace
{

// Renders the CSV header line for the child query's select list. A field name
// is enclosed when it carries the delimiter, the quote character or a line
// break; embedded quote characters are doubled. This mirrors the escaping that
// ObCsvFileWriter applies to data fields, so read_csv can parse the result.
int render_csv_header(ObIAllocator &alloc,
                      const ObSelectStmt &stmt,
                      const char delim_ch,
                      const char quote,
                      const char line_term,
                      ObString &out)
{
  int ret = OB_SUCCESS;
  ObSqlString buf;
  const int64_t col_cnt = stmt.get_select_item_size();
  for (int64_t i = 0; OB_SUCC(ret) && i < col_cnt; ++i) {
    ObString name = stmt.get_select_item(i).alias_name_;
    char fallback[64];
    if (name.empty()) {
      const int64_t len = snprintf(fallback, sizeof(fallback), "column%ld", i);
      if (len > 0) {
        name.assign_ptr(fallback, static_cast<int32_t>(len));
      }
    }
    bool need_quote = false;
    if (0 != quote) {
      for (int32_t k = 0; !need_quote && k < name.length(); ++k) {
        const char c = name.ptr()[k];
        need_quote = (c == delim_ch || c == quote || c == '\n' || c == '\r');
      }
    }
    if (need_quote && OB_FAIL(buf.append_fmt("%c", quote))) {
    }
    for (int32_t k = 0; OB_SUCC(ret) && k < name.length(); ++k) {
      const char c = name.ptr()[k];
      if (need_quote && c == quote && OB_FAIL(buf.append_fmt("%c", quote))) {
        break;
      }
      if (OB_SUCC(ret) && OB_FAIL(buf.append_fmt("%c", c))) {
      }
    }
    if (OB_SUCC(ret)) {
      if (need_quote && OB_FAIL(buf.append_fmt("%c", quote))) {
      } else {
        const char sep = (i == col_cnt - 1) ? line_term : delim_ch;
        if (OB_SUCC(ret) && OB_FAIL(buf.append_fmt("%c", sep))) {
        }
      }
    }
  }
  if (OB_SUCC(ret) && OB_FAIL(ob_write_string(alloc, buf.string(), out))) {
    LOG_WARN("failed to copy csv header", K(ret));
  }
  return ret;
}

}  // namespace

int ObCopyToResolver::canonicalize_local_path(const ObString &path, ObString &canonical)
{
  int ret = OB_SUCCESS;
  char path_buf[OB_MAX_FILE_NAME_LENGTH];
  char real_buf[OB_MAX_FILE_NAME_LENGTH];
  if (path.empty() || path.length() >= static_cast<int32_t>(sizeof(path_buf))) {
    ret = OB_INVALID_ARGUMENT;
  } else {
    MEMCPY(path_buf, path.ptr(), path.length());
    path_buf[path.length()] = '\0';
    const char *p = path_buf;
    while (OB_SUCC(ret) && NULL != p) {
      if (0 == strncmp(p, "..", 2) && (p == path_buf || p[-1] == '/') &&
          (p[2] == '\0' || p[2] == '/')) {
        ret = OB_INVALID_ARGUMENT;
        LOG_USER_ERROR(OB_INVALID_ARGUMENT, "path traversal is not allowed");
      } else {
        p = strchr(p + 1, '/');
      }
    }
    if (OB_SUCC(ret)) {
      char *rp = realpath(path_buf, real_buf);
      if (NULL != rp) {
        OZ(ob_write_string(*allocator_, ObString(static_cast<int32_t>(strlen(real_buf)), real_buf), canonical));
      } else {
        // target may not exist yet: canonicalize the deepest existing ancestor
        char tail_buf[OB_MAX_FILE_NAME_LENGTH];
        strncpy(tail_buf, path_buf, sizeof(tail_buf) - 1);
        tail_buf[sizeof(tail_buf) - 1] = '\0';
        char *slash = strrchr(tail_buf, '/');
        bool done = false;
        if (NULL != slash) {
          *slash = '\0';
          const char *base = slash + 1;
          if ('\0' != base[0] && 0 != strcmp(base, ".") && 0 != strcmp(base, "..")) {
            char *rp2 = realpath(tail_buf, real_buf);
            if (NULL != rp2) {
              const int64_t dlen = strlen(real_buf);
              const int64_t blen = strlen(base);
              if (dlen + 1 + blen < static_cast<int64_t>(sizeof(real_buf))) {
                real_buf[dlen] = '/';
                MEMCPY(real_buf + dlen + 1, base, blen);
                real_buf[dlen + 1 + blen] = '\0';
                OZ(ob_write_string(*allocator_,
                                   ObString(static_cast<int32_t>(dlen + 1 + blen), real_buf),
                                   canonical));
                done = true;
              }
            }
          }
        }
        if (!done && OB_SUCC(ret)) {
          ret = OB_INVALID_ARGUMENT;
        }
      }
    }
  }
  return ret;
}

int ObCopyToResolver::resolve(const ParseNode &parse_tree)
{
  int ret = OB_SUCCESS;
  ParseNode *select_node = NULL;
  ParseNode *path_node = NULL;
  ParseNode *options_node = NULL;
  if (OB_UNLIKELY(T_COPY_TO != parse_tree.type_)
      || OB_UNLIKELY(parse_tree.num_child_ != 2 && parse_tree.num_child_ != 3)
      || OB_ISNULL(parse_tree.children_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("invalid COPY TO parse tree", K(parse_tree.type_), K(parse_tree.num_child_));
  } else if (OB_ISNULL(select_node = parse_tree.children_[0])
             || OB_ISNULL(path_node = parse_tree.children_[1])
             || OB_UNLIKELY(T_VARCHAR != path_node->type_ && T_CHAR != path_node->type_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("invalid COPY TO parse tree children", K(ret));
  } else if (OB_ISNULL(allocator_)) {
    ret = OB_ERR_UNEXPECTED;
  } else {
    options_node = (3 == parse_tree.num_child_) ? parse_tree.children_[2] : NULL;
  }

  // enable_file_sql must be on: COPY reads/writes the local filesystem
  bool file_sql_enabled = false;
  if (OB_FAIL(ret)) {
  } else if (OB_FAIL(session_info_->get_ob_enable_file_sql(file_sql_enabled))) {
    LOG_WARN("failed to read enable_file_sql", K(ret));
  } else if (!file_sql_enabled) {
    ret = OB_NOT_SUPPORTED;
    LOG_USER_ERROR(OB_NOT_SUPPORTED, "COPY TO requires enable_file_sql=ON");
  }

  // path canonicalization + secure_file_priv confinement
  ObString canonical_path;
  ObString secure_dir;
  if (OB_FAIL(ret)) {
  } else if (OB_FAIL(canonicalize_local_path(
               ObString(path_node->str_len_, path_node->str_value_), canonical_path))) {
    LOG_USER_ERROR(OB_INVALID_ARGUMENT, "invalid copy path");
  } else if (OB_FAIL(session_info_->get_secure_file_priv(secure_dir))) {
    LOG_WARN("failed to read secure_file_priv", K(ret));
  } else if (OB_FAIL(ObResolverUtils::check_secure_path(secure_dir, canonical_path))) {
    LOG_USER_ERROR(OB_INVALID_ARGUMENT, "copy path is outside secure_file_priv");
  }

  // fail if the target already exists (checked before running the child query)
  if (OB_FAIL(ret)) {
  } else {
    char path_buf[OB_MAX_FILE_NAME_LENGTH];
    if (canonical_path.empty()
        || canonical_path.length() >= static_cast<int32_t>(sizeof(path_buf))) {
      ret = OB_INVALID_ARGUMENT;
      LOG_USER_ERROR(OB_INVALID_ARGUMENT, "invalid copy target path");
    } else {
      MEMCPY(path_buf, canonical_path.ptr(), canonical_path.length());
      path_buf[canonical_path.length()] = '\0';
      struct stat st;
      if (0 == ::stat(path_buf, &st)) {
        ret = OB_FILE_ALREADY_EXIST;
        LOG_USER_ERROR(OB_FILE_ALREADY_EXIST, "copy target file already exists");
      }
    }
  }

  // CSV options (DuckDB-compatible names). Defaults: ',' delimiter, '"' quote,
  // '"' escape (doubled quotes), no header, NULL written as "NULL".
  ObString opt_delim = ObString::make_string(",");
  ObString opt_quote = ObString::make_string("\"");
  ObString opt_escape = ObString::make_string("\"");
  ObString opt_nullstr;
  bool has_header = false;
  if (OB_FAIL(ret)) {
  } else if (NULL != options_node) {
    if (OB_UNLIKELY(T_EXPR_LIST != options_node->type_)) {
      ret = OB_ERR_UNEXPECTED;
    }
    for (int64_t i = 0; OB_SUCC(ret) && i < options_node->num_child_; ++i) {
      ParseNode *pnd = options_node->children_[i];
      if (OB_ISNULL(pnd) || OB_UNLIKELY(T_EXPR_LIST != pnd->type_ || pnd->num_child_ < 1)
          || OB_ISNULL(pnd->children_[0])
          || OB_ISNULL(pnd->children_[0]->str_value_)) {
        ret = OB_ERR_UNEXPECTED;
      } else {
        ParseNode *name_nd = pnd->children_[0];
        ParseNode *value_nd = (pnd->num_child_ >= 2) ? pnd->children_[1] : NULL;
        ObString pname(name_nd->str_len_, name_nd->str_value_);
        ObString vstr;
        if (NULL != value_nd && NULL != value_nd->str_value_) {
          vstr.assign_ptr(value_nd->str_value_, static_cast<int32_t>(value_nd->str_len_));
        }
        if (pname.case_compare("header") == 0) {
          if (NULL == value_nd) {
            has_header = true;
          } else if (NULL == value_nd->str_value_) {
            // header=true / header=1 parse as integer or boolean literal nodes
            has_header = (0 != value_nd->value_);
          } else if (0 == vstr.case_compare("true") || 0 == vstr.case_compare("1")) {
            has_header = true;
          } else if (0 == vstr.case_compare("false") || 0 == vstr.case_compare("0")) {
            has_header = false;
          } else {
            ret = OB_ERR_PARSE_SQL;
            LOG_USER_ERROR(OB_ERR_PARSE_SQL, "invalid value for header");
          }
        } else if (pname.case_compare("format") == 0) {
          if (0 != vstr.case_compare("csv") && 0 != vstr.case_compare("auto")) {
            ret = OB_ERR_PARSE_SQL;
            LOG_USER_ERROR(OB_ERR_PARSE_SQL, "only csv format is supported");
          }
        } else if (pname.case_compare("delimiter") == 0
                   || pname.case_compare("sep") == 0) {
          if (vstr.length() != 1) {
            ret = OB_ERR_PARSE_SQL;
            LOG_USER_ERROR(OB_ERR_PARSE_SQL, "delimiter must be a single character");
          } else {
            opt_delim = vstr;
          }
        } else if (pname.case_compare("quote") == 0) {
          if (vstr.length() > 1) {
            ret = OB_ERR_PARSE_SQL;
            LOG_USER_ERROR(OB_ERR_PARSE_SQL, "quote must be a single character");
          } else if (vstr.empty()) {
            // no enclosing at all
            opt_quote = ObString();
          } else {
            opt_quote = vstr;
          }
        } else if (pname.case_compare("escape") == 0) {
          if (vstr.length() > 1) {
            ret = OB_ERR_PARSE_SQL;
            LOG_USER_ERROR(OB_ERR_PARSE_SQL, "escape must be a single character");
          } else if (vstr.empty()) {
            opt_escape = ObString();
          } else {
            opt_escape = vstr;
          }
        } else if (pname.case_compare("nullstr") == 0
                   || pname.case_compare("null") == 0) {
          if (NULL == value_nd) {
            ret = OB_ERR_PARSE_SQL;
            LOG_USER_ERROR(OB_ERR_PARSE_SQL, "nullstr requires a value");
          } else {
            opt_nullstr = vstr;
          }
        } else {
          ret = OB_ERR_PARSE_SQL;
          LOG_USER_ERROR(OB_ERR_PARSE_SQL, "unknown COPY option");
        }
      }
    }
  }

  // resolve the child query; it becomes the statement, carrying a synthesized
  // SELECT INTO OUTFILE item so the export runs on the select-into executor
  ObSelectStmt *sel_stmt = NULL;
  if (OB_FAIL(ret)) {
  } else {
    ObStmt *child_stmt = NULL;
    ObResolver child_resolver(params_);
    if (OB_FAIL(child_resolver.resolve(ObResolver::IS_NOT_PREPARED_STMT,
                                       *select_node,
                                       child_stmt))) {
      LOG_WARN("failed to resolve copy child select", K(ret));
    } else if (OB_ISNULL(child_stmt) || !child_stmt->is_select_stmt()) {
      ret = OB_ERR_UNEXPECTED;
    } else {
      sel_stmt = static_cast<ObSelectStmt *>(child_stmt);
    }
  }

  // materialize the write spec, copying every string out of the parse pool
  ObString field_str;
  ObString quote_str;
  ObString escape_str;
  ObString null_str;
  if (OB_FAIL(ret)) {
  } else if (OB_FAIL(ob_write_string(*allocator_, opt_delim, field_str))) {
  } else if (!opt_quote.empty()
             && OB_FAIL(ob_write_string(*allocator_, opt_quote, quote_str))) {
  } else if (!opt_escape.empty()
             && OB_FAIL(ob_write_string(*allocator_, opt_escape, escape_str))) {
  } else if (!opt_nullstr.empty()
             && OB_FAIL(ob_write_string(*allocator_, opt_nullstr, null_str))) {
  }

  ObSelectIntoItem *into_item = NULL;
  if (OB_FAIL(ret)) {
  } else if (OB_ISNULL(into_item = static_cast<ObSelectIntoItem *>(
                           allocator_->alloc(sizeof(ObSelectIntoItem))))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
  } else {
    new (into_item) ObSelectIntoItem();
    into_item->into_type_ = T_INTO_OUTFILE;
    into_item->is_copy_to_ = true;
    // exactly one row per line, no parallel fan-out, no compression
    into_item->is_single_ = true;
    ObObj name_obj;
    name_obj.set_varchar(canonical_path);
    name_obj.set_collation_type(ObCharset::get_system_collation());
    into_item->outfile_name_ = name_obj;
    into_item->field_str_.set_varchar(field_str);
    into_item->field_str_.set_collation_type(ObCharset::get_system_collation());
    if (!quote_str.empty()) {
      into_item->closed_cht_.set_char_value(quote_str.ptr(),
                                            static_cast<int32_t>(quote_str.length()));
      into_item->is_optional_ = 1;  // quote string fields only
    }
    if (!escape_str.empty()) {
      into_item->escaped_cht_.set_char_value(escape_str.ptr(),
                                             static_cast<int32_t>(escape_str.length()));
    }
    if (!null_str.empty()) {
      ObObj null_obj;
      null_obj.set_varchar(null_str);
      null_obj.set_collation_type(ObCharset::get_system_collation());
      into_item->null_str_ = null_obj;
    } else {
      // default: NULL is written as the reader's default NULL text
      ObString default_null = ObString::make_string("NULL");
      ObObj null_obj;
      null_obj.set_varchar(default_null);
      null_obj.set_collation_type(ObCharset::get_system_collation());
      into_item->null_str_ = null_obj;
    }
    if (has_header) {
      ObString header;
      const char delim_ch = field_str.empty() ? ',' : field_str.ptr()[0];
      const char quote_ch = quote_str.empty() ? 0 : quote_str.ptr()[0];
      const char line_term = into_item->line_str_.get_varchar().empty()
                                 ? '\n' : into_item->line_str_.get_varchar().ptr()[0];
      if (OB_FAIL(render_csv_header(*allocator_, *sel_stmt, delim_ch, quote_ch,
                                    line_term, header))) {
        LOG_WARN("failed to render csv header", K(ret));
      } else {
        ObObj header_obj;
        header_obj.set_varchar(header);
        header_obj.set_collation_type(ObCharset::get_system_collation());
        into_item->header_str_ = header_obj;
      }
    }
    if (OB_SUCC(ret)) {
      sel_stmt->set_select_into(into_item);
      // hand the resolved query back to ObResolver: it becomes this statement
      set_basic_stmt(sel_stmt);
    }
  }
  return ret;
}

}  // namespace sql
}  // namespace oceanbase
