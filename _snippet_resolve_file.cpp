int ObDMLResolver::resolve_file_table_item(const ParseNode &table_node, TableItem *&table_item)
{
  int ret = OB_SUCCESS;
  ObDMLStmt *dml_stmt = get_stmt();
  TableItem *new_table_item = NULL;
  ParseNode *alias_node = NULL;
  ParseNode *path_node = NULL;
  ParseNode *param_node = NULL;
  ObString func_name;
  ObString path_str;
  ObString alias_name;

  // children layout produced by the grammar:
  //   3 children: [func, path, alias]
  //   4 children: [func, path, params, alias]
  if (OB_ISNULL(dml_stmt) || OB_ISNULL(allocator_) || OB_ISNULL(session_info_)) {
    ret = OB_ERR_UNEXPECTED;
  } else if (table_node.num_child_ != 3 && table_node.num_child_ != 4) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected file table node", K(table_node.num_child_));
  } else {
    func_name.assign_ptr(table_node.children_[0]->str_value_,
                         static_cast<int32_t>(table_node.children_[0]->str_len_));
    path_node = table_node.children_[1];
    param_node = (4 == table_node.num_child_) ? table_node.children_[2] : NULL;
    alias_node = table_node.children_[table_node.num_child_ - 1];
  }

  // gate: enable_file_sql must be ON
  bool file_sql_enabled = false;
  if (OB_SUCC(ret)) {
    if (OB_FAIL(session_info_->get_ob_enable_file_sql(file_sql_enabled))) {
      LOG_WARN("failed to read enable_file_sql", K(ret));
    } else if (!file_sql_enabled) {
      ret = OB_NOT_SUPPORTED;
      LOG_USER_ERROR(OB_NOT_SUPPORTED, "file table function requires enable_file_sql=ON");
    }
  }

  // resolve path literal
  if (OB_FAIL(ret)) {
  } else if (OB_FAIL(resolve_str_const(*path_node, path_str))) {
    LOG_WARN("failed to resolve file path literal", K(ret));
  }

  // secure_file_priv check: resolved path (dirname) must live under secure_file_priv
  if (OB_FAIL(ret)) {
    ObString secure_dir;
    if (OB_FAIL(session_info_->get_secure_file_priv(secure_dir))) {
      LOG_WARN("failed to read secure_file_priv", K(ret));
    } else if (OB_FAIL(ObResolverUtils::check_secure_file_priv(path_str, secure_dir))) {
      LOG_USER_ERROR(OB_INVALID_ARGUMENT, "file path is outside secure_file_priv");
    }
  }

  // read the whole file (small-file competition scope; streaming plan requires
  // a physical operator which follows in a later iteration)
  char *buf = NULL;
  int64_t file_len = 0;
  if (OB_FAIL(ret)) {
  } else if (OB_FAIL(read_local_file(path_str, *allocator_, buf, file_len))) {
    LOG_USER_ERROR(OB_INVALID_ARGUMENT, "cannot read file");
  }

  // collect header + rows and infer the schema
  ObFileSchema schema;
  ObArray<ObFileRow> rows;
  if (OB_FAIL(ret)) {
  } else if (func_name.case_compare("glob") == 0) {
    // glob: expand pattern to matching file list; one `file` VARCHAR column
    ObFileSchema::Column col;
    char *nb = static_cast<char *>(allocator_->alloc(5));
    if (OB_ISNULL(nb)) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
    } else {
      MEMCPY(nb, "file", 4); nb[4] = '\0';
      col.name_.assign_ptr(nb, 4);
      col.type_ = ObFileColumnType::VARCHAR;
      col.nullable_ = false;
      if (OB_FAIL(schema.columns_.push_back(col))) {
      } else if (OB_FAIL(collect_glob_matches(path_str, *allocator_, rows))) {
      }
    }
  } else {
    ObArray<ObString> header;
    if (func_name.case_compare("read_csv") == 0) {
      bool has_header = true;  // competition spec: header=true is the default; only -1 sample
      ObCsvCursor cursor(buf, file_len, has_header);
      header = cursor.header();
      ObFileRow r;
      while (OB_SUCC(ret)) {
        int gr = cursor.next_row(r);
        if (OB_ITER_END == gr) { break; }
        else if (OB_SUCCESS != gr) { ret = gr; LOG_WARN("bad csv row", K(cursor.current_line())); }
        else if (OB_FAIL(rows.push_back(r))) {}
      }
    } else if (func_name.case_compare("read_json") == 0
               || func_name.case_compare("read_ndjson") == 0) {
      ObJsonlCursor cursor(buf, file_len, *allocator_);
      // first pass: collect the union of keys in order
      ObArray<ObString> keys;
      ObFileRow r;
      while (OB_SUCC(ret)) {
        int gr = cursor.next_row(r);
        if (OB_ITER_END == gr) { break; }
        else if (OB_SUCCESS != gr) { ret = gr; LOG_WARN("bad jsonl row", K(cursor.current_line())); }
        else {
          // r.cells_ holds [k1, v1, k2, v2, ...]
          for (int64_t i = 0; OB_SUCC(ret) && i + 1 < r.cells_.count(); i += 2) {
            bool found = false;
            for (int64_t k = 0; !found && k < keys.count(); ++k) {
              found = (keys.at(k) == r.cells_.at(i));
            }
            if (!found && OB_FAIL(keys.push_back(r.cells_.at(i)))) {}
          }
          if (OB_FAIL(rows.push_back(r))) {}
        }
      }
      for (int64_t k = 0; OB_SUCC(ret) && k < keys.count(); ++k) {
        header.push_back(keys.at(k));
      }
    } else if (func_name.case_compare("read_parquet") == 0) {
      ObMiniParquetReader::ReadResult rr;
      if (OB_FAIL(ObMiniParquetReader::read_file(buf, file_len, rr))) {
        LOG_USER_ERROR(OB_INVALID_ARGUMENT, "invalid parquet file");
      } else {
        for (int64_t c = 0; OB_SUCC(ret) && c < rr.column_names_.count(); ++c) {
          char *nb = static_cast<char *>(allocator_->alloc(rr.column_names_.at(c).length() + 1));
          if (OB_ISNULL(nb)) {
            ret = OB_ALLOCATE_MEMORY_FAILED;
          } else {
            MEMCPY(nb, rr.column_names_.at(c).ptr(), rr.column_names_.at(c).length());
            nb[rr.column_names_.at(c).length()] = '\0';
            common::ObString n(static_cast<int32_t>(rr.column_names_.at(c).length()), nb);
            if (OB_FAIL(header.push_back(n))) {}
          }
        }
        // convert plain columns to text rows for the shared path below
        if (OB_SUCC(ret)) {
          ret = parquet_plain_to_text_rows(rr, *allocator_, rows);
        }
      }
    } else {
      ret = OB_ERR_UNEXPECTED;
    }
    if (OB_SUCC(ret)) {
      if (OB_FAIL(infer_file_schema(header, rows, *allocator_, schema))) {
        LOG_WARN("schema inference failed", K(ret));
      }
    }
  }

  // build a values-table def with ACCESS_OBJ rows and typed columns
  ObValuesTableDef *table_def = NULL;
  if (OB_FAIL(ret)) {
  } else if (OB_ISNULL(new_table_item = dml_stmt->create_table_item(*allocator_))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
  } else if (OB_ISNULL(table_def = static_cast<ObValuesTableDef*>(
                 allocator_->alloc(sizeof(ObValuesTableDef))))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
  } else {
    new_table_item->values_table_def_ = new (table_def) ObValuesTableDef();
    table_def->access_type_ = ObValuesTableDef::ACCESS_OBJ;
    table_def->column_cnt_ = schema.columns_.count();
  }

  // column types
  ObArray<ObRawExprResType> col_types;
  if (OB_FAIL(ret)) {
  } else {
    for (int64_t c = 0; OB_SUCC(ret) && c < schema.columns_.count(); ++c) {
      ObRawExprResType rt;
      switch (schema.columns_.at(c).type_) {
        case ObFileColumnType::BOOLEAN: rt.set_type(ObTinyIntType); break;
        case ObFileColumnType::BIGINT: rt.set_type(ObIntType); break;
        case ObFileColumnType::DOUBLE: rt.set_type(ObDoubleType); break;
        case ObFileColumnType::DATE: rt.set_type(ObDateType); break;
        case ObFileColumnType::DATETIME: rt.set_type(ObDateTimeType); break;
        default: rt.set_type(ObVarcharType); break;
      }
      rt.set_collation_type(CS_TYPE_UTF8MB4_BIN);
      rt.set_length(ObAccuracy::MAX_ACCURACY2[0][rt.get_type()].get_length());
      if (OB_FAIL(col_types.push_back(rt))) {}
    }
  }

  // fill access_objs_ with typed ObObjParams row by row
  if (OB_FAIL(ret)) {
  } else {
    for (int64_t r = 0; OB_SUCC(ret) && r < rows.count(); ++r) {
      const ObFileRow &row = rows.at(r);
      for (int64_t c = 0; OB_SUCC(ret) && c < schema.columns_.count(); ++c) {
        ObObjParam op;
        // locate the cell: CSV rows are positional; JSONL rows are [k,v,...]
        ObString raw;
        bool is_null = true;
        if (func_name.case_compare("read_json") == 0
            || func_name.case_compare("read_ndjson") == 0) {
          const ObString &key = schema.columns_.at(c).name_;
          for (int64_t i = 0; i + 1 < row.cells_.count(); i += 2) {
            if (row.cells_.at(i) == key) {
              raw = row.cells_.at(i + 1);
              is_null = raw.empty();
              break;
            }
          }
        } else if (c < row.cells_.count()) {
          raw = row.cells_.at(c);
          is_null = raw.empty() || raw.case_compare("NULL") == 0 || raw.case_compare("\\N") == 0;
        }
        if (is_null) {
          op.set_null();
        } else {
          ObString text;
          if (OB_FAIL(file_cell_to_text(schema.columns_.at(c).type_, raw, false,
                                        *allocator_, text))) {
          } else {
            switch (schema.columns_.at(c).type_) {
              case ObFileColumnType::BOOLEAN: {
                op.set_tinyint(text == "1" ? 1 : 0);
                break;
              }
              case ObFileColumnType::BIGINT: {
                int64_t v = 0;
                if (OB_FAIL(parse_int64_strict(text, v))) {
                  op.set_null();
                } else {
                  op.set_int(v);
                }
                break;
              }
              case ObFileColumnType::DOUBLE: {
                double v = 0;
                char *endp = NULL;
                v = ObCharset::strtod(text.ptr(), &endp);
                op.set_double(v);
                break;
              }
              case ObFileColumnType::DATE: {
                int64_t v = 0;
                ObDateValue date_val;
                if (OB_FAIL(ObTimeConverter::str_to_date(text, date_val))) {
                  op.set_null();
                } else {
                  op.set_date(date_val.date_int());
                }
                break;
              }
              case ObFileColumnType::DATETIME: {
                int64_t v = 0;
                ObTime ot;
                if (OB_FAIL(ObTimeConverter::str_to_ob_time(text, ot, false))) {
                  op.set_null();
                } else {
                  ObTimeConverter::ob_time_to_datetime(ot, v);
                  op.set_datetime(v);
                }
                break;
              }
              default: {
                char *tbuf = static_cast<char *>(allocator_->alloc(text.length()));
                if (OB_ISNULL(tbuf)) {
                  ret = OB_ALLOCATE_MEMORY_FAILED;
                } else {
                  MEMCPY(tbuf, text.ptr(), text.length());
                  op.set_varchar(ObString(static_cast<int32_t>(text.length()), tbuf));
                  op.set_collation_type(CS_TYPE_UTF8MB4_BIN);
                }
                break;
              }
            }
          }
        }
        if (OB_FAIL(ret)) {
        } else if (OB_FAIL(table_def->access_objs_.push_back(op))) {
        }
      }
    }
    if (OB_SUCC(ret)) {
      table_def->row_cnt_ = rows.count();
      table_def->is_const_ = true;
    }
  }

  // alias + register
  if (OB_FAIL(ret)) {
  } else if (OB_ISNULL(alias_node) || 0 == alias_node->str_len_) {
    ret = OB_ERR_TABLE_WITHOUT_ALIAS;
    LOG_USER_ERROR(OB_ERR_TABLE_WITHOUT_ALIAS);
  } else {
    alias_name.assign_ptr(alias_node->str_value_, static_cast<int32_t>(alias_node->str_len_));
    new_table_item->table_id_ = generate_table_id();
    new_table_item->table_name_ = alias_name;
    new_table_item->alias_name_ = alias_name;
    new_table_item->type_ = TableItem::VALUES_TABLE;
    new_table_item->is_view_table_ = false;
    if (OB_FAIL(dml_stmt->add_table_item(session_info_, new_table_item))) {
    } else if (OB_FAIL(gen_values_table_column_items_with_names(
                   schema.columns_, col_types, *new_table_item))) {
    } else {
      table_item = new_table_item;
    }
  }
  return ret;
}
