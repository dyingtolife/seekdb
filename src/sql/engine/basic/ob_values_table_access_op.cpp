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

#include "sql/engine/basic/ob_values_table_access_op.h"
#include "sql/engine/ob_exec_context.h"
#include "sql/engine/expr/ob_obj_cast_runtime.h"

namespace oceanbase
{
using namespace common;
namespace sql
{
OB_SERIALIZE_MEMBER_INHERIT(ObValuesTableAccessSpec,
                            ObOpSpec,
                            access_type_,
                            column_exprs_,
                            value_exprs_,
                            start_param_idx_,
                            end_param_idx_,
                            obj_params_,
                            file_format_,
                            file_path_,
                            file_delimiter_,
                            file_quote_,
                            file_escape_,
                            file_nullstr_,
                            file_format_name_,
                            file_has_header_,
                            file_sample_size_,
                            file_sig_size_,
                            file_sig_mtime_sec_,
                            file_sig_mtime_nsec_,
                            file_col_names_,
                            file_col_types_,
                            file_col_nullables_);

ObValuesTableAccessOp::ObValuesTableAccessOp(ObExecContext &exec_ctx,
                                             const ObOpSpec &spec,
                                             ObOpInput *input)
  : ObOperator(exec_ctx, spec, input), datum_caster_(), cm_(CM_NONE),
    row_idx_(0), row_cnt_(0), file_cursor_(NULL), file_alloc_("FileTblBatch"),
    file_batch_idx_(0), file_eof_(false) {}

int ObValuesTableAccessOp::inner_open()
{
  int ret = OB_SUCCESS;
  row_idx_ = 0;
  const int32_t result_flag = 0;
  ObPhysicalPlanCtx *plan_ctx = GET_PHY_PLAN_CTX(ctx_);
  if (OB_ISNULL(plan_ctx) || OB_ISNULL(ctx_.get_sql_ctx())) {
    ret = OB_ERR_UNEXPECTED;
  } else if (OB_UNLIKELY(MY_SPEC.column_exprs_.empty())) {
    ret = OB_ERR_UNEXPECTED;
  } else if (OB_FAIL(datum_caster_.init(eval_ctx_.exec_ctx_))) {
  } else if (OB_FAIL(ObSQLUtils::get_default_cast_mode(false, 0, GET_MY_SESSION(ctx_), cm_))) {
  } else {
    cm_ = cm_ | CM_COLUMN_CONVERT;
    if (OB_SUCC(ret)) {
      if (ObValuesTableDef::FOLD_ACCESS_EXPR == MY_SPEC.access_type_) {
        if (OB_UNLIKELY(MY_SPEC.start_param_idx_ < 0 ||
                        MY_SPEC.start_param_idx_ > MY_SPEC.end_param_idx_ ||
                        MY_SPEC.end_param_idx_ >= plan_ctx->get_param_store().count())) {
          ret = OB_ERR_UNEXPECTED;
        } else {
          const ObObj &first_column_node = plan_ctx->get_param_store().at(MY_SPEC.start_param_idx_);
          if (OB_FAIL(first_column_node.get_real_param_count(row_cnt_))) {
          }
        }
      } else if (ObValuesTableDef::ACCESS_PARAM == MY_SPEC.access_type_) {
        int64_t column_cnt = MY_SPEC.column_exprs_.count();
        row_cnt_ = (MY_SPEC.end_param_idx_ - MY_SPEC.start_param_idx_ + 1) / column_cnt;
      } else if (ObValuesTableDef::ACCESS_OBJ == MY_SPEC.access_type_) {
        int64_t column_cnt = MY_SPEC.column_exprs_.count();
        row_cnt_ = MY_SPEC.obj_params_.count() / column_cnt;
      } else if (ObValuesTableDef::ACCESS_FILE_CURSOR == MY_SPEC.access_type_) {
        bool identity_ok = false;
        if (OB_FAIL(open_file_cursor(identity_ok))) {
          LOG_WARN("failed to open file cursor", K(ret));
        }
      } else {
        int64_t column_cnt = MY_SPEC.column_exprs_.count();
        row_cnt_ = MY_SPEC.value_exprs_.count() / column_cnt;
      }
    }
  }
  return ret;
}

int ObValuesTableAccessOp::open_file_cursor(bool &identity_ok)
{
  int ret = OB_SUCCESS;
  identity_ok = false;
  ObFileParamSpec params;
  params.has_header_ = MY_SPEC.file_has_header_;
  params.sample_size_ = MY_SPEC.file_sample_size_;
  params.delimiter_ = MY_SPEC.file_delimiter_;
  params.quote_ = MY_SPEC.file_quote_;
  params.escape_ = MY_SPEC.file_escape_;
  params.nullstr_ = MY_SPEC.file_nullstr_;
  params.format_ = MY_SPEC.file_format_name_;
  // change detection: the file must still look like it did at resolve time
  ObFileSignature now_sig;
  const ObString path = MY_SPEC.file_path_;
  if (OB_FAIL(stat_file_signature(path, now_sig))) {
    ret = OB_INVALID_ARGUMENT;
    LOG_USER_ERROR(OB_INVALID_ARGUMENT, "file disappeared or became unreadable");
  } else {
    ObFileSignature resolve_sig;
    resolve_sig.size_ = MY_SPEC.file_sig_size_;
    resolve_sig.mtime_sec_ = MY_SPEC.file_sig_mtime_sec_;
    resolve_sig.mtime_nsec_ = MY_SPEC.file_sig_mtime_nsec_;
    if (resolve_sig.is_valid() && now_sig != resolve_sig) {
      ret = OB_INVALID_ARGUMENT;
      LOG_USER_ERROR(OB_INVALID_ARGUMENT, "file changed between resolve and execution");
    }
  }
  // column pruning mask: columns beyond the projected count are not decoded
  ObSEArray<bool, 8> needed;
  const int64_t col_cnt = MY_SPEC.file_col_names_.count();
  for (int64_t c = 0; OB_SUCC(ret) && c < col_cnt; ++c) {
    if (OB_FAIL(needed.push_back(c < MY_SPEC.column_exprs_.count()))) {}
  }
  if (OB_FAIL(ret)) {
  } else if (OB_FAIL(make_file_row_cursor(
                 static_cast<ObFileSourceFormat>(MY_SPEC.file_format_),
                 path, params, needed, file_alloc_, file_cursor_))) {
    LOG_USER_ERROR(OB_INVALID_ARGUMENT, "cannot open file");
  } else if (OB_ISNULL(file_cursor_)) {
    ret = OB_ERR_UNEXPECTED;
  } else if (OB_FAIL(file_cursor_->open())) {
    LOG_USER_ERROR(OB_INVALID_ARGUMENT, "cannot read file");
  } else {
    file_open_sig_ = now_sig;
    identity_ok = true;
  }
  return ret;
}

int ObValuesTableAccessOp::refill_file_batch()
{
  int ret = OB_SUCCESS;
  file_batch_.reset();
  file_alloc_.reuse();
  file_batch_idx_ = 0;
  const int64_t col_cnt = MY_SPEC.file_col_names_.count();
  ObFileRow row;
  while (OB_SUCC(ret) && file_batch_.count() < kFileBatchRows * col_cnt) {
    row.cells_.reset();
    int gr = file_cursor_->next_row(row);
    if (OB_ITER_END == gr) {
      file_eof_ = true;
      break;
    } else if (OB_INVALID_DATA == gr) {
      ret = OB_INVALID_DATA;
      LOG_USER_ERROR(OB_INVALID_DATA, "bad row in file, at line %ld", file_cursor_->current_line());
    } else if (OB_SUCCESS != gr) {
      ret = gr;
    } else {
      // convert to typed ObObjParams; JSONL rows carry [k,v,...] pairs
      const bool is_jsonl = static_cast<ObFileSourceFormat>(MY_SPEC.file_format_)
          == ObFileSourceFormat::JSONL;
      for (int64_t c = 0; OB_SUCC(ret) && c < col_cnt; ++c) {
        ObObjParam op;
        ObString raw;
        bool is_null = true;
        if (is_jsonl) {
          const ObString &key = MY_SPEC.file_col_names_.at(c);
          for (int64_t i = 0; i + 1 < row.cells_.count(); i += 2) {
            if (row.cells_.at(i) == key) {
              raw = row.cells_.at(i + 1);
              is_null = raw.empty();
              break;
            }
          }
        } else if (c < row.cells_.count()) {
          raw = row.cells_.at(c);
          is_null = raw.empty();
        }
        if (is_null) {
          op.set_null();
        } else if (OB_FAIL(file_cell_to_obj(
                static_cast<ObFileColumnType>(MY_SPEC.file_col_types_.at(c)),
                raw, false, file_alloc_, op))) {
          op.set_null();
        }
        if (OB_SUCC(ret) && OB_FAIL(file_batch_.push_back(op))) {
        }
      }
    }
  }
  return ret;
}

void ObValuesTableAccessOp::close_file_cursor()
{
  if (NULL != file_cursor_) {
    file_cursor_->close();
    file_alloc_.free(file_cursor_);
    file_cursor_ = NULL;
  }
  file_batch_.reset();
  file_batch_idx_ = 0;
  file_eof_ = false;
}

int ObValuesTableAccessOp::inner_rescan()
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(ObOperator::inner_rescan())) {
  } else if (ObValuesTableDef::ACCESS_FILE_CURSOR == MY_SPEC.access_type_) {
    // identity check: the file must be unchanged since open; then restart
    ObFileSignature now_sig;
    if (OB_FAIL(stat_file_signature(MY_SPEC.file_path_, now_sig))) {
      ret = OB_INVALID_ARGUMENT;
      LOG_USER_ERROR(OB_INVALID_ARGUMENT, "file disappeared on rescan");
    } else if (now_sig != file_open_sig_) {
      ret = OB_INVALID_ARGUMENT;
      LOG_USER_ERROR(OB_INVALID_ARGUMENT, "file changed during execution");
    } else {
      close_file_cursor();
      bool identity_ok = false;
      if (OB_FAIL(open_file_cursor(identity_ok))) {
        LOG_WARN("failed to reopen file cursor on rescan", K(ret));
      }
    }
    row_idx_ = 0;
    file_batch_idx_ = 0;
    file_batch_.reset();
    file_eof_ = false;
  } else {
    row_idx_ = 0;
  }
  return ret;
}

int ObValuesTableAccessOp::switch_iterator()
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(ObOperator::inner_switch_iterator())) {
  } else if (ObValuesTableDef::ACCESS_FILE_CURSOR == MY_SPEC.access_type_) {
    // restart the stream (same statement scan): close and re-open
    row_idx_ = 0;
    file_batch_idx_ = 0;
    file_batch_.reset();
    file_eof_ = false;
    if (NULL != file_cursor_) {
      close_file_cursor();
      bool identity_ok = false;
      if (OB_FAIL(open_file_cursor(identity_ok))) {
        LOG_WARN("failed to reopen file cursor on switch_iterator", K(ret));
      }
    }
  } else {
    row_idx_ = 0;
  }
  return ret;
}

int ObValuesTableAccessOp::inner_get_next_row()
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(try_check_status())) {
  } else {
    do {
      clear_evaluated_flag();
      if (OB_FAIL(calc_next_row())) {
        if(OB_ITER_END != ret) {
        }
      } else {
        LOG_DEBUG("output row", "row", ROWEXPR2STR(eval_ctx_, MY_SPEC.output_));
      }
      break;
    } while (OB_SUCC(ret));
  }
  return ret;
}

void ObValuesTableAccessOp::update_src_meta(const ObObjMeta &src_obj_meta,
                                            const ObAccuracy &src_obj_acc,
                                            ObDatumMeta &src_meta)
{
  src_meta.type_ = src_obj_meta.get_type();
  src_meta.cs_type_ = src_obj_meta.get_collation_type();
  src_meta.scale_ = src_obj_acc.get_scale();
  src_meta.precision_ = src_obj_acc.get_precision();
}

int ObValuesTableAccessOp::get_real_src_obj_type(const int64_t row_idx,
                                                 ObExpr &src_expr,
                                                 ObDatumMeta &src_meta,
                                                 ObObjMeta &src_obj_meta)
{
  int ret = OB_SUCCESS;
  if (T_QUESTIONMARK == src_expr.type_) {
    int64_t param_idx = src_expr.extra_;
    ObPhysicalPlanCtx *plan_ctx = GET_PHY_PLAN_CTX(ctx_);
    const ObSqlArrayObj *array_obj = NULL;
    if (OB_UNLIKELY(param_idx < 0 || param_idx >= plan_ctx->get_param_store().count())) {
      ret = OB_INVALID_ARGUMENT;
    } else if (plan_ctx->get_param_store().at(param_idx).is_ext_sql_array()) {
      // If the parameter is is_ext_sql_array
      if (OB_ISNULL(array_obj = reinterpret_cast<const ObSqlArrayObj*>(
                                            plan_ctx->get_param_store().at(param_idx).get_ext()))) {
        ret = OB_ERR_UNEXPECTED;
      } else if (array_obj->count_ <= row_idx) {
        ret = OB_ERR_UNEXPECTED;
      } else {
        src_obj_meta = array_obj->data_[row_idx].meta_;
        const ObAccuracy &src_obj_acc = array_obj->data_[row_idx].get_accuracy();
        update_src_meta(src_obj_meta, src_obj_acc, src_meta);
      }
    } else if (src_expr.frame_idx_ < spec_.plan_->get_expr_frame_info().const_frame_.count() +
                                     spec_.plan_->get_expr_frame_info().param_frame_.count()) {
      src_obj_meta = plan_ctx->get_param_store().at(param_idx).meta_;
      const ObAccuracy &src_obj_acc = plan_ctx->get_param_store().at(param_idx).get_accuracy();
      update_src_meta(src_obj_meta, src_obj_acc, src_meta);
    }
  }
  return ret;
}

int ObValuesTableAccessOp::calc_datum_from_expr(const int64_t col_idx,
                                                const int64_t row_idx,
                                                ObExpr *src_expr,
                                                ObExpr *dst_expr)
{
  int ret = OB_SUCCESS;
  ObDatumMeta src_meta = src_expr->datum_meta_;
  ObObjMeta src_obj_meta = src_expr->obj_meta_;
  ObDatum *datum = NULL;
  bool need_adjust_decimal_int = false;
  if (src_expr == dst_expr) {
    /* actually should not happened */
  } else if (T_QUESTIONMARK == src_expr->type_ &&
             OB_FAIL(get_real_src_obj_type(row_idx, *src_expr, src_meta, src_obj_meta))) {
    LOG_WARN("failed to get real obj type");
  } else {
    need_adjust_decimal_int = src_meta.type_ == ObDecimalIntType &&
                              dst_expr->datum_meta_.type_ == ObDecimalIntType &&
                              ObDatumCast::need_scale_decimalint(src_meta.scale_,
                                                                 src_meta.precision_,
                                                                 dst_expr->datum_meta_.scale_,
                                                                 dst_expr->datum_meta_.precision_);
  }
  if (OB_FAIL(ret)) {
  } else if (src_meta.type_ == dst_expr->datum_meta_.type_ &&
             src_meta.cs_type_ == dst_expr->datum_meta_.cs_type_ &&
             !need_adjust_decimal_int) {
    // when type and collation_type is same. src has the same type to dst;
    if (OB_FAIL(src_expr->eval(eval_ctx_, datum))) {
    } else {
      dst_expr->locate_datum_for_write(eval_ctx_) = *datum;
      dst_expr->set_evaluated_projected(eval_ctx_);
    }
  } else {
    if (OB_FAIL(ObCharset::check_valid_implicit_convert(src_meta.cs_type_,
                                                        dst_expr->datum_meta_.cs_type_))) {
    } else {
      ObExpr real_src_expr = *src_expr;
      real_src_expr.datum_meta_ = src_meta;
      real_src_expr.obj_meta_ = src_obj_meta;
      real_src_expr.obj_datum_map_ = ObDatum::get_obj_datum_map_type(src_meta.type_);
      if (OB_FAIL(datum_caster_.to_type(dst_expr->datum_meta_, real_src_expr, cm_, datum))) {
      } else {
        ObDatum &dst_datum = dst_expr->locate_datum_for_write(eval_ctx_);
        if (ObObjDatumMapType::OBJ_DATUM_STRING == dst_expr->obj_datum_map_) {
          ObExprStrResAlloc res_alloc(*dst_expr, eval_ctx_);
          if (OB_FAIL(dst_datum.deep_copy(*datum, res_alloc))) {
          }
        } else {
          ObDataBuffer res_alloc(const_cast<char*>(dst_datum.ptr_), dst_expr->res_buf_len_);
          if (OB_FAIL(dst_datum.deep_copy(*datum, res_alloc))) {
          }
        }
        dst_expr->set_evaluated_projected(eval_ctx_);
      }
    }
  }
  return ret;
}

int ObValuesTableAccessOp::calc_datum_from_param(const ObObj &src_obj, ObExpr *dst_expr)
{
  int ret = OB_SUCCESS;
  ObDatum &dst_datum = dst_expr->locate_datum_for_write(eval_ctx_);
  const ObObjType &src_type = src_obj.get_type();
  const ObObjType &dst_type =  dst_expr->obj_meta_.get_type();
  const ObObjParam src_obj_param = static_cast<const ObObjParam>(src_obj);
  bool need_adjust_decimal_int = ob_is_decimal_int_tc(src_type)
                                 && ob_is_decimal_int_tc(dst_expr->datum_meta_.type_)
                                 && ObDatumCast::need_scale_decimalint(src_obj_param.get_scale(),
                                                                       src_obj_param.get_precision(),
                                                                       dst_expr->datum_meta_.scale_,
                                                                       dst_expr->datum_meta_.precision_);
  if (ob_is_decimal_int_tc(src_type) && (ob_is_number_tc(dst_type) || need_adjust_decimal_int)) {
    ObObj dst_obj;
    const ObDataTypeCastParams dtc_params = ObBasicSessionInfo::create_dtc_params(GET_MY_SESSION(ctx_));
    ObAccuracy dst_accuracy = ObAccuracy::DDL_DEFAULT_ACCURACY2[0][dst_type];
    if (ob_is_decimal_int_tc(dst_type)) {
      dst_accuracy.set_scale(dst_expr->datum_meta_.scale_);
      dst_accuracy.set_precision(dst_expr->datum_meta_.precision_);
    }
    ObCastCtx cast_ctx(&eval_ctx_.exec_ctx_.get_allocator(), &dtc_params, cm_, dst_expr->obj_meta_.get_collation_type(), &dst_accuracy);
    ObSqlObjCastRuntime cast_runtime(&eval_ctx_.exec_ctx_);
    cast_ctx.runtime_ = &cast_runtime;
    if (OB_FAIL(ObObjCaster::to_type(dst_type, dst_expr->obj_meta_.get_collation_type(), cast_ctx,
                                     src_obj, dst_obj))) {
    } else if (OB_FAIL(dst_datum.from_obj(dst_obj))) {
    } else {
      dst_expr->set_evaluated_projected(eval_ctx_);
    }
  } else if (OB_FAIL(dst_datum.from_obj(src_obj))) {
  } else {
    dst_expr->set_evaluated_projected(eval_ctx_);
  }
  return ret;
}

OB_INLINE int ObValuesTableAccessOp::calc_next_row()
{
  int ret = OB_SUCCESS;
  int64_t col_num = MY_SPEC.column_exprs_.count();
  int64_t col_idx = 0;
  ObPhysicalPlanCtx *plan_ctx = GET_PHY_PLAN_CTX(ctx_);
  if (ObValuesTableDef::ACCESS_FILE_CURSOR == MY_SPEC.access_type_) {
    // streaming: pull the next batch of typed objects when exhausted
    while (OB_SUCC(ret) && file_batch_idx_ >= file_batch_.count() && !file_eof_) {
      if (OB_FAIL(refill_file_batch())) {
      }
    }
    if (OB_FAIL(ret)) {
    } else if (file_batch_idx_ >= file_batch_.count()) {
      ret = OB_ITER_END;
    }
  } else if (row_idx_ == row_cnt_) {
    ret = OB_ITER_END;
  } else if (ObValuesTableDef::FOLD_ACCESS_EXPR == MY_SPEC.access_type_ &&
             OB_FAIL(plan_ctx->replace_batch_param_datum(row_idx_,
                                          MY_SPEC.start_param_idx_,
                                          MY_SPEC.end_param_idx_ - MY_SPEC.start_param_idx_ + 1))) {
  }
  if (OB_SUCC(ret) && OB_ITER_END != ret) {
    while (OB_SUCC(ret) && col_idx < col_num) {
      ObExpr *col_expr = MY_SPEC.column_exprs_.at(col_idx);
      switch (MY_SPEC.access_type_)
      {
        case ObValuesTableDef::ACCESS_EXPR : {
          int64_t idx = row_idx_ * col_num + col_idx;
          ObExpr *src_expr = MY_SPEC.value_exprs_.at(idx);
          if (OB_FAIL(calc_datum_from_expr(col_idx, row_idx_, src_expr, col_expr))) {
          }
          break;
        }
        case ObValuesTableDef::FOLD_ACCESS_EXPR : {
          ObExpr *src_expr = MY_SPEC.value_exprs_.at(col_idx);
          if (OB_FAIL(calc_datum_from_expr(col_idx, row_idx_, src_expr, col_expr))) {
          }
          break;
        }
        case ObValuesTableDef::ACCESS_PARAM: {
          int64_t param_idx = MY_SPEC.start_param_idx_ + col_idx + row_idx_ * col_num;
          const ObObjParam &param = plan_ctx->get_param_store().at(param_idx);
          if (OB_FAIL(calc_datum_from_param(param, col_expr))) {
          }
          break;
        }
        case ObValuesTableDef::ACCESS_OBJ: {
          int64_t idx = row_idx_ * col_num + col_idx;
          const ObObj &param = MY_SPEC.obj_params_.at(idx);
          if (OB_FAIL(calc_datum_from_param(param, col_expr))) {
          }
          break;
        }
        case ObValuesTableDef::ACCESS_FILE_CURSOR: {
          int64_t idx = file_batch_idx_ + col_idx;
          if (idx >= file_batch_.count()) {
            ret = OB_ERR_UNEXPECTED;
          } else {
            const ObObj &param = file_batch_.at(idx);
            if (OB_FAIL(calc_datum_from_param(param, col_expr))) {
            }
          }
          break;
        }
      };
      col_idx++;
    } // while end

    for (int64_t i = 0; OB_SUCC(ret) && i < MY_SPEC.output_.count(); i++) {
      ObExpr *output = MY_SPEC.output_.at(i);
      if (i < col_num && output == MY_SPEC.column_exprs_.at(i)) {
        /* need do nothing */
      } else {
        ObDatum *dst_datum = NULL;
        MY_SPEC.output_.at(i)->eval(eval_ctx_, dst_datum);
      }
    }
    if (OB_SUCC(ret)) {
      if (ObValuesTableDef::ACCESS_FILE_CURSOR == MY_SPEC.access_type_) {
        file_batch_idx_ += col_num;
      } else {
        ++row_idx_;
      }
    }
  }
  return ret;
}

int ObValuesTableAccessOp::inner_close()
{
  int ret = OB_SUCCESS;
  row_idx_ = 0;
  if (ObValuesTableDef::ACCESS_FILE_CURSOR == MY_SPEC.access_type_) {
    close_file_cursor();
  }
  if (OB_FAIL(datum_caster_.destroy())) {
  }
  return ret;
}
} // end namespace sql
} // end namespace oceanbase
