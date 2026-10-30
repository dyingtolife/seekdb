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

#include "lib/container/ob_array.h"
#include "lib/string/ob_string.h"

namespace oceanbase
{
namespace sql
{

// Minimal read-only Parquet support for the file-scan feature. Only the
// subset produced by the competition fixtures is decoded: single-column
// (or few-column) PLAIN/RLE-encoded values of INT32/INT64/FLOAT/DOUBLE/
// BOOLEAN/BYTE_ARRAY with UNCOMPRESSED or SNAPPY compression, one or more
// row groups, optional and required fields, no page indexes.
class ObMiniParquetReader
{
public:
  struct ColumnChunkData
  {
    int32_t physical_type_ = 0;      // parquet Type enum value
    common::ObString plain_bytes_;   // decoded plain-encoded values (not decompressed pages)
    int64_t num_values_ = 0;
    bool ok_ = false;
  };

  struct ReadResult
  {
    int64_t num_rows_ = 0;
    common::ObArray<common::ObString> column_names_;
    common::ObArray<ColumnChunkData> columns_;
    bool ok_ = false;
  };

  // Parses the footer of a whole in-memory file (files are expected to be
  // small) and reports the schema: leaf column names in file order, the
  // physical type and value count of the first row group, and the total row
  // count. Values are decoded per row group by read_group_as_rows(); the
  // ColumnChunkData::plain_bytes_ of a ReadResult is therefore left empty.
  static int read_file(const char *buf, int64_t len, ReadResult &result);

  // Decode one row group, converting the columns listed in `needed_cols`
  // (empty means all) into one text cell per value, column-major order:
  // cells = [row0_col0, row0_col1, ..., row1_col0, ...]. `group_cnt` receives
  // the total number of row groups in the file. Returns OB_ITER_END when
  // group_idx is beyond the last group.
  static int read_group_as_rows(const char *buf, int64_t len, int64_t group_idx,
                                int64_t &group_cnt,
                                const common::ObIArray<bool> &needed_cols,
                                common::ObIAllocator &allocator,
                                common::ObArray<common::ObString> &cells);
};

// Minimal SNAPPY raw-format decompressor (decoder only).
class ObSnappyDecoder
{
public:
  // 'src' must contain a complete snappy block; uncompressed length is read
  // from the varint header. Output buffer must be pre-sized by the caller.
  static int decompress(const char *src, int64_t src_len,
                        char *dst, int64_t dst_cap, int64_t &dst_len);
};

}  // namespace sql
}  // namespace oceanbase
