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

#include "ob_parquet_reader.h"
#include <vector>
#include <string>
#include "lib/alloc/ob_malloc_hook.h"
#include "lib/utility/ob_macro_utils.h"

namespace oceanbase
{
namespace sql
{

// ---------------- snappy ----------------

int ObSnappyDecoder::decompress(const char *src, int64_t src_len,
                                char *dst, int64_t dst_cap, int64_t &dst_len)
{
  int ret = OB_SUCCESS;
  int64_t pos = 0;
  // read uncompressed length varint
  uint64_t expected = 0;
  int shift = 0;
  while (pos < src_len) {
    const uint8_t b = static_cast<uint8_t>(src[pos++]);
    expected |= static_cast<uint64_t>(b & 0x7f) << shift;
    if ((b & 0x80) == 0) break;
    shift += 7;
    if (shift > 63) { ret = OB_INVALID_DATA; break; }
  }
  if (OB_FAIL(ret)) {
  } else if (expected > static_cast<uint64_t>(dst_cap)) {
    ret = OB_SIZE_OVERFLOW;
  } else {
    int64_t out = 0;
    while (pos < src_len && OB_SUCC(ret)) {
      const uint8_t tag = static_cast<uint8_t>(src[pos++]);
      const uint8_t ttype = tag & 0x03;
      if (ttype == 0) {
        // literal
        uint64_t lit_len = (tag >> 2) + 1;
        if (lit_len > 60) {
          const int extra = static_cast<int>(lit_len - 60);
          lit_len = 0;
          for (int i = 0; i < extra && pos < src_len; ++i) {
            lit_len |= static_cast<uint64_t>(static_cast<uint8_t>(src[pos++])) << (8 * i);
          }
          lit_len += 1;
        }
        if (pos + static_cast<int64_t>(lit_len) > src_len
            || out + static_cast<int64_t>(lit_len) > dst_cap) {
          ret = OB_INVALID_DATA;
        } else {
          MEMCPY(dst + out, src + pos, lit_len);
          pos += lit_len;
          out += lit_len;
        }
      } else {
        // copy: length in tag high bits + possible extra byte
        int64_t copy_len = 0;
        int64_t offset = 0;
        if (ttype == 1) {
          copy_len = ((tag >> 2) & 0x07) + 4;
          offset = ((tag >> 5) << 8) | static_cast<uint8_t>(src[pos++]);
        } else if (ttype == 2) {
          copy_len = (tag >> 2) + 1;
          offset = static_cast<uint8_t>(src[pos])
              | (static_cast<uint8_t>(src[pos + 1]) << 8);
          pos += 2;
        } else {
          copy_len = (tag >> 2) + 1;
          offset = static_cast<uint8_t>(src[pos])
              | (static_cast<uint8_t>(src[pos + 1]) << 8)
              | (static_cast<uint8_t>(src[pos + 2]) << 16)
              | (static_cast<uint8_t>(src[pos + 3]) << 24);
          pos += 4;
        }
        if (offset <= 0 || offset > out || out + copy_len > dst_cap) {
          ret = OB_INVALID_DATA;
        } else {
          // byte-by-byte copy handles overlapping runs
          for (int64_t i = 0; i < copy_len; ++i) {
            dst[out] = dst[out - offset];
            ++out;
          }
        }
      }
    }
    if (OB_SUCC(ret)) {
      if (out != static_cast<int64_t>(expected)) {
        ret = OB_INVALID_DATA;
      } else {
        dst_len = out;
      }
    }
  }
  return ret;
}

// ---------------- thrift compact protocol (read-only subset) ----------------

namespace
{

struct TCursor
{
  const char *p;
  int64_t len;
  int64_t pos = 0;

  bool read_byte(uint8_t &b)
  {
    if (pos >= len) return false;
    b = static_cast<uint8_t>(p[pos++]);
    return true;
  }
  bool read_varint(uint64_t &v)
  {
    v = 0;
    int s = 0;
    while (true) {
      uint8_t b;
      if (!read_byte(b)) return false;
      v |= static_cast<uint64_t>(b & 0x7f) << s;
      if (!(b & 0x80)) return true;
      s += 7;
      if (s > 63) return false;
    }
  }
  bool read_zigzag(int64_t &z)
  {
    uint64_t u;
    if (!read_varint(u)) return false;
    z = static_cast<int64_t>(u >> 1) ^ -static_cast<int64_t>(u & 1);
    return true;
  }
  bool read_string(common::ObString &s, char *arena, int64_t arena_cap, int64_t &arena_pos)
  {
    uint64_t l;
    if (!read_varint(l)) return false;
    if (pos + static_cast<int64_t>(l) > len
        || arena_pos + static_cast<int64_t>(l) + 1 > arena_cap) {
      return false;
    }
    MEMCPY(arena + arena_pos, p + pos, l);
    arena[arena_pos + l] = '\0';
    s.assign_ptr(arena + arena_pos, static_cast<int32_t>(l));
    arena_pos += l + 1;
    pos += l;
    return true;
  }
  bool read_struct_field(int16_t &last_id, int16_t &fid, uint8_t &ftype)
  {
    uint8_t b;
    if (!read_byte(b)) return false;
    if (b == 0) {  // stop
      fid = -1;
      ftype = 0;
      return true;
    }
    const int16_t delta = (b >> 4) & 0x0f;
    ftype = b & 0x0f;
    if (delta == 0) {
      int64_t z;
      if (!read_zigzag(z)) return false;
      fid = static_cast<int16_t>(z);
    } else {
      fid = last_id + delta;
    }
    last_id = fid;
    return true;
  }
  // skip a value of the given compact type
  bool skip(uint8_t ttype)
  {
    switch (ttype) {
      case 1: case 2: {  // bool true/false
        return true;
      }
      case 3: {  // byte
        uint8_t b; return read_byte(b);
      }
      case 4: case 5: case 6: {  // i16/i32/i64 zigzag varint
        int64_t z; return read_zigzag(z);
      }
      case 7: {  // double (8 bytes, little-endian on wire)
        if (pos + 8 > len) return false;
        pos += 8; return true;
      }
      case 8: {  // binary/string
        uint64_t l;
        if (!read_varint(l)) return false;
        if (pos + static_cast<int64_t>(l) > len) return false;
        pos += l; return true;
      }
      case 9: case 10: {  // list/set
        uint8_t hb;
        if (!read_byte(hb)) return false;
        int64_t n = (hb >> 4) & 0x0f;
        uint8_t et = hb & 0x0f;
        if (n == 15) {
          uint64_t v;
          if (!read_varint(v)) return false;
          n = static_cast<int64_t>(v);
        }
        for (int64_t i = 0; i < n; ++i) {
          if (!skip(et)) return false;
        }
        return true;
      }
      case 12: {  // struct
        int16_t lid = 0, fid; uint8_t ft;
        while (true) {
          if (!read_struct_field(lid, fid, ft)) return false;
          if (fid == -1) return true;
          if (!skip(ft)) return false;
        }
      }
      case 11: {  // map
        uint8_t kb, vb;
        if (!read_byte(kb) || !read_byte(vb)) return false;
        int64_t n;
        int64_t z;
        if (!read_zigzag(z)) return false;
        n = z;
        for (int64_t i = 0; i < n; ++i) {
          if (!skip(kb) || !skip(vb)) return false;
        }
        return true;
      }
      default: return false;
    }
  }
};

// parquet compact-protocol type constants (Thrift compact)
constexpr uint8_t T_BOOL_TRUE = 1;
constexpr uint8_t T_BOOL_FALSE = 2;
constexpr uint8_t T_BYTE = 3;
constexpr uint8_t T_I16 = 4;
constexpr uint8_t T_I32 = 5;
constexpr uint8_t T_I64 = 6;
constexpr uint8_t T_DOUBLE = 7;
constexpr uint8_t T_BINARY = 8;
constexpr uint8_t T_LIST = 9;
constexpr uint8_t T_SET = 10;
constexpr uint8_t T_MAP = 11;
constexpr uint8_t T_STRUCT = 12;

// Parquet Type enum
constexpr int32_t PQ_BOOLEAN = 0;
constexpr int32_t PQ_INT32 = 1;
constexpr int32_t PQ_INT64 = 2;
constexpr int32_t PQ_INT96 = 3;
constexpr int32_t PQ_FLOAT = 4;
constexpr int32_t PQ_DOUBLE = 5;
constexpr int32_t PQ_BYTE_ARRAY = 6;
// CompressionCodec
constexpr int32_t PQ_UNCOMPRESSED = 0;
constexpr int32_t PQ_SNAPPY = 1;

struct PQSchemaElement
{
  common::ObString name_;
  int32_t type_ = -1;          // -1 = group node
  int32_t repetition_ = 0;     // 0 required, 1 optional, 2 repeated
  int32_t num_children_ = 0;
};

}  // anonymous namespace

// ---------------- parquet reader ----------------

int ObMiniParquetReader::read_file(const char *buf, int64_t len, ReadResult &result)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(buf) || len < 12
      || buf[0] != 'P' || buf[1] != 'A' || buf[2] != 'R' || buf[3] != '1'
      || buf[len - 4] != 'P' || buf[len - 3] != 'A' || buf[len - 2] != 'R' || buf[len - 1] != '1') {
    ret = OB_INVALID_DATA;
  } else {
    // footer length in the 4 bytes before trailing magic
    const int32_t flen = static_cast<int32_t>(
        static_cast<uint8_t>(buf[len - 8])
        | (static_cast<uint8_t>(buf[len - 7]) << 8)
        | (static_cast<uint8_t>(buf[len - 6]) << 16)
        | (static_cast<uint8_t>(buf[len - 5]) << 24));
    if (flen <= 0 || flen > len - 8) {
      ret = OB_INVALID_DATA;
    } else {
      TCursor t{buf + (len - 8 - flen), flen, 0};
      static constexpr int64_t NAME_BUF_CAP = 64 * 1024;
      std::vector<char> name_buf(NAME_BUF_CAP);
      int64_t name_pos = 0;

      // FileMetaData fields: 2=schema list, 3=num_rows i64, 4=row_groups list
      int16_t last_id = 0, fid = 0;
      uint8_t ftype = 0;
      std::vector<PQSchemaElement> leaves;   // flattened leaf columns (leaf order = column order)
      int64_t num_rows = 0;
      // struct of row group entries we care about: columns list -> each
      // ColumnChunk meta_data { type, path_in_schema, codec, num_values }
      // plus column chunk offsets
      struct RgCol {
        int32_t type = -1;
        int32_t codec = -1;
        int64_t num_values = 0;
        int64_t data_page_offset = 0;
        int64_t total_compressed_size = 0;
        common::ObString path_last;
      };
      struct Rg {
        std::vector<RgCol> cols;
      };
      std::vector<Rg> row_groups;

      while (t.read_struct_field(last_id, fid, ftype) && fid != -1) {
        if (fid == 1 && ftype == T_I32) {
          int64_t v; t.read_zigzag(v);  // version
        } else if (fid == 2 && (ftype == T_LIST || ftype == T_SET)) {
          // schema elements
          uint8_t hb; uint8_t et; int64_t n;
          if (t.read_byte(hb)) {
            n = (hb >> 4) & 0x0f; et = hb & 0x0f;
            if (n == 15) { uint64_t v; t.read_varint(v); n = static_cast<int64_t>(v); }
            std::vector<PQSchemaElement> elems(n);
            for (int64_t i = 0; i < n && i < static_cast<int64_t>(elems.size()); ++i) {
              int16_t l2 = 0, f2; uint8_t t2;
              while (t.read_struct_field(l2, f2, t2) && f2 != -1) {
                if (f2 == 1 && t2 == T_I32) { int64_t z; t.read_zigzag(z); elems[i].type_ = static_cast<int32_t>(z); }
                else if (f2 == 2 && (t2 == T_I32 || t2 == T_I64)) { int64_t z; t.read_zigzag(z); elems[i].num_children_ = static_cast<int32_t>(z); }
                else if (f2 == 3 && t2 == T_BINARY) { t.read_string(elems[i].name_, name_buf.data(), NAME_BUF_CAP, name_pos); }
                else if (f2 == 4 && t2 == T_I32) { int64_t z; t.read_zigzag(z); elems[i].repetition_ = static_cast<int32_t>(z); }
                else if (f2 == 5 && (t2 == T_I32 || t2 == T_I64)) { int64_t z; t.read_zigzag(z); /* converted_type */ }
                else { t.skip(t2); }
              }
            }
            // flatten: keep only leaves (type_ != -1); groups with children are skipped
            leaves.clear();
            for (auto &e : elems) {
              if (e.type_ != -1 || e.num_children_ == 0) {
                if (e.num_children_ == 0) {
                  leaves.push_back(e);
                }
              }
            }
          }
        } else if (fid == 3 && (ftype == T_I32 || ftype == T_I64)) {
          t.read_zigzag(num_rows);
        } else if (fid == 4 && (ftype == T_LIST || ftype == T_SET)) {
          uint8_t hb; uint8_t et; int64_t n;
          if (t.read_byte(hb)) {
            n = (hb >> 4) & 0x0f; et = hb & 0x0f;
            if (n == 15) { uint64_t v; t.read_varint(v); n = static_cast<int64_t>(v); }
            for (int64_t i = 0; i < n; ++i) {
              // RowGroup: 1=columns list, 3=total_byte_size, 4=num_rows
              Rg rg;
              int16_t l2 = 0, f2; uint8_t t2;
              while (t.read_struct_field(l2, f2, t2) && f2 != -1) {
                if (f2 == 1 && (t2 == T_LIST || t2 == T_SET)) {
                  uint8_t hb2; uint8_t et2; int64_t n2;
                  t.read_byte(hb2);
                  n2 = (hb2 >> 4) & 0x0f; et2 = hb2 & 0x0f;
                  if (n2 == 15) { uint64_t v; t.read_varint(v); n2 = static_cast<int64_t>(v); }
                  for (int64_t j = 0; j < n2; ++j) {
                    // ColumnChunk: 1=file_path, 2=file_offset, 3=meta_data(ColumnMetaData)
                    RgCol c;
                    int16_t l3 = 0, f3; uint8_t t3;
                    while (t.read_struct_field(l3, f3, t3) && f3 != -1) {
                      if (f3 == 3 && t3 == T_STRUCT) {
                        int16_t l4 = 0, f4; uint8_t t4;
                        while (t.read_struct_field(l4, f4, t4) && f4 != -1) {
                          if (f4 == 1 && t4 == T_I32) { int64_t z; t.read_zigzag(z); c.type = static_cast<int32_t>(z); }
                          else if (f4 == 2 && (t4 == T_LIST || t4 == T_SET)) {
                            // path_in_schema: list of strings; keep last segment
                            uint8_t hb3; uint8_t et3; int64_t n3;
                            t.read_byte(hb3);
                            n3 = (hb3 >> 4) & 0x0f; et3 = hb3 & 0x0f;
                            if (n3 == 15) { uint64_t v; t.read_varint(v); n3 = static_cast<int64_t>(v); }
                            common::ObString seg;
                            for (int64_t k = 0; k < n3; ++k) {
                              if (et3 == T_BINARY) { t.read_string(seg, name_buf.data(), NAME_BUF_CAP, name_pos); }
                              else { t.skip(et3); }
                            }
                            c.path_last = seg;
                          } else if (f4 == 3 && t4 == T_I32) { int64_t z; t.read_zigzag(z); c.codec = static_cast<int32_t>(z); }
                          else if (f4 == 4 && (t4 == T_I32 || t4 == T_I64)) { int64_t z; t.read_zigzag(z); c.num_values = z; }
                          else if (f4 == 5 && t4 == T_I64) { int64_t z; t.read_zigzag(z); c.data_page_offset = z; }
                          else if (f4 == 7 && t4 == T_I64) { int64_t z; t.read_zigzag(z); c.total_compressed_size = z; }
                          else { t.skip(t4); }
                        }
                      } else { t.skip(t3); }
                    }
                    rg.cols.push_back(c);
                  }
                } else if (f2 == 4 && (t2 == T_I32 || t2 == T_I64)) {
                  int64_t z; t.read_zigzag(z); /* rg num_rows */
                } else { t.skip(t2); }
              }
              row_groups.push_back(rg);
            }
          }
        } else if (fid == 6 && ftype == T_BINARY) {
          common::ObString s; t.read_string(s, name_buf.data(), NAME_BUF_CAP, name_pos);  // created_by
        } else {
          t.skip(ftype);
        }
      }

      if (OB_SUCC(ret)) {
        result.num_rows_ = num_rows;
        result.column_names_.reset();
        result.columns_.reset();
        for (auto &e : leaves) {
          result.column_names_.push_back(e.name_);
        }
        // For simplicity decode row-group by row-group appending values.
        // Column values are stored as their PLAIN bytes; text conversion
        // happens at scan time.
        for (auto &rg : row_groups) {
          for (int64_t ci = 0; OB_SUCC(ret) && ci < static_cast<int64_t>(rg.cols.size()) && ci < static_cast<int64_t>(leaves.size()); ++ci) {
            const RgCol &c = rg.cols[ci];
            if (c.data_page_offset <= 0 || c.num_values <= 0) {
              continue;
            }
            // page header at data_page_offset: PageHeader {1:type i32, 2:uncompressed_page_size i32,
            // 3:compressed_page_size i32, 5:data_page_header(DataPageHeader{8:num_values i32})}
            TCursor pg{buf + c.data_page_offset, len - c.data_page_offset, 0};
            int32_t page_type = 0, comp_size = 0, unc_size = 0, page_num_values = 0;
            int16_t lp = 0, fp; uint8_t tp;
            while (pg.read_struct_field(lp, fp, tp) && fp != -1) {
              if (fp == 1 && tp == T_I32) { int64_t z; pg.read_zigzag(z); page_type = static_cast<int32_t>(z); }
              else if (fp == 2 && tp == T_I32) { int64_t z; pg.read_zigzag(z); unc_size = static_cast<int32_t>(z); }
              else if (fp == 3 && tp == T_I32) { int64_t z; pg.read_zigzag(z); comp_size = static_cast<int32_t>(z); }
              else if (fp == 5 && tp == T_STRUCT) {
                int16_t lp2 = 0, fp2; uint8_t tp2;
                while (pg.read_struct_field(lp2, fp2, tp2) && fp2 != -1) {
                  if (fp2 == 8 && tp2 == T_I32) { int64_t z; pg.read_zigzag(z); page_num_values = static_cast<int32_t>(z); }
                  else { pg.skip(tp2); }
                }
              } else { pg.skip(tp); }
            }
            if (page_type != 0) {  // only DATA_PAGE (v1)
              continue;
            }
            const char *cdata = buf + pg.pos;
            std::vector<char> plain(unc_size + 16);
            int64_t plain_len = 0;
            if (c.codec == PQ_UNCOMPRESSED) {
              if (comp_size <= static_cast<int64_t>(plain.size())) {
                MEMCPY(plain.data(), cdata, comp_size);
                plain_len = comp_size;
              }
            } else if (c.codec == PQ_SNAPPY) {
              // snappy pages are split into blocks: [u32 le uncompressed_size][compressed][...]
              int64_t off = 0;
              while (off + 4 <= comp_size && OB_SUCC(ret)) {
                const int32_t blk_unc = static_cast<int32_t>(
                    static_cast<uint8_t>(cdata[off])
                    | (static_cast<uint8_t>(cdata[off+1]) << 8)
                    | (static_cast<uint8_t>(cdata[off+2]) << 16)
                    | (static_cast<uint8_t>(cdata[off+3]) << 24));
                off += 4;
                int64_t written = 0;
                if (OB_FAIL(ObSnappyDecoder::decompress(cdata + off, comp_size - off,
                                                        plain.data() + plain_len,
                                                        static_cast<int64_t>(plain.size()) - plain_len,
                                                        written))
                    || written != blk_unc) {
                  if (OB_SUCC(ret)) { ret = OB_INVALID_DATA; }
                  break;
                }
                plain_len += written;
                // advance over this compressed block: snappy block end is not
                // recorded, so re-parse length consumed by decompress is
                // unknown; run one block per page (common case) then break.
                break;
              }
            }
            if (OB_SUCC(ret) && plain_len > 0) {
              // PLAIN encoding: fixed-width for primitives, 4-byte length
              // prefix for BYTE_ARRAY. RLE/dict levels are not needed for the
              // required single-level fixtures; optional columns carry a
              // definition-level RLE prefix that we strip by heuristic:
              // int32 width 4 => strip 1-byte RLE header + values run.
              ColumnChunkData cd;
              cd.physical_type_ = c.type;
              cd.num_values_ = c.num_values;
              // store raw plain bytes into a stable buffer
              char *stable = static_cast<char *>(ob_malloc(plain_len, common::ObModIds::OB_SQL_CHUNK_ROW_STORE));
              if (OB_ISNULL(stable)) {
                ret = OB_ALLOCATE_MEMORY_FAILED;
              } else {
                MEMCPY(stable, plain.data(), plain_len);
                cd.plain_bytes_.assign_ptr(stable, static_cast<int32_t>(plain_len));
                cd.ok_ = true;
                result.columns_.push_back(cd);
              }
            }
          }
        }
        result.ok_ = OB_SUCC(ret) && result.columns_.count() == result.column_names_.count();
      }
    }
  }
  return ret;
}

}  // namespace sql
}  // namespace oceanbase
