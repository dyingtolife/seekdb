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
#include "lib/allocator/ob_malloc.h"


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

namespace
{

// Parquet PageType / Encoding enums (only the values this reader understands).
constexpr int32_t PQ_PAGE_DATA = 0;
constexpr int32_t PQ_PAGE_INDEX = 1;
constexpr int32_t PQ_PAGE_DICTIONARY = 2;
constexpr int32_t PQ_PAGE_DATA_V2 = 3;
constexpr int32_t PQ_ENC_PLAIN = 0;

constexpr int64_t PQ_NAME_BUF_CAP = 64 * 1024;

// parquet integers on the wire are little-endian; thrift compact is zigzag
// varint. These helpers only handle the former.
inline int32_t pq_le32(const char *p)
{
  return static_cast<int32_t>(static_cast<uint32_t>(static_cast<uint8_t>(p[0]))
      | (static_cast<uint32_t>(static_cast<uint8_t>(p[1])) << 8)
      | (static_cast<uint32_t>(static_cast<uint8_t>(p[2])) << 16)
      | (static_cast<uint32_t>(static_cast<uint8_t>(p[3])) << 24));
}

// One column chunk of a row group, as described by the footer metadata.
struct PQGroupColMeta
{
  int32_t type = -1;               // PhysicalType
  int32_t codec = -1;              // CompressionCodec
  int64_t num_values = 0;
  int64_t data_page_offset = 0;    // offset of the first page of the chunk
  int64_t total_compressed_size = 0;
  common::ObString path_last;
  int32_t repetition = 0;
};

struct PQPageHeader
{
  int32_t type_ = -1;
  int32_t uncompressed_page_size_ = 0;
  int32_t compressed_page_size_ = 0;
  int32_t num_values_ = 0;
  int32_t encoding_ = -1;
  int32_t def_levels_len_ = 0;     // DataPageHeaderV2
  int32_t rep_levels_len_ = 0;     // DataPageHeaderV2
  bool is_compressed_ = true;      // DataPageHeaderV2 (default true)
};

bool read_list_header(TCursor &t, int64_t &n, uint8_t &et)
{
  uint8_t hb = 0;
  if (!t.read_byte(hb)) return false;
  n = (hb >> 4) & 0x0f;
  et = hb & 0x0f;
  if (15 == n) {
    uint64_t v = 0;
    if (!t.read_varint(v)) return false;
    n = static_cast<int64_t>(v);
  }
  return true;
}

// PageHeader {1:type, 2:uncompressed_page_size, 3:compressed_page_size,
//             5:data_page_header, 8:data_page_header_v2}
// DataPageHeader   {1:num_values, 2:encoding, 3:def_level_encoding,
//                   4:rep_level_encoding, 5:statistics}
// DataPageHeaderV2 {1:num_values, 2:num_nulls, 3:num_rows, 4:encoding,
//                   5:definition_levels_byte_length,
//                   6:repetition_levels_byte_length, 7:is_compressed,
//                   8:statistics}
int read_page_header(TCursor &pg, PQPageHeader &ph)
{
  int ret = OB_SUCCESS;
  int16_t last_id = 0;
  int16_t fid = 0;
  uint8_t ftype = 0;
  int64_t z = 0;
  while (OB_SUCC(ret) && pg.read_struct_field(last_id, fid, ftype) && fid != -1) {
    if (fid == 1 && ftype == T_I32) {
      if (!pg.read_zigzag(z)) { ret = OB_INVALID_DATA; }
      else { ph.type_ = static_cast<int32_t>(z); }
    } else if (fid == 2 && ftype == T_I32) {
      if (!pg.read_zigzag(z)) { ret = OB_INVALID_DATA; }
      else { ph.uncompressed_page_size_ = static_cast<int32_t>(z); }
    } else if (fid == 3 && ftype == T_I32) {
      if (!pg.read_zigzag(z)) { ret = OB_INVALID_DATA; }
      else { ph.compressed_page_size_ = static_cast<int32_t>(z); }
    } else if (fid == 5 && ftype == T_STRUCT) {
      int16_t l = 0;
      int16_t f = 0;
      uint8_t t = 0;
      while (OB_SUCC(ret) && pg.read_struct_field(l, f, t) && f != -1) {
        if (f == 1 && t == T_I32) {
          if (!pg.read_zigzag(z)) { ret = OB_INVALID_DATA; }
          else { ph.num_values_ = static_cast<int32_t>(z); }
        } else if (f == 2 && t == T_I32) {
          if (!pg.read_zigzag(z)) { ret = OB_INVALID_DATA; }
          else { ph.encoding_ = static_cast<int32_t>(z); }
        } else if (!pg.skip(t)) {
          ret = OB_INVALID_DATA;
        }
      }
    } else if (fid == 8 && ftype == T_STRUCT) {
      int16_t l = 0;
      int16_t f = 0;
      uint8_t t = 0;
      while (OB_SUCC(ret) && pg.read_struct_field(l, f, t) && f != -1) {
        if (f == 1 && t == T_I32) {
          if (!pg.read_zigzag(z)) { ret = OB_INVALID_DATA; }
          else { ph.num_values_ = static_cast<int32_t>(z); }
        } else if (f == 4 && t == T_I32) {
          if (!pg.read_zigzag(z)) { ret = OB_INVALID_DATA; }
          else { ph.encoding_ = static_cast<int32_t>(z); }
        } else if (f == 5 && t == T_I32) {
          if (!pg.read_zigzag(z)) { ret = OB_INVALID_DATA; }
          else { ph.def_levels_len_ = static_cast<int32_t>(z); }
        } else if (f == 6 && t == T_I32) {
          if (!pg.read_zigzag(z)) { ret = OB_INVALID_DATA; }
          else { ph.rep_levels_len_ = static_cast<int32_t>(z); }
        } else if (f == 7 && (t == T_BOOL_TRUE || t == T_BOOL_FALSE)) {
          ph.is_compressed_ = (T_BOOL_TRUE == t);
        } else if (!pg.skip(t)) {
          ret = OB_INVALID_DATA;
        }
      }
    } else if (!pg.skip(ftype)) {
      ret = OB_INVALID_DATA;
    }
  }
  return ret;
}

// Decode a Parquet RLE / bit-packed hybrid level stream. `src` must start at
// the hybrid header, i.e. any 4-byte length prefix of a v1 page must already
// have been consumed. `want` limits how many levels are materialised.
int decode_levels(const char *src, int64_t src_len, int32_t bit_width,
                  int64_t want, std::vector<int32_t> &levels)
{
  int ret = OB_SUCCESS;
  levels.clear();
  if (OB_ISNULL(src) || src_len < 0 || bit_width < 0 || bit_width > 32 || want < 0) {
    ret = OB_INVALID_DATA;
  } else if (0 == bit_width) {
    // A zero bit width stores no bytes at all: every level is 0.
  } else {
    const int32_t byte_width = (bit_width + 7) / 8;
    int64_t pos = 0;
    while (OB_SUCC(ret) && static_cast<int64_t>(levels.size()) < want) {
      uint64_t h = 0;
      int shift = 0;
      bool got = false;
      while (pos < src_len) {
        const uint8_t b = static_cast<uint8_t>(src[pos++]);
        h |= static_cast<uint64_t>(b & 0x7f) << shift;
        if (0 == (b & 0x80)) { got = true; break; }
        shift += 7;
        if (shift > 63) { break; }
      }
      if (!got) {
        ret = OB_INVALID_DATA;
      } else if (0 == (h & 1)) {
        // RLE run: <run length> then the repeated value in byte_width bytes
        const int64_t run = static_cast<int64_t>(h >> 1);
        if (pos + byte_width > src_len) {
          ret = OB_INVALID_DATA;
        } else {
          uint32_t v = 0;
          for (int32_t b = 0; b < byte_width; ++b) {
            v |= static_cast<uint32_t>(static_cast<uint8_t>(src[pos++])) << (8 * b);
          }
          for (int64_t i = 0; i < run && static_cast<int64_t>(levels.size()) < want; ++i) {
            levels.push_back(static_cast<int32_t>(v));
          }
        }
      } else {
        // bit-packed groups of 8 values, least significant bit first
        const int64_t groups = static_cast<int64_t>(h >> 1);
        const int64_t need_bytes = groups * bit_width;
        if (need_bytes < 0 || pos + need_bytes > src_len) {
          ret = OB_INVALID_DATA;
        } else {
          for (int64_t g = 0; g < groups * 8 && static_cast<int64_t>(levels.size()) < want; ++g) {
            uint32_t v = 0;
            for (int32_t b = 0; b < bit_width; ++b) {
              const int64_t bit = g * bit_width + b;
              const uint8_t byte = static_cast<uint8_t>(src[pos + bit / 8]);
              v |= static_cast<uint32_t>((byte >> (bit % 8)) & 1u) << b;
            }
            levels.push_back(static_cast<int32_t>(v));
          }
          pos += need_bytes;
        }
      }
    }
    if (OB_SUCC(ret) && static_cast<int64_t>(levels.size()) < want) {
      ret = OB_INVALID_DATA;
    }
  }
  return ret;
}

int push_text_cell(const char *s, int64_t len, common::ObIAllocator &allocator,
                   common::ObArray<common::ObString> &cells)
{
  int ret = OB_SUCCESS;
  char *b = static_cast<char *>(allocator.alloc(len > 0 ? len : 1));
  if (OB_ISNULL(b)) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
  } else {
    if (len > 0) { MEMCPY(b, s, len); }
    if (OB_FAIL(cells.push_back(common::ObString(static_cast<int32_t>(len), b)))) {}
  }
  return ret;
}

// Convert `want` PLAIN-encoded values held in `p` into text cells.
int append_plain_values(const char *p, int64_t n, int32_t physical_type,
                        int64_t want, common::ObIAllocator &allocator,
                        common::ObArray<common::ObString> &cells)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(p) || n < 0 || want < 0) {
    ret = OB_INVALID_DATA;
  } else {
    int64_t off = 0;
    char tmp[64];
    for (int64_t i = 0; OB_SUCC(ret) && i < want; ++i) {
      switch (physical_type) {
        case PQ_BOOLEAN: {
          // PLAIN booleans are bit-packed, one bit per value
          const int64_t byte_idx = off + i / 8;
          if (byte_idx >= n) { ret = OB_INVALID_DATA; }
          else {
            const uint8_t byte = static_cast<uint8_t>(p[byte_idx]);
            const char *v = (0 != ((byte >> (i % 8)) & 1u)) ? "1" : "0";
            if (OB_FAIL(cells.push_back(common::ObString(1, const_cast<char *>(v))))) {}
          }
          break;
        }
        case PQ_INT32: {
          if (off + 4 > n) { ret = OB_INVALID_DATA; }
          else {
            int32_t v = 0;
            MEMCPY(&v, p + off, 4);
            const int l = snprintf(tmp, sizeof(tmp), "%d", v);
            if (OB_FAIL(push_text_cell(tmp, l, allocator, cells))) {}
            off += 4;
          }
          break;
        }
        case PQ_INT64: {
          if (off + 8 > n) { ret = OB_INVALID_DATA; }
          else {
            int64_t v = 0;
            MEMCPY(&v, p + off, 8);
            const int l = snprintf(tmp, sizeof(tmp), "%ld", static_cast<long>(v));
            if (OB_FAIL(push_text_cell(tmp, l, allocator, cells))) {}
            off += 8;
          }
          break;
        }
        case PQ_FLOAT: {
          if (off + 4 > n) { ret = OB_INVALID_DATA; }
          else {
            float v = 0;
            MEMCPY(&v, p + off, 4);
            const int l = snprintf(tmp, sizeof(tmp), "%g", static_cast<double>(v));
            if (OB_FAIL(push_text_cell(tmp, l, allocator, cells))) {}
            off += 4;
          }
          break;
        }
        case PQ_DOUBLE: {
          if (off + 8 > n) { ret = OB_INVALID_DATA; }
          else {
            double v = 0;
            MEMCPY(&v, p + off, 8);
            const int l = snprintf(tmp, sizeof(tmp), "%g", v);
            if (OB_FAIL(push_text_cell(tmp, l, allocator, cells))) {}
            off += 8;
          }
          break;
        }
        case PQ_BYTE_ARRAY: {
          if (off + 4 > n) { ret = OB_INVALID_DATA; }
          else {
            const int32_t l = pq_le32(p + off);
            if (l < 0 || off + 4 + l > n) { ret = OB_INVALID_DATA; }
            else {
              if (OB_FAIL(push_text_cell(p + off + 4, l, allocator, cells))) {}
              off += 4 + l;
            }
          }
          break;
        }
        default:
          ret = OB_NOT_SUPPORTED;
          break;
      }
    }
  }
  return ret;
}

// Decode one data page (v1 or v2) into text cells. `max_def` is the maximum
// definition level of the column (0 for a REQUIRED column, 1 for OPTIONAL).
// `value_cnt` receives the number of rows represented by this page, i.e. the
// page's num_values, so a chunk spanning several pages stays aligned.
int decode_data_page(const char *body, int64_t body_len, const PQPageHeader &ph,
                     int32_t physical_type, int32_t max_def, int32_t codec,
                     common::ObIAllocator &allocator,
                     common::ObArray<common::ObString> &cells, int64_t &value_cnt)
{
  int ret = OB_SUCCESS;
  value_cnt = 0;
  const char *val_src = NULL;
  int64_t val_src_len = 0;
  int64_t val_unc_len = 0;
  bool val_compressed = false;
  std::vector<int32_t> def_levels;

  if (OB_ISNULL(body) || body_len < 0 || ph.num_values_ < 0
      || ph.compressed_page_size_ < 0 || ph.uncompressed_page_size_ < 0) {
    ret = OB_INVALID_DATA;
  } else if (PQ_ENC_PLAIN != ph.encoding_ && -1 != ph.encoding_) {
    // PLAIN_DICTIONARY / RLE_DICTIONARY / DELTA_* need a dictionary page that
    // this reader does not retain; refuse rather than emit wrong values.
    ret = OB_NOT_SUPPORTED;
  } else if (PQ_PAGE_DATA_V2 == ph.type_) {
    const int64_t rep_len = ph.rep_levels_len_;
    const int64_t def_len = ph.def_levels_len_;
    if (rep_len < 0 || def_len < 0 || rep_len + def_len > body_len
        || rep_len + def_len > ph.compressed_page_size_
        || rep_len + def_len > ph.uncompressed_page_size_) {
      ret = OB_INVALID_DATA;
    } else if (max_def > 0
               && OB_FAIL(decode_levels(body + rep_len, def_len, 1,
                                        ph.num_values_, def_levels))) {
      LOG_WARN("failed to decode the v2 definition levels", K(ret), K(def_len),
               K(ph.num_values_));
    } else {
      // v2 keeps the level sections uncompressed ahead of the values
      val_src = body + rep_len + def_len;
      val_src_len = ph.compressed_page_size_ - rep_len - def_len;
      val_unc_len = ph.uncompressed_page_size_ - rep_len - def_len;
      val_compressed = ph.is_compressed_;
    }
  } else if (PQ_PAGE_DATA == ph.type_) {
    int64_t off = 0;
    if (max_def > 0) {
      // v1 levels are RLE preceded by their own 4-byte little-endian length
      if (off + 4 > body_len) {
        ret = OB_INVALID_DATA;
      } else {
        const int32_t dl = pq_le32(body + off);
        off += 4;
        if (dl < 0 || off + dl > body_len) {
          ret = OB_INVALID_DATA;
        } else if (OB_FAIL(decode_levels(body + off, dl, 1, ph.num_values_, def_levels))) {
          LOG_WARN("failed to decode the v1 definition levels", K(ret), K(dl));
        } else {
          off += dl;
        }
      }
    }
    if (OB_SUCC(ret)) {
      if (off > body_len || off > ph.compressed_page_size_
          || off > ph.uncompressed_page_size_) {
        ret = OB_INVALID_DATA;
      } else {
        val_src = body + off;
        val_src_len = ph.compressed_page_size_ - off;
        val_unc_len = ph.uncompressed_page_size_ - off;
        val_compressed = (PQ_UNCOMPRESSED != codec);
      }
    }
  } else {
    ret = OB_INVALID_DATA;
  }

  if (OB_FAIL(ret)) {
  } else if (0 == ph.num_values_) {
    // empty page (all values filtered out by the writer): nothing to emit
  } else {
    std::vector<char> valbuf;
    const char *vals = NULL;
    int64_t vals_len = 0;
    if (val_unc_len < 0 || val_src_len < 0) {
      ret = OB_INVALID_DATA;
    } else if (!val_compressed) {
      if (val_src_len < val_unc_len) {
        ret = OB_INVALID_DATA;
      } else {
        vals = val_src;
        vals_len = val_unc_len;
      }
    } else if (PQ_SNAPPY == codec) {
      // one parquet page body is a single raw snappy block (varint length
      // header + tags), never the [u32][block] Hadoop framing
      valbuf.resize(val_unc_len > 0 ? val_unc_len : 1);
      int64_t written = 0;
      if (OB_FAIL(ObSnappyDecoder::decompress(val_src, val_src_len, valbuf.data(),
                                              val_unc_len, written))
          || written != val_unc_len) {
        ret = OB_INVALID_DATA;
        LOG_WARN("failed to decompress a parquet page", K(ret), K(val_src_len),
                 K(val_unc_len), K(written));
      } else {
        vals = valbuf.data();
        vals_len = written;
      }
    } else {
      ret = OB_NOT_SUPPORTED;
    }

    if (OB_FAIL(ret)) {
    } else {
      int64_t defined = 0;
      for (int32_t i = 0; i < ph.num_values_; ++i) {
        const bool present = (0 == max_def)
            || (i < static_cast<int32_t>(def_levels.size()) && def_levels[i] >= max_def);
        if (present) { ++defined; }
      }
      common::ObArray<common::ObString> vals_cells;
      if (OB_FAIL(append_plain_values(vals, vals_len, physical_type, defined,
                                      allocator, vals_cells))) {
        LOG_WARN("failed to decode parquet plain values", K(ret), K(physical_type),
                 K(defined), K(vals_len));
      } else if (static_cast<int64_t>(vals_cells.count()) != defined) {
        ret = OB_ERR_UNEXPECTED;
      } else {
        int64_t k = 0;
        for (int32_t i = 0; OB_SUCC(ret) && i < ph.num_values_; ++i) {
          const bool present = (0 == max_def)
              || (i < static_cast<int32_t>(def_levels.size()) && def_levels[i] >= max_def);
          if (present && k < vals_cells.count()) {
            if (OB_FAIL(cells.push_back(vals_cells.at(k)))) {}
            ++k;
          } else if (OB_FAIL(cells.push_back(common::ObString()))) {
            // definition level below max_def: a real NULL, not an empty string
          }
        }
        value_cnt = ph.num_values_;
      }
    }
  }
  return ret;
}

// Parse the footer far enough to know the leaf-column metas, the row-group
// layouts and the total row count. Field ids follow parquet.thrift:
//   FileMetaData   {1:version, 2:schema, 3:num_rows, 4:row_groups, 6:created_by}
//   SchemaElement  {1:type, 2:type_length, 3:repetition_type, 4:name,
//                   5:num_children, 6:converted_type, ...}
//   RowGroup       {1:columns, 2:total_byte_size, 3:num_rows, ...}
//   ColumnChunk    {1:file_path, 2:file_offset, 3:meta_data, ...}
//   ColumnMetaData {1:type, 2:encodings, 3:path_in_schema, 4:codec,
//                   5:num_values, 6:total_uncompressed_size,
//                   7:total_compressed_size, 9:data_page_offset,
//                   11:dictionary_page_offset, 12:statistics}
int parse_footer_meta(const char *buf, int64_t len,
                      std::vector<PQSchemaElement> &leaves,
                      std::vector<std::vector<PQGroupColMeta>> &row_groups,
                      int64_t &num_rows)
{
  int ret = OB_SUCCESS;
  leaves.clear();
  row_groups.clear();
  num_rows = 0;
  if (OB_ISNULL(buf) || len < 12
      || buf[0] != 'P' || buf[1] != 'A' || buf[2] != 'R' || buf[3] != '1'
      || buf[len - 4] != 'P' || buf[len - 3] != 'A' || buf[len - 2] != 'R' || buf[len - 1] != '1') {
    ret = OB_INVALID_DATA;
  } else {
    const int32_t flen = pq_le32(buf + len - 8);
    if (flen <= 0 || flen > len - 8) {
      ret = OB_INVALID_DATA;
    } else {
      TCursor t{buf + (len - 8 - flen), flen, 0};
      std::vector<char> name_buf(PQ_NAME_BUF_CAP);
      int64_t name_pos = 0;
      int16_t last_id = 0;
      int16_t fid = 0;
      uint8_t ftype = 0;
      int64_t z = 0;
      while (OB_SUCC(ret) && t.read_struct_field(last_id, fid, ftype) && fid != -1) {
        if (fid == 1 && ftype == T_I32) {
          if (!t.read_zigzag(z)) { ret = OB_INVALID_DATA; }  // version
        } else if (fid == 2 && (ftype == T_LIST || ftype == T_SET)) {
          int64_t n = 0;
          uint8_t et = 0;
          if (!read_list_header(t, n, et)) {
            ret = OB_INVALID_DATA;
          } else {
            std::vector<PQSchemaElement> elems(n > 0 ? n : 0);
            for (int64_t i = 0; OB_SUCC(ret) && i < n; ++i) {
              int16_t l2 = 0;
              int16_t f2 = 0;
              uint8_t t2 = 0;
              PQSchemaElement *e = i < static_cast<int64_t>(elems.size()) ? &elems[i] : NULL;
              while (OB_SUCC(ret) && t.read_struct_field(l2, f2, t2) && f2 != -1) {
                if (f2 == 1 && t2 == T_I32) {
                  if (!t.read_zigzag(z)) { ret = OB_INVALID_DATA; }
                  else if (OB_NOT_NULL(e)) { e->type_ = static_cast<int32_t>(z); }
                } else if (f2 == 3 && t2 == T_I32) {
                  if (!t.read_zigzag(z)) { ret = OB_INVALID_DATA; }
                  else if (OB_NOT_NULL(e)) { e->repetition_ = static_cast<int32_t>(z); }
                } else if (f2 == 4 && t2 == T_BINARY) {
                  common::ObString s;
                  if (!t.read_string(s, name_buf.data(), PQ_NAME_BUF_CAP, name_pos)) {
                    ret = OB_INVALID_DATA;
                  } else if (OB_NOT_NULL(e)) {
                    e->name_ = s;
                  }
                } else if (f2 == 5 && t2 == T_I32) {
                  if (!t.read_zigzag(z)) { ret = OB_INVALID_DATA; }
                  else if (OB_NOT_NULL(e)) { e->num_children_ = static_cast<int32_t>(z); }
                } else if (!t.skip(t2)) {
                  ret = OB_INVALID_DATA;
                }
              }
            }
            // A leaf carries a physical type; the root and intermediate nodes
            // are groups and only carry children.
            if (OB_SUCC(ret)) {
              leaves.clear();
              for (int64_t i = 0; i < static_cast<int64_t>(elems.size()); ++i) {
                if (elems[i].type_ >= 0) {
                  leaves.push_back(elems[i]);
                }
              }
            }
          }
        } else if (fid == 3 && (ftype == T_I32 || ftype == T_I64)) {
          if (!t.read_zigzag(num_rows)) { ret = OB_INVALID_DATA; }
        } else if (fid == 4 && (ftype == T_LIST || ftype == T_SET)) {
          int64_t n = 0;
          uint8_t et = 0;
          if (!read_list_header(t, n, et)) {
            ret = OB_INVALID_DATA;
          } else {
            for (int64_t i = 0; OB_SUCC(ret) && i < n; ++i) {
              std::vector<PQGroupColMeta> cols;
              int16_t l2 = 0;
              int16_t f2 = 0;
              uint8_t t2 = 0;
              while (OB_SUCC(ret) && t.read_struct_field(l2, f2, t2) && f2 != -1) {
                if (f2 == 1 && (t2 == T_LIST || t2 == T_SET)) {
                  int64_t n2 = 0;
                  uint8_t et2 = 0;
                  if (!read_list_header(t, n2, et2)) {
                    ret = OB_INVALID_DATA;
                  } else {
                    for (int64_t j = 0; OB_SUCC(ret) && j < n2; ++j) {
                      PQGroupColMeta c;
                      int16_t l3 = 0;
                      int16_t f3 = 0;
                      uint8_t t3 = 0;
                      int64_t dict_page_offset = 0;
                      while (OB_SUCC(ret) && t.read_struct_field(l3, f3, t3) && f3 != -1) {
                        if (f3 == 3 && t3 == T_STRUCT) {
                          int16_t l4 = 0;
                          int16_t f4 = 0;
                          uint8_t t4 = 0;
                          while (OB_SUCC(ret) && t.read_struct_field(l4, f4, t4) && f4 != -1) {
                            if (f4 == 1 && t4 == T_I32) {
                              if (!t.read_zigzag(z)) { ret = OB_INVALID_DATA; }
                              else { c.type = static_cast<int32_t>(z); }
                            } else if (f4 == 3 && (t4 == T_LIST || t4 == T_SET)) {
                              int64_t n3 = 0;
                              uint8_t et3 = 0;
                              common::ObString seg;
                              if (!read_list_header(t, n3, et3)) {
                                ret = OB_INVALID_DATA;
                              } else {
                                for (int64_t k = 0; OB_SUCC(ret) && k < n3; ++k) {
                                  if (et3 == T_BINARY) {
                                    if (!t.read_string(seg, name_buf.data(),
                                                       PQ_NAME_BUF_CAP, name_pos)) {
                                      ret = OB_INVALID_DATA;
                                    }
                                  } else if (!t.skip(et3)) {
                                    ret = OB_INVALID_DATA;
                                  }
                                }
                                c.path_last = seg;
                              }
                            } else if (f4 == 4 && t4 == T_I32) {
                              if (!t.read_zigzag(z)) { ret = OB_INVALID_DATA; }
                              else { c.codec = static_cast<int32_t>(z); }
                            } else if (f4 == 5 && (t4 == T_I32 || t4 == T_I64)) {
                              if (!t.read_zigzag(z)) { ret = OB_INVALID_DATA; }
                              else { c.num_values = z; }
                            } else if (f4 == 7 && (t4 == T_I32 || t4 == T_I64)) {
                              if (!t.read_zigzag(z)) { ret = OB_INVALID_DATA; }
                              else { c.total_compressed_size = z; }
                            } else if (f4 == 9 && (t4 == T_I32 || t4 == T_I64)) {
                              if (!t.read_zigzag(z)) { ret = OB_INVALID_DATA; }
                              else { c.data_page_offset = z; }
                            } else if (f4 == 11 && (t4 == T_I32 || t4 == T_I64)) {
                              if (!t.read_zigzag(z)) { ret = OB_INVALID_DATA; }
                              else { dict_page_offset = z; }
                            } else if (!t.skip(t4)) {
                              ret = OB_INVALID_DATA;
                            }
                          }
                          // a chunk that owns a dictionary page starts there
                          if (dict_page_offset > 0
                              && (c.data_page_offset <= 0
                                  || dict_page_offset < c.data_page_offset)) {
                            c.data_page_offset = dict_page_offset;
                          }
                        } else if (!t.skip(t3)) {
                          ret = OB_INVALID_DATA;
                        }
                      }
                      cols.push_back(c);
                    }
                  }
                } else if (!t.skip(t2)) {
                  ret = OB_INVALID_DATA;
                }
              }
              if (OB_SUCC(ret)) {
                row_groups.push_back(cols);
              }
            }
          }
        } else if (fid == 6 && ftype == T_BINARY) {
          common::ObString s;  // created_by
          if (!t.read_string(s, name_buf.data(), PQ_NAME_BUF_CAP, name_pos)) {
            ret = OB_INVALID_DATA;
          }
        } else if (!t.skip(ftype)) {
          ret = OB_INVALID_DATA;
        }
      }
    }
  }
  return ret;
}

// Decode one column chunk into per-value text cells. Walks every data page of
// the chunk so multi-page chunks stay correct; PLAIN values, RLE definition
// levels, UNCOMPRESSED or SNAPPY.
int decode_chunk_as_text(const char *buf, int64_t buf_len, const PQGroupColMeta &c,
                         int32_t physical_type, int32_t repetition,
                         common::ObIAllocator &allocator,
                         common::ObArray<common::ObString> &cells)
{
  int ret = OB_SUCCESS;
  if (repetition < 0 || repetition > 1) {
    // repeated fields change the row count and are not supported
    ret = OB_NOT_SUPPORTED;
  } else if (c.num_values < 0) {
    ret = OB_INVALID_DATA;
  } else if (0 == c.num_values) {
    // nothing to emit
  } else if (c.data_page_offset <= 0) {
    // no data page at all: every value is NULL
    for (int64_t i = 0; OB_SUCC(ret) && i < c.num_values; ++i) {
      if (OB_FAIL(cells.push_back(common::ObString()))) {}
    }
  } else if (c.data_page_offset >= buf_len) {
    ret = OB_INVALID_DATA;
  } else {
    const int32_t max_def = (1 == repetition) ? 1 : 0;

    int64_t chunk_end = buf_len;
    if (c.total_compressed_size > 0) {
      chunk_end = c.data_page_offset + c.total_compressed_size;
      if (chunk_end > buf_len) { chunk_end = buf_len; }
    }
    int64_t pos = c.data_page_offset;
    int64_t emitted = 0;
    while (OB_SUCC(ret) && emitted < c.num_values && pos < chunk_end) {
      TCursor pg{buf + pos, chunk_end - pos, 0};
      PQPageHeader ph;
      if (OB_FAIL(read_page_header(pg, ph))) {
        LOG_WARN("failed to parse a parquet page header", K(ret), K(pos));
      } else if (ph.compressed_page_size_ < 0 || ph.uncompressed_page_size_ < 0) {
        ret = OB_INVALID_DATA;
      } else if (0 == ph.compressed_page_size_
                 && PQ_PAGE_DATA_V2 != ph.type_ && PQ_PAGE_DATA != ph.type_) {
        ret = OB_INVALID_DATA;  // never spin on a zero-length non-data page
      } else {
        const int64_t body_off = pos + pg.pos;
        if (body_off + ph.compressed_page_size_ > chunk_end) {
          ret = OB_INVALID_DATA;
        } else if (PQ_PAGE_DATA == ph.type_ || PQ_PAGE_DATA_V2 == ph.type_) {
          int64_t page_cnt = 0;
          if (OB_FAIL(decode_data_page(buf + body_off, ph.compressed_page_size_, ph,
                                       physical_type, max_def, c.codec, allocator,
                                       cells, page_cnt))) {
            LOG_WARN("failed to decode a parquet data page", K(ret), K(ph.type_),
                     K(ph.num_values_), K(ph.encoding_));
          } else {
            emitted += page_cnt;
          }
        }
        // dictionary / index pages carry no values of their own: skip them
        if (OB_SUCC(ret)) {
          if (0 == ph.compressed_page_size_) {
            ret = OB_INVALID_DATA;
          } else {
            pos = body_off + ph.compressed_page_size_;
          }
        }
      }
    }

    if (OB_SUCC(ret) && emitted != c.num_values) {
      LOG_WARN("parquet chunk row count mismatch", K(emitted), K(c.num_values),
               K(c.data_page_offset));
      ret = OB_INVALID_DATA;
    }
  }
  return ret;
}

}  // anonymous namespace

int ObMiniParquetReader::read_file(const char *buf, int64_t len, ReadResult &result)
{
  int ret = OB_SUCCESS;
  std::vector<PQSchemaElement> leaves;
  std::vector<std::vector<PQGroupColMeta>> row_groups;
  int64_t num_rows = 0;
  if (OB_FAIL(parse_footer_meta(buf, len, leaves, row_groups, num_rows))) {
    LOG_WARN("failed to parse the parquet footer", K(ret), K(len));
  } else {

    result.num_rows_ = num_rows;
    result.column_names_.reset();
    result.columns_.reset();
    for (int64_t i = 0; i < static_cast<int64_t>(leaves.size()); ++i) {
      if (OB_FAIL(result.column_names_.push_back(leaves[i].name_))) {
        LOG_WARN("failed to store a parquet column name", K(ret), K(i));
      }
    }
    // Report the physical type and value count of the first row group; the
    // values themselves are decoded per row group by read_group_as_rows().
    if (OB_SUCC(ret) && !row_groups.empty()) {
      const std::vector<PQGroupColMeta> &cols = row_groups[0];
      for (size_t i = 0; OB_SUCC(ret) && i < cols.size(); ++i) {
        ColumnChunkData cd;
        cd.physical_type_ = cols[i].type;
        cd.num_values_ = cols[i].num_values;
        cd.ok_ = true;
        if (OB_FAIL(result.columns_.push_back(cd))) {
          LOG_WARN("failed to store a parquet column meta", K(ret), K(i));
        }
      }
    }
    if (OB_SUCC(ret)) {
      result.ok_ = result.columns_.count() == result.column_names_.count();
    }
  }
  return ret;
}

int ObMiniParquetReader::read_group_as_rows(const char *buf, int64_t len, int64_t group_idx,
                                            int64_t &group_cnt,
                                            const common::ObIArray<bool> &needed_cols,
                                            common::ObIAllocator &allocator,
                                            common::ObArray<common::ObString> &cells)
{
  int ret = OB_SUCCESS;
  std::vector<PQSchemaElement> leaves;
  std::vector<std::vector<PQGroupColMeta>> row_groups;
  int64_t num_rows = 0;
  group_cnt = 0;
  if (OB_FAIL(parse_footer_meta(buf, len, leaves, row_groups, num_rows))) {
    LOG_WARN("failed to parse the parquet footer", K(ret), K(len));
  } else {
    group_cnt = static_cast<int64_t>(row_groups.size());
    if (group_idx >= group_cnt) {
      ret = OB_ITER_END;
    } else if (static_cast<int64_t>(row_groups[group_idx].size())
               != static_cast<int64_t>(leaves.size())) {
      LOG_WARN("parquet row group column count mismatch", K(group_idx),
               K(static_cast<int64_t>(row_groups[group_idx].size())),
               K(static_cast<int64_t>(leaves.size())));
      ret = OB_INVALID_DATA;
    } else {
      // convert needed columns only
      common::ObArray<common::ObArray<common::ObString>> col_cells;
      for (int64_t ci = 0; OB_SUCC(ret) && ci < static_cast<int64_t>(leaves.size()); ++ci) {
        const bool needed = needed_cols.count() == 0
            ? true : (ci < needed_cols.count() ? needed_cols.at(ci) : true);
        if (!needed) {
          common::ObArray<common::ObString> empty_col;
          if (OB_FAIL(col_cells.push_back(empty_col))) {
            LOG_WARN("failed to store an empty parquet column", K(ret), K(ci));
          }
          continue;
        }
        common::ObArray<common::ObString> one;
        if (OB_FAIL(decode_chunk_as_text(buf, len, row_groups[group_idx][ci],
                                         leaves[ci].type_, leaves[ci].repetition_,
                                         allocator, one))) {
          LOG_WARN("failed to decode a parquet column chunk", K(ret), K(ci),
                   K(leaves[ci].name_), K(leaves[ci].type_),
                   K(leaves[ci].repetition_));
        } else if (OB_FAIL(col_cells.push_back(one))) {
          LOG_WARN("failed to store a parquet column", K(ret), K(ci));
        }
      }
      // stitch into column-major cells
      int64_t row_cnt = 0;
      for (int64_t ci = 0; OB_SUCC(ret) && ci < col_cells.count(); ++ci) {
        const bool needed = needed_cols.count() == 0
            ? true : (ci < needed_cols.count() ? needed_cols.at(ci) : true);
        if (needed && col_cells.at(ci).count() > row_cnt) {
          row_cnt = col_cells.at(ci).count();
        }
      }

      for (int64_t r = 0; OB_SUCC(ret) && r < row_cnt; ++r) {
        for (int64_t ci = 0; OB_SUCC(ret) && ci < col_cells.count(); ++ci) {
          const bool needed = needed_cols.count() == 0
              ? true : (ci < needed_cols.count() ? needed_cols.at(ci) : true);
          if (!needed) {
            if (OB_FAIL(cells.push_back(common::ObString()))) {}
          } else if (r < col_cells.at(ci).count()) {
            if (OB_FAIL(cells.push_back(col_cells.at(ci).at(r)))) {}
          } else {
            // ragged group: treat missing as NULL
            if (OB_FAIL(cells.push_back(common::ObString()))) {}
          }
        }
      }
    }
  }
  return ret;
}
}  // namespace sql
}  // namespace oceanbase
