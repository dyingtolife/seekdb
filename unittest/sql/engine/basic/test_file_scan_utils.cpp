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

#include <gtest/gtest.h>
#include <cstdio>
#include <cstdlib>
#include <unistd.h>
#include <sys/stat.h>
#include <sys/types.h>

#define private public
#define protected public
#include "lib/string/ob_sql_string.h"
#include "sql/engine/file/ob_file_scan_utils.h"
#include "sql/engine/file/ob_parquet_reader.h"
#undef private
#undef protected

using namespace oceanbase;
using namespace oceanbase::common;
using namespace oceanbase::sql;
using namespace testing;

class TestFileScanUtils : public ::testing::Test
{
public:
  TestFileScanUtils() : alloc_("TestFileScan") {}
  virtual void SetUp()
  {
    // One private directory per test: a shared directory made the glob test
    // depend on files left behind by earlier runs (a crashed run never reaches
    // TearDown, so stale *.csv files were picked up by the pattern).
    char tmpl[] = "/tmp/ob_file_scan_test_XXXXXX";
    const char *dir = mkdtemp(tmpl);
    ASSERT_NE(nullptr, dir);
    if (nullptr != dir) {
      const int64_t dlen = static_cast<int64_t>(strlen(dir));
      char *dcopy = static_cast<char *>(alloc_.alloc(dlen + 1));
      ASSERT_NE(nullptr, dcopy);
      if (nullptr != dcopy) {
        MEMCPY(dcopy, dir, dlen);
        dcopy[dlen] = '\0';
        dir_.assign_ptr(dcopy, static_cast<int32_t>(dlen));
      }
    }
  }
  virtual void TearDown()
  {
    for (int64_t i = 0; i < files_.count(); ++i) {
      char buf[OB_MAX_FILE_NAME_LENGTH];
      MEMCPY(buf, files_.at(i).ptr(), files_.at(i).length());
      buf[files_.at(i).length()] = '\0';
      unlink(buf);
    }
    if (!dir_.empty()) {
      char dbuf[OB_MAX_FILE_NAME_LENGTH];
      MEMCPY(dbuf, dir_.ptr(), dir_.length());
      dbuf[dir_.length()] = '\0';
      rmdir(dbuf);
    }
  }
  ObString write_file(const char *name, const char *content)
  {
    char pathbuf[OB_MAX_FILE_NAME_LENGTH];
    snprintf(pathbuf, sizeof(pathbuf), "%.*s/%s", static_cast<int>(dir_.length()), dir_.ptr(), name);
    FILE *fp = fopen(pathbuf, "wb");
    EXPECT_NE(nullptr, fp);
    if (nullptr != fp) {
      fwrite(content, 1, strlen(content), fp);
      fclose(fp);
    }
    // The path must outlive this call: callers keep the returned ObString and
    // also fopen(files_.at(i).ptr()) later, so copy it onto the fixture arena
    // instead of returning a view into a dead stack frame.
    const int64_t plen = static_cast<int64_t>(strlen(pathbuf));
    char *pcopy = static_cast<char *>(alloc_.alloc(plen + 1));
    EXPECT_NE(nullptr, pcopy);
    ObString path;
    if (nullptr != pcopy) {
      MEMCPY(pcopy, pathbuf, plen);
      pcopy[plen] = '\0';
      path.assign_ptr(pcopy, static_cast<int32_t>(plen));
      files_.push_back(path);
    }
    return path;
  }

  ObArenaAllocator alloc_;
  ObString dir_;
  ObArray<ObString> files_;
};


static bool ob_str_ends_with(const common::ObString &s, const char *suffix)
{
  const int64_t n = static_cast<int64_t>(strlen(suffix));
  if (s.length() < n) {
    return false;
  }
  return common::ObString(n, s.ptr() + s.length() - n) == common::ObString(n, suffix);
}

// ---------------- CSV cursor ----------------

TEST_F(TestFileScanUtils, CsvBasicHeaderAndRows)
{
  ObString path = write_file("basic.csv",
      "id,name,score\n"
      "1,Alice,98.5\n"
      "2,Bob,87\n");
  ObFileParamSpec params;
  ObCsvFileCursor cursor(path, params, alloc_);
  ASSERT_EQ(OB_SUCCESS, cursor.open());
  ASSERT_EQ(3, cursor.header().count());
  ASSERT_TRUE(ObString("id") == cursor.header().at(0));
  ObFileRow row;
  int gr = cursor.next_row(row);
  ASSERT_EQ(OB_SUCCESS, gr);
  ASSERT_EQ(3, row.cells_.count());
  ASSERT_TRUE(row.cells_.at(0) == "1");
  ASSERT_TRUE(row.cells_.at(1) == "Alice");
  ASSERT_TRUE(row.cells_.at(2) == "98.5");
  gr = cursor.next_row(row);
  ASSERT_EQ(OB_SUCCESS, gr);
  ASSERT_TRUE(row.cells_.at(1) == "Bob");
  ASSERT_TRUE(row.cells_.at(2) == "87");
  ASSERT_EQ(OB_ITER_END, cursor.next_row(row));
  cursor.close();
}

TEST_F(TestFileScanUtils, CsvQuotesEscapesEmbeddedNewlineCRLF)
{
  ObString path = write_file("quotes.csv",
      "id,note\r\n"
      "1,\"hello,\r\nworld\"\r\n"
      "2,\"say \"\"hi\"\"\"\r\n"
      "3,plain\r\n");
  ObFileParamSpec params;
  ObCsvFileCursor cursor(path, params, alloc_);
  ASSERT_EQ(OB_SUCCESS, cursor.open());
  ObFileRow row;
  ASSERT_EQ(OB_SUCCESS, cursor.next_row(row));
  ASSERT_EQ(2, row.cells_.count());
  ASSERT_TRUE(row.cells_.at(1) == "hello,\nworld");
  ASSERT_EQ(OB_SUCCESS, cursor.next_row(row));
  ASSERT_TRUE(row.cells_.at(1) == "say \"hi\"");
  ASSERT_EQ(OB_SUCCESS, cursor.next_row(row));
  ASSERT_TRUE(row.cells_.at(1) == "plain");
  ASSERT_EQ(OB_ITER_END, cursor.next_row(row));
  cursor.close();
}

TEST_F(TestFileScanUtils, CsvLargeRecordGrowsWindow)
{
  std::string big;
  big += "a,b\n";
  big += "1,";
  const int64_t fill = 600 * 1024;
  char *filler = static_cast<char *>(malloc(fill + 1));
  memset(filler, 'x', fill);
  filler[fill] = '\0';
  big += filler;
  big += "\n";
  big += "2,end\n";
  ObString path = write_file("big.csv", big.c_str());
  free(filler);
  ObFileParamSpec params;
  ObCsvFileCursor cursor(path, params, alloc_);
  ASSERT_EQ(OB_SUCCESS, cursor.open());
  ObFileRow row;
  // open() already consumed and exposed the header row (cursor.header());
  // the first next_row() therefore yields the first data row.
  ASSERT_EQ(OB_SUCCESS, cursor.next_row(row));
  ASSERT_EQ(fill, row.cells_.at(1).length());
  ASSERT_EQ(OB_SUCCESS, cursor.next_row(row));
  ASSERT_TRUE(row.cells_.at(1) == "end");
  ASSERT_EQ(OB_ITER_END, cursor.next_row(row));
  cursor.close();
}

TEST_F(TestFileScanUtils, CsvBadRowDiagnostics)
{
  ObString path = write_file("bad.csv",
      "id,note\n"
      "1,\"unterminated\n");
  ObFileParamSpec params;
  ObCsvFileCursor cursor(path, params, alloc_);
  ASSERT_EQ(OB_SUCCESS, cursor.open());
  ObFileRow row;
  // open() already consumed and exposed the header row (cursor.header())
  ASSERT_EQ(OB_INVALID_DATA, cursor.next_row(row));
  ASSERT_GE(cursor.current_line(), 2);
  cursor.close();
}

TEST_F(TestFileScanUtils, CsvNullstrParam)
{
  ObString path = write_file("nulls.csv",
      "id,v\n"
      "1,NULL\n"
      "2,\\N\n"
      "3,\n");
  ObFileParamSpec params;
  ObCsvFileCursor cursor(path, params, alloc_);
  ASSERT_EQ(OB_SUCCESS, cursor.open());
  ObFileRow row;
  ASSERT_EQ(OB_SUCCESS, cursor.next_row(row));
  ASSERT_EQ(0, row.cells_.at(1).length());  // NULL text -> null marker
  ASSERT_EQ(OB_SUCCESS, cursor.next_row(row));
  ASSERT_EQ(0, row.cells_.at(1).length());
  ASSERT_EQ(OB_SUCCESS, cursor.next_row(row));
  ASSERT_EQ(0, row.cells_.at(1).length());
  cursor.close();
}

// ---------------- JSONL cursor ----------------

TEST_F(TestFileScanUtils, JsonlFlatObjectsAndKeyValues)
{
  ObString path = write_file("basic.jsonl",
      "{\"id\":1,\"name\":\"Alice\"}\n"
      "{\"id\":2,\"day\":\"2026-08-02\"}\n");
  ObFileParamSpec params;
  ObJsonlFileCursor cursor(path, params, alloc_);
  ASSERT_EQ(OB_SUCCESS, cursor.open());
  ObFileRow row;
  ASSERT_EQ(OB_SUCCESS, cursor.next_row(row));
  ASSERT_EQ(4, row.cells_.count());  // id,1,name,Alice
  ASSERT_TRUE(row.cells_.at(0) == "id");
  ASSERT_TRUE(row.cells_.at(1) == "1");
  ASSERT_EQ(OB_SUCCESS, cursor.next_row(row));
  ASSERT_EQ(4, row.cells_.count());
  ASSERT_TRUE(row.cells_.at(2) == "day");
  ASSERT_EQ(OB_ITER_END, cursor.next_row(row));
  cursor.close();
}

TEST_F(TestFileScanUtils, JsonlRejectsNested)
{
  ObString path = write_file("nested.jsonl",
      "{\"id\":1,\"nested\":{\"a\":1}}\n");
  ObFileParamSpec params;
  ObJsonlFileCursor cursor(path, params, alloc_);
  ASSERT_EQ(OB_SUCCESS, cursor.open());
  ObFileRow row;
  ASSERT_EQ(OB_INVALID_DATA, cursor.next_row(row));
  cursor.close();

  ObString path2 = write_file("nested2.jsonl",
      "{\"id\":1,\"arr\":[1,2]}\n");
  ObJsonlFileCursor cursor2(path2, params, alloc_);
  ASSERT_EQ(OB_SUCCESS, cursor2.open());
  ASSERT_EQ(OB_INVALID_DATA, cursor2.next_row(row));
  cursor2.close();
}

TEST_F(TestFileScanUtils, JsonlBadLineDiagnostics)
{
  ObString path = write_file("bad.jsonl",
      "{\"id\":1}\n"
      "not json\n");
  ObFileParamSpec params;
  ObJsonlFileCursor cursor(path, params, alloc_);
  ASSERT_EQ(OB_SUCCESS, cursor.open());
  ObFileRow row;
  ASSERT_EQ(OB_SUCCESS, cursor.next_row(row));
  ASSERT_EQ(OB_INVALID_DATA, cursor.next_row(row));
  ASSERT_GE(cursor.current_line(), 2);
  cursor.close();
}

// ---------------- glob cursor ----------------

TEST_F(TestFileScanUtils, GlobSortedDeterministic)
{
  write_file("b.csv", "x\n");
  write_file("a.csv", "x\n");
  write_file("c.txt", "x\n");
  char patternbuf[OB_MAX_FILE_NAME_LENGTH];
  snprintf(patternbuf, sizeof(patternbuf), "%.*s/*.csv", static_cast<int>(dir_.length()), dir_.ptr());
  ObString pattern(patternbuf);
  ObGlobCursor cursor(pattern, alloc_);
  ASSERT_EQ(OB_SUCCESS, cursor.open());
  ObFileRow row;
  ASSERT_EQ(OB_SUCCESS, cursor.next_row(row));
  ASSERT_TRUE(ob_str_ends_with(row.cells_.at(0), "/a.csv"));
  ASSERT_EQ(OB_SUCCESS, cursor.next_row(row));
  ASSERT_TRUE(ob_str_ends_with(row.cells_.at(0), "/b.csv"));
  ASSERT_EQ(OB_ITER_END, cursor.next_row(row));
  cursor.close();
}

// ---------------- schema inference ----------------

TEST_F(TestFileScanUtils, InferenceWideningAndBoundaries)
{
  // BIGINT boundary values keep BIGINT; mixed int/double widens to DOUBLE
  ObArray<ObString> header;
  header.push_back(ObString("i64"));
  header.push_back(ObString("mix"));
  header.push_back(ObString("b"));
  header.push_back(ObString("d"));
  header.push_back(ObString("dt"));
  header.push_back(ObString("s"));
  header.push_back(ObString("allnull"));
  ObArray<ObFileRow> rows;
  struct RowMaker
  {
    static ObFileRow make(std::initializer_list<const char *> cells)
    {
      ObFileRow r;
      for (const char *c : cells) {
        r.cells_.push_back(ObString::make_string(c));
      }
      return r;
    }
  };
  rows.push_back(RowMaker::make({"9223372036854775807", "1", "true", "2026-08-01",
                                 "2026-08-01 10:11:12", "x", "NULL"}));
  rows.push_back(RowMaker::make({"-9223372036854775808", "1.5", "false", "2026-08-02",
                                 "2026-08-02T10:11:12", "y", ""}));
  ObFileSchema schema;
  ASSERT_EQ(OB_SUCCESS, infer_file_schema(header, rows, alloc_, schema));
  ASSERT_EQ(7, schema.columns_.count());
  ASSERT_EQ(ObFileColumnType::BIGINT, schema.columns_.at(0).type_);
  ASSERT_EQ(ObFileColumnType::DOUBLE, schema.columns_.at(1).type_);
  ASSERT_EQ(ObFileColumnType::BOOLEAN, schema.columns_.at(2).type_);
  ASSERT_EQ(ObFileColumnType::DATE, schema.columns_.at(3).type_);
  ASSERT_EQ(ObFileColumnType::DATETIME, schema.columns_.at(4).type_);
  ASSERT_EQ(ObFileColumnType::VARCHAR, schema.columns_.at(5).type_);
  ASSERT_EQ(ObFileColumnType::NIL, schema.columns_.at(6).type_);
  ASSERT_TRUE(schema.columns_.at(6).nullable_);
}

TEST_F(TestFileScanUtils, InferenceDateDatetimeWiden)
{
  ObArray<ObString> header;
  header.push_back(ObString("d"));
  ObArray<ObFileRow> rows;
  ObFileRow r1;
  r1.cells_.push_back(ObString::make_string("2026-08-01"));
  rows.push_back(r1);
  ObFileRow r2;
  r2.cells_.push_back(ObString::make_string("2026-08-02 10:00:00"));
  rows.push_back(r2);
  ObFileSchema schema;
  ASSERT_EQ(OB_SUCCESS, infer_file_schema(header, rows, alloc_, schema));
  ASSERT_EQ(ObFileColumnType::DATETIME, schema.columns_.at(0).type_);
}

TEST_F(TestFileScanUtils, InferenceInvalidDateFallsToVarchar)
{
  ObArray<ObString> header;
  header.push_back(ObString("d"));
  ObArray<ObFileRow> rows;
  ObFileRow r1;
  r1.cells_.push_back(ObString::make_string("2026-13-40"));  // invalid month/day
  rows.push_back(r1);
  ObFileSchema schema;
  ASSERT_EQ(OB_SUCCESS, infer_file_schema(header, rows, alloc_, schema));
  ASSERT_EQ(ObFileColumnType::VARCHAR, schema.columns_.at(0).type_);
}

// ---------------- file signature ----------------

TEST_F(TestFileScanUtils, FileSignatureChangeDetection)
{
  ObString path = write_file("sig.csv", "a\n1\n");
  ObFileSignature sig;
  ASSERT_EQ(OB_SUCCESS, stat_file_signature(path, sig));
  ASSERT_TRUE(sig.is_valid());
  ObFileSignature sig2;
  ASSERT_EQ(OB_SUCCESS, stat_file_signature(path, sig2));
  ASSERT_TRUE(sig == sig2);
  // modify the file
  FILE *fp = fopen(files_.at(0).ptr(), "ab");
  ASSERT_NE(nullptr, fp);
  fwrite("2\n", 1, 2, fp);
  fclose(fp);
  ObFileSignature sig3;
  ASSERT_EQ(OB_SUCCESS, stat_file_signature(path, sig3));
  ASSERT_TRUE(sig3 != sig);
  // missing file
  ASSERT_EQ(OB_FILE_NOT_EXIST, stat_file_signature(ObString("/tmp/definitely_missing_zz"), sig3));
}

// ---------------- parquet reader ----------------

TEST_F(TestFileScanUtils, DISABLED_ParquetFixtureDecode)
{
  // fixture is generated by python tooling; enable when fixture available
  // at test runtime (see mysqltest file_sql suite for end-to-end coverage).
}

int main(int argc, char **argv)
{
  ::testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
