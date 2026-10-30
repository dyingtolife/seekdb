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

#define USING_LOG_PREFIX SQL_TEST

#include <gtest/gtest.h>
#include "sql/parser/ob_parser.h"
#include "lib/allocator/ob_malloc.h"

using namespace oceanbase;
using namespace oceanbase::sql;
using namespace oceanbase::common;

class TestFileSqlParser : public ::testing::Test
{
public:
  virtual void SetUp() {}
  virtual void TearDown() {}
};

static int parse_sql(ObIAllocator &alloc, const char *sql, ParseResult &res)
{
  ObParser parser(alloc, DEFAULT_MYSQL_MODE);
  int ret = parser.parse(ObString(static_cast<int32_t>(strlen(sql)), sql), res);
  return ret;
}

static void free_result(ObIAllocator &alloc, ParseResult &res)
{
  ObParser parser(alloc, DEFAULT_MYSQL_MODE);
  parser.free_result(res);
}

static ParseNode *find_file_table_node(ParseNode *node)
{
  if (OB_ISNULL(node)) {
    return NULL;
  }
  if (T_FILE_TABLE_EXPRESSION == node->type_) {
    return node;
  }
  for (int64_t i = 0; i < node->num_child_; ++i) {
    ParseNode *r = find_file_table_node(node->children_[i]);
    if (NULL != r) {
      return r;
    }
  }
  return NULL;
}

static ParseNode *find_copy_node(ParseNode *node)
{
  if (OB_ISNULL(node)) {
    return NULL;
  }
  if (T_COPY_TO == node->type_) {
    return node;
  }
  for (int64_t i = 0; i < node->num_child_; ++i) {
    ParseNode *r = find_copy_node(node->children_[i]);
    if (NULL != r) {
      return r;
    }
  }
  return NULL;
}

TEST_F(TestFileSqlParser, ReadCsvBasic)
{
  ObArenaAllocator alloc(ObModIds::OB_SQL_PARSER);
  ParseResult res;
  ASSERT_EQ(OB_SUCCESS, parse_sql(alloc,
      "SELECT * FROM read_csv('/data/a.csv') AS t", res));
  ParseNode *root = res.result_tree_;
  ASSERT_NE((void *)NULL, root);
  ParseNode *ft = find_file_table_node(root);
  ASSERT_NE((void *)NULL, ft);
  // children: [func, path, alias]
  ASSERT_EQ(3, ft->num_child_);
  ASSERT_TRUE(ObString(ft->children_[0]->str_len_, ft->children_[0]->str_value_) == "read_csv");
  ASSERT_TRUE(ObString(ft->children_[1]->str_len_, ft->children_[1]->str_value_) == "/data/a.csv");
  ASSERT_TRUE(ObString(ft->children_[2]->str_len_, ft->children_[2]->str_value_) == "t");
}

TEST_F(TestFileSqlParser, ReadCsvWithParams)
{
  ObArenaAllocator alloc(ObModIds::OB_SQL_PARSER);
  ParseResult res;
  ASSERT_EQ(OB_SUCCESS, parse_sql(alloc,
      "SELECT count(*) FROM read_csv('/data/a.csv', header = true, sample_size = -1) AS t", res));
  ParseNode *ft = find_file_table_node(res.result_tree_);
  ASSERT_NE((void *)NULL, ft);
  ASSERT_EQ(4, ft->num_child_);  // func, path, params, alias
  ParseNode *params = ft->children_[2];
  ASSERT_EQ(T_EXPR_LIST, params->type_);
  ASSERT_EQ(2, params->num_child_);
  ParseNode *p0 = params->children_[0];
  ASSERT_EQ(T_EXPR_LIST, p0->type_);
  ASSERT_TRUE(ObString(p0->children_[0]->str_len_, p0->children_[0]->str_value_) == "header");
}

TEST_F(TestFileSqlParser, ReadJsonNdjsonAndParquetAndGlob)
{
  ObArenaAllocator alloc(ObModIds::OB_SQL_PARSER);
  ParseResult res;
  ASSERT_EQ(OB_SUCCESS, parse_sql(alloc,
      "SELECT * FROM read_json('/data/a.jsonl', format = 'newline_delimited') AS t", res));
  ParseNode *ft = find_file_table_node(res.result_tree_);
  ASSERT_NE((void *)NULL, ft);
  ASSERT_TRUE(ObString(ft->children_[0]->str_len_, ft->children_[0]->str_value_) == "read_json");

  ParseResult res2;
  ASSERT_EQ(OB_SUCCESS, parse_sql(alloc,
      "SELECT * FROM read_ndjson('/data/b.ndjson') AS t", res2));
  ft = find_file_table_node(res2.result_tree_);
  ASSERT_NE((void *)NULL, ft);
  ASSERT_TRUE(ObString(ft->children_[0]->str_len_, ft->children_[0]->str_value_) == "read_ndjson");

  ParseResult res3;
  ASSERT_EQ(OB_SUCCESS, parse_sql(alloc,
      "SELECT * FROM read_parquet('/data/a.parquet') AS t", res3));
  ft = find_file_table_node(res3.result_tree_);
  ASSERT_NE((void *)NULL, ft);
  ASSERT_TRUE(ObString(ft->children_[0]->str_len_, ft->children_[0]->str_value_) == "read_parquet");

  ParseResult res4;
  ASSERT_EQ(OB_SUCCESS, parse_sql(alloc,
      "SELECT * FROM glob('/data/*.csv') AS t", res4));
  ft = find_file_table_node(res4.result_tree_);
  ASSERT_NE((void *)NULL, ft);
  ASSERT_TRUE(ObString(ft->children_[0]->str_len_, ft->children_[0]->str_value_) == "glob");
}

TEST_F(TestFileSqlParser, ReadCsvWithoutAlias)
{
  ObArenaAllocator alloc(ObModIds::OB_SQL_PARSER);
  ParseResult res;
  ASSERT_EQ(OB_SUCCESS, parse_sql(alloc,
      "SELECT count(*) FROM read_csv('/data/a.csv')", res));
  ParseNode *ft = find_file_table_node(res.result_tree_);
  ASSERT_NE((void *)NULL, ft);
}

TEST_F(TestFileSqlParser, CopyToBasic)
{
  ObArenaAllocator alloc(ObModIds::OB_SQL_PARSER);
  ParseResult res;
  ASSERT_EQ(OB_SUCCESS, parse_sql(alloc,
      "COPY (SELECT id, name FROM t1 ORDER BY id) TO '/data/export.csv'", res));
  ParseNode *cp = find_copy_node(res.result_tree_);
  ASSERT_NE((void *)NULL, cp);
  ASSERT_EQ(2, cp->num_child_);
  ASSERT_TRUE(ObString(cp->children_[1]->str_len_, cp->children_[1]->str_value_) == "/data/export.csv");
}

TEST_F(TestFileSqlParser, CopyToWithOptions)
{
  ObArenaAllocator alloc(ObModIds::OB_SQL_PARSER);
  ParseResult res;
  ASSERT_EQ(OB_SUCCESS, parse_sql(alloc,
      "COPY (SELECT * FROM t1) TO '/data/export.csv' (format csv, header, nullstr 'NULL')", res));
  ParseNode *cp = find_copy_node(res.result_tree_);
  ASSERT_NE((void *)NULL, cp);
  ASSERT_EQ(3, cp->num_child_);
  ParseNode *opts = cp->children_[2];
  ASSERT_EQ(T_EXPR_LIST, opts->type_);
  ASSERT_EQ(3, opts->num_child_);
}

TEST_F(TestFileSqlParser, CopyToWithOptionsParen)
{
  ObArenaAllocator alloc(ObModIds::OB_SQL_PARSER);
  ParseResult res;
  ASSERT_EQ(OB_SUCCESS, parse_sql(alloc,
      "COPY (SELECT * FROM t1) TO '/data/export.csv' (format csv)", res));
  ParseNode *cp = find_copy_node(res.result_tree_);
  ASSERT_NE((void *)NULL, cp);
}

static ParseNode *find_explain_node(ParseNode *node)
{
  if (OB_ISNULL(node)) {
    return NULL;
  }
  if (T_EXPLAIN == node->type_) {
    return node;
  }
  for (int64_t i = 0; i < node->num_child_; ++i) {
    ParseNode *r = find_explain_node(node->children_[i]);
    if (NULL != r) {
      return r;
    }
  }
  return NULL;
}

TEST_F(TestFileSqlParser, DescribeFileTableParses)
{
  // DESCRIBE SELECT * FROM read_csv(...) resolves through the explain path.
  // ObParser::parse wraps every top level statement in a statement list
  // (verified for `DESCRIBE t1`, `EXPLAIN SELECT ...` and plain selects), so
  // the DESCRIBE outcome is asserted as "an explain node exists in the tree"
  // rather than "the root is T_EXPLAIN".
  ObArenaAllocator alloc(ObModIds::OB_SQL_PARSER);
  ParseResult res;
  ASSERT_EQ(OB_SUCCESS, parse_sql(alloc,
      "DESCRIBE SELECT * FROM read_csv('/data/a.csv') AS t", res));
  ParseNode *root = res.result_tree_;
  ASSERT_NE((void *)NULL, root);
  ParseNode *explain = find_explain_node(root);
  ASSERT_NE((void *)NULL, explain) << "DESCRIBE must resolve through the explain path";
  ParseNode *ft = find_file_table_node(root);
  ASSERT_NE((void *)NULL, ft);
}

int main(int argc, char **argv)
{
  ::testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
