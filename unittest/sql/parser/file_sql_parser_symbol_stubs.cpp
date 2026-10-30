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

// Link seams for parser-only unit tests.
//
// //src/sql/parser:sql_parser_driver is the standalone SQL parser. In a server
// build its references into the rest of the system are satisfied by
// sql_runtime and the PL runtime; a parser-only unit test deliberately links
// neither (that would drag the whole SQL engine into the test binary), so the
// three external symbols it needs are supplied locally here. The suite feeds
// only plain DML/DDL text through ObParser, so none of these stubs sits on an
// exercised path. No project header is included on purpose: unit tests must
// not depend on implementation-only header targets.

#include <cstddef>
#include <cstdint>

// Referenced by the generated C parser (src/sql/parser/parse_node.c); the
// production definition lives in src/sql/ob_sql_utils.cpp.
extern "C" bool check_stack_overflow_c()
{
  return false;  // unit tests never recurse deeply enough to overflow
}

// Referenced by the generated C parser when binding PL variables; the
// production definition lives in src/pl/ob_pl_stmt.cpp.
extern "C" int lookup_pl_symbol(const void *pl_ns, const char *symbol,
                                size_t len, int64_t *find_idx)
{
  (void)pl_ns;
  (void)symbol;
  (void)len;
  (void)find_idx;
  return -1;  // "not found": PL statements are outside this suite's scope
}

namespace oceanbase
{
namespace common
{
// Production definition lives in src/share/ob_errno.cpp; cf. the sibling
// unittest/sql/ob_test_error_name.cpp seam for the same rationale.
const char *ob_errpkt_strerror(const int err)
{
  (void)err;
  return "Unknown error";
}
}  // namespace common
}  // namespace oceanbase

// oceanbase::pl::ObPLParser::parse(const ObString &, const ObString &,
//                                  ObParseResult &, bool)
// Production definition lives in src/sql/pl/parser/ob_pl_parser.cpp. The class
// header is not includable from a unit test (it pulls PL implementation
// headers), so the symbol is supplied through its mangled name; the parameters
// are opaque here because the function is never called by the suite.
extern "C" int file_sql_pl_parser_parse_stub(void *self, const void *stmt_block,
                                             const void *orig_stmt_block,
                                             void *parse_result,
                                             bool is_inner_parse)
    __asm__("_ZN9oceanbase2pl10ObPLParser5parseERKNS_6common8ObStringES5_R13ObParseResultb");

extern "C" int file_sql_pl_parser_parse_stub(void *self, const void *stmt_block,
                                             const void *orig_stmt_block,
                                             void *parse_result,
                                             bool is_inner_parse)
{
  (void)self;
  (void)stmt_block;
  (void)orig_stmt_block;
  (void)parse_result;
  (void)is_inner_parse;
  return -4007;  // OB_NOT_SUPPORTED (src/oblib/lib/ob_errno.h)
}
