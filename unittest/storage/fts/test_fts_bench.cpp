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
 *
 * Micro benchmark for the fulltext tokenizer hot path (not part of the scored
 * competition harness). Used for controlled before/after measurement of the
 * ObAddWord / stopword checker / whitespace tokenizer changes.
 *
 * Exercises the exact production entry point used by all scored metrics:
 *   ObFTParseHelper::segment(meta, text, len, doc_len, word_map)
 * which internally runs ObBasicEnglishFTParserDesc::segment() (whitespace
 * tokenizer -> basic english normalizer) feeding ObAddWord::process_word().
 *
 * Requires ObFTParseData::init_global() (stopword checker + dict hub).
 *
 * Run via:
 *   bazel run //unittest/storage/fts:test_fts_bench -c opt -- \
 *     --gtest_also_run_disabled_tests --gtest_filter=*tokenize_bench*
 */

#define USING_LOG_PREFIX STORAGE_FTS

#include "storage/fts/ob_fts_parser_helper.h"
#include "lib/charset/ob_charset.h"
#include <gtest/gtest.h>
#include <chrono>
#include <cstdio>

namespace oceanbase
{
namespace storage
{
using namespace common;

namespace
{

// Synthetic but realistic English-ish corpus: documents of ~40 words with
// common stopwords mixed in, plus punctuation and mixed case.
const char *WORD_POOL[] = {
    "the", "of", "and", "to", "in", "a", "is", "that", "for", "it",
    "database", "storage", "index", "query", "performance", "system",
    "memory", "engine", "table", "column", "row", "data", "value",
    "optimization", "benchmark", "fulltext", "search", "retrieval",
    "token", "parser", "inverted", "btree", "log", "structure", "merge",
    "OceanBase", "MySQL", "engine."};
const int64_t WORD_POOL_CNT = static_cast<int64_t>(sizeof(WORD_POOL) / sizeof(WORD_POOL[0]));

uint64_t g_seed = 0x20261029ULL;

uint64_t next_rand()
{
  // xorshift64
  g_seed ^= g_seed << 13;
  g_seed ^= g_seed >> 7;
  g_seed ^= g_seed << 17;
  return g_seed;
}

void build_doc(ObArenaAllocator &allocator, const int64_t target_len, ObString &out)
{
  int64_t cap = target_len * 16 + 64;
  char *buf = static_cast<char *>(allocator.alloc(cap));
  int64_t pos = 0;
  int64_t remaining = target_len;
  while (remaining > 0) {
    const char *w = WORD_POOL[next_rand() % WORD_POOL_CNT];
    int64_t wl = STRLEN(w);
    MEMCPY(buf + pos, w, wl);
    pos += wl;
    buf[pos++] = (0 == (next_rand() % 7)) ? ',' : ' ';
    remaining -= wl + 1;
  }
  out.assign_ptr(buf, static_cast<int32_t>(pos));
}

} // anonymous namespace

// Disabled by default so that `bazel test` does not run it; executed explicitly
// with --gtest_also_run_disabled_tests. Measures the combined tokenizer +
// stopword checker + groupby hot path shared by all 9 scored metrics.
TEST(DISABLED_FTSBench, tokenize_bench)
{
  const int64_t n_iters = 30;

  ASSERT_EQ(OB_SUCCESS, ObFTParseData::init_global());

  ObFTParseHelper helper;
  ObArenaAllocator helper_allocator("BenchHelper");
  ASSERT_EQ(OB_SUCCESS, helper.init(&helper_allocator, ObString("beng.1"), ObString("")));

  ObObjMeta meta;
  meta.set_varchar();
  meta.set_collation_type(CS_TYPE_UTF8MB4_GENERAL_CI);

  // build corpus once: 200 documents x ~40 words (~260 chars each)
  ObArenaAllocator corpus_allocator("BenchCorpus");
  const int64_t REAL_DOCS = 200;
  ObString docs[REAL_DOCS];
  for (int64_t d = 0; d < REAL_DOCS; ++d) {
    build_doc(corpus_allocator, 260, docs[d]);
  }

  double total_ms = 0;
  int64_t checksum = 0;
  int64_t doc_length = 0;
  for (int64_t iter = 0; iter < n_iters; ++iter) {
    ObArenaAllocator allocator("BenchSeg");
    auto t0 = std::chrono::steady_clock::now();
    for (int64_t d = 0; d < REAL_DOCS; ++d) {
      ObFTWordMap word_map;
      ASSERT_EQ(OB_SUCCESS, word_map.create(128, "BenchMap", "BenchMap"));
      ASSERT_EQ(OB_SUCCESS, helper.segment(meta, docs[d].ptr(), docs[d].length(),
          doc_length, word_map));
      checksum += word_map.size();
      word_map.destroy();
    }
    auto t1 = std::chrono::steady_clock::now();
    total_ms += std::chrono::duration<double, std::milli>(t1 - t0).count();
    allocator.reset();
  }

  double avg_us_per_doc = total_ms * 1000.0 / (static_cast<double>(n_iters) * REAL_DOCS);
  printf("FTSBENCH docs=%ld iters=%ld total_ms=%.3f avg_us_per_doc=%.4f checksum=%ld\n",
      static_cast<long>(REAL_DOCS), static_cast<long>(n_iters), total_ms,
      avg_us_per_doc, static_cast<long>(checksum));
  fprintf(stderr, "FTSBENCH avg_us_per_doc=%.4f checksum=%ld\n",
      avg_us_per_doc, static_cast<long>(checksum));

  ObFTParseData::deinit_global();
}

} // end namespace storage
} // end namespace oceanbase

int main(int argc, char **argv)
{
  ::testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
