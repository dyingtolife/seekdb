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

#define USING_LOG_PREFIX STORAGE_FTS

#include "storage/fts/ob_fts_stop_word.h"
#include "storage/fts/ob_fts_parser_helper.h"
#include "lib/charset/ob_charset.h"
#include <gtest/gtest.h>

namespace oceanbase
{
namespace storage
{
using namespace common;

class TestFTSStopWordOpt : public ::testing::Test
{
public:
  void SetUp() override
  {
    ASSERT_EQ(OB_SUCCESS, ObCharset::init_charset());
    // ObAddWord::check_stopword dereferences the process-global ObFTParseData
    // (same as production, where ObServer::init_fts() initializes it once).
    ASSERT_EQ(OB_SUCCESS, ObFTParseData::init_global());
    ASSERT_EQ(OB_SUCCESS, stop_word_checker_.init());
  }

  void TearDown() override
  {
    stop_word_checker_.destroy();
    ObFTParseData::deinit_global();
  }

  static ObFTWord make_word(const char *str, ObCollationType coll)
  {
    ObObjMeta meta;
    meta.set_varchar();
    meta.set_collation_type(coll);
    return ObFTWord(STRLEN(str), str, meta);
  }

protected:
  ObStopWordChecker stop_word_checker_;
};

// The stopword checker must classify the built-in stopword list correctly and
// keep non-stop words (this covers the fast path added for same-collation
// words: the fast path bypasses charset_convert and must stay equivalent).
TEST_F(TestFTSStopWordOpt, stopword_classification)
{
  const char *stopwords[] = {"a", "the", "The", "WITH", "www", "com", "about"};
  for (int64_t i = 0; i < ARRAYSIZEOF(stopwords); ++i) {
    ObFTWord word = make_word(stopwords[i], CS_TYPE_UTF8MB4_GENERAL_CI);
    bool is_stopword = false;
    ASSERT_EQ(OB_SUCCESS, stop_word_checker_.check_stopword(word, is_stopword));
    ASSERT_TRUE(is_stopword) << "expected stopword: " << stopwords[i];
  }

  const char *nonstop[] = {"hello", "world", "database", "Theatre", "history"};
  for (int64_t i = 0; i < ARRAYSIZEOF(nonstop); ++i) {
    ObFTWord word = make_word(nonstop[i], CS_TYPE_UTF8MB4_GENERAL_CI);
    bool is_stopword = true;
    ASSERT_EQ(OB_SUCCESS, stop_word_checker_.check_stopword(word, is_stopword));
    ASSERT_FALSE(is_stopword) << "expected non-stopword: " << nonstop[i];
  }
}

// Words in a binary collation must go through the convert path and produce the
// same classification as words already in the stopword collation.
TEST_F(TestFTSStopWordOpt, cross_collation_classification)
{
  const char *stopwords[] = {"a", "the", "The", "WITH", "www"};
  for (int64_t i = 0; i < ARRAYSIZEOF(stopwords); ++i) {
    bool is_stopword_fast = true;
    bool is_stopword_slow = true;
    ObFTWord word_ci = make_word(stopwords[i], CS_TYPE_UTF8MB4_GENERAL_CI);
    ObFTWord word_bin = make_word(stopwords[i], CS_TYPE_UTF8MB4_BIN);
    ASSERT_EQ(OB_SUCCESS, stop_word_checker_.check_stopword(word_ci, is_stopword_fast));
    ASSERT_EQ(OB_SUCCESS, stop_word_checker_.check_stopword(word_bin, is_stopword_slow));
    ASSERT_EQ(is_stopword_fast, is_stopword_slow)
        << "collation mismatch result for: " << stopwords[i];
    ASSERT_TRUE(is_stopword_slow) << "expected stopword: " << stopwords[i];
  }

  const char *nonstop[] = {"hello", "Database", "history"};
  for (int64_t i = 0; i < ARRAYSIZEOF(nonstop); ++i) {
    bool is_stopword_fast = true;
    bool is_stopword_slow = true;
    ObFTWord word_ci = make_word(nonstop[i], CS_TYPE_UTF8MB4_GENERAL_CI);
    ObFTWord word_bin = make_word(nonstop[i], CS_TYPE_UTF8MB4_BIN);
    ASSERT_EQ(OB_SUCCESS, stop_word_checker_.check_stopword(word_ci, is_stopword_fast));
    ASSERT_EQ(OB_SUCCESS, stop_word_checker_.check_stopword(word_bin, is_stopword_slow));
    ASSERT_EQ(is_stopword_fast, is_stopword_slow)
        << "collation mismatch result for: " << nonstop[i];
    ASSERT_FALSE(is_stopword_slow) << "expected non-stopword: " << nonstop[i];
  }
}

// groupby_word aggregation: repeated tokens must accumulate their frequency in
// place, unique tokens insert with count 1. This pins the exact observable
// content of the word map that the single-lookup optimization rewrote.
TEST_F(TestFTSStopWordOpt, groupby_word_accumulation)
{
  ObArenaAllocator allocator("TestGrpWord");
  ObFTWordMap word_map;
  ASSERT_EQ(OB_SUCCESS, word_map.create(64, "TestGrpMap", "TestGrpMap"));

  ObObjMeta meta;
  meta.set_varchar();
  meta.set_collation_type(CS_TYPE_UTF8MB4_GENERAL_CI);
  ObAddWordFlag flag;
  flag.set_groupby_word();
  ObFTParserProperty property;
  ObAddWord add_word(property, meta, flag, allocator, word_map);

  const char *tokens[] = {"apple", "banana", "apple", "cherry", "apple", "banana"};
  const int64_t freqs[] = {1, 1, 2, 1, 1, 3};
  const int64_t token_cnt = ARRAYSIZEOF(tokens);
  for (int64_t i = 0; i < token_cnt; ++i) {
    ASSERT_EQ(OB_SUCCESS, add_word.process_word(tokens[i], STRLEN(tokens[i]),
        static_cast<int64_t>(STRLEN(tokens[i])), freqs[i]));
  }

  int64_t apple_cnt = 0;
  int64_t banana_cnt = 0;
  int64_t cherry_cnt = 0;
  ObFTWord apple = make_word("apple", CS_TYPE_UTF8MB4_GENERAL_CI);
  ObFTWord banana = make_word("banana", CS_TYPE_UTF8MB4_GENERAL_CI);
  ObFTWord cherry = make_word("cherry", CS_TYPE_UTF8MB4_GENERAL_CI);
  ASSERT_EQ(OB_SUCCESS, word_map.get_refactored(apple, apple_cnt));
  ASSERT_EQ(OB_SUCCESS, word_map.get_refactored(banana, banana_cnt));
  ASSERT_EQ(OB_SUCCESS, word_map.get_refactored(cherry, cherry_cnt));
  // apple appears at i=0(1), i=2(2), i=4(1) => 1+2+1 = 4
  // banana appears at i=1(1), i=5(3) => 1+3 = 4
  ASSERT_EQ(4, apple_cnt);
  ASSERT_EQ(4, banana_cnt);
  ASSERT_EQ(1, cherry_cnt);
  ASSERT_EQ(3, word_map.size());

  word_map.destroy();
}

// No-groupby mode keeps one row per token with count 1 (last write wins in the
// legacy map with overwrite semantics - preserved verbatim by the change).
TEST_F(TestFTSStopWordOpt, groupby_disabled_keeps_last_write)
{
  ObArenaAllocator allocator("TestGrpWord");
  ObFTWordMap word_map;
  ASSERT_EQ(OB_SUCCESS, word_map.create(64, "TestGrpMap", "TestGrpMap"));

  ObObjMeta meta;
  meta.set_varchar();
  meta.set_collation_type(CS_TYPE_UTF8MB4_GENERAL_CI);
  ObAddWordFlag flag;
  ObFTParserProperty property;
  ObAddWord add_word(property, meta, flag, allocator, word_map);

  ASSERT_EQ(OB_SUCCESS, add_word.process_word("dup", 3, 3, 1));
  // the legacy non-groupby branch inserts via set_refactored without the
  // overwrite flag, so a duplicate token reports OB_HASH_EXIST (-4200) - the
  // untouched pre-existing behavior that this change does not alter.
  ASSERT_EQ(OB_HASH_EXIST, add_word.process_word("dup", 3, 3, 1));

  int64_t dup_cnt = 0;
  ObFTWord dup = make_word("dup", CS_TYPE_UTF8MB4_GENERAL_CI);
  ASSERT_EQ(OB_SUCCESS, word_map.get_refactored(dup, dup_cnt));
  ASSERT_EQ(1, dup_cnt);
  ASSERT_EQ(1, word_map.size());

  word_map.destroy();
}

// process_word counters with BENG parser flags (min_max + stopword + groupby):
// stopwords and out-of-range lengths are skipped, the rest accumulates.
TEST_F(TestFTSStopWordOpt, beng_flags_pipeline)
{
  ObArenaAllocator allocator("TestGrpWord");
  ObFTWordMap word_map;
  ASSERT_EQ(OB_SUCCESS, word_map.create(64, "TestGrpMap", "TestGrpMap"));

  ObObjMeta meta;
  meta.set_varchar();
  meta.set_collation_type(CS_TYPE_UTF8MB4_GENERAL_CI);
  ObAddWordFlag flag;
  flag.set_min_max_word();
  flag.set_stop_word();
  flag.set_groupby_word();
  ObFTParserProperty property;
  // default min_token_size_ is 3 (FT_DEFAULT_MIN_TOKEN_SIZE); make the bound
  // explicit so the 2-char word "ox" below is inside the accepted range.
  property.min_token_size_ = 1;
  property.max_token_size_ = 64;
  ObAddWord add_word(property, meta, flag, allocator, word_map);

  // "the" is a stopword; "ox"/"zebra" are regular words inside [1, 64] chars.
  ASSERT_EQ(OB_SUCCESS, add_word.process_word("the", 3, 3, 1));
  ASSERT_EQ(OB_SUCCESS, add_word.process_word("ox", 2, 2, 1));
  ASSERT_EQ(OB_SUCCESS, add_word.process_word("ox", 2, 2, 1));
  ASSERT_EQ(OB_SUCCESS, add_word.process_word("zebra", 5, 5, 1));

  // the -> stopword, ox -> 1, ox -> +1, zebra -> 1 => non_stopword_cnt = 3
  ASSERT_EQ(3, add_word.get_add_word_count());

  int64_t ox_cnt = 0;
  int64_t zebra_cnt = 0;
  ObFTWord ox = make_word("ox", CS_TYPE_UTF8MB4_GENERAL_CI);
  ObFTWord zebra = make_word("zebra", CS_TYPE_UTF8MB4_GENERAL_CI);
  ASSERT_EQ(OB_SUCCESS, word_map.get_refactored(ox, ox_cnt));
  ASSERT_EQ(OB_SUCCESS, word_map.get_refactored(zebra, zebra_cnt));
  ASSERT_EQ(2, ox_cnt);
  ASSERT_EQ(1, zebra_cnt);
  ASSERT_EQ(2, word_map.size());

  word_map.destroy();
}

// min/max token filtering: tokens outside [min, max] char counts are dropped.
TEST_F(TestFTSStopWordOpt, min_max_filtering)
{
  ObArenaAllocator allocator("TestGrpWord");
  ObFTWordMap word_map;
  ASSERT_EQ(OB_SUCCESS, word_map.create(64, "TestGrpMap", "TestGrpMap"));

  ObObjMeta meta;
  meta.set_varchar();
  meta.set_collation_type(CS_TYPE_UTF8MB4_GENERAL_CI);
  ObAddWordFlag flag;
  flag.set_min_max_word();
  flag.set_groupby_word();
  ObFTParserProperty property;
  property.min_token_size_ = 2;
  property.max_token_size_ = 4;
  ObAddWord add_word(property, meta, flag, allocator, word_map);

  ASSERT_EQ(OB_SUCCESS, add_word.process_word("a", 1, 1, 1));       // too short
  ASSERT_EQ(OB_SUCCESS, add_word.process_word("abc", 3, 3, 1));     // kept
  ASSERT_EQ(OB_SUCCESS, add_word.process_word("toolong", 7, 7, 1)); // too long

  ASSERT_EQ(1, word_map.size());
  ASSERT_EQ(1, add_word.get_add_word_count());

  word_map.destroy();
}

} // end namespace storage
} // end namespace oceanbase

int main(int argc, char **argv)
{
  ::testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
