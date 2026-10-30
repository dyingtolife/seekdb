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

#pragma once

#include "sql/resolver/ob_stmt_resolver.h"

namespace oceanbase
{
namespace sql
{

// COPY (query) TO 'file' [options]. Resolves the child SELECT as the outer
// statement and attaches a synthesized SELECT INTO OUTFILE item carrying the
// CSV options, so the export executes on the battle-tested select-into path.
class ObCopyToResolver : public ObStmtResolver
{
public:
  explicit ObCopyToResolver(ObResolverParams &params) : ObStmtResolver(params) {}
  virtual ~ObCopyToResolver() {}
  virtual int resolve(const ParseNode &parse_tree);
private:
  int canonicalize_local_path(const common::ObString &path, common::ObString &canonical);
};

}  // namespace sql
}  // namespace oceanbase
