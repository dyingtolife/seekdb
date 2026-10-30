/*
 * Test-only definition of common::ob_error_name. The production symbol lives
 * in src/share/ob_errno.cpp, which unit tests must not link (the module
 * dependency aspect forbids //src/share implementation targets). OB_LOG macros
 * in the file-scan implementation only need the symbol to exist at link time.
 */

namespace oceanbase
{
namespace common
{
const char *ob_error_name(const int err)
{
  (void)err;
  return "Unknown error";
}
}  // namespace common
}  // namespace oceanbase
