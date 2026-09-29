#include "test_harness.hpp"

#include <cstdint>
#include <iostream>
#include <string>
#include <utility>
#include <vector>

namespace airflow_test {
namespace {

std::uint64_t g_checks = 0;
std::uint64_t g_failures = 0;
std::uint64_t g_case_checks = 0;
std::uint64_t g_case_failures = 0;
std::string g_current_case;

}  // namespace

std::vector<TestCase>& registry() {
  // A function-local static, because a test source registers its cases from
  // static initialisers that may run before any global object in this file.
  static std::vector<TestCase> cases;
  return cases;
}

Registrar::Registrar(const char* name, Body body) {
  registry().push_back(TestCase{name, std::move(body)});
}

void count_check() {
  g_checks += 1;
  g_case_checks += 1;
}

void report_failure(const char* file, int line, const std::string& expression,
                    const std::string& detail) {
  g_failures += 1;
  g_case_failures += 1;
  std::cout << "FAIL " << g_current_case << " " << file << ":" << line << ": " << expression;
  if (!detail.empty()) {
    std::cout << " [" << detail << "]";
  }
  std::cout << "\n";
  // Flushed immediately: a case that kills the process later must not take the
  // record of the failure that explained it down with it.
  std::cout.flush();
}

std::string display(const std::string& value) { return "\"" + value + "\""; }
std::string display(const char* value) {
  return value == nullptr ? std::string("<null>") : display(std::string(value));
}
std::string display(bool value) { return value ? "true" : "false"; }
std::string display(std::nullptr_t) { return "<null>"; }

int run_all() {
  std::uint64_t cases = 0;
  for (const TestCase& test : registry()) {
    g_current_case = test.name;
    g_case_checks = 0;
    g_case_failures = 0;
    test.body();
    cases += 1;
    std::cout << "case " << test.name << ": checks=" << g_case_checks
              << " failures=" << g_case_failures << "\n";
    std::cout.flush();
  }
  g_current_case.clear();
  // A suite that performed no check at all is a failure, not a pass: it has
  // proved nothing, and reporting success for it would be a lie.
  const bool passed = g_failures == 0 && g_checks != 0;
  std::cout << (passed ? "pass " : "FAIL ") << "checks=" << g_checks
            << " failures=" << g_failures << " cases=" << cases << "\n";
  std::cout.flush();
  return passed ? 0 : 1;
}

}  // namespace airflow_test

int main() { return ::airflow_test::run_all(); }
