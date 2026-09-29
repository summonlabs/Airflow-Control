# Contributing to Airflow Control

Airflow Control is licensed under the Apache License 2.0. Contributions are
accepted under the same license.

## Licensing of contributions

By submitting a contribution to this repository you agree that your contribution is
provided under the terms of the Apache License 2.0, without additional terms or
conditions, as described in section 5 of that license. There is no Contributor
License Agreement (CLA) and no copyright assignment requirement. You retain the
copyright to your contribution.

Do not add copyright headers that attribute work to anyone other than the actual
author, and do not add `Co-authored-by` trailers or other attribution trailers to
commits in this repository.

## Building and testing

Requirements: CMake 3.21 or newer and a C++20 compiler. The primary exercised
platform is Windows with MSVC (Visual Studio 2022, toolset 19.44 or newer).

```sh
cmake -S . -B build -DCMAKE_BUILD_TYPE=Release
cmake --build build --config Release
ctest --test-dir build -C Release --output-on-failure
```

The build must be warning-free. First-party warnings are errors: MSVC builds with
`/W4 /WX /permissive-`, other compilers with `-Wall -Wextra -Wpedantic -Werror`.
Do not disable a warning globally to make a change compile; fix the defect or, when
a warning is genuinely wrong for a specific construct, suppress it narrowly at the
site with a comment that explains why.

Tests must not use timeouts. CTest timeouts, shell timeout wrappers, watchdog
processes, and process-limit wrappers are prohibited. A test that hangs is a defect
to diagnose, not to bound.

## Code quality requirements

- Portable C++20. Standard library only; no third-party dependencies.
- Authoritative quantities (airflow, differential pressure, temperature
  obligations, fan setpoints, sequence numbers, generations) use checked integer
  arithmetic in exact fixed units. Floating point is never used for an
  authoritative value, and a limit is never derived here: the limits this runtime
  enforces are consumed, with provenance, from the authority that owns them.
- Every mutation that depends on current state must carry explicit preconditions.
  Stale authority is refused, never merged.
- Acknowledgement is not effect. Telemetry is not authority. Persisted state is not
  current evidence. Zero is not unknown. Unsupported is not unavailable.
- Containment breach and contradictory sensor evidence always take precedence over
  an optimization request, and the precedence order is deterministic.
- Persisted and external input is untrusted: bound sizes before allocating, reject
  malformed, truncated, oversized, wrong-version, wrong-endian, and
  path-manipulated state, and never adopt an unrelated store.
- Concurrency changes must preserve the documented lock order (see the concurrency
  section of the README) and must keep the read path free of locks held across
  callback or I/O boundaries.
- Do not claim behavior that is not proven by an executable check in this
  repository. Label real, synthetic, and unsupported evidence precisely; no
  hardware behavior may be claimed without a hardware test that actually ran.

## Adding tests

Every behavioral change needs a test. Prefer deterministic tests with injected
logical instants over tests that depend on wall-clock time. Property and randomized
tests must use fixed seeds so failures reproduce exactly. Adapter behavior is
modeled with deterministic synthetic adapters, never with timing assumptions.
