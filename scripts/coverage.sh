#!/bin/bash
set -euo pipefail

# Setup coverage instrumentation environment
eval "$(cargo llvm-cov show-env --sh)"
cargo llvm-cov clean --workspace

# Build server binaries with coverage instrumentation
cargo build --bin hbbs --bin hbbr

# Run integration tests
# Server processes inherit LLVM_PROFILE_FILE and write profraw via atexit
# when the test binary's atexit handler sends SIGTERM before exit.
cargo test --tests 2>&1 | tee /tmp/coverage-tests.log
TEST_EXIT=${PIPESTATUS[0]}

# Generate HTML report covering all instrumented binaries
cargo llvm-cov report --html 2>&1 | tail -30
echo ""
echo "Coverage report: target/llvm-cov/html/index.html"
exit "$TEST_EXIT"
