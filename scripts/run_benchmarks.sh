#!/usr/bin/env bash

set -euo pipefail

PROJECT_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
BENCHMARK_DIR="${PROJECT_ROOT}/tests/benchmark"
SERVER_SCRIPT="${BENCHMARK_DIR}/test_00_bench_server.py"
OUTPUT_DIR="${PROJECT_ROOT}/.benchmarks"
PYTHON_EXEC="python3"

TEST_SUITES=("${BENCHMARK_DIR}"/test_0[1-9]_*.py)

mkdir -p "${OUTPUT_DIR}"
SERVER_PID=""

cleanup() {
    if [[ -n "${SERVER_PID}" ]]; then
        kill -9 "${SERVER_PID}" >/dev/null 2>&1 || true
    fi
}
trap cleanup SIGINT SIGTERM EXIT

echo "Benchmark Suite Initialized"
echo "Runtime: $($PYTHON_EXEC --version) | Loop: uvloop"

for test_file in "${TEST_SUITES[@]}"; do
    test_name=$(basename "${test_file}" .py)
    json_output="${OUTPUT_DIR}/${test_name}.json"

    $PYTHON_EXEC -O "${SERVER_SCRIPT}" >/dev/null 2>&1 &
    SERVER_PID=$!
    
    sleep 2

    echo ""
    echo "[BENCHMARK] ${test_name}"
    
    $PYTHON_EXEC -m pytest -q "${test_file}" \
        --benchmark-only \
        --benchmark-json="${json_output}" \
        --benchmark-columns=min,max,mean,median,stddev,ops \
        --benchmark-sort=name

    kill -9 "${SERVER_PID}"
    wait "${SERVER_PID}" >/dev/null 2>&1 || true
    SERVER_PID=""
    
    sleep 1
done

echo ""
echo "Artifacts: ${OUTPUT_DIR}"