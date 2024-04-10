#!/usr/bin/env bash

result_dir=${result_dir:-"./local-data/collected_tests"}
mkdir -p $result_dir

find_tests() {
  kafka_client_version="$1"
  test_start_with="$2"
  output_var="$3"

  KAFKA_CLIENT_VERSION=$kafka_client_version ./gradlew test --test-dry-run
  TESTS=$(basename --suffix=.html -- app/build/reports/tests/test/classes/*.html)
  tests_arg=$(echo $TESTS | python3 -c "
import json, sys, math
maxn = 6
tests = [t for t in sys.stdin.read().split() if t.startswith(\"$test_start_with\")]
l = len(tests)
if l <= maxn:
    print(json.dumps([
        {'kafka_client_version': \"$kafka_client_version\", 'tests_arg': f'--tests {t}'}
        for t in tests
    ]))
else:
    n = math.ceil(l / maxn)
    print(json.dumps([
        {'kafka_client_version': \"$kafka_client_version\", 'tests_arg': t}
        for t in
        [' '.join(map(lambda x: '--tests ' + x, tests[i:min(i + n, l)])) for i in range(0, l, n)]
    ]))
  ")

  echo "=> found tests: $tests_arg"
  echo $tests_arg > $result_dir/$output_var.json
}

collect_results() {
    echo $@ | python3 -c "
import json, sys
includes = []
for t in sys.stdin.read().split():
    with open(\"$result_dir/\" + t + \".json\") as f:
        data = json.load(f)
        includes.extend(data)

tests = list(set([d['tests_arg'] for d in includes]))
client_versions = list(set([d['kafka_client_version'] for d in includes]))
print(json.dumps({'tests': tests, 'includes': includes, 'client_versions': client_versions}))
    "
}

find_tests "0.11.0.0" "io.hstream.kafka" "legacy_tests_arg_0_11"
find_tests "3.5.1"    "io.hstream.kafka" "legacy_tests_arg_3_5"
results=$(collect_results "legacy_tests_arg_0_11" "legacy_tests_arg_3_5")
legacy_includes=$(echo $results| jq -cM '.includes')
echo "==> legacy_includes: $legacy_includes"
echo $legacy_includes > $result_dir/legacy_tests.json

find_tests "3.5.1" "kafka" "tests_arg_3_5"
results=$(collect_results "tests_arg_3_5")
includes=$(echo $results| jq -cM '.includes')
echo "==> includes: $includes"
echo "$includes" > $result_dir/tests.json
