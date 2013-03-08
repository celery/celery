#!/bin/sh

# If you make changes to the celeryd init script,
# you can use this test script to verify you didn't break the universe

SERVICE="celeryd"
SERVICE_CMD="sudo /sbin/service $SERVICE"

run_test() {
    local msg="$1"
    local cmd="$2"
    local expected_retval="${3:-0}"
    local n=${#msg}

    echo
    echo `printf "%$((${n}+4))s" | tr " " "#"`
    echo "# $msg #"
    echo `printf "%$((${n}+4))s" | tr " " "#"`

    $cmd
    local retval=$?
    if [[ "$retval" == "$expected_retval" ]]; then
        echo "[PASSED]"
    else
        echo "[FAILED]"
        echo "Exit status: $retval, but expected: $expected_retval"
        exit $retval
    fi
}

run_test "stop should succeed" "$SERVICE_CMD stop" 0
run_test "status on a stopped service should return 1" "$SERVICE_CMD status" 1
run_test "stopping a stopped celery should not fail" "$SERVICE_CMD stop" 0
run_test "start should succeed" "$SERVICE_CMD start" 0
run_test "status on a running service should return 0" "$SERVICE_CMD status" 0
run_test "starting a running service should fail" "$SERVICE_CMD start" 1
run_test "restarting a running service should succeed" "$SERVICE_CMD restart" 0
run_test "status on a restarted service should return 0" "$SERVICE_CMD status" 0
run_test "stop should succeed" "$SERVICE_CMD stop" 0

echo "All tests passed!"
