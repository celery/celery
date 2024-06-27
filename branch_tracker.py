import os
from branch_dictionary import branch_coverage, branch_totals

# Function to print branch coverage
def print_branch_coverage():
    print("\nBRANCH COVERAGE:\n")

    covered_branches = {
        "_handle_conf_update": 0,
        "worker_main": 0,
        "Inspect._prepare": 0,
        "Worker.on_start": 0
    }

    print("Branch Hits:\n")
    s = ""
    for key, value in branch_coverage.items():
        s += f"{key} is covered: {value}\n"
    print(s)

    for key, value in branch_coverage.items():
        if value:
            if key.startswith("worker_main"):
                covered_branches["worker_main"] += 1
            elif key.startswith("_handle_conf_update"):
                covered_branches["_handle_conf_update"] += 1
            elif key.startswith("Inspect._prepare"):
                covered_branches["Inspect._prepare"] += 1
            elif key.startswith("Worker.on_start"):
                covered_branches["Worker.on_start"] += 1
    
    print("Branch Coverage Percentage:\n")
    for key, value in branch_totals.items():
        covered = covered_branches[key]
        coverage_percentage = (covered / value) * 100
        print(f"{key}: {covered} out of {value} branches covered ({coverage_percentage:.2f}%)")

# Function to run all tests and print coverage
def run_coverage():
    import pytest
    pytest.main(['t/unit/apps/test_worker.py'])

    print_branch_coverage()

if __name__ == '__main__':
    run_coverage()