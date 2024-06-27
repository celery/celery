import os
from branch_dictionary import branch_coverage, branch_totals

# Function to print branch coverage
def print_branch_coverage():
    print("\nBRANCH COVERAGE:\n")

    covered_branches = {
        "_handle_conf_update": 0,
        "worker_main": 0,
        "TermLogger.info": 0,
        "CeleryOption.get_default": 0
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
            elif key.startswith("TermLogger.info"):
                covered_branches["TermLogger.info"] += 1
            elif key.startswith("CeleryOption.get_default"):
                covered_branches["CeleryOption.get_default"] += 1
    
    print("Branch Coverage Percentage:\n")
    for key, value in branch_totals.items():
        covered = covered_branches[key]
        coverage_percentage = (covered / value) * 100
        print(f"{key}: {covered} out of {value} branches covered ({coverage_percentage:.2f}%)")

# Function to run all tests and print coverage
def run_coverage():
    import pytest
    pytest.main(["t/unit/app/test_celery.py"])

    print_branch_coverage()

if __name__ == '__main__':
    run_coverage()