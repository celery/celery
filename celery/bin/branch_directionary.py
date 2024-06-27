branch_coverage = {
    "_handle_conf_update1": False,      # if 'task_routes' in kwargs or 'task_routes' in args
    "_handle_conf_update2": False,      # else branch

    "worker_main1": False,     # if argv is None
    "worker_main2": False,     # else branch
    "worker_main3": False,     # 'worker' not in argv
    "worker_main4": False,      # else branch

    "TermLogger.info1": False,
    "TermLogger.info2": False,

    "CeleryOption.get_default1": False,
    "CeleryOption.get_default2": False
}

branch_totals = {
    "_handle_conf_update": 2,
    "worker_main": 4,
    "TermLogger.info": 2,
    "CeleryOption.get_default": 2
}