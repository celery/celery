branch_coverage = {
    "_handle_conf_update1": False,      # if 'task_routes' in kwargs or 'task_routes' in args
    "_handle_conf_update2": False,      # else branch

    "worker_main1": False,     # if argv is None
    "worker_main2": False,     # else branch
    "worker_main3": False,     # 'worker' not in argv
    "worker_main4": False,      # else branch

    "Inspect._prepare1": False,
    "Inspect._prepare2": False,
    "Inspect._prepare3": False,
    "Inspect._prepare4": False,
    "Inspect._prepare5": False,
    "Inspect._prepare6": False,
    "Inspect._prepare7": False,
    "Inspect._prepare8": False,

    "Worker.on_start1": False,
    "Worker.on_start2": False,
    "Worker.on_start3": False,
    "Worker.on_start4": False,
    "Worker.on_start5": False,
    "Worker.on_start6": False,
    "Worker.on_start7": False,
    "Worker.on_start8": False,
    "Worker.on_start9": False,
    "Worker.on_start10": False,
    "Worker.on_start11": False,
    "Worker.on_start12": False
}

branch_totals = {
    "_handle_conf_update": 2,
    "worker_main": 4,
    "Inspect._prepare": 8,
    "Worker.on_start": 12
}