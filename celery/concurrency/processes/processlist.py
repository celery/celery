import os
import signal

__all__ = ["get_processestree_pids", "kill_processtree"]

if os.platform == 'posix':
    # In UNIX it could probably be done more properly using setpgrp/killpg
    # but when you only have a hammer...
    try:
        import psutil
    except ImportError:
        raise ImportError("`psutil` have to be installed in POSIX platforms")

    def get_all_processes_pids():
        """Return a dictionary with all processes pids as keys and their 
           parents as value. Ignore processes with no parents.
        """
        proclist = psutil.get_process_list()
        return dict([(proc.pid, proc.ppid) for proc in proclist if proc.ppid])
    
elif os.platform == 'nt':
    # psutil supports win32, but it's painfully slow. So to avoid adding big
    # dependencies like pywin32 a ctypes based solution is preferred

    # Code based on the winappdbg project http://winappdbg.sourceforge.net/ 
    from ctypes import byref, sizeof, windll, Structure, WinError, POINTER
    from ctypes.wintypes import DWORD, c_size_t, LONG, c_char, c_void_p

    ERROR_NO_MORE_FILES=18
    INVALID_HANDLE_VALUE=c_void_p(-1).value

    class PROCESSENTRY32(Structure):
        _fields_ = [
            ('dwSize',              DWORD),
            ('cntUsage',            DWORD),
            ('th32ProcessID',       DWORD),
            ('th32DefaultHeapID',   c_size_t),
            ('th32ModuleID',        DWORD),
            ('cntThreads',          DWORD),
            ('th32ParentProcessID', DWORD),
            ('pcPriClassBase',      LONG),
            ('dwFlags',             DWORD),
            ('szExeFile',           c_char * 260),
        ]
    LPPROCESSENTRY32 = POINTER(PROCESSENTRY32)

    def CreateToolhelp32Snapshot(dwFlags = 2, th32ProcessID = 0):
        hSnapshot = windll.kernel32.CreateToolhelp32Snapshot(dwFlags, th32ProcessID)
        if hSnapshot == INVALID_HANDLE_VALUE:
            raise WinError()
        return hSnapshot

    def Process32First(hSnapshot):
        pe        = PROCESSENTRY32()
        pe.dwSize = sizeof(PROCESSENTRY32)
        success = windll.kernel32.Process32First(hSnapshot, byref(pe))
        if not success:
            if windll.kernel32.GetLastError() == ERROR_NO_MORE_FILES:
                return None
            raise WinError()
        return pe

    def Process32Next(hSnapshot, pe = None):
        if pe is None:
            pe = PROCESSENTRY32()
        pe.dwSize = sizeof(PROCESSENTRY32)
        success = windll.kernel32.Process32Next(hSnapshot, byref(pe))
        if not success:
            if windll.kernel32.GetLastError() == ERROR_NO_MORE_FILES:
                return None
            raise WinError()
        return pe

    def get_all_processes_pids():
        """Return a dictionary with all processes pids as keys and their 
           parents as value. Ignore processes with no parents.
        """
        h = CreateToolhelp32Snapshot()
        parents = {}
        pe = Process32First(h)
        while pe:
            if pe.th32ParentProcessID:
                parents[pe.th32ProcessID] = pe.th32ParentProcessID
            pe = Process32Next(h, pe)

        return parents


def get_processestree_pids(pid, include_parent=True):
    """Return a list with all the pids of a process tree"""
    parents = get_all_processes_pids()
    all_pids = parents.keys()
    pids = set([pid])
    while True:
        pids_new = pids.copy()

        for _pid in all_pids:
            if parents[_pid] in pids:
                pids_new.add(_pid)

        if pids_new == pids:
            break

        pids = pids_new.copy()

    if not include_parent:
        pids.remove(pid)

    return list(pids)

def kill_processtree(pid, signum):
    """Kill a process and all its descendants"""
    family_pids = get_processtree_pids(pid)

    for _pid in family_pids:
        os.kill(_pid, signum)

