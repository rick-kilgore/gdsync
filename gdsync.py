#!/usr/bin/env python3

import argparse
import os
import re
import sys

BACKUP_ONLY = "backup"

import subprocess

from datetime import datetime
from subprocess import Popen, PIPE, STDOUT
from typing import Any, Dict, List, Optional, Tuple

os.environ['PATH'] = os.environ['PATH'] + ':' + os.environ['HOME'] + '/bin'

thishost: str = os.environ['thishost']
macbook: str = "macbook"
gdrive: str = "gdrive"
home: str = os.environ['HOME']

syncdirs: Dict[str, Dict[str, Any]] = {
  "vault": { "local": f"{home}/vault", "remote": f"{gdrive}:shared/vault" },
  "local_dots": { "local": f"{home}/backup/local", "remote": f"{gdrive}:{thishost}/backup/local", BACKUP_ONLY: True },
  "localsrc": { "local": f"{home}/local", "remote": f"{gdrive}:{thishost}/local", BACKUP_ONLY: True },
  "bin": { "local": f"{home}/bin", "remote": f"{macbook}:bin" },
  "shared_dots": { "local": f"{home}/backup/shared", "remote": f"{macbook}:backup/shared" },
  "misc": { "local": f"{home}/misc", "remote": f"{macbook}:misc" },
  "notes": { "local": f"{home}/notes", "remote": f"{macbook}:notes" },
  "jobsearch": { "local": f"{home}/jobsearch_2023", "remote": f"{macbook}:jobsearch_2023" },
  "learning": { "local": f"{home}/learning", "remote": f"{macbook}:learning" },
  "records": { "local": f"{home}/records", "remote": f"{macbook}:records" },
  "sharedsrc": { "local": f"{home}/share", "remote": f"{macbook}:share" },
  "tips-howtos": { "local": f"{home}/tips-howtos", "remote": f"{macbook}:tips-howtos" },
  "test": { "local": f"{home}/tmp/gdtest", "remote": f"{gdrive}:{thishost}/test", BACKUP_ONLY: True },
}

modes: Dict[str, List[str]] = {
  "volatile": [ "bin", "local_dots", "shared_dots", "vault" ],
  "files": [ "notes", "jobsearch", "misc", "learning", "records", "tips-howtos" ],
  "localsrc": [ "localsrc" ],
  "sharedsrc": [ "sharedsrc" ],
  "macos": [ "bin", "local_dots", "localsrc", "shared_dots", "sharedsrc" ],
}

def datestr() -> str:
  now = datetime.now()
  ampm = "am" if now.hour < 12 else "pm"
  hour = 12 if now.hour == 12 else now.hour % 12
  ms: str = str(now.microsecond // 1000).rjust(3, '0')
  return f"{hour:02}:{now.minute:02}:{now.second:02}.{ms} {ampm}"


def log(msg: str, nonl: bool = False) -> None:
  end: str = "" if nonl else "\n"
  print(f"{datestr()}: {msg}", end=end, flush=True)


def runcmd(cmd: List[str]) -> Tuple[int, str, str]:
  proc = Popen(cmd, stdout=PIPE)
  (output, errout) = proc.communicate()
  exit_code = proc.wait()
  out: str = output.decode("utf-8") if output is not None else ""
  err: str = errout.decode("utf-8") if errout is not None else ""
  return exit_code, out, err

def set_envs():
  ec, out, _ = runcmd(["cat", f"{os.environ['HOME']}/.env"])
  for sline in out.split("\n"):
    if len(sline) > 0:
      name, val = re.search("([A-Z0-9_]+)\s*=\s*(.+)", sline).groups()
      val = val.replace("$HOME", os.environ['HOME'])
      #if sys.stdout.isatty():
      #  print(f"setting env var {name}={val}", flush=True)
      os.environ[name] = val


def is_ignored_line(txt: str, mode: str = "") -> bool:
  for ignored in [
    "There was nothing to transfer",
    "Cryptomator/ipc.socket",
    "matching files",
    r"INFO\s*:\s*$",
    r"^Elapsed time:",
    r"^\s$",
  ]:
    if re.search(ignored, txt, flags=re.IGNORECASE):
      return True

  if mode == "check":
    for ignored in [
      "errors while checking",
      r"INFO\s*:\s*$",
      r"^Transferred:",
      r"^Errors:",
      r"^Checks:",
      r"files? missing$",
      r"differences found$",
      r"sizes differ",
      r"ERROR\s*:.*file not in",
      r"ERROR\s*:.*MD5 differ",
      r"Using md5 for hash comparisons",
    ]:
      if re.search(ignored, txt, flags=re.IGNORECASE):
        return True

  return False


class RunConfig:
  def __init__(self, dryrun: bool, sync: bool, verbose: bool):
    self.dryrun = dryrun
    self.sync = sync
    self.verbose = verbose


# return value indicates whether to proceed with sync
def check_for_files_not_on_both(local: str, remote: str, cfg: RunConfig, tstamp_file: str) -> bool:
  only_remote_file: str = f"{os.environ['HOME']}/tmp/onlyremote.txt"
  only_local_file: str = f"{os.environ['HOME']}/tmp/onlylocal.txt"
  cmd: List[str] = f"rclone check --missing-on-dst {only_local_file} --missing-on-src {only_remote_file} {local} {remote}".split(" ")
  if cfg.verbose:
    print(f"rclone cmd: {' '.join(cmd)}", flush=True)
  with Popen(cmd, stdout=PIPE, stderr=STDOUT) as proc:
    assert proc.stdout is not None
    for b in proc.stdout:
      s = b.decode("utf-8")
      if not is_ignored_line(s, "check"):
        print(s, end="", flush=True)

  if not handle_only_local_files(local, remote, only_local_file, tstamp_file, cfg):
    return False

  return handle_only_remote_files(local, remote, only_remote_file, cfg)


# return value indicates whether to proceed with sync
def handle_only_local_files(
  local: str,
  remote: str,
  only_local_file: str,
  tstamp_file: str,
  cfg: RunConfig
) -> bool:
  if not os.path.exists(only_local_file) or os.path.getsize(only_local_file) == 0:
    os.remove(only_local_file)
    return True

  # check if any files are older than last time sync was run
  oldfiles: List[str] = []
  if os.path.isfile(tstamp_file):
    synctime = os.stat(tstamp_file).st_mtime
    with open(only_local_file, "r") as loc:
      for fname in loc:
        fname = fname.strip()
        ftime = os.stat(f"{local}/{fname}").st_mtime
        if ftime < synctime:
          oldfiles.append(fname)
    os.remove(only_local_file)

  if len(oldfiles) == 0:
    return True

  # ask user what to do
  print("the following files are only on local and are older than the last sync", flush=True)
  print("likely they have been purposefully deleted on another host:", flush=True)
  print("  " + "\n  ".join(oldfiles), flush=True)
  rsp: str = "halbrand is sauron"
  while rsp.lower() not in [ "y", "n", "a", "q", "" ]:
    rsp = input("\nRemove these local files before proceeding? (Y/n/a[bort]): ")

  should_continue: bool = False
  match rsp.lower():
    case "a":
      print(f"aborting {local} <--> {remote} sync", flush=True)
      should_continue = False
    case "q":
      sys.exit(f"aborting {local} <--> {remote} sync AND STOPPING")
    case "n":
      should_continue = True
    case _:
      if cfg.dryrun:
        print("not removing files because --dryrun was passed", flush=True)
      else:
        subprocess.run(["/bin/rm", "-rfdv"] + oldfiles, cwd=local)
        rm_empty_folders(local, oldfiles)
        input("\nHit return to continue: ")
      should_continue = True

  if should_continue:
    print(f"continuing with {local} <--> {remote} sync\n", flush=True)
  else:
    print(f"aborting {local} <--> {remote} sync\n", flush=True)
  return should_continue


def rm_empty_folders(local: str, oldfiles: List[str]) -> None:
  dirs: Set[str] = set()
  for f in oldfiles:
    while "/" in f:
      d = re.search("(.+)/[^/]+", f).group(1)
      dirs.add(d)
      f = d

  if len(dirs) > 0:
    ordered: List[str] = sorted(list(dirs), key=len, reverse=True)
    for d in ordered:
      fullpath: str = f"{local}/{d}"
      if len(os.listdir(fullpath)) == 0:
        print(f"removing empty dir {d}")
        os.rmdir(fullpath)



# return value indicates whether to proceed with sync
def handle_only_remote_files(local: str, remote: str, only_remote_file: str, cfg: RunConfig) -> bool:
  if not os.path.exists(only_remote_file) or os.path.getsize(only_remote_file) == 0:
    return True

  print("\x1b[1;33mFOUND REMOTE-ONLY FILES - BE CAREFUL:\x1b[m", flush=True)
  subprocess.run(["cat", only_remote_file])

  rsp: str = "halbrand is sauron"
  while rsp.lower() not in [ "y", "n", "q", "c", "" ]:
    rsp = input("\nProceed with sync? switch to copy if you don't want to delete these files (y/n/C): ")

  should_continue: bool
  match rsp.lower():
    case "y":
      print(f"proceeding with sync (\x1b[1;33mFILES WILL BE DELETED from {remote}\x1b[m).")
      should_continue = True

    case "n":
      print(f"aborting {local} <--> {remote} sync")
      should_continue = False

    case "q":
      sys.exit(f"aborting {local} <--> {remote} sync AND STOPPING")

    case _:
      print("switching to copy mode")
      cfg.sync = False
      should_continue = True

  os.remove(only_remote_file)
  return should_continue


def run_rclone(src: str, dest: str, upload: bool, cfg: RunConfig):
  copy_cmd = "sync" if cfg.sync and upload else "copy"
  rclone_cmd = f"rclone {copy_cmd} -u {'--delete-excluded ' if upload else ''}{src} {dest}"
  if cfg.verbose:
    print(f"rclone cmd: {rclone_cmd}", flush=True)
  if cfg.dryrun:
    rclone_cmd = rclone_cmd.replace("-u", "-un")
  cmd: List[str] = ["bash", "-c", f"stdbuf -o0 -e0 {rclone_cmd}"]

  printed: bool = False
  with Popen(cmd, stdout=PIPE, stderr=STDOUT) as proc:
    assert proc.stdout is not None
    for b in proc.stdout:
      s = b.decode("utf-8")
      if not is_ignored_line(s):
        if not printed:
          print(
              f"copying \x1b[1;{'32mup to' if upload else '34mdown from'} "
              f"{dest if upload else src}\x1b[m",
              flush=True
          )
          printed = True
        s = re.sub("skipped delete", "\x1b[1;31mSKIPPED DELETE\x1b[m", s, flags=re.IGNORECASE)
        print(s, end="", flush=True)


def is_already_running(modes: List[str], dirs: List[str]) -> bool:
  pid: int = os.getpid()
  ppid: int = os.getppid()
  srch_args = '|'.join(modes if len(modes) > 0 else dirs)
  grepstr: str = f"[g]dsync.*({srch_args})"
  ecode, stdout, stderr = runcmd(
    ["bash", "-c", f"ps -eaf | grep -vEw '{pid}|{ppid}|tail -F' | grep -iE '{grepstr}'"]
  )
  if ecode == 0 or len(stdout) > 0:
    print(stdout, flush=True)
    return True

  return False


# return True means proceed with sync
def conflicts_check_is_ok(dirconf: Dict[str, str], cfg: RunConfig, tstamp_file: str) -> bool:
  if BACKUP_ONLY in dirconf or not cfg.sync: # or cfg.dryrun:
    return True

  return check_for_files_not_on_both(dirconf["local"], dirconf["remote"], cfg, tstamp_file)


def run(mode: str, dirs: List[str], cfg: RunConfig):
  print("\n--------------------------------------------------------------------------------", flush=True)
  set_envs()
  log(f"\x1b[1;35msync {mode}...\x1b[0m")

  for dirtosync in dirs:
    dirconf: Dict[str, str] = syncdirs[dirtosync]
    repo: str = re.sub(":.*", "", dirconf["remote"])
    log(f"sync \x1b[1;33m{dirtosync} on {repo}\x1b[0m: ")
    tstamp_file: str = f"{os.environ['HOME']}/tmp/var/gdsync.{dirtosync}.tstamp"
    if conflicts_check_is_ok(dirconf, cfg, tstamp_file):
      if not cfg.dryrun:
        open(tstamp_file, "w").close()

      run_rclone(dirconf["local"], dirconf["remote"], True, cfg)
      if BACKUP_ONLY not in dirconf:
        run_rclone(dirconf["remote"], dirconf["local"], False, cfg)

  log("\x1b[1;35mdone.\x1b[0m")


parser = argparse.ArgumentParser(description="Sync folders with Google Drive")
parser.add_argument("-l", "--list", help="list modes and dirs available for sync", action="store_true")
parser.add_argument("-i", "--info", help="list info for sync dir", metavar="DIR")
parser.add_argument("-d", "--dirs", nargs="*")
parser.add_argument("-n", "--dryrun", action="store_true")
parser.add_argument("-c", "--copy", help="copy mode: copy instead of syncing - disables deleting", action="store_true")
parser.add_argument("-v", "--verbose", help="show rclone commands run", action="store_true")
parser.add_argument("modes", default=None, nargs="*", metavar="mode", help="specify mode, which implies to run associated sync dirs")
args = parser.parse_args()

if args.list:
  for mode in modes:
    print(f"{(mode+':').ljust(15)}{' '.join(modes[mode])}", flush=True)
elif args.info:
  print(f" local: {syncdirs[args.info]['local']}", flush=True)
  print(f"remote: {syncdirs[args.info]['remote']}", flush=True)
elif is_already_running(args.modes, args.dirs):
  print(f"already running: mypid={os.getpid()}", flush=True)
  sys.exit(1)
else:
  run_cfg = RunConfig(args.dryrun, not args.copy, args.verbose)
  if args.modes and len(args.modes) > 0:
    modes2run: List[str]
    if len(args.modes) == 1 and args.modes[0] == "all":
      modes2run = list(filter(lambda m: m != 'macos', modes.keys()))
    else:
      modes2run = args.modes

    for mode in modes2run:
      run(mode, modes[mode], run_cfg)

  elif args.dirs and len(args.dirs) > 0:
    run("", args.dirs, run_cfg)

