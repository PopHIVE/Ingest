#!/usr/bin/env python3
"""Pre-merge checks for data changes in PopHIVE/Ingest.

Each check targets a problem that reached main through a pull request:

  archive       .gz / .xz files that fail to decompress, and parquet files with
                missing or broken magic bytes (truncated writes or downloads)
  file-size     files over GitHub's 50 MB recommended limit
  raw-file      large uncompressed files added under data/*/raw/
  local-path    machine-specific paths (C:/Users/..., /Users/..., /home/...)
                in process.json or datapackage.json
  crlf-hash     process.json hashes computed on a CRLF (Windows autocrlf)
                checkout, which never match the LF files CI hashes
  failed-run    process.json state committed from a failed ingest run
  row-drop      datapackage.json resources whose row_count drops sharply
  scope         many data directories touched at once (warning only)

A pull request made only of reverts restores an earlier state, so the state
checks (local-path, crlf-hash, failed-run, row-drop) are reported as warnings
for it; the file integrity checks still fail it.

Usage, from the repository root:

  python3 .claude/skills/ingest-code-review/scripts/check_data_changes.py
  python3 .claude/skills/ingest-code-review/scripts/check_data_changes.py --base origin/main --head my-branch
  python3 .claude/skills/ingest-code-review/scripts/check_data_changes.py --allow-row-drop

On Windows use `py -I` in place of `python3`. Output is always UTF-8.

Only files changed between the merge base of --base and --head are checked.
File contents are read from git objects, so this works in a partial clone
(missing blobs are fetched on demand) without the data files checked out.
Exit status is 1 if any error was found and 0 otherwise.
"""

import argparse
import gzip
import hashlib
import io
import json
import lzma
import os
import re
import subprocess
import sys
import zlib
from collections import namedtuple

MB = 1024 * 1024
LFS_POINTER = b"version https://git-lfs.github.com/spec/"
MD5_RE = re.compile(r"[0-9a-f]{32}")
# Machine-specific paths. /home/runner is the GitHub Actions workspace.
LOCAL_PATH_RE = re.compile(
    r"(?<![A-Za-z])[A-Za-z]:[\\/]{1,2}(?:Users|Documents and Settings)[\\/]"
    r"|/Users/[^/\"\s]+/"
    r"|/home/(?!runner/)[^/\"\s]+/"
)
# Text files that dcf may hash; binary formats are never converted to CRLF.
TEXT_EXT_RE = re.compile(r"\.(?:json|csv|tsv|txt|r|md|ya?ml)$", re.IGNORECASE)
RAW_TEXT_RE = re.compile(r"^data/[^/]+/raw/.+\.(?:csv|tsv|txt|json)$", re.IGNORECASE)

Finding = namedtuple("Finding", "level check path message")
STATE_CHECKS = {"local-path", "crlf-hash", "failed-run", "row-drop"}


# ---------------------------------------------------------------- git helpers

def git(*args):
    proc = subprocess.run(["git", *args], capture_output=True)
    if proc.returncode != 0:
        err = proc.stderr.decode("utf-8", "replace").strip()
        sys.exit(f"git {' '.join(args)} failed: {err}")
    return proc.stdout.decode("utf-8", "replace")


def read_blob(ref, path):
    """Contents of path at ref as bytes, or None if it does not exist there."""
    proc = subprocess.run(["git", "cat-file", "blob", f"{ref}:{path}"],
                          capture_output=True)
    return proc.stdout if proc.returncode == 0 else None


def read_json(ref, path):
    data = read_blob(ref, path)
    if data is None:
        return None
    try:
        return json.loads(data.decode("utf-8-sig"))
    except (UnicodeDecodeError, json.JSONDecodeError):
        return None


def changed_files(base, head):
    """(status, path) pairs for files changed between base and head."""
    out = git("diff", "--name-status", "-z", "--no-renames", base, head)
    tokens = [t for t in out.split("\0") if t]
    return [(tokens[i][0], tokens[i + 1]) for i in range(0, len(tokens) - 1, 2)]


# ---------------------------------------------------------------- the checks

def archive_problem(path, data):
    """Why a compressed or parquet file is unreadable, or None if it is fine."""
    lower = path.lower()
    try:
        if lower.endswith(".gz"):
            with gzip.GzipFile(fileobj=io.BytesIO(data)) as f:
                while f.read(16 * MB):
                    pass
        elif lower.endswith(".xz"):
            with lzma.LZMAFile(io.BytesIO(data)) as f:
                while f.read(16 * MB):
                    pass
        elif lower.endswith(".parquet"):
            if len(data) < 12 or data[:4] != b"PAR1" or data[-4:] != b"PAR1":
                return "missing PAR1 magic bytes (truncated or not parquet)"
            footer = int.from_bytes(data[-8:-4], "little")
            if footer <= 0 or footer > len(data) - 12:
                return f"invalid parquet footer length {footer}"
    except EOFError:
        return "file ends before the end of the compressed stream (truncated)"
    except (OSError, zlib.error, lzma.LZMAError) as e:
        return f"cannot be decompressed: {e}"
    return None


def walk_md5_entries(obj):
    """Yield (path_key, md5) pairs anywhere in a parsed process.json."""
    if isinstance(obj, dict):
        for key, value in obj.items():
            if isinstance(value, str) and MD5_RE.fullmatch(value):
                yield key, value
            else:
                yield from walk_md5_entries(value)
    elif isinstance(obj, list):
        for value in obj:
            yield from walk_md5_entries(value)


def walk_strings(obj, in_key=False):
    """Yield (text, is_key) for every key and string value in parsed JSON."""
    if isinstance(obj, dict):
        for key, value in obj.items():
            yield key, True
            yield from walk_strings(value)
    elif isinstance(obj, list):
        for value in obj:
            yield from walk_strings(value)
    elif isinstance(obj, str):
        yield obj, in_key


def resolve_state_path(key, source_dir):
    """Repository paths a process.json state key may refer to."""
    key = key.replace("\\", "/")
    if key.startswith("./"):
        return [key[2:]]
    if key.startswith("data/"):
        return [key]
    idx = key.rfind("/data/")
    if idx >= 0 and (key.startswith("/") or re.match(r"[A-Za-z]:/", key)):
        return [key[idx + 1:]]
    return [f"{source_dir}/{key}", f"{source_dir}/raw/{key}"]


ANSI_RE = re.compile(r"\x1b\[[0-9;]*m|[\r\u2800-\u28ff]")


def clean(text):
    """Drop terminal colour codes, carriage returns and spinner glyphs."""
    return " ".join(ANSI_RE.sub("", text).split())


def to_crlf(data):
    return data.replace(b"\r\n", b"\n").replace(b"\n", b"\r\n")


def check_process_json(path, base, head, allow_failed, findings):
    new = read_json(head, path)
    if not isinstance(new, dict):
        findings.append(Finding("error", "archive", path, "not valid JSON"))
        return
    old = read_json(base, path) or {}
    source_dir = path.rsplit("/", 1)[0]

    # Hashes computed on a CRLF checkout of an LF file.
    seen, crlf_files = set(), []
    for key, md5 in walk_md5_entries(new):
        if not TEXT_EXT_RE.search(key) or key in seen:
            continue
        seen.add(key)
        for candidate in resolve_state_path(key, source_dir):
            content = read_blob(head, candidate)
            if content is None:
                continue
            actual = hashlib.md5(content).hexdigest()
            if md5 != actual and md5 == hashlib.md5(to_crlf(content)).hexdigest():
                crlf_files.append(candidate)
            break
    if crlf_files:
        findings.append(Finding(
            "error", "crlf-hash", path,
            f"{len(crlf_files)} hash(es) were computed on CRLF copies of LF files "
            f"(Windows core.autocrlf), e.g. {crlf_files[0]}, so CI will never match "
            "them. Fix line endings (see .gitattributes), re-checkout and re-run, "
            "or revert this file."))

    # Machine-specific paths in keys (state tracking) or values (logs).
    key_hits, value_hits = [], []
    for text, is_key in walk_strings(new):
        if LOCAL_PATH_RE.search(text):
            (key_hits if is_key else value_hits).append(clean(text))
    if key_hits:
        findings.append(Finding(
            "error", "local-path", path,
            f"{len(key_hits)} state key(s) use a machine-specific path, e.g. "
            f"'{key_hits[0][:120]}'. Run dcf from the repo root with a relative "
            "path, as CI does: dcf::dcf_process('<name>', '.')"))
    if value_hits:
        findings.append(Finding(
            "warning", "local-path", path,
            f"{len(value_hits)} logged value(s) contain a machine-specific path, "
            f"e.g. '{value_hits[0][:120]}'"))

    # State from a failed run that this change introduced.
    old_runs = {s.get("path"): s.get("last_run")
                for s in old.get("scripts", []) if isinstance(s, dict)}
    for script in new.get("scripts", []):
        if not isinstance(script, dict):
            continue
        status = script.get("last_status") or {}
        if status.get("success") is False and source_dir.split("/")[-1] not in allow_failed \
                and script.get("last_run") != old_runs.get(script.get("path")):
            log = status.get("log")
            if isinstance(log, list):
                log = " ".join(str(x) for x in log)
            findings.append(Finding(
                "error", "failed-run", path,
                f"records a failed run of {script.get('path')} "
                f"({script.get('last_run')}): {str(log)[:200]}. "
                "Revert this file rather than committing a failed local run."))


def check_datapackage(path, base, head, allow_drop, max_drop, findings):
    old, new = read_json(base, path), read_json(head, path)
    if not isinstance(old, dict) or not isinstance(new, dict):
        return

    def by_name(dp):
        out = {}
        for r in dp.get("resources", []) or []:
            if isinstance(r, dict):
                out[r.get("name") or r.get("filename") or r.get("path")] = r
        return out

    old_r, new_r = by_name(old), by_name(new)
    level = "warning" if allow_drop else "error"
    hint = "" if allow_drop else " Add the allow-row-drop label if this is intended."
    for name, res in old_r.items():
        before = res.get("row_count")
        if not isinstance(before, int) or before <= 0:
            continue
        if name not in new_r:
            findings.append(Finding("warning", "row-drop", path,
                                    f"resource '{name}' ({before:,} rows) was removed"))
            continue
        after = new_r[name].get("row_count")
        if not isinstance(after, int) or after <= 0:
            findings.append(Finding(level, "row-drop", path,
                                    f"'{name}' went from {before:,} rows to {after}.{hint}"))
        elif after < before * (1 - max_drop):
            findings.append(Finding(
                level, "row-drop", path,
                f"'{name}' dropped {1 - after / before:.0%} "
                f"({before:,} -> {after:,} rows).{hint}"))


def run_checks(args):
    base = git("merge-base", args.base, args.head).strip()
    files = changed_files(base, args.head)
    findings = []
    touched_dirs = set()

    for status, path in files:
        parts = path.split("/")
        if len(parts) > 2 and parts[0] == "data":
            touched_dirs.add(parts[1])
        if status == "D":
            if re.match(r"data/[^/]+/(standard|dist)/", path):
                findings.append(Finding("warning", "deleted", path, "data file deleted"))
            continue

        data = read_blob(args.head, path)
        if data is None:
            continue
        if len(data) < 1024 and data.startswith(LFS_POINTER):
            continue  # Git LFS pointer; the content lives in LFS storage
        size_mb = len(data) / MB

        if size_mb > args.max_file_mb:
            findings.append(Finding(
                "error", "file-size", path,
                f"{size_mb:.1f} MB exceeds {args.max_file_mb} MB (GitHub warns at "
                "50 MB and rejects 100 MB). Keep it out of git or use Git LFS."))
        elif size_mb > args.warn_file_mb:
            findings.append(Finding("warning", "file-size", path, f"{size_mb:.1f} MB"))

        if RAW_TEXT_RE.match(path) and size_mb > args.max_raw_text_mb:
            findings.append(Finding(
                "error" if status == "A" else "warning", "raw-file", path,
                f"{size_mb:.1f} MB uncompressed file in raw/. Commit a compressed "
                "copy (.csv.xz) or add it to the source's raw/.gitignore."))
            if not data.endswith(b"\n"):
                findings.append(Finding(
                    "warning", "raw-file", path,
                    "does not end with a newline, so it may be a partial download"))

        problem = archive_problem(path, data)
        if problem:
            findings.append(Finding("error", "archive", path, problem))

        if path.endswith("/process.json"):
            check_process_json(path, base, args.head, args.allow_failed, findings)
        elif path.endswith("datapackage.json") and status == "M":
            check_datapackage(path, base, args.head, args.allow_row_drop,
                              args.max_row_drop, findings)
        elif path.endswith(".json") and path.startswith("data/"):
            text = data.decode("utf-8", "replace")
            hit = LOCAL_PATH_RE.search(text)
            if hit:
                findings.append(Finding("warning", "local-path", path,
                                        f"contains a machine-specific path near '{hit.group(0)}'"))

    subjects = git("log", "--no-merges", "--format=%s", f"{base}..{args.head}").splitlines()
    if subjects and all(s.startswith("Revert") for s in subjects):
        findings = [f._replace(level="warning", message=f.message + " (revert: restores an earlier state)")
                    if f.level == "error" and f.check in STATE_CHECKS else f
                    for f in findings]
        print("All commits are reverts: state checks are reported as warnings.")

    if len(touched_dirs) > args.max_dirs:
        findings.append(Finding(
            "warning", "scope", "data/",
            f"{len(touched_dirs)} data directories changed. Output of a full local "
            "dcf_build() belongs to the scheduled CI build; consider reverting "
            "directories unrelated to this change."))
    return base, files, findings


# ---------------------------------------------------------------- reporting

def annotate(f):
    msg = f.message.replace("%", "%25").replace("\r", "%0D").replace("\n", "%0A")
    kind = "error" if f.level == "error" else "warning"
    print(f"::{kind} file={f.path},title={f.check}::{msg}")


def main():
    # Logs in process.json hold non-ASCII text (spinner glyphs, en dashes); a
    # Windows console (cp1252) would otherwise crash on the first one printed.
    for stream in (sys.stdout, sys.stderr):
        if hasattr(stream, "reconfigure"):
            stream.reconfigure(encoding="utf-8", errors="replace")
    p = argparse.ArgumentParser(description=__doc__.split("\n")[0])
    p.add_argument("--base", default="origin/main")
    p.add_argument("--head", default="HEAD")
    p.add_argument("--allow-row-drop", action="store_true",
                   default=os.environ.get("ALLOW_ROW_DROP") == "true")
    p.add_argument("--max-row-drop", type=float, default=0.2,
                   help="fractional row_count drop that is an error (default 0.2)")
    p.add_argument("--max-file-mb", type=float, default=50)
    p.add_argument("--warn-file-mb", type=float, default=25)
    p.add_argument("--max-raw-text-mb", type=float, default=5)
    p.add_argument("--allow-failed", nargs="*", default=["kinsa_ili"],
                   help="sources whose failed runs are expected, as in build.yaml "
                        "(default: kinsa_ili, which needs credentials)")
    p.add_argument("--max-dirs", type=int, default=5,
                   help="data directories changed before a scope warning")
    args = p.parse_args()

    base, files, findings = run_checks(args)
    errors = [f for f in findings if f.level == "error"]
    warnings = [f for f in findings if f.level != "error"]

    print(f"Checked {len(files)} changed file(s) between {base[:9]} and {args.head}.")
    for label, group in (("ERROR", errors), ("WARNING", warnings)):
        for f in group:
            print(f"{label:7} [{f.check}] {f.path}: {f.message}")
    counts = {}
    for f in findings:
        counts.setdefault(f.check, [0, 0])[0 if f.level == "error" else 1] += 1
    if counts:
        print("By check: " + ", ".join(
            f"{c} {e}E/{w}W" for c, (e, w) in sorted(counts.items())))
    print(f"{len(errors)} error(s), {len(warnings)} warning(s).")

    if os.environ.get("GITHUB_ACTIONS") == "true":
        for f in errors + warnings:
            annotate(f)
    summary = os.environ.get("GITHUB_STEP_SUMMARY")
    if summary:
        with open(summary, "a", encoding="utf-8") as out:
            out.write(f"### Data checks: {len(errors)} error(s), {len(warnings)} warning(s)\n\n")
            if findings:
                out.write("| Level | Check | File | Detail |\n|---|---|---|---|\n")
                for f in errors + warnings:
                    detail = f.message.replace("|", "\\|")
                    out.write(f"| {f.level} | {f.check} | `{f.path}` | {detail} |\n")
    return 1 if errors else 0


if __name__ == "__main__":
    sys.exit(main())
