#!/usr/bin/env python3
# batch_viz_render.py
from __future__ import annotations
import argparse, glob, json, os, sys, time, traceback
from concurrent.futures import ProcessPoolExecutor, as_completed
from pathlib import Path
from typing import List, Tuple, Optional
import yaml
import viz2video

def _read_out_path(spec_path: Path) -> Optional[Path]:
    try:
        with open(spec_path, "r", encoding="utf-8") as f:
            d = yaml.safe_load(f)
        out = d.get("out")
        return Path(out) if out else None
    except Exception:
        return None

def _render_one(spec_path: str) -> Tuple[str, str, float]:
    t0 = time.time()
    try:
        out = viz2video.run(spec_path)
        dur = time.time() - t0
        return ("ok", out, dur)
    except Exception as e:
        tb = traceback.format_exc(limit=5)
        dur = time.time() - t0
        return ("err", f"{spec_path}\n{e}\n{tb}", dur)

def main(argv=None):
    ap = argparse.ArgumentParser(description="Batch render many viz2video YAML specs")
    ap.add_argument("--glob", required=True, help="Glob of spec files, e.g. 'specs/*.yaml'")
    ap.add_argument("--max-workers", type=int, default=max(1, (os.cpu_count() or 2) // 2))
    ap.add_argument("--skip-existing", action="store_true")
    ap.add_argument("--log-jsonl", default="")
    args = ap.parse_args(argv)

    spec_paths = sorted([Path(p) for p in glob.glob(args.glob)])
    if not spec_paths:
        print(f"No specs found for pattern: {args.glob}", file=sys.stderr)
        sys.exit(2)

    todo: List[Path] = []
    for sp in spec_paths:
        if args.skip_existing:
            out = _read_out_path(sp)
            if out and out.exists():
                print(f"⏭  Skipping {sp} (exists: {out})")
                continue
        todo.append(sp)

    if not todo:
        print("Nothing to do. All outputs already exist.")
        return

    print(f"🧵 Workers: {args.max_workers}  |  Jobs: {len(todo)}")
    print(f"🎯 First/last: {todo[0]}  …  {todo[-1]}")

    log_file = Path(args.log_jsonl) if args.log_jsonl else None
    if log_file:
        log_file.parent.mkdir(parents=True, exist_ok=True)

    started = time.time()
    ok_count = 0
    err_count = 0

    with ProcessPoolExecutor(max_workers=args.max_workers) as ex:
        futs = {ex.submit(_render_one, str(p)): p for p in todo}
        for i, fut in enumerate(as_completed(futs), 1):
            status, msg, dur = fut.result()
            if status == "ok":
                ok_count += 1
                print(f"[{i}/{len(todo)}] ✅ {msg}  ({dur:.1f}s)")
                if log_file:
                    with log_file.open("a", encoding="utf-8") as lf:
                        lf.write(json.dumps({"t": time.time(), "spec": str(futs[fut]), "out": msg, "status": "ok", "sec": round(dur, 3)}) + "\n")
            else:
                err_count += 1
                print(f"[{i}/{len(todo)}] ❌ ERROR in {futs[fut]} ({dur:.1f}s)\n{msg}\n", file=sys.stderr)
                if log_file:
                    with log_file.open("a", encoding="utf-8") as lf:
                        lf.write(json.dumps({"t": time.time(), "spec": str(futs[fut]), "status": "err", "err": msg.splitlines()[-1][:500], "sec": round(dur, 3)}) + "\n")

    total = time.time() - started
    print(f"\n🏁 Done: {ok_count} ok, {err_count} errors, {len(todo)} total in {total:.1f}s")
    if err_count:
        sys.exit(1)

if __name__ == "__main__":
    main()
