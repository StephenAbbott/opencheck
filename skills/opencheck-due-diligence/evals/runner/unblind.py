"""Copy blind grading_A/B.json back to <iteration>/<case>/<config>/grading.json.

usage: unblind.py <iteration_dir> <blind_root>
"""
import json, os, sys

it, bl = sys.argv[1], sys.argv[2]
key = json.load(open(os.path.join(bl, "key.json")))
missing = []
for case, m in sorted(key.items()):
    for lab in "AB":
        src = os.path.join(bl, case, f"grading_{lab}.json")
        if not os.path.exists(src):
            missing.append(f"{case}/{lab}")
            continue
        g = json.load(open(src))
        exp = g.get("expectations", [])
        p = sum(1 for e in exp if e.get("passed"))
        g["summary"] = {"passed": p, "failed": len(exp) - p, "total": len(exp),
                        "pass_rate": round(p / len(exp), 3) if exp else 0.0}
        run = os.path.join(it, case, m[lab])
        tp = os.path.join(run, "timing.json")
        if os.path.exists(tp):
            t = json.load(open(tp))
            g["timing"] = {"total_duration_seconds": t.get("total_duration_seconds")}
        json.dump(g, open(os.path.join(run, "grading.json"), "w"), indent=1, ensure_ascii=False)
print("missing:", missing or "none")
