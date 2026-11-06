"""Copy each case's two answers to <blind>/<case>/A and B in random order.

usage: blind2.py <iteration_dir> <blind_root> [case ...]
The key (which letter is which configuration) goes to <blind_root>/key.json,
which the graders are told not to read.
"""
import json, os, random, shutil, sys

it, bl = sys.argv[1], sys.argv[2]
cases = sys.argv[3:] or sorted(c for c in os.listdir(it) if os.path.isdir(os.path.join(it, c)))
os.makedirs(bl, exist_ok=True)
kp = os.path.join(bl, "key.json")
key = json.load(open(kp)) if os.path.exists(kp) else {}
for case in cases:
    cfgs = ["with_skill", "without_skill"]
    random.shuffle(cfgs)
    key[case] = {"A": cfgs[0], "B": cfgs[1]}
    for lab, c in zip("AB", cfgs):
        d = os.path.join(bl, case, lab)
        shutil.rmtree(d, ignore_errors=True)
        shutil.copytree(os.path.join(it, case, c, "outputs"), d,
                        ignore=shutil.ignore_patterns("__pycache__"))
    shutil.copy(os.path.join(it, case, "eval_metadata.json"), os.path.join(bl, case, "eval_metadata.json"))
json.dump(key, open(kp, "w"), indent=1)
print(len(cases), "cases blinded into", bl)
