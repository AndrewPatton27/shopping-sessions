"""Line-level profile from `perf script -F ip,sym,symoff` output (stdin).

Maps every sampled address to its inlined source frames with one addr2line
call, then prints two tables: samples by the innermost frame in our own
sources (main.rs / ts.rs / pq.rs / halloc.rs), and by the innermost frame
anywhere. Usage: perf script -F ip,sym,symoff | python perf_lines.py BIN [N]
"""
import collections
import re
import subprocess
import sys

OWN = ("main.rs", "ts.rs", "pq.rs", "halloc.rs")
binp = sys.argv[1]
top = int(sys.argv[2]) if len(sys.argv) > 2 else 50

cnt = collections.Counter()
total = 0
for line in sys.stdin:
    m = re.match(r"\s*(?:[0-9a-f]+\s+)?(.+)\+0x([0-9a-f]+)\s*(?:\(.*\))?\s*$", line)
    if not m:
        continue
    total += 1
    cnt[(m.group(1).strip(), int(m.group(2), 16))] += 1

syms = {}
nm = subprocess.run(["nm", "-C", "--defined-only", binp], capture_output=True, text=True).stdout
for l in nm.splitlines():
    p = l.split(" ", 2)
    if len(p) == 3:
        syms.setdefault(p[2], int(p[0], 16))

keys = [k for k in cnt if k[0] in syms]
addrs = [hex(syms[s] + o) for s, o in keys]
out = subprocess.run(["addr2line", "-e", binp, "-i", "-a"] + addrs, capture_output=True, text=True).stdout.splitlines()
frames = {}
cur = None
for l in out:
    if l.startswith("0x"):
        cur = int(l, 16)
        frames[cur] = []
    elif cur is not None:
        frames[cur].append(l.rsplit("/", 1)[-1].split(" ")[0])

own = collections.Counter()
inner = collections.Counter()
for (s, o), c in cnt.items():
    if s not in syms:
        own["[" + s + "]"] += c
        inner["[" + s + "]"] += c
        continue
    fr = frames.get(syms[s] + o, ["?"])
    inner[fr[0]] += c
    mine = [f for f in fr if f.split(":")[0] in OWN]
    own[mine[0] if mine else fr[0]] += c

print(f"samples {total}")
print("--- innermost own-source line")
for k, c in own.most_common(top):
    print(f"{100 * c / total:5.2f}%  {k}")
print("--- innermost line anywhere")
for k, c in inner.most_common(25):
    print(f"{100 * c / total:5.2f}%  {k}")
