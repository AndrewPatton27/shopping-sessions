"""Line-level profile from `perf script -F sym,symoff` output (stdin): the top
sample addresses mapped to inlined source lines with addr2line. Usage:
perf script -i perf.data -F sym,symoff | python perf_lines.py BIN [N]"""
import collections, re, subprocess, sys

binp = sys.argv[1]
top = int(sys.argv[2]) if len(sys.argv) > 2 else 60
cnt = collections.Counter()
total = 0
for line in sys.stdin:
    m = re.match(r"\s*(.+)\+0x([0-9a-f]+)\s*$", line)
    total += 1
    if m:
        cnt[(m.group(1).strip(), int(m.group(2), 16))] += 1
syms = {}
for l in subprocess.run(["nm", "-C", "--defined-only", binp], capture_output=True, text=True).stdout.splitlines():
    p = l.split(" ", 2)
    if len(p) == 3:
        syms.setdefault(p[2], int(p[0], 16))
# aggregate by source line (innermost inline frame + its caller line)
byline = collections.Counter()
for (sym, off), c in cnt.most_common(400):
    if sym not in syms:
        byline[sym + " (no nm)"] += c
        continue
    out = subprocess.run(["addr2line", "-e", binp, "-i", "-C", "-f", hex(syms[sym] + off)], capture_output=True, text=True).stdout.splitlines()
    locs = [out[i + 1].split("/")[-1] + ":" + out[i].split("::")[-1][:30] for i in range(0, len(out) - 1, 2)]
    byline[" <- ".join(locs[:3])] += c
for k, c in byline.most_common(top):
    print(f"{100 * c / total:5.2f}%  {k}")
