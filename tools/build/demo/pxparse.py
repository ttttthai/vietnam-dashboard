import re, itertools


def parse(path):
    s = open(path, "rb").read().decode("latin1")
    meta = {}
    head, data = s.split("DATA=", 1)
    for m in re.finditer(r'([A-Z\-]+)(\("([^"]*)"\))?=(.*?);\s*\n', head, re.S):
        key, sub, val = m.group(1), m.group(3), m.group(4)
        vals = re.findall(r'"([^"]*)"', val)
        meta[(key, sub)] = vals if vals else val.strip()
    stub = meta.get(("STUB", None), [])
    heading = meta.get(("HEADING", None), [])
    tokens = re.findall(r'"[^"]*"|[-\d.eE]+', data.rstrip().rstrip(";"))
    nums = []
    for t in tokens:
        if t.startswith('"'):
            nums.append(None)
        else:
            nums.append(float(t))
    dims = stub + heading
    sizes = [len(meta[("VALUES", d)]) for d in dims]
    out = {}
    idx = 0
    for combo in itertools.product(*[range(n) for n in sizes]):
        out[combo] = nums[idx]
        idx += 1
    assert idx == len(nums), (idx, len(nums))
    return dims, [meta[("VALUES", d)] for d in dims], out
