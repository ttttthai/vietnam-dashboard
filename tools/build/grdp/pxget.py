import sys, re, requests
from bs4 import BeautifulSoup

RX = "?rxid=34c3b56d-63c6-4c9f-b4ce-2b17a89f4d75"
BASES = {
    "ta_new": "https://pxweb.nso.gov.vn/pxweb/vi/PLV03T%c3%a0i%20kho%e1%ba%a3n%20qu%e1%bb%91c%20gia/PLV03T%c3%a0i%20kho%e1%ba%a3n%20qu%e1%bb%91c%20gia/",
    "ta_old": "https://pxweb.nso.gov.vn/pxweb/vi/T%c3%a0i%20kho%e1%ba%a3n%20qu%e1%bb%91c%20gia/T%c3%a0i%20kho%e1%ba%a3n%20qu%e1%bb%91c%20gia/",
    "old": "https://pxweb.nso.gov.vn/pxweb/vi/D%c3%a2n%20s%e1%bb%91%20v%c3%a0%20lao%20%c4%91%e1%bb%99ng/D%c3%a2n%20s%e1%bb%91%20v%c3%a0%20lao%20%c4%91%e1%bb%99ng/",
    "new": "https://pxweb.nso.gov.vn/pxweb/vi/PLV02D%c3%a2n%20s%e1%bb%91%20v%c3%a0%20lao%20%c4%91%e1%bb%99ng/PLV02D%c3%a2n%20s%e1%bb%91%20v%c3%a0%20lao%20%c4%91%e1%bb%99ng/",
}


def get(base, table, out, fmt="FileTypeCsvWithHeadingAndSemiColon"):
    url = BASES[base] + table + "/" + RX
    s = requests.Session()
    r = s.get(url, timeout=60)
    soup = BeautifulSoup(r.text, "html.parser")
    form = soup.find("form")
    data = {}
    for inp in form.find_all("input"):
        n = inp.get("name")
        if not n:
            continue
        t = (inp.get("type") or "text").lower()
        if t in ("submit", "image", "checkbox"):
            continue
        data[n] = inp.get("value", "")
    sels = []
    for sel in form.find_all("select"):
        n = sel.get("name")
        if "ValuesListBox" in n:
            vals = [o.get("value") for o in sel.find_all("option")]
            sels.append((n, vals))
        elif "OutputFormat" in n:
            data[n] = fmt
        else:
            o = sel.find("option", selected=True) or sel.find("option")
            data[n] = o.get("value") if o else ""
    payload = list(data.items())
    for n, vals in sels:
        for v in vals:
            payload.append((n, v))
    btn = form.find("input", {"type": "submit", "name": re.compile("ButtonViewTable")})
    payload.append((btn["name"], btn.get("value", "")))
    action = form.get("action")
    post_url = requests.compat.urljoin(url, action)
    r2 = s.post(post_url, data=payload, timeout=120)
    ct = r2.headers.get("content-type", "")
    open(out, "wb").write(r2.content)
    print(table, r2.status_code, ct, len(r2.content), file=sys.stderr)


if __name__ == "__main__":
    base, table, out = sys.argv[1:4]
    fmt = sys.argv[4] if len(sys.argv) > 4 else "FileTypeCsvWithHeadingAndSemiColon"
    get(base, table, out, fmt)
