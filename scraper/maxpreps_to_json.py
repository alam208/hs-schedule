import json, re, time, random, argparse, os, sys
from concurrent.futures import ThreadPoolExecutor, as_completed
from urllib.parse import urljoin
from datetime import datetime
import requests
from bs4 import BeautifulSoup

HEADERS = {
    "User-Agent": ("Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
                   "AppleWebKit/537.36 (KHTML, like Gecko) "
                   "Chrome/120.0 Safari/537.36")
}

STATE_CODES = [
 "al","ak","ar","az","ca","co","ct","dc","de","fl","ga","hi","ia","id","il","in",
 "ks","ky","la","ma","md","me","mi","mn","mo","ms","mt","nc","nd","ne","nh","nj",
 "nm","nv","ny","oh","ok","or","pa","ps","ri","sc","sd","tn","tx","ut","va","vt",
 "wa","wi","wv","wy"
]

BASE = "https://www.maxpreps.com"
SPORT = "football"

def get(url, session, retries=3, backoff=1.7):
    for i in range(retries):
        r = session.get(url, headers=HEADERS, timeout=30)
        if r.ok:
            return r
        time.sleep((backoff ** i) + random.uniform(0, 0.3))
    r.raise_for_status()
    return r

def ldjson_first(soup):
    for tag in soup.select('script[type="application/ld+json"]'):
        try:
            data = json.loads(tag.string or "{}")
            if isinstance(data, dict): return data
            if isinstance(data, list) and data: return data[0]
        except Exception:
            pass
    return {}

def parse_dt_from_text(text, year):
    """
    Heuristik dari deskripsi: "Friday, September 26 @ 7:00 PM", "Sep 26 @ 7p", dll.
    Return ISO tanpa zona: YYYY-MM-DDTHH:MM:SS (biar UI tampilkan pakai local time visitor).
    """
    if not text: return ""
    m = re.search(r"(Jan|Feb|Mar|Apr|May|Jun|Jul|Aug|Sep|Sept|Oct|Nov|Dec)[a-z]*\s+(\d{1,2})", text, re.I)
    t = re.search(r"@?\s*(\d{1,2})(?::(\d{2}))?\s*(a|p)\.?m?\.?", text, re.I)
    if not m:
        return ""
    mon = m.group(1).lower()[:3]
    month_num = ["jan","feb","mar","apr","may","jun","jul","aug","sep","oct","nov","dec"].index(mon) + 1
    day = int(m.group(2))
    hour, minute = 19, 0
    if t:
        hour = int(t.group(1))
        minute = int(t.group(2) or 0)
        ampm = t.group(3).lower()
        if ampm == "p" and hour != 12: hour += 12
        if ampm == "a" and hour == 12: hour = 0
    return f"{year:04d}-{month_num:02d}-{day:02d}T{hour:02d}:{minute:02d}:00"

def clean_team(s):
    if not s: return ""
    s = re.sub(r"\s*\(\d+\-\d+\)$", "", s).strip()
    return s

def parse_game_page(url, session, default_year):
    r = get(url, session)
    soup = BeautifulSoup(r.text, "html.parser")

    def txt(sel):
        el = soup.select_one(sel)
        return (el.get_text(strip=True) if el else "") or ""

    # Tim home/away (selector bisa berubah; tambahkan fallback)
    home = txt('div.team-overview__team:nth-of-type(1) .team-overview__team-name a') or \
           txt('div.team-overview__team:nth-of-type(1) .team-overview__team-name')
    away = txt('div.team-overview__team:nth-of-type(2) .team-overview__team-name a') or \
           txt('div.team-overview__team:nth-of-type(2) .team-overview__team-name')
    home, away = clean_team(home), clean_team(away)

    desc  = txt('p.contest-description')
    venue = txt('p.contest-location')

    # Coba ambil startDate dari LD+JSON
    ld = ldjson_first(soup)
    start_iso = ""
    if isinstance(ld, dict):
        start_iso = (ld.get("startDate") or
                     (ld.get("event") or {}).get("startDate") or "")

    # Jika kosong, parse heuristik dari deskripsi
    if not start_iso:
        start_iso = parse_dt_from_text(desc, default_year)

    # State dari venue (heuristik)
    city, state = "", ""
    if venue:
        m = re.search(r",\s*([A-Z]{2})(?:\s|$)", venue)
        if m: state = m.group(1)

    return {
        "teamA": home or "Team A",
        "teamB": away or "Team B",
        "sport": "Football",
        "league": "",
        "venue": venue,
        "kick": start_iso,       # ISO lokal tanpa zona
        "stream": "#",
        "chat": "#",
        "school": "",
        "city": city,
        "state": state
    }

def parse_state_scores(state_code, date_str, session, default_year):
    url = f"{BASE}/{state_code}/{SPORT}/scores/?date={date_str}"
    r = get(url, session)
    soup = BeautifulSoup(r.text, "html.parser")

    # Kumpulkan link game detail
    links = []
    for a in soup.select('a[href*="/game/"]'):
        href = a.get("href")
        if href and "/game/" in href:
            links.append(urljoin(BASE, href))
    if not links:
        links = [urljoin(BASE, a.get("href")) for a in soup.select(".c a") if a.get("href")]

    links = sorted(set(links))
    results = []
    with ThreadPoolExecutor(max_workers=6) as ex:
        futs = [ex.submit(parse_game_page, u, session, default_year) for u in links]
        for f in as_completed(futs):
            try:
                results.append(f.result())
            except Exception:
                pass
    return results

def scrape_all(date_str="9/26/2025", states=None):
    states = states or STATE_CODES
    default_year = int(date_str.split("/")[-1])
    out = []
    with requests.Session() as session:
        for st in states:
            try:
                out.extend(parse_state_scores(st, date_str, session, default_year))
            except Exception:
                pass
            time.sleep(0.6 + random.uniform(0, 0.5))  # jeda sopan
    return out

def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--date", default="9/26/2025", help="mm/dd/YYYY (sesuai URL MaxPreps)")
    ap.add_argument("--states", default=",".join(STATE_CODES),
                    help="Comma-separated state codes (e.g., tx,ca,fl). Default: all")
    ap.add_argument("--outdir", default="data", help="Folder output JSON")
    args = ap.parse_args()

    states = [s.strip().lower() for s in args.states.split(",") if s.strip()]
    data = scrape_all(args.date, states=states)

    # Simpan nama file konsisten: hsfb-YYYY-MM-DD.json
    mm, dd, yyyy = args.date.split("/")
    out_name = f"hsfb-{int(yyyy):04d}-{int(mm):02d}-{int(dd):02d}.json"
    os.makedirs(args.outdir, exist_ok=True)
    out_path = os.path.join(args.outdir, out_name)
    with open(out_path, "w", encoding="utf-8") as f:
        json.dump(data, f, ensure_ascii=False, indent=2)
    print(f"Saved {len(data)} items → {out_path}")

if __name__ == "__main__":
    sys.exit(main())
