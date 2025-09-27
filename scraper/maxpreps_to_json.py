import json, re, time, random, argparse, os, sys
from concurrent.futures import ThreadPoolExecutor, as_completed
from urllib.parse import urljoin
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

# ---------------- HTTP ----------------
def get(url, session, retries=3, backoff=1.7):
    for i in range(retries):
        r = session.get(url, headers=HEADERS, timeout=30)
        if r.ok:
            return r
        time.sleep((backoff ** i) + random.uniform(0, 0.3))
    r.raise_for_status()
    return r

# ---------------- Helpers ----------------
def ldjson_first(soup):
    for tag in soup.select('script[type="application/ld+json"]'):
        try:
            data = json.loads(tag.string or "{}")
            if isinstance(data, dict): return data
            if isinstance(data, list) and data: return data[0]
        except Exception:
            pass
    return {}

def text_one(soup, sel, default=""):
    el = soup.select_one(sel)
    return (el.get_text(strip=True) if el else default) or default

def clean_team(s):
    if not s: return ""
    return re.sub(r"\s*\(\d+\-\d+\)$", "", s).strip()

def parse_ampm(h, m, ampm):
    h = int(h); m = int(m or 0)
    ampm = (ampm or "").lower()
    if ampm.startswith("p") and h != 12: h += 12
    if ampm.startswith("a") and h == 12: h = 0
    return f"{h:02d}:{m:02d}:00"

def parse_desc_today(desc, ref_date_ymd):
    if not desc: return ""
    m = re.search(r"\btoday\b\s*@\s*(\d{1,2})(?::(\d{2}))?\s*([ap])\.?m?\.?", desc, re.I)
    if not m:
        m = re.search(r"\btoday\b\s*@\s*(\d{1,2})(?::(\d{2}))?\s*(AM|PM|A\.M\.|P\.M\.)", desc, re.I)
    if m:
        hhmmss = parse_ampm(m.group(1), m.group(2), m.group(3))
        return f"{ref_date_ymd}T{hhmmss}"
    return ""

def parse_desc_monthday(desc, year):
    if not desc: return ""
    m = re.search(r"(Jan|Feb|Mar|Apr|May|Jun|Jul|Aug|Sep|Sept|Oct|Nov|Dec)[a-z]*\s+(\d{1,2})", desc, re.I)
    t = re.search(r"@\s*(\d{1,2})(?::(\d{2}))?\s*([ap])\.?m?\.?", desc, re.I) or \
        re.search(r"@\s*(\d{1,2})(?::(\d{2}))?\s*(AM|PM|A\.M\.|P\.M\.)", desc, re.I)
    if not m: return ""
    mon = m.group(1).lower()[:3]
    month_num = ["jan","feb","mar","apr","may","jun","jul","aug","sep","oct","nov","dec"].index(mon) + 1
    day = int(m.group(2))
    hhmmss = "19:00:00"
    if t:
        hhmmss = parse_ampm(t.group(1), t.group(2), t.group(3))
    return f"{int(year):04d}-{month_num:02d}-{day:02d}T{hhmmss}"

def parse_city_state_from_text(text):
    if not text: return "", ""
    m = re.search(r"\(([A-Za-z .'-]+),\s*([A-Z]{2})\)", text)
    if m: return m.group(1).strip(), m.group(2).strip()
    return "", ""

# ---------------- Halaman game (1 request per game) ----------------
def parse_game_page(url, session, default_year, ref_date_ymd, state_hint=""):
    r = get(url, session)
    soup = BeautifulSoup(r.text, "html.parser")

    # Tim + link tim (tanpa buka halaman tim)
    home = (text_one(soup, 'div.team-overview__team:nth-of-type(1) .team-overview__team-name a') or
            text_one(soup, 'div.team-overview__team:nth-of-type(1) .team-overview__team-name'))
    away = (text_one(soup, 'div.team-overview__team:nth-of-type(2) .team-overview__team-name a') or
            text_one(soup, 'div.team-overview__team:nth-of-type(2) .team-overview__team-name'))
    home, away = clean_team(home), clean_team(away)

    # Deskripsi, venue, mascot (teks)
    desc  = text_one(soup, 'p.contest-description') or text_one(soup, 'div.contest-description')
    venue = text_one(soup, 'p.contest-location') or text_one(soup, 'div.contest-location')
    mascotA = text_one(soup, 'div.team-details:nth-of-type(1) a.team-details__mascot') or \
              text_one(soup, 'div.team-details:nth-of-type(1) .team-details__mascot') or ""
    mascotB = text_one(soup, 'div.team-details:nth-of-type(2) a.team-details__mascot') or \
              text_one(soup, 'div.team-details:nth-of-type(2) .team-details__mascot') or ""

    # Kickoff
    ld = ldjson_first(soup)
    start_iso = ""
    if isinstance(ld, dict):
        start_iso = (ld.get("startDate") or (ld.get("event") or {}).get("startDate") or "")
    if not start_iso:
        start_iso = parse_desc_today(desc, ref_date_ymd)
    if not start_iso:
        start_iso = parse_desc_monthday(desc, default_year)

    # City/State
    city, state = "", (state_hint or "").upper()
    if venue:
        if not state:
            m_state = re.search(r",\s*([A-Z]{2})(?:\s|$)", venue)
            if m_state: state = m_state.group(1)
        if not city:
            m_city = re.search(r"([^,]+),\s*[A-Z]{2}\b", venue)
            if m_city: city = m_city.group(1).strip()
    if (not city or not state) and desc:
        c2, s2 = parse_city_state_from_text(desc)
        city = city or c2
        state = state or s2

    return {
        "teamA": home or "Team A",
        "teamB": away or "Team B",
        "sport": "Football",
        "league": "",
        "venue": venue,
        "kick": start_iso,
        "stream": "#",
        "chat": "#",
        "school": "",          # tak enrich, biarkan kosong
        "city": city,
        "state": state,
        "mascotA": mascotA or "",
        "mascotB": mascotB or "",
        "description": desc or ""
    }

# ---------------- Halaman state (scores list) ----------------
def parse_state_scores(state_code, date_str, session, default_year, ref_date_ymd):
    url = f"{BASE}/{state_code}/{SPORT}/scores/?date={date_str}"
    r = get(url, session)
    soup = BeautifulSoup(r.text, "html.parser")

    links = []
    for a in soup.select('a[href*="/game/"]'):
        href = a.get("href")
        if href and "/game/" in href:
            links.append(urljoin(BASE, href))
    if not links:
        links = [urljoin(BASE, a.get("href")) for a in soup.select(".c a") if a.get("href")]

    links = sorted(set(links))
    cap_env = os.getenv("LIMIT_PER_STATE")
    if cap_env:
        try:
            cap = int(cap_env)
            links = links[:cap]
        except Exception:
            pass

    results = []
    # Concurrency moderat (request ringan)
    with ThreadPoolExecutor(max_workers=6) as ex:
        futs = [ex.submit(parse_game_page, u, session, default_year, ref_date_ymd, state_code.upper()) for u in links]
        for f in as_completed(futs):
            try:
                results.append(f.result())
            except Exception:
                pass
    return results

# ---------------- Orchestrator ----------------
def scrape_all(date_str="9/26/2025", states=None):
    states = states or STATE_CODES
    mm, dd, yyyy = date_str.split("/")
    ref_date_ymd = f"{int(yyyy):04d}-{int(mm):02d}-{int(dd):02d}"
    default_year = int(yyyy)

    out = []
    with requests.Session() as session:
        for st in states:
            try:
                out.extend(parse_state_scores(st, date_str, session, default_year, ref_date_ymd))
            except Exception:
                pass
            time.sleep(0.4 + random.uniform(0, 0.4))  # jeda sopan
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

    mm, dd, yyyy = args.date.split("/")
    out_name = f"hsfb-{int(yyyy):04d}-{int(mm):02d}-{int(dd):02d}.json"
    os.makedirs(args.outdir, exist_ok=True)
    out_path = os.path.join(args.outdir, out_name)
    with open(out_path, "w", encoding="utf-8") as f:
        json.dump(data, f, ensure_ascii=False, indent=2)
    print(f"Saved {len(data)} items → {out_path}")

if __name__ == "__main__":
    sys.exit(main())
