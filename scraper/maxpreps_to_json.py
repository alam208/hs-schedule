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

TEAM_CACHE = {}  # cache enrichment tim

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

def attr_one(soup, sel, name, default=""):
    el = soup.select_one(sel)
    if el and el.has_attr(name):
        return el.get(name) or default
    for cand in ["data-src", "data-original", "data-lazy-src"]:
        if el and el.has_attr(cand):
            return el.get(cand) or default
    return default

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
    m = re.search(r"\btoday\b\s*@\s*(\d{1,2})(?::(\d{2}))?\s*([ap])\.?m?\.?", desc, re.I) \
        or re.search(r"\btoday\b\s*@\s*(\d{1,2})(?::(\d{2}))?\s*(AM|PM|A\.M\.|P\.M\.)", desc, re.I)
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

def parse_city_state_from_parens(text):
    if not text:
        return "", ""
    m = re.search(r"\(([A-Za-z .'-]+),\s*([A-Z]{2})\)", text)
    if m:
        return m.group(1).strip(), m.group(2).strip()
    return "", ""

# ---------------- Enrichment dari halaman tim ----------------
def parse_team_page(team_url, session):
    if not team_url:
        return {"logo":"", "city":"", "state":"", "school":"", "mascot":""}
    if team_url in TEAM_CACHE:
        return TEAM_CACHE[team_url]
    out = {"logo":"", "city":"", "state":"", "school":"", "mascot":""}
    try:
        r = get(team_url, session)
        soup = BeautifulSoup(r.text, "html.parser")
        out["logo"] = (
            attr_one(soup, 'meta[property="og:image"]', 'content') or
            attr_one(soup, '.TeamHeader img, .team-header img, .team-logo img, .school-logo img, .avatar img, img[alt*="logo" i]', 'src')
        )
        out["school"] = (
            text_one(soup, '.TeamHeader h1, .team-header h1, h1[itemprop="name"]') or
            text_one(soup, 'nav.breadcrumbs a:last-child, .breadcrumbs a:last-child')
        )
        out["mascot"] = (
            text_one(soup, '.mascot, .team-mascot, a.team-details__mascot') or
            out.get("mascot", "")
        )
        loc = (
            text_one(soup, '.TeamHeader .location, .team-header .location') or
            text_one(soup, '.profile-header .location') or
            text_one(soup, 'meta[name="description"]')
        )
        m_cs = re.search(r"([A-Za-z .'-]+),\s*([A-Z]{2})(?:\b|$)", loc or "")
        if m_cs:
            out["city"] = m_cs.group(1).strip()
            out["state"] = m_cs.group(2).strip()
        else:
            m_state_url = re.search(r"/([a-z]{2})/", team_url)
            if m_state_url:
                out["state"] = m_state_url.group(1).upper()
    except Exception:
        pass
    TEAM_CACHE[team_url] = out
    return out

# ---------------- Halaman game ----------------
def parse_game_page(url, session, default_year, ref_date_ymd):
    r = get(url, session)
    soup = BeautifulSoup(r.text, "html.parser")

    # Tim + link tim
    home = (text_one(soup, 'div.team-overview__team:nth-of-type(1) .team-overview__team-name a') or
            text_one(soup, 'div.team-overview__team:nth-of-type(1) .team-overview__team-name'))
    away = (text_one(soup, 'div.team-overview__team:nth-of-type(2) .team-overview__team-name a') or
            text_one(soup, 'div.team-overview__team:nth-of-type(2) .team-overview__team-name'))
    home, away = clean_team(home), clean_team(away)

    home_href = attr_one(soup, 'div.team-overview__team:nth-of-type(1) .team-overview__team-name a', 'href')
    away_href = attr_one(soup, 'div.team-overview__team:nth-of-type(2) .team-overview__team-name a', 'href')
    home_url = urljoin(BASE, home_href) if home_href else ""
    away_url = urljoin(BASE, away_href) if away_href else ""

    # Mascot (jika tersedia di game page)
    mascotA = text_one(soup, 'div.team-details:nth-of-type(1) a.team-details__mascot') or \
              text_one(soup, 'div.team-details:nth-of-type(1) .team-details__mascot') or ""
    mascotB = text_one(soup, 'div.team-details:nth-of-type(2) a.team-details__mascot') or \
              text_one(soup, 'div.team-details:nth-of-type(2) .team-details__mascot') or ""

    # Deskripsi & venue
    desc  = text_one(soup, 'p.contest-description') or text_one(soup, 'div.contest-description')
    venue = text_one(soup, 'p.contest-location') or text_one(soup, 'div.contest-location')

    # Logo langsung di page (fallback luas)
    logoA = (
        attr_one(soup, 'div.team-overview__team:nth-of-type(1) .team-overview__logo img', 'src') or
        attr_one(soup, 'div.team-overview__team:nth-of-type(1) .team-details__logo img', 'src') or
        attr_one(soup, 'div.team-overview__team:nth-of-type(1) .school-logo img', 'src') or
        attr_one(soup, 'div.team-overview__team:nth-of-type(1) .avatar img', 'src') or
        attr_one(soup, 'div.team-overview__team:nth-of-type(1) img[alt*="logo" i]', 'src') or
        attr_one(soup, 'div.team-overview__team:nth-of-type(1) img', 'src')
    )
    logoB = (
        attr_one(soup, 'div.team-overview__team:nth-of-type(2) .team-overview__logo img', 'src') or
        attr_one(soup, 'div.team-overview__team:nth-of-type(2) .team-details__logo img', 'src') or
        attr_one(soup, 'div.team-overview__team:nth-of-type(2) .school-logo img', 'src') or
        attr_one(soup, 'div.team-overview__team:nth-of-type(2) .avatar img', 'src') or
        attr_one(soup, 'div.team-overview__team:nth-of-type(2) img[alt*="logo" i]', 'src') or
        attr_one(soup, 'div.team-overview__team:nth-of-type(2) img', 'src')
    )

    # Kickoff dari LD+JSON; fallback parse dari deskripsi
    ld = ldjson_first(soup)
    start_iso = ""
    if isinstance(ld, dict):
        start_iso = (ld.get("startDate") or (ld.get("event") or {}).get("startDate") or "")
    if not start_iso:
        start_iso = parse_desc_today(desc, ref_date_ymd)
    if not start_iso:
        start_iso = parse_desc_monthday(desc, default_year)

    # Enrich dari halaman tim
    enrichA = parse_team_page(home_url, session) if home_url else {"logo":"", "city":"", "state":"", "school":"", "mascot":""}
    enrichB = parse_team_page(away_url, session) if away_url else {"logo":"", "city":"", "state":"", "school":"", "mascot":""}
    logoA = logoA or enrichA.get("logo", "")
    logoB = logoB or enrichB.get("logo", "")
    if not mascotA: mascotA = enrichA.get("mascot", "")
    if not mascotB: mascotB = enrichB.get("mascot", "")

    # City/State prioritas dari team page → venue → deskripsi
    city = enrichA.get("city") or enrichB.get("city") or ""
    state = enrichA.get("state") or enrichB.get("state") or ""
    if venue:
        if not state:
            m_state = re.search(r",\s*([A-Z]{2})(?:\s|$)", venue)
            if m_state: state = m_state.group(1)
        if not city:
            m_city = re.search(r"([^,]+),\s*[A-Z]{2}\b", venue)
            if m_city: city = m_city.group(1).strip()
    if (not city or not state) and desc:
        c2, s2 = parse_city_state_from_parens(desc)
        city = city or c2
        state = state or s2

    school = enrichA.get("school") or enrichB.get("school") or ""

    return {
        "teamA": home or "Team A",
        "teamB": away or "Team B",
        "sport": "Football",
        "league": "",
        "venue": venue,
        "kick": start_iso,
        "stream": "#",
        "chat": "#",
        "school": school,
        "city": city,
        "state": state,
        "logoA": logoA or "",
        "logoB": logoB or "",
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
    results = []
    with ThreadPoolExecutor(max_workers=4) as ex:
        futs = [ex.submit(parse_game_page, u, session, default_year, ref_date_ymd) for u in links]
        for f in as_completed(futs):
            try:
                results.append(f.result())
            except Exception:
                pass
    return results

# ---------------- Orchestrator + Filtering ----------------
def is_informative(ev, strict=False):
    """
    Return False kalau entry 'kosong' dan harus di-skip.
    Kriteria minimal:
      - teamA & teamB terisi (bukan 'Team A/B'), DAN
      - ada salah satu informasi penting: kick/description/city/state/venue/mascot/logo.
    Kalau strict=True, butuh minimal 2 informasi penting selain tim.
    """
    if not ev.get("teamA") or not ev.get("teamB"):
        return False
    if ev["teamA"] == "Team A" or ev["teamB"] == "Team B":
        return False

    infos = [
        bool(ev.get("kick")),
        bool(ev.get("description")),
        bool(ev.get("city")),
        bool(ev.get("state")),
        bool(ev.get("venue")),
        bool(ev.get("mascotA")),
        bool(ev.get("mascotB")),
        bool(ev.get("logoA")),
        bool(ev.get("logoB")),
    ]
    count = sum(1 for x in infos if x)
    return count >= (2 if strict else 1)

def scrape_all(date_str="9/26/2025", states=None, drop_empty=True, strict=False):
    states = states or STATE_CODES
    mm, dd, yyyy = date_str.split("/")
    ref_date_ymd = f"{int(yyyy):04d}-{int(mm):02d}-{int(dd):02d}"
    default_year = int(yyyy)

    out = []
    with requests.Session() as session:
        for st in states:
            try:
                batch = parse_state_scores(st, date_str, session, default_year, ref_date_ymd)
                if drop_empty:
                    batch = [ev for ev in batch if is_informative(ev, strict=strict)]
                out.extend(batch)
            except Exception:
                pass
            time.sleep(0.6 + random.uniform(0, 0.6))
    return out

def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--date", default="9/26/2025", help="mm/dd/YYYY (sesuai URL MaxPreps)")
    ap.add_argument("--states", default=",".join(STATE_CODES),
                    help="Comma-separated state codes (e.g., tx,ca,fl). Default: all")
    ap.add_argument("--outdir", default="data", help="Folder output JSON")
    ap.add_argument("--no-drop-empty", action="store_true",
                    help="JANGAN filter entry kosong (tulis semua).")
    ap.add_argument("--strict", action="store_true",
                    help="Filter lebih ketat: butuh ≥2 info penting selain team.")
    args = ap.parse_args()

    states = [s.strip().lower() for s in args.states.split(",") if s.strip()]
    data = scrape_all(args.date, states=states, drop_empty=not args.no_drop_empty, strict=args.strict)

    mm, dd, yyyy = args.date.split("/")
    out_name = f"hsfb-{int(yyyy):04d}-{int(mm):02d}-{int(dd):02d}.json"
    os.makedirs(args.outdir, exist_ok=True)
    out_path = os.path.join(args.outdir, out_name)
    with open(out_path, "w", encoding="utf-8") as f:
        json.dump(data, f, ensure_ascii=False, indent=2)
    print(f"Saved {len(data)} items → {out_path}")

if __name__ == "__main__":
    sys.exit(main())
