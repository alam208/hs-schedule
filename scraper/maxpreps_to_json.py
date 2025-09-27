# scraper/maxpreps_to_json.py
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

TEAM_CACHE = {}  # cache enrichment dari halaman tim

# ---------------- HTTP ----------------
def get(url, session, retries=3, backoff=1.7):
    for i in range(retries):
        r = session.get(url, headers=HEADERS, timeout=30)
        if r.ok:
            return r
        time.sleep((backoff ** i) + random.uniform(0, 0.3))
    r.raise_for_status()
    return r

# ---------------- Helpers (HTML) ----------------
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

def _pick_from_srcset(val):
    if not val: return ""
    first = val.split(",", 1)[0].strip()
    return first.split(" ", 1)[0].strip()

def _extract_bg_url(style_text):
    if not style_text: return ""
    m = re.search(r"background-image\s*:\s*url\((['\"]?)(.*?)\1\)", style_text)
    return m.group(2) if m else ""

def find_img_url(soup, scope_css):
    """
    Cari URL gambar di dalam elemen 'scope_css':
    - <img src|data-*|srcset>
    - <picture><source srcset>
    - style="background-image:url(...)"
    - global <meta property="og:image">
    """
    wrap = soup.select_one(scope_css) if scope_css else soup
    if not wrap: return ""

    # 1) <img>
    img = wrap.select_one("img")
    if img:
        for attr in ["src", "data-src", "data-original", "data-lazy-src"]:
            if img.get(attr):
                return img.get(attr)
        if img.get("srcset"):
            return _pick_from_srcset(img.get("srcset"))

    # 2) <picture><source>
    src = wrap.select_one("picture source[srcset]")
    if src and src.get("srcset"):
        return _pick_from_srcset(src.get("srcset"))

    # 3) style background-image (di scope atau anak-anaknya)
    url = _extract_bg_url(getattr(wrap, "get", lambda *_: None)("style"))
    if url:
        return url
    for el in wrap.select("[style]"):
        url = _extract_bg_url(el.get("style"))
        if url:
            return url

    # 4) og:image
    og = soup.select_one('meta[property="og:image"]')
    if og and og.get("content"):
        return og.get("content")

    return ""

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

# ---------------- Helpers (Playwright) ----------------
def try_playwright_get_images(url, selectors=None, timeout_ms=60000):
    """
    Render halaman dengan Chromium headless, lalu kumpulkan URL gambar dari selector:
    - background-image (computed style)
    - <img src> dan srcset
    Return: list[str] unik
    """
    selectors = selectors or [
        ".mascot-image",
        ".team-overview__logo",
        ".team-details__logo",
        ".team-logo",
        ".school-logo",
        ".avatar",
        "img[alt*='logo' i]",
        "img"
    ]
    try:
        from playwright.sync_api import sync_playwright
        with sync_playwright() as p:
            browser = p.chromium.launch(headless=True)
            page = browser.new_page(user_agent=HEADERS["User-Agent"])
            page.goto(url, timeout=timeout_ms)
            page.wait_for_load_state("networkidle")
            urls = set()
            for sel in selectors:
                for el in page.query_selector_all(sel):
                    # background-image
                    try:
                        style = el.evaluate("e => getComputedStyle(e).backgroundImage")
                        if style and "url(" in style:
                            m = re.search(r"url\\((['\\\"]?)(.*?)\\1\\)", style)
                            if m and m.group(2):
                                urls.add(m.group(2))
                    except Exception:
                        pass
                    # img src / srcset
                    src = el.get_attribute("src")
                    if src: urls.add(src)
                    srcset = el.get_attribute("srcset")
                    if srcset:
                        first = srcset.split(",", 1)[0].strip().split(" ", 1)[0]
                        if first: urls.add(first)
            browser.close()
            return list(urls)
    except Exception:
        return []

# ---------------- Enrichment dari halaman tim ----------------
def parse_team_page(team_url, session):
    if not team_url:
        return {"logo":"", "city":"", "state":"", "school":"", "mascot":"", "mascotImg":""}
    if team_url in TEAM_CACHE:
        return TEAM_CACHE[team_url]
    out = {"logo":"", "city":"", "state":"", "school":"", "mascot":"", "mascotImg":""}
    try:
        r = get(team_url, session)
        soup = BeautifulSoup(r.text, "html.parser")
        out["logo"] = (
            find_img_url(soup, '.TeamHeader') or
            find_img_url(soup, '.team-header') or
            find_img_url(soup, '.team-logo') or
            find_img_url(soup, '.school-logo') or
            find_img_url(soup, '.avatar') or
            find_img_url(soup, 'body')
        )
        out["school"] = (
            text_one(soup, '.TeamHeader h1, .team-header h1, h1[itemprop="name"]') or
            text_one(soup, 'nav.breadcrumbs a:last-child, .breadcrumbs a:last-child')
        )
        out["mascot"] = (
            text_one(soup, '.mascot, .team-mascot, a.team-details__mascot') or
            out.get("mascot", "")
        )
        out["mascotImg"] = (
            find_img_url(soup, '.mascot-image') or
            find_img_url(soup, '.team-details__logo') or
            ""
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

        # Fallback Playwright kalau logo/mascotImg belum ada
        if not out["logo"] or not out["mascotImg"]:
            imgs = try_playwright_get_images(team_url)
            if imgs:
                if not out["mascotImg"]:
                    out["mascotImg"] = next((u for u in imgs if "school-mascot" in u), out["mascotImg"])
                if not out["logo"]:
                    out["logo"] = next((u for u in imgs if ("school" in u or "logo" in u) and "mascot" not in u), out["logo"])

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

    home_href = soup.select_one('div.team-overview__team:nth-of-type(1) .team-overview__team-name a')
    away_href = soup.select_one('div.team-overview__team:nth-of-type(2) .team-overview__team-name a')
    home_url = urljoin(BASE, home_href.get("href")) if home_href and home_href.get("href") else ""
    away_url = urljoin(BASE, away_href.get("href")) if away_href and away_href.get("href") else ""

    # Mascot name (game page)
    mascotA = text_one(soup, 'div.team-details:nth-of-type(1) a.team-details__mascot') or \
              text_one(soup, 'div.team-details:nth-of-type(1) .team-details__mascot') or ""
    mascotB = text_one(soup, 'div.team-details:nth-of-type(2) a.team-details__mascot') or \
              text_one(soup, 'div.team-details:nth-of-type(2) .team-details__mascot') or ""

    # Mascot image (background-image / logo blok)
    mascotImgA = (
        find_img_url(soup, 'div.team-overview__team:nth-of-type(1) .mascot-image') or
        find_img_url(soup, 'div.team-overview__team:nth-of-type(1) .team-details__logo') or
        ""
    )
    mascotImgB = (
        find_img_url(soup, 'div.team-overview__team:nth-of-type(2) .mascot-image') or
        find_img_url(soup, 'div.team-overview__team:nth-of-type(2) .team-details__logo') or
        ""
    )
    mascotLetterA = text_one(soup, 'div.team-overview__team:nth-of-type(1) .mascot-image--letter')
    mascotLetterB = text_one(soup, 'div.team-overview__team:nth-of-type(2) .mascot-image--letter')

    # Deskripsi & venue
    desc  = text_one(soup, 'p.contest-description') or text_one(soup, 'div.contest-description')
    venue = text_one(soup, 'p.contest-location') or text_one(soup, 'div.contest-location')

    # Logo langsung di page (fallback luas)
    logoA = (
        find_img_url(soup, 'div.team-overview__team:nth-of-type(1) .team-overview__logo') or
        find_img_url(soup, 'div.team-overview__team:nth-of-type(1) .team-details__logo') or
        find_img_url(soup, 'div.team-overview__team:nth-of-type(1) .school-logo') or
        find_img_url(soup, 'div.team-overview__team:nth-of-type(1) .avatar') or
        find_img_url(soup, 'div.team-overview__team:nth-of-type(1)')
    )
    logoB = (
        find_img_url(soup, 'div.team-overview__team:nth-of-type(2) .team-overview__logo') or
        find_img_url(soup, 'div.team-overview__team:nth-of-type(2) .team-details__logo') or
        find_img_url(soup, 'div.team-overview__team:nth-of-type(2) .school-logo') or
        find_img_url(soup, 'div.team-overview__team:nth-of-type(2) .avatar') or
        find_img_url(soup, 'div.team-overview__team:nth-of-type(2)')
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
    enrichA = parse_team_page(home_url, session) if home_url else {"logo":"", "city":"", "state":"", "school":"", "mascot":"", "mascotImg":""}
    enrichB = parse_team_page(away_url, session) if away_url else {"logo":"", "city":"", "state":"", "school":"", "mascot":"", "mascotImg":""}
    logoA = logoA or enrichA.get("logo", "")
    logoB = logoB or enrichB.get("logo", "")
    if not mascotA: mascotA = enrichA.get("mascot", "")
    if not mascotB: mascotB = enrichB.get("mascot", "")
    if not mascotImgA: mascotImgA = enrichA.get("mascotImg", "")
    if not mascotImgB: mascotImgB = enrichB.get("mascotImg", "")

    # --- Fallback Playwright kalau masih kosong ---
    need_pw = (not logoA) or (not logoB) or (not mascotImgA) or (not mascotImgB)
    if need_pw:
        pw_imgs = try_playwright_get_images(url)
        if pw_imgs:
            if not mascotImgA:
                mascotImgA = next((u for u in pw_imgs if "school-mascot" in u), mascotImgA)
            if not mascotImgB:
                mascotImgB = next((u for u in pw_imgs if "school-mascot" in u and u != mascotImgA), mascotImgB)

            if not logoA:
                logoA = next((u for u in pw_imgs if ("school" in u or "logo" in u) and "mascot" not in u), logoA)
            if not logoB:
                logoB = next((u for u in pw_imgs if ("school" in u or "logo" in u) and u != logoA and "mascot" not in u), logoB)

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
        "mascotImgA": mascotImgA or "",
        "mascotImgB": mascotImgB or "",
        "mascotLetterA": text_one(soup, 'div.team-overview__team:nth-of-type(1) .mascot-image--letter'),
        "mascotLetterB": text_one(soup, 'div.team-overview__team:nth-of-type(2) .mascot-image--letter'),
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
    # max_workers moderat karena tiap game bisa trigger headless fallback
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
    Drop entri 'kosong'.
    Minimal: teamA/B harus terisi (bukan default) + ada ≥1 info penting.
    strict=True: butuh ≥2 info penting.
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
        bool(ev.get("mascotImgA")),
        bool(ev.get("mascotImgB")),
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
                    help="JANGAN filter entri kosong (tulis semua).")
    ap.add_argument("--strict", action="store_true",
                    help="Filter lebih ketat: butuh ≥2 info penting.")
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
