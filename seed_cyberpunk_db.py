#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Cyberpunk 2077 DB Seeder (Fandom API → DB)
- Targets: Night City Sub-districts, OS/Arms Cyberware, Consumables
- API: MediaWiki (cyberpunk.fandom.com/api.php)
- DB: PostgreSQL via SQLAlchemy (DATABASE_URL) or SQLite fallback (sqlite:///cyberpunk.db)

Usage (examples):
  # 1) Install deps
  pip install requests sqlalchemy psycopg2-binary python-dotenv mwparserfromhell

  # 2) Set DB URL (Postgres recommended). If omitted, falls back to ./cyberpunk.db (SQLite).
  export DATABASE_URL='postgresql+psycopg2://user:pass@localhost:5432/cpunk'

  # 3) Run
  python seed_cyberpunk_db.py --all

Notes:
- Respects rate limiting (~1 req/sec) and uses pageid/revid to detect diffs.
- Stores raw wikitext and parsed summaries when possible.
- Writes a JSON snapshot to ./out/ for quick inspection.
"""

import os
import re
import json
import time
import argparse
import datetime as dt
from typing import Dict, List, Any, Optional

import requests
from sqlalchemy import (
    create_engine, text
)
from sqlalchemy.engine import Engine
from sqlalchemy.exc import SQLAlchemyError

try:
    import mwparserfromhell as mwp
except Exception:
    mwp = None  # Optional, parsing falls back to plain text

# ---------- Config ----------
API = "https://cyberpunk.fandom.com/api.php"
RATE_DELAY = 1.0  # ~1 req/sec (polite)
HEADERS = {"User-Agent": "Aria-Cyberpunk-DB/1.0 (respectful bot; contact: local-user)"}

# Categories
CAT_SUBDISTRICTS = "Category:Cyberpunk_2077_Sub-districts"
CAT_OS = "Category:Cyberpunk_2077_Cyberware_-_Operating_System"
CAT_OS_DECKS = "Category:Cyberpunk_2077_Cyberware_-_Cyberdecks"
CAT_OS_SAND = "Category:Cyberpunk_2077_Cyberware_-_Sandevistan_Operating_system"
CAT_OS_BERS = "Category:Cyberpunk_2077_Cyberware_-_Berserk_Operating_system"
CAT_ARMS = "Category:Cyberpunk_2077_Cyberware_-_Arms"
CAT_CONSUMABLES = "Category:Cyberpunk_2077_Consumables"

OUT_DIR = Path("out")
OUT_DIR.mkdir(exist_ok=True)

# ---------- Helpers ----------

def get_engine() -> Engine:
    db_url = os.getenv("DATABASE_URL", "sqlite:///cyberpunk.db")
    engine = create_engine(db_url, future=True)
    return engine

def run_sql(engine: Engine, sql: str, **params):
    with engine.begin() as conn:
        return conn.execute(text(sql), params)

def init_schema(engine: Engine):
    sql = """
    CREATE TABLE IF NOT EXISTS sources(
      id INTEGER PRIMARY KEY AUTOINCREMENT,
      site TEXT, url TEXT, license TEXT, notes TEXT
    );
    CREATE TABLE IF NOT EXISTS pages(
      id INTEGER PRIMARY KEY AUTOINCREMENT,
      source_id INTEGER REFERENCES sources(id),
      title TEXT, slug TEXT, url TEXT, lang TEXT,
      pageid BIGINT, revid BIGINT, last_seen_at TEXT,
      wikitext TEXT, summary TEXT
    );
    CREATE TABLE IF NOT EXISTS subdistricts(
      id INTEGER PRIMARY KEY AUTOINCREMENT,
      name TEXT, parent_district TEXT,
      description TEXT, page_id INTEGER REFERENCES pages(id),
      aliases TEXT
    );
    CREATE TABLE IF NOT EXISTS cyberware(
      id INTEGER PRIMARY KEY AUTOINCREMENT,
      name TEXT, slot TEXT, manufacturer TEXT,
      rarity_min TEXT, rarity_max TEXT, description TEXT,
      page_id INTEGER REFERENCES pages(id)
    );
    CREATE TABLE IF NOT EXISTS cyberware_variants(
      id INTEGER PRIMARY KEY AUTOINCREMENT,
      cyberware_id INTEGER REFERENCES cyberware(id),
      rarity TEXT, effects_json TEXT, requirements_json TEXT,
      price TEXT, page_id INTEGER
    );
    CREATE TABLE IF NOT EXISTS items(
      id INTEGER PRIMARY KEY AUTOINCREMENT,
      name TEXT, category TEXT, subcategory TEXT,
      description TEXT, page_id INTEGER REFERENCES pages(id)
    );
    CREATE TABLE IF NOT EXISTS item_stats(
      id INTEGER PRIMARY KEY AUTOINCREMENT,
      item_id INTEGER REFERENCES items(id),
      stat_key TEXT, stat_value TEXT, unit TEXT, source_note TEXT
    );
    """
    # SQLite vs Postgres AUTOINCREMENT compatibility
    if engine.url.get_backend_name().startswith("postgresql"):
        sql = sql.replace("INTEGER PRIMARY KEY AUTOINCREMENT", "SERIAL PRIMARY KEY")
        sql = sql.replace("TEXT", "TEXT")
        sql = sql.replace("BIGINT", "BIGINT")
    run_sql(engine, sql)

def ensure_source(engine: Engine, site: str, url: str, license_txt: str, notes: str = "") -> int:
    q = "SELECT id FROM sources WHERE site=:site AND url=:url"
    res = run_sql(engine, q, site=site, url=url).fetchone()
    if res:
        return int(res[0])
    ins = """
    INSERT INTO sources(site,url,license,notes) VALUES(:site,:url,:license,:notes)
    RETURNING id
    """
    if engine.url.get_backend_name().startswith("sqlite"):
        ins = "INSERT INTO sources(site,url,license,notes) VALUES(:site,:url,:license,:notes)"
        run_sql(engine, ins, site=site, url=url, license=license_txt, notes=notes)
        return int(run_sql(engine, "SELECT last_insert_rowid()").fetchone()[0])
    else:
        return int(run_sql(engine, ins, site=site, url=url, license=license_txt, notes=notes).fetchone()[0])

def slugify(title: str) -> str:
    s = title.lower()
    s = re.sub(r"[^a-z0-9]+", "-", s)
    return s.strip("-")

def mw_get(params: Dict[str, Any]) -> Dict[str, Any]:
    """GET with polite delay and error handling."""
    time.sleep(RATE_DELAY)
    params = {**params, "format": "json"}
    r = requests.get(API, params=params, headers=HEADERS, timeout=30)
    r.raise_for_status()
    return r.json()

def get_category_members(cattitle: str) -> List[Dict[str, Any]]:
    params = dict(action="query", list="categorymembers", cmtitle=cattitle, cmlimit="200")
    out = []
    while True:
        data = mw_get(params)
        out.extend(data.get("query", {}).get("categorymembers", []))
        if "continue" in data:
            params.update(data["continue"])
        else:
            break
    return out

def get_pages_details_by_ids(pageids: List[int]) -> Dict[str, Any]:
    if not pageids:
        return {}
    # Split into chunks to avoid URL length issues
    pages = {}
    CHUNK = 40
    for i in range(0, len(pageids), CHUNK):
        chunk = pageids[i:i+CHUNK]
        params = dict(
            action="query",
            prop="revisions|categories|info",
            rvprop="ids|timestamp|content",
            cllimit="500",
            inprop="url",
            pageids=",".join(map(str, chunk)),
        )
        data = mw_get(params)
        pages.update(data.get("query", {}).get("pages", {}))
    return pages

def parse_wikitext_to_summary(wikitext: str) -> str:
    """Create a short plain-text summary from wikitext (first paragraph-like)."""
    if not wikitext:
        return ""
    if mwp:
        try:
            code = mwp.parse(wikitext)
            text = code.strip_code().strip()
        except Exception:
            text = wikitext
    else:
        # crude fallback: strip templates/links
        text = re.sub(r"\{\{.*?\}\}", "", wikitext, flags=re.S)
        text = re.sub(r"\[\[(?:[^\]|]+\|)?([^\]]+)\]\]", r"\1", text)
    # take first 500 chars approx.
    text = re.sub(r"\n{2,}", "\n", text).strip()
    return text[:500]

def upsert_page(engine: Engine, src_id: int, p: Dict[str, Any]) -> int:
    pageid = p.get("pageid")
    title = p.get("title")
    fullurl = p.get("fullurl", "")
    lang = "en"  # Fandom default (adjust if localized)
    revs = p.get("revisions", [])
    revid = revs[0]["revid"] if revs else None
    wikitext = revs[0].get("*") if revs else None
    summary = parse_wikitext_to_summary(wikitext or "")

    # Does it exist?
    row = run_sql(engine, "SELECT id, revid FROM pages WHERE pageid=:pid", pid=pageid).fetchone()
    now = dt.datetime.utcnow().isoformat()
    if row:
        pid, old_revid = int(row[0]), row[1]
        if old_revid != revid:
            run_sql(engine, """
                UPDATE pages SET title=:title, slug=:slug, url=:url, lang=:lang,
                       revid=:revid, last_seen_at=:now, wikitext=:wikitext, summary=:summary
                 WHERE id=:id
            """, title=title, slug=slugify(title), url=fullurl, lang=lang, revid=revid,
                 now=now, wikitext=wikitext, summary=summary, id=pid)
        else:
            run_sql(engine, "UPDATE pages SET last_seen_at=:now WHERE id=:id", now=now, id=pid)
        return pid
    else:
        # Insert
        ins = """
        INSERT INTO pages(source_id,title,slug,url,lang,pageid,revid,last_seen_at,wikitext,summary)
        VALUES(:src,:title,:slug,:url,:lang,:pageid,:revid,:now,:wikitext,:summary)
        """
        run_sql(engine, ins, src=src_id, title=title, slug=slugify(title), url=fullurl,
                lang=lang, pageid=pageid, revid=revid, now=now, wikitext=wikitext, summary=summary)
        return int(run_sql(engine, "SELECT last_insert_rowid()" if engine.url.get_backend_name().startswith("sqlite") else
                           "SELECT currval(pg_get_serial_sequence('pages','id'))").fetchone()[0])

# --- Domain-specific extractors (heuristic; adjust as needed) ---

def extract_parent_district_from_categories(categories: List[Dict[str, Any]]) -> Optional[str]:
    # Try to infer parent from category titles like "... City Center subdistricts" (if any)
    # Fallback None; user may backfill later.
    if not categories:
        return None
    names = [c.get("title","") for c in categories]
    # Heuristic: check mentions of known districts
    KNOWN = ["Watson", "Westbrook", "City Center", "Santo Domingo", "Heywood", "Pacifica", "Dogtown", "Badlands"]
    for k in KNOWN:
        for n in names:
            if k.lower() in n.lower():
                return k
    return None

def upsert_subdistrict(engine: Engine, page_id: int, page_title: str, categories: List[Dict[str, Any]], summary: str):
    name = page_title.replace("(2077)", "").strip()
    parent = extract_parent_district_from_categories(categories)
    desc = summary
    existing = run_sql(engine, "SELECT id FROM subdistricts WHERE page_id=:pid", pid=page_id).fetchone()
    if existing:
        run_sql(engine, "UPDATE subdistricts SET name=:name, parent_district=:parent, description=:desc WHERE page_id=:pid",
                name=name, parent_district=parent, desc=desc, pid=page_id)
    else:
        run_sql(engine, "INSERT INTO subdistricts(name,parent_district,description,page_id,aliases) VALUES(:name,:parent,:desc,:pid,:aliases)",
                name=name, parent=parent, desc=desc, pid=page_id, aliases=None)

def infer_slot_from_categories(categories: List[Dict[str, Any]]) -> Optional[str]:
    if not categories:
        return None
    joined = " | ".join([c.get("title","") for c in categories]).lower()
    if "arms" in joined:
        return "Arms"
    if "operating system" in joined:
        return "Operating System"
    if "cyberdecks" in joined:
        return "Operating System / Cyberdeck"
    if "sandevistan" in joined:
        return "Operating System / Sandevistan"
    if "berserk" in joined:
        return "Operating System / Berserk"
    return None

def upsert_cyberware(engine: Engine, page_id: int, page_title: str, categories: List[Dict[str, Any]], summary: str):
    slot = infer_slot_from_categories(categories)
    name = page_title.replace("(Cyberware)", "").strip()
    existing = run_sql(engine, "SELECT id FROM cyberware WHERE page_id=:pid", pid=page_id).fetchone()
    if existing:
        run_sql(engine, "UPDATE cyberware SET name=:name, slot=:slot, description=:desc WHERE page_id=:pid",
                name=name, slot=slot, desc=summary, pid=page_id)
    else:
        run_sql(engine, "INSERT INTO cyberware(name,slot,manufacturer,rarity_min,rarity_max,description,page_id) VALUES(:name,:slot,:man,:rmin,:rmax,:desc,:pid)",
                name=name, slot=slot, man=None, rmin=None, rmax=None, desc=summary, pid=page_id)

def upsert_item(engine: Engine, page_id: int, page_title: str, categories: List[Dict[str, Any]], summary: str):
    # Coarse category inference
    joined = " | ".join([c.get("title","") for c in categories]).lower() if categories else ""
    category = "Consumable" if "consumables" in joined else None
    subcategory = None
    name = page_title.strip()
    existing = run_sql(engine, "SELECT id FROM items WHERE page_id=:pid", pid=page_id).fetchone()
    if existing:
        run_sql(engine, "UPDATE items SET name=:name, category=:cat, subcategory=:sub, description=:desc WHERE page_id=:pid",
                name=name, cat=category, sub=subcategory, desc=summary, pid=page_id)
    else:
        run_sql(engine, "INSERT INTO items(name,category,subcategory,description,page_id) VALUES(:name,:cat,:sub,:desc,:pid)",
                name=name, cat=category, sub=subcategory, desc=summary, pid=page_id)

# ---------- Pipelines ----------

def fetch_and_upsert_group(engine: Engine, src_id: int, members: List[Dict[str, Any]], group_kind: str):
    pageids = [m["pageid"] for m in members if "pageid" in m]
    pages = get_pages_details_by_ids(pageids)
    snapshot = []
    for _, p in pages.items():
        pid = upsert_page(engine, src_id, p)
        title = p.get("title","")
        cats = p.get("categories", [])
        summary = parse_wikitext_to_summary(p.get("revisions",[{}])[0].get("*",""))
        if group_kind == "subdistrict":
            upsert_subdistrict(engine, pid, title, cats, summary)
        elif group_kind == "cyberware":
            upsert_cyberware(engine, pid, title, cats, summary)
        elif group_kind == "item":
            upsert_item(engine, pid, title, cats, summary)
        snapshot.append({"title": title, "pageid": p.get("pageid"), "revid": p.get("revisions",[{}])[0].get("revid"), "url": p.get("fullurl")})
    return snapshot

def pipeline(all_:bool=False, subdistricts:bool=False, os_:bool=False, arms:bool=False, consumables:bool=False):
    engine = get_engine()
    init_schema(engine)
    src_id = ensure_source(engine, "Fandom", "https://cyberpunk.fandom.com", "CC BY-SA 3.0", "Data via MediaWiki API")

    out_manifest = {"generated_at": dt.datetime.utcnow().isoformat(), "groups": {}}

    # A) Sub-districts
    if all_ or subdistricts:
        subs = get_category_members(CAT_SUBDISTRICTS)
        snap = fetch_and_upsert_group(engine, src_id, subs, "subdistrict")
        (OUT_DIR / "subdistricts.json").write_text(json.dumps(snap, indent=2, ensure_ascii=False))
        out_manifest["groups"]["subdistricts"] = {"count": len(snap)}

    # B) OS (Cyberdecks, Sandevistan, Berserk)
    if all_ or os_:
        group_snap = []
        for cat in (CAT_OS_DECKS, CAT_OS_SAND, CAT_OS_BERS):
            members = get_category_members(cat)
            group_snap += fetch_and_upsert_group(engine, src_id, members, "cyberware")
        (OUT_DIR / "cyberware_os.json").write_text(json.dumps(group_snap, indent=2, ensure_ascii=False))
        out_manifest["groups"]["cyberware_os"] = {"count": len(group_snap)}

    # C) Arms
    if all_ or arms:
        members = get_category_members(CAT_ARMS)
        snap = fetch_and_upsert_group(engine, src_id, members, "cyberware")
        (OUT_DIR / "cyberware_arms.json").write_text(json.dumps(snap, indent=2, ensure_ascii=False))
        out_manifest["groups"]["cyberware_arms"] = {"count": len(snap)}

    # D) Consumables
    if all_ or consumables:
        members = get_category_members(CAT_CONSUMABLES)
        snap = fetch_and_upsert_group(engine, src_id, members, "item")
        (OUT_DIR / "consumables.json").write_text(json.dumps(snap, indent=2, ensure_ascii=False))
        out_manifest["groups"]["consumables"] = {"count": len(snap)}

    (OUT_DIR / "manifest.json").write_text(json.dumps(out_manifest, indent=2, ensure_ascii=False))
    print(json.dumps(out_manifest, indent=2, ensure_ascii=False))

def main():
    ap = argparse.ArgumentParser(description="Seed Cyberpunk 2077 DB from Fandom API")
    ap.add_argument("--all", action="store_true", help="Fetch everything (subdistricts, OS, arms, consumables)")
    ap.add_argument("--subdistricts", action="store_true", help="Fetch sub-districts")
    ap.add_argument("--os", dest="os_", action="store_true", help="Fetch OS cyberware")
    ap.add_argument("--arms", action="store_true", help="Fetch Arms cyberware")
    ap.add_argument("--consumables", action="store_true", help="Fetch consumables")
    args = ap.parse_args()

    try:
        pipeline(all_=args.all, subdistricts=args.subdistricts, os_=args.os_, arms=args.arms, consumables=args.consumables)
    except requests.HTTPError as e:
        print(f"[HTTPError] {e}")
    except SQLAlchemyError as e:
        print(f"[DBError] {e}")
    except Exception as e:
        print(f"[Error] {e}")

if __name__ == "__main__":
    main()
