#!/usr/bin/env python3
"""
combined.py — memory-only JS capture + CSV pipeline

This script:
  0) Attempts to capture specified JS files from the page (Playwright if available,
     otherwise HTTP fallback). Captured content is kept *in memory only*.
  1) stage_scraper: parse embedded JSON from the captured JS (or disk fallback) -> writes maprunner_data.csv
  2) stage_parq_maker: optional excel->csv converter (skipped if CSV already exists)
  3) stage_corupt: robust parse/repair pipeline -> writes maprunner_data_corupt.csv + repairs_qc.csv

JS/HTTP downloads are stored only in RAM (IN_MEM_FILES) and not written to disk.
"""

import os
import sys
import time
import re
import json
import codecs
import csv
import tempfile
import traceback
import urllib.request
import urllib.error
from urllib.parse import urljoin, urlparse

VERBOSE = os.environ.get("VERBOSE") == "1"
if not VERBOSE:
    def print(*args, **kwargs):
        return None

def debug_exc():
    if VERBOSE:
        traceback.print_exc()

BASE_DIR = os.path.dirname(os.path.abspath(__file__))

REGION_LIST = [
    ("US_01", "Michigan"),
    ("US_02", "Alaska"),
    ("RU_02", "Taymyr"),
    ("RU_03", "Kola Peninsula"),
    ("US_04", "Yukon"),
    ("US_03", "Wisconsin"),
    ("RU_04", "Amur"),
    ("RU_05", "Don"),
    ("US_06", "Maine"),
    ("US_07", "Tennessee"),
    ("RU_08", "Glades"),
    ("US_09", "Ontario"),
    ("US_10", "British Columbia"),
    ("US_11", "Scandinavia"),
    ("US_12", "North Carolina"),
    ("RU_13", "Almaty"),
    ("US_14", "Austria"),
    ("US_15", "Quebec"),
    ("US_16", "Washington"),
    ("RU_17", "Zurdania"),
]
REGION_ORDER = [r for r, _ in REGION_LIST]
REGION_LOOKUP = dict(REGION_LIST)
CATEGORY_PRIORITY = ["_CONTRACTS", "_TASKS", "_CONTESTS"]
TYPE_PRIORITY = ["truckDelivery", "cargoDelivery", "exploration"]
ALLOWED_CATEGORIES = set(CATEGORY_PRIORITY)

# In-memory storage for downloaded files (name -> bytes)
IN_MEM_FILES = {}
IN_MEM_META = {}

# --- small helper utilities (place right after IN_MEM_FILES = {}) ---
def store_in_memory(name: str, data: bytes, url=None):
    """Store bytes under a name in the in-memory store (overwrite allowed)."""
    if not name:
        return
    IN_MEM_FILES[name] = data
    if url:
        IN_MEM_META[name] = {"url": url}

def get_file_bytes_or_mem(name: str):
    """Return bytes for `name` from IN_MEM_FILES if present, otherwise try to read from disk path `name`. Returns None if not found."""
    # Prefer memory
    if name in IN_MEM_FILES:
        return IN_MEM_FILES[name]
    # then disk
    try:
        if os.path.exists(name) and os.path.isfile(name):
            with open(name, "rb") as f:
                return f.read()
    except Exception:
        pass
    # Try common variants (strip query, path)
    try:
        base = os.path.basename(name)
        if base in IN_MEM_FILES:
            return IN_MEM_FILES[base]
        if os.path.exists(base) and os.path.isfile(base):
            with open(base, "rb") as f:
                return f.read()
    except Exception:
        pass
    return None

def decode_bytes_to_text(bs: bytes):
    """
    Robustly decode bytes -> text using best-effort heuristics.
    Uses chardet if available, otherwise tries utf-8 with fallbacks.
    """
    if bs is None:
        return None
    if isinstance(bs, str):
        return bs
    # Try BOM-safe utf-8 first
    try:
        txt = bs.decode("utf-8")
        return txt
    except Exception:
        pass
    # Try chardet if installed
    try:
        import chardet
        det = chardet.detect(bs)
        enc = det.get("encoding") or "utf-8"
        try:
            return bs.decode(enc, errors="replace")
        except Exception:
            pass
    except Exception:
        # chardet not available; continue
        pass
    # Try common encodings/fallbacks
    for enc in ("utf-8", "latin-1", "windows-1252", "iso-8859-1"):
        try:
            return bs.decode(enc, errors="replace")
        except Exception:
            continue
    # Final fallback: str()
    try:
        return str(bs)
    except Exception:
        return None

def ensure_working_dir():
    """Force a stable working directory (helps when launching via double-click)."""
    try:
        os.chdir(BASE_DIR)
    except Exception:
        pass

def http_get(url, timeout=15):
    req = urllib.request.Request(
        url,
        headers={
            "User-Agent": "Mozilla/5.0",
            "Accept": "*/*",
        },
    )
    with urllib.request.urlopen(req, timeout=timeout) as resp:
        data = resp.read()
        headers = {k.lower(): v for k, v in resp.headers.items()}
    return data, headers

# --- Step 0: download_js (Playwright + fallback) ---
# --- Replace target names + download logic with heuristic-based discovery ---
# (drop-in replacement for TARGET_NAMES and the two functions)

# don't hardcode the exact filenames anymore
URL = "https://www.maprunner.info/michigan/black-river?loc=_CL_1"
TIMEOUT_SECONDS = 30

# map canonical roles -> memory filename
CANONICAL_NAMES = {
    "data": "data.js",   # contains JSON.parse(...) with the main payload
    "desc": "desc.js",   # contains localization / "s:" description table etc.
}

def looks_like_js_response(headers, url):
    ctype = (headers.get("content-type") or "").lower()
    # basic checks: JS content-type OR known cdn path fragment
    return ("javascript" in ctype) or ("/mr/" in url) or url.endswith(".js")

def score_data_js(text: str):
    if not text:
        return 0
    tl = text.lower()
    if "json.parse" not in tl:
        return 0
    score = 0
    if '"category"' in tl:
        score += 10
    if '"objectives"' in tl:
        score += 8
    if '"rewards"' in tl:
        score += 8
    if '"key"' in tl:
        score += 10
    if "_contracts" in tl or "_tasks" in tl or "_contests" in tl:
        score += 12
    if '"truckdelivery"' in tl or '"cargodelivery"' in tl or '"exploration"' in tl:
        score += 6
    score += min(len(text) // 5000, 20)
    score += min(tl.count('"category"'), 30)
    score += min(tl.count('"key"'), 30)
    return score

def score_desc_js(text: str):
    if not text:
        return 0
    score = 0
    if "UI_" in text:
        score += min(text.count("UI_"), 50)
    if re.search(r'\bs\s*:\s*(?:"|\')', text):
        score += 10
    if "_NAME" in text or "_DESC" in text:
        score += 6
    score += min(len(text) // 5000, 20)
    return score

def choose_best_js_roles():
    best_data = (0, None)
    best_desc = (0, None)
    for name, bs in IN_MEM_FILES.items():
        if not name.lower().endswith(".js"):
            continue
        text = decode_bytes_to_text(bs)
        if not text:
            continue
        data_score = score_data_js(text)
        desc_score = score_desc_js(text)
        meta_url = IN_MEM_META.get(name, {}).get("url", "")
        if "maprunner.info" in meta_url or "/mr/" in meta_url:
            data_score += 3
            desc_score += 3
        if data_score > best_data[0]:
            best_data = (data_score, name)
        if desc_score > best_desc[0]:
            best_desc = (desc_score, name)
    if best_data[1]:
        store_in_memory(CANONICAL_NAMES["data"], IN_MEM_FILES[best_data[1]])
    if best_desc[1]:
        store_in_memory(CANONICAL_NAMES["desc"], IN_MEM_FILES[best_desc[1]])

def identify_js_role(text):
    """Return 'data', 'desc', or None based on heuristics applied to JS text."""
    if not text:
        return None
    data_score = score_data_js(text)
    desc_score = score_desc_js(text)
    if data_score >= 15 and data_score >= desc_score:
        return "data"
    if desc_score >= 10 and desc_score > data_score:
        return "desc"
    return None

def fallback_download_candidates_to_mem(page_url, html):
    """
    Find <script src="..."> candidates from the page and attempt to download
    those whose path looks promising (e.g. contains /mr/ or endswith .js).
    Identify role by inspecting the content and store canonical names into memory.
    Returns list of canonical names actually stored (e.g. ['data.js','desc.js'])
    """
    found_roles = []
    candidates = []
    for match in re.finditer(r'<script[^>]+src=["\']([^"\']+)["\']', html, flags=re.IGNORECASE):
        src = match.group(1)
        abs_url = urljoin(page_url, src)
        # only consider likely CDN/script paths (cheap filter)
        if "/mr/" in abs_url or abs_url.endswith(".js") or "cdn" in abs_url:
            candidates.append(abs_url)

    # optional: deduplicate and sort (prefer ones with /mr/ first)
    candidates = sorted(set(candidates), key=lambda u: ("/mr/" not in u, u))

    for abs_url in candidates:
        try:
            data, headers = http_get(abs_url, timeout=15)
            if not looks_like_js_response(headers, abs_url):
                continue
            txt = decode_bytes_to_text(data)
            role = identify_js_role(txt)
            if role:
                canon = CANONICAL_NAMES[role]
                # store both under canonical name and original basename (optional)
                filename = os.path.basename(urlparse(abs_url).path) or canon
                store_in_memory(filename, data, abs_url)
                if canon != filename:
                    store_in_memory(canon, data, abs_url)
                if canon not in found_roles:
                    found_roles.append(canon)
            else:
                # If it looks promising but role unknown, keep it under original name for later analysis
                filename = os.path.basename(urlparse(abs_url).path) or "unknown.js"
                store_in_memory(filename, data, abs_url)
        except Exception:
            continue
    return found_roles

def download_js_step():
    """
    Capture JS using Playwright if available; otherwise fallback to HTML+urllib.
    Instead of exact filenames, identify files by content and store them under stable canonical names.
    """
    print("=== Step 0 (heuristic): download_js_step (memory-only capture) ===")
    found = set()

    try:
        from playwright.sync_api import sync_playwright
    except Exception as e:
        try:
            data, _ = http_get(URL, timeout=15)
            html = decode_bytes_to_text(data) or ""
            fallback_found = fallback_download_candidates_to_mem(URL, html)
            for fn in fallback_found:
                found.add(fn)
            choose_best_js_roles()
            for canon in CANONICAL_NAMES.values():
                if canon in IN_MEM_FILES:
                    found.add(canon)
        except Exception:
            pass
        return

    # Playwright capture path
    try:
        with sync_playwright() as p:
            browser = p.chromium.launch(headless=True)
            context = browser.new_context()
            page = context.new_page()

            def on_response(response):
                try:
                    url = response.url
                    # quick filter by path/headers before reading body
                    try:
                        headers = {k.lower(): v for k, v in (response.headers.items())}
                    except Exception:
                        headers = {}
                    if not looks_like_js_response(headers, url):
                        return
                    # read body bytes
                    try:
                        body = response.body()
                    except Exception:
                        return
                    if not body:
                        return
                    text = decode_bytes_to_text(body)
                    role = identify_js_role(text)
                    filename = os.path.basename(urlparse(url).path) or None
                    if role:
                        canon = CANONICAL_NAMES[role]
                        # store canonical and original if possible
                        if filename:
                            store_in_memory(filename, body, url)
                        store_in_memory(canon, body, url)
                        print(f"[response] captured {url} -> role={role} stored as '{canon}'")
                        found.add(canon)
                    else:
                        # still keep it under its basename so fallback/parsing can inspect later
                        if filename:
                            store_in_memory(filename, body, url)
                            print(f"[response] captured candidate {url} stored as '{filename}' (role unknown)")
                except Exception as e:
                    print(f"[response handler error]: {e}")

            page.on("response", on_response)

            start = time.time()
            try:
                page.goto(URL, wait_until="networkidle", timeout=15000)
            except Exception:
                print("[step0 warning] initial navigation timed out/failed; continuing to listen.")

            # wait until we have at least the data file or timeout
            while (("data.js" not in found) and (time.time() - start) < TIMEOUT_SECONDS):
                time.sleep(0.2)

            # if still missing roles, try fallback parsing of page content
            if "data.js" not in found or "desc.js" not in found:
                try:
                    html = page.content()
                    fallback_found = fallback_download_candidates_to_mem(URL, html)
                    for fn in fallback_found:
                        found.add(fn)
                except Exception:
                    pass

            choose_best_js_roles()
            for canon in CANONICAL_NAMES.values():
                if canon in IN_MEM_FILES:
                    found.add(canon)

            browser.close()
    except Exception:
        debug_exc()
        # final fallback via HTTP
        try:
            data, _ = http_get(URL, timeout=15)
            html = decode_bytes_to_text(data) or ""
            fallback_found = fallback_download_candidates_to_mem(URL, html)
            for fn in fallback_found:
                found.add(fn)
            choose_best_js_roles()
            for canon in CANONICAL_NAMES.values():
                if canon in IN_MEM_FILES:
                    found.add(canon)
        except Exception:
            pass

# ---------------------------------------------------------------------------
# Pipeline: scraper -> parq maker -> corupt (adapted to read from RAM if available)
# ---------------------------------------------------------------------------


# --- helper: read text either from memory (IN_MEM_FILES) or disk ---
def get_text_or_none(name: str):
    bs = get_file_bytes_or_mem(name)
    if bs is None:
        return None
    return decode_bytes_to_text(bs)

def choose_first_available(candidates):
    for name in candidates:
        if get_file_bytes_or_mem(name) is not None:
            return name
    return candidates[0] if candidates else None

def write_csv_atomic(path, rows, fieldnames):
    if not rows:
        return
    out_dir = os.path.dirname(path) or "."
    tmp_path = None
    try:
        with tempfile.NamedTemporaryFile("w", delete=False, dir=out_dir, encoding="utf-8", newline="") as f:
            writer = csv.DictWriter(f, fieldnames=fieldnames, extrasaction="ignore")
            writer.writeheader()
            for row in rows:
                writer.writerow(row)
            tmp_path = f.name
        os.replace(tmp_path, path)
    finally:
        if tmp_path and os.path.exists(tmp_path):
            try:
                os.unlink(tmp_path)
            except Exception:
                pass

def to_int(value, default=0):
    try:
        if value is None or value == "":
            return default
        return int(float(value))
    except Exception:
        return default

# --- stage 1: scraper (writes CSV directly) ---
def stage_scraper():
    try:
        region_order = REGION_ORDER
        category_priority = CATEGORY_PRIORITY
        type_priority = TYPE_PRIORITY
        region_lookup = REGION_LOOKUP

        input_file = choose_first_available([CANONICAL_NAMES["data"], "YH6qh9rH.js"])
        desc_js_file = choose_first_available([CANONICAL_NAMES["desc"], "CKSuO70b.js"])
        csv_output = "maprunner_data.csv"

        def clean_text(s):
            if s is None:
                return ""
            if not isinstance(s, str):
                try:
                    s = s.decode("utf-8", errors="replace")
                except Exception:
                    s = str(s)
            candidates = [s]
            try:
                cand = bytes(s, "utf-8").decode("unicode_escape")
                candidates.append(cand)
            except Exception:
                pass
            try:
                candidates.append(s.encode("latin-1", errors="replace").decode("utf-8", errors="replace"))
            except Exception:
                pass
            try:
                candidates.append(s.encode("utf-8", errors="replace").decode("latin-1", errors="replace"))
            except Exception:
                pass
            try:
                cand = bytes(s, "utf-8").decode("unicode_escape").encode("latin-1", errors="replace").decode("utf-8", errors="replace")
                candidates.append(cand)
            except Exception:
                pass
            def score(x):
                if not x:
                    return 999999
                return x.count("�") + x.count("Ã") + x.count("Â") + x.count("\ufffd")
            best = min(candidates, key=score)
            return best.strip()

        def load_desc_js(fn):
            txt = get_text_or_none(fn)
            if not txt:
                return {}
            pattern = re.compile(r'(?:"([^"]+)"|([A-Z0-9_\-]+))\s*:\s*\{.*?s\s*:\s*(?:"((?:\\.|[^"\\])*)"|\'((?:\\.|[^\'\\])*)\')', re.DOTALL)
            m = pattern.findall(txt)
            result = {}
            for quoted_key, bare_key, val_dq, val_sq in m:
                key = quoted_key or bare_key
                raw_val = val_dq if val_dq else val_sq
                try:
                    val = bytes(raw_val, "utf-8").decode("unicode_escape")
                except Exception:
                    val = raw_val
                val = clean_text(val)
                result[key] = val
            return result

        def try_load_localization():
            candidates = ["localization.json", "strings.json", "strings_en.json", "locale_en.json"]
            for fn in candidates:
                txt = get_text_or_none(fn)
                if txt:
                    try:
                        data = json.loads(txt)
                        flat = {}
                        def flatten(obj):
                            if isinstance(obj, dict):
                                for k, v in obj.items():
                                    if isinstance(v, str):
                                        flat[k] = clean_text(v)
                                    else:
                                        flatten(v)
                        flatten(data)
                        if flat:
                            return flat
                        return {k: clean_text(v) for k, v in data.items() if isinstance(v, str)}
                    except Exception:
                        pass
            parsed = load_desc_js(desc_js_file)
            if parsed:
                return parsed
            return None

        localization = try_load_localization()

        def translate_token(tok):
            if not tok:
                return ""
            if not localization:
                return clean_text(tok)
            for candidate in (tok, tok.upper(), tok.lower()):
                if candidate in localization:
                    return clean_text(localization[candidate])
            stripped = tok.replace("UI_", "").replace("_NAME", "").replace("_DESC", "")
            if stripped in localization:
                return clean_text(localization[stripped])
            return clean_text(localization.get(tok, tok))

        def collect_types(obj):
            types = set()
            if isinstance(obj, dict):
                t = obj.get("type")
                if isinstance(t, str) and t.strip():
                    types.add(t.strip())
                for v in obj.values():
                    types.update(collect_types(v))
            elif isinstance(obj, list):
                for item in obj:
                    types.update(collect_types(item))
            return types

        def pretty_cargo_name(raw_name):
            if not raw_name:
                return "Unknown"
            s = raw_name.replace("UI_CARGO_", "").replace("_NAME", "").replace("Cargo", "")
            s = s.replace("_", " ").strip()
            return " ".join([p.capitalize() for p in s.split()])

        def collect_cargo(obj):
            cargos = []
            if isinstance(obj, dict):
                if "cargo" in obj and isinstance(obj["cargo"], list):
                    for c in obj["cargo"]:
                        if isinstance(c, dict):
                            count = str(c.get("count", "") or "")
                            name = c.get("name") or c.get("key") or "Unknown"
                            name = pretty_cargo_name(name)
                            cargos.append(f"{count}× {name}" if count and count != "-1" else name)
                for v in obj.values():
                    cargos.extend(collect_cargo(v))
            elif isinstance(obj, list):
                for v in obj:
                    cargos.extend(collect_cargo(v))
            return cargos

        def humanize_key(k):
            parts = k.split("_")
            return " ".join([p.capitalize() for p in parts if p])

        def extract_js_parse_string(txt):
            idx = txt.find("JSON.parse")
            if idx == -1:
                return None
            i = txt.find("(", idx)
            if i == -1:
                return None
            j = i + 1
            while j < len(txt) and txt[j].isspace():
                j += 1
            if j >= len(txt) or txt[j] not in ("'", '"'):
                return None
            quote = txt[j]
            start = j + 1
            k = start
            escaped = False
            while k < len(txt):
                ch = txt[k]
                if escaped:
                    escaped = False
                    k += 1
                    continue
                if ch == "\\":
                    escaped = True
                    k += 1
                    continue
                if ch == quote:
                    return txt[start:k]
                k += 1
            return None

        def unescape_js_string(s):
            if s is None:
                return ""
            try:
                normalized = s.replace(r'\/', '/')
                return codecs.decode(normalized, "unicode_escape")
            except Exception:
                return s

        def load_embedded_json(filename):
            bs = get_file_bytes_or_mem(filename)
            if bs is None:
                return None
            txt = decode_bytes_to_text(bs)
            embedded = extract_js_parse_string(txt)
            if embedded is None:
                return None
            json_text = unescape_js_string(embedded)
            try:
                return json.loads(json_text)
            except Exception:
                try:
                    repaired = clean_text(json_text)
                    return json.loads(repaired)
                except Exception:
                    try:
                        embedded_bytes = embedded.encode("utf-8", errors="replace")
                        candidate = embedded_bytes.decode("latin-1", errors="replace")
                        candidate = unescape_js_string(candidate)
                        return json.loads(candidate)
                    except Exception:
                        return None

        data = load_embedded_json(input_file)
        if not data:
            return

        allowed_categories = ALLOWED_CATEGORIES
        rows = []
        wanted_columns = [
            "key", "displayName", "category", "region", "region_name", "type",
            "cargo_needed", "experience", "money", "descriptionText", "Source"
        ]

        def walk(o):
            if isinstance(o, dict):
                if "category" in o and "key" in o:
                    key = o["key"].upper()
                    region = "_".join(key.split("_")[:2]) if "_" in key else ""
                    if o.get("category") in allowed_categories and region in region_lookup:
                        exp = money = None
                        if isinstance(o.get("rewards"), list):
                            for r in o["rewards"]:
                                if isinstance(r, dict):
                                    exp = r.get("experience", exp)
                                    money = r.get("money", money)
                        types = collect_types(o.get("objectives", []))
                        cargos = collect_cargo(o.get("objectives", []))
                        if "truckDelivery" in types:
                            type_str = "truckDelivery"
                        elif cargos:
                            type_str = "cargoDelivery"
                        else:
                            type_str = "exploration"
                        cargo_str = "; ".join(cargos) if cargos else None

                        name_field = o.get("name") or ""
                        if name_field and not name_field.startswith("UI_"):
                            display = translate_token(name_field) if localization else clean_text(name_field)
                        else:
                            if localization and name_field:
                                display = translate_token(name_field)
                            elif localization:
                                display = translate_token(key)
                            else:
                                display = clean_text(humanize_key(key))

                        raw_desc = o.get("subtitle") or o.get("description") or o.get("descriptionText") or ""
                        descriptionText = translate_token(raw_desc) if raw_desc else ""
                        descriptionText = clean_text(descriptionText)

                        source = (o.get("category") or "").lstrip("_")

                        rows.append({
                            "key": key,
                            "displayName": clean_text(display),
                            "category": o.get("category"),
                            "region": region,
                            "region_name": region_lookup.get(region, ""),
                            "type": type_str,
                            "cargo_needed": cargo_str,
                            "experience": exp,
                            "money": money,
                            "descriptionText": descriptionText,
                            "Source": source,
                        })
            for v in (o.values() if isinstance(o, dict) else (o if isinstance(o, list) else [])):
                walk(v)

        walk(data)
        if not rows:
            return

        seen = set()
        unique_rows = []
        for row in rows:
            k = row.get("key")
            if not k or k in seen:
                continue
            seen.add(k)
            unique_rows.append(row)

        region_map = {r: i for i, r in enumerate(region_order)}
        category_map = {cat: i for i, cat in enumerate(category_priority)}
        type_map = {t: i for i, t in enumerate(type_priority)}

        num_re = re.compile(r'(\d+)')
        def numeric_groups_from_key(k, max_groups=4):
            nums = num_re.findall(k)
            nums = [int(x) for x in nums]
            pad = [99999] * max_groups
            return (nums + pad)[:max_groups]

        def sort_key(r):
            nums = numeric_groups_from_key(r.get("key", ""))
            money_num = to_int(r.get("money"))
            exp_num = to_int(r.get("experience"))
            return (
                region_map.get(r.get("region"), 9999),
                nums[0], nums[1], nums[2], nums[3],
                category_map.get(r.get("category"), 9999),
                type_map.get(r.get("type"), 9999),
                -money_num,
                -exp_num,
                r.get("displayName") or "",
            )

        rows_sorted = sorted(unique_rows, key=sort_key)
        write_csv_atomic(csv_output, rows_sorted, wanted_columns)
    except Exception:
        debug_exc()
        return

# --- stage : convert to csv (keeps compatibility but skips if CSV exists) ---
def stage_parq_maker():
    return

# --- stage 3: corupt (writes CSV + QC) ---
def stage_corupt():
    try:
        # config
        input_file = "json.txt"  # this can be a disk file or present in IN_MEM_FILES
        desc_js_file = "desc.txt"
        csv_output = "maprunner_data_corupt.csv"
        qc_csv = "repairs_qc.csv"

        region_order = REGION_ORDER
        category_priority = CATEGORY_PRIORITY
        type_priority = TYPE_PRIORITY
        allowed_categories = ALLOWED_CATEGORIES
        region_lookup = REGION_LOOKUP

        # helpers (from corupt.py)
        def extract_js_parse_string(txt):
            idx = txt.find("JSON.parse")
            if idx == -1:
                return None
            i = txt.find("(", idx)
            if i == -1:
                return None
            j = i + 1
            while j < len(txt) and txt[j].isspace():
                j += 1
            if j >= len(txt) or txt[j] not in ("'", '"'):
                return None
            quote = txt[j]
            start = j + 1
            k = start
            escaped = False
            while k < len(txt):
                ch = txt[k]
                if escaped:
                    escaped = False
                    k += 1
                    continue
                if ch == "\\":
                    escaped = True
                    k += 1
                    continue
                if ch == quote:
                    return txt[start:k]
                k += 1
            return None

        def unescape_js_string(s):
            if s is None:
                return ""
            try:
                normalized = s.replace(r'\/', '/')
                return codecs.decode(normalized, "unicode_escape")
            except Exception:
                return s

        def clean_text(s):
            if s is None:
                return ""
            if not isinstance(s, str):
                try:
                    s = s.decode("utf-8", errors="replace")
                except Exception:
                    s = str(s)
            try:
                import ftfy
                fixed = ftfy.fix_text(s)
                if fixed and fixed != s:
                    return fixed.strip()
            except Exception:
                pass
            candidates = [("orig", s)]
            try:
                ue = codecs.decode(s, "unicode_escape")
                candidates.append(("unicode_escape", ue))
            except Exception:
                pass
            try:
                l1u8 = s.encode("latin-1", errors="surrogateescape").decode("utf-8", errors="replace")
                candidates.append(("latin1->utf8", l1u8))
            except Exception:
                pass
            try:
                u8l1 = s.encode("utf-8", errors="surrogateescape").decode("latin-1", errors="replace")
                candidates.append(("utf8->latin1", u8l1))
            except Exception:
                pass
            try:
                tmp = codecs.decode(s, "unicode_escape")
                tmp2 = tmp.encode("latin-1", errors="surrogateescape").decode("utf-8", errors="replace")
                candidates.append(("unicode_escape+latin1->utf8", tmp2))
            except Exception:
                pass
            def score_text(x):
                if not x:
                    return 999999
                bad = x.count("Ã") + x.count("Â") + x.count("â") + x.count("Ä") + x.count("Ð") + x.count("�")
                good = sum(1 for ch in x if ch.isalpha() or ch.isspace())
                return bad - (good * 0.001)
            best_tag, best_val = min(candidates, key=lambda p: score_text(p[1]))
            return (best_val or "").strip()

        def decode_bytes_with_chardet(bs):
            try:
                import chardet
                det = chardet.detect(bs)
                enc = det.get("encoding") or "utf-8"
                s = bs.decode(enc, errors="replace")
            except Exception:
                s = bs.decode("utf-8", errors="replace")
            return clean_text(s)

        def load_desc_js(fn):
            txt = get_text_or_none(fn)
            if not txt:
                return {}
            pattern = re.compile(
                r'(?:"([^"]+)"|([A-Z0-9_\-]+))\s*:\s*\{.*?s\s*:\s*(?:"((?:\\.|[^"\\])*)"|\'((?:\\.|[^\'\\])*)\')',
                re.DOTALL,
            )
            m = pattern.findall(txt)
            result = {}
            for quoted_key, bare_key, val_dq, val_sq in m:
                key = quoted_key or bare_key
                raw_val = val_dq if val_dq else val_sq
                val = unescape_js_string(raw_val)
                val = clean_text(val)
                result[key] = val
            return result

        def try_load_localization():
            candidates = ["localization.json", "strings.json", "strings_en.json", "locale_en.json"]
            for fn in candidates:
                txt = get_text_or_none(fn)
                if txt:
                    try:
                        data = json.loads(txt)
                        flat = {}
                        def flatten(obj):
                            if isinstance(obj, dict):
                                for k, v in obj.items():
                                    if isinstance(v, str):
                                        flat[k] = clean_text(v)
                                    else:
                                        flatten(v)
                        flatten(data)
                        if flat:
                            return flat
                        return {k: clean_text(v) for k, v in data.items() if isinstance(v, str)}
                    except Exception:
                        pass
            parsed = load_desc_js(desc_js_file)
            if parsed:
                return parsed
            return None

        localization = try_load_localization()

        def translate_token(tok):
            if not tok:
                return ""
            if not localization:
                return clean_text(tok)
            for candidate in (tok, tok.upper(), tok.lower()):
                if candidate in localization:
                    return clean_text(localization[candidate])
            stripped = tok.replace("UI_", "").replace("_NAME", "").replace("_DESC", "")
            if stripped in localization:
                return clean_text(localization[stripped])
            return clean_text(localization.get(tok, tok) if isinstance(tok, str) else tok)

        def collect_types(obj):
            types = set()
            if isinstance(obj, dict):
                t = obj.get("type")
                if isinstance(t, str) and t.strip():
                    types.add(t.strip())
                for v in obj.values():
                    types.update(collect_types(v))
            elif isinstance(obj, list):
                for item in obj:
                    types.update(collect_types(item))
            return types

        def pretty_cargo_name(raw_name):
            if not raw_name:
                return "Unknown"
            s = raw_name.replace("UI_CARGO_", "").replace("_NAME", "").replace("Cargo", "")
            s = s.replace("_", " ").strip()
            return " ".join([p.capitalize() for p in s.split()])

        def collect_cargo(obj):
            cargos = []
            if isinstance(obj, dict):
                if "cargo" in obj and isinstance(obj["cargo"], list):
                    for c in obj["cargo"]:
                        if isinstance(c, dict):
                            count = str(c.get("count", "") or "")
                            name = c.get("name") or c.get("key") or "Unknown"
                            name = pretty_cargo_name(name)
                            cargos.append(f"{count}× {name}" if count and count != "-1" else name)
                for v in obj.values():
                    cargos.extend(collect_cargo(v))
            elif isinstance(obj, list):
                for v in obj:
                    cargos.extend(collect_cargo(v))
            return cargos

        def humanize_key(k):
            parts = k.split("_")
            return " ".join([p.capitalize() for p in parts if p])

        def load_embedded_json(filename):
            bs = get_file_bytes_or_mem(filename)
            if bs is None:
                return None
            txt = decode_bytes_to_text(bs)
            embedded = extract_js_parse_string(txt)
            if embedded is None:
                return None
            json_text = unescape_js_string(embedded)
            # attempt multiple loads with repair fallbacks
            try:
                return json.loads(json_text)
            except Exception:
                try:
                    repaired = clean_text(json_text)
                    return json.loads(repaired)
                except Exception:
                    pass
                try:
                    embedded_bytes = embedded.encode("utf-8", errors="replace")
                    candidate = embedded_bytes.decode("latin-1", errors="replace")
                    candidate = unescape_js_string(candidate)
                    return json.loads(candidate)
                except Exception:
                    return None

        def build_rows_from_data(data):
            rows = []
            def walk(o):
                if isinstance(o, dict):
                    if "category" in o and "key" in o:
                        key = o["key"].upper()
                        region = "_".join(key.split("_")[:2]) if "_" in key else ""
                        if o.get("category") in allowed_categories and region in region_lookup:
                            exp = money = None
                            if isinstance(o.get("rewards"), list):
                                for r in o["rewards"]:
                                    if isinstance(r, dict):
                                        exp = r.get("experience", exp)
                                        money = r.get("money", money)
                            types = collect_types(o.get("objectives", []))
                            cargos = collect_cargo(o.get("objectives", []))
                            if "truckDelivery" in types:
                                type_str = "truckDelivery"
                            elif cargos:
                                type_str = "cargoDelivery"
                            else:
                                type_str = "exploration"
                            cargo_str = "; ".join(cargos) if cargos else None

                            name_field = o.get("name") or ""
                            if name_field and not name_field.startswith("UI_"):
                                display_raw = translate_token(name_field) if localization else name_field
                            else:
                                if localization and name_field:
                                    display_raw = translate_token(name_field)
                                elif localization:
                                    display_raw = translate_token(key)
                                else:
                                    display_raw = humanize_key(key)

                            raw_display = display_raw if isinstance(display_raw, str) else str(display_raw)
                            cleaned_display = clean_text(raw_display)

                            raw_desc = o.get("subtitle") or o.get("description") or o.get("descriptionText") or ""
                            description_raw = translate_token(raw_desc) if raw_desc else ""
                            description_clean = clean_text(description_raw)

                            source = (o.get("category") or "").lstrip("_")

                            rows.append({
                                "key": key,
                                "raw_displayName": raw_display,
                                "displayName": cleaned_display,
                                "category": o.get("category"),
                                "region": region,
                                "region_name": region_lookup.get(region, ""),
                                "type": type_str,
                                "cargo_needed": cargo_str,
                                "experience": exp,
                                "money": money,
                                "raw_descriptionText": description_raw,
                                "descriptionText": description_clean,
                                "Source": source,
                            })
                if isinstance(o, dict):
                    for v in o.values():
                        walk(v)
                elif isinstance(o, list):
                    for v in o:
                        walk(v)
            walk(data)
            return rows

        data = load_embedded_json(input_file)
        if not data:
            return

        rows = build_rows_from_data(data)
        if not rows:
            return

        wanted_columns = [
            "key", "displayName", "raw_displayName", "category", "region", "region_name", "type",
            "cargo_needed", "experience", "money", "descriptionText", "raw_descriptionText", "Source"
        ]

        seen = set()
        unique_rows = []
        for row in rows:
            k = row.get("key")
            if not k or k in seen:
                continue
            seen.add(k)
            unique_rows.append(row)

        region_map = {r: i for i, r in enumerate(region_order)}
        category_map = {cat: i for i, cat in enumerate(category_priority)}
        type_map = {t: i for i, t in enumerate(type_priority)}

        num_re = re.compile(r'(\d+)')
        def numeric_groups_from_key(k, max_groups=4):
            nums = num_re.findall(k)
            nums = [int(x) for x in nums]
            pad = [99999] * max_groups
            return (nums + pad)[:max_groups]

        def sort_key(r):
            nums = numeric_groups_from_key(r.get("key", ""))
            money_num = to_int(r.get("money"))
            exp_num = to_int(r.get("experience"))
            return (
                region_map.get(r.get("region"), 9999),
                nums[0], nums[1], nums[2], nums[3],
                category_map.get(r.get("category"), 9999),
                type_map.get(r.get("type"), 9999),
                -money_num,
                -exp_num,
                r.get("displayName") or "",
            )

        rows_sorted = sorted(unique_rows, key=sort_key)
        write_csv_atomic(csv_output, rows_sorted, wanted_columns)

        qc_rows = []
        for r in rows_sorted:
            raw = str(r.get("raw_displayName") or "")
            cleaned = str(r.get("displayName") or "")
            if raw != cleaned:
                qc_rows.append({
                    "key": r.get("key", ""),
                    "raw_displayName": raw,
                    "cleaned_displayName": cleaned,
                })
        if qc_rows:
            write_csv_atomic(qc_csv, qc_rows, ["key", "raw_displayName", "cleaned_displayName"])
    except Exception:
        debug_exc()
        return

# --- Runner ---
def main():
    ensure_working_dir()
    try:
        # This will capture JS into IN_MEM_FILES (memory) instead of saving files to disk.
        download_js_step()
    except Exception:
        debug_exc()
    stage_scraper()      # Stage 1 (reads from memory or disk) -> maprunner_data.csv
    stage_parq_maker()   # Stage 2 (optional excel->csv; skipped if CSV exists)
    stage_corupt()       # Stage 3 (reads from memory or disk) -> maprunner_data_corupt.csv + repairs_qc.csv

if __name__ == "__main__":
    try:
        main()
    except Exception:
        debug_exc()
        pass
