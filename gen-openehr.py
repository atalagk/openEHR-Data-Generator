#!/usr/bin/env python3
"""
openEHR Synthetic Data Generator
"""

import os
import json
import copy
import datetime as dt
import xml.etree.ElementTree as ET
import random
import asyncio
import urllib.parse
import zipfile
import time
import aiohttp
from typing import Optional


# ── directories ────────────────────────────────────────────────────────────────

BASE           = "source_models"
OPT_DIR        = os.path.join(BASE, "opts")
WT_DIR         = os.path.join(BASE, "opt_webtemplates")
USER_COMPS_DIR = os.path.join(BASE, "user_compositions")
FLAT_DIR       = os.path.join(BASE, "flat_composition_skeletons")
DIST_DIR       = os.path.join("dist", "compositions")
CONFIG_FILE    = "ehrbase_config.json"

_AQL_PAGE: int = 10  # compositions per paginated AQL query

for d in (OPT_DIR, WT_DIR, USER_COMPS_DIR, FLAT_DIR, DIST_DIR):
    os.makedirs(d, exist_ok=True)


# ── webtemplate index ──────────────────────────────────────────────────────────

def build_wt_index(wt_root: dict) -> dict[str, dict]:
    """Index all WT nodes by id-path (joining 'id' fields root-to-leaf)."""
    index: dict[str, dict] = {}

    def walk(node: dict, parts: list[str]) -> None:
        index["/".join(parts)] = node
        for child in node.get("children") or []:
            walk(child, parts + [child["id"]])

    walk(wt_root, [wt_root["id"]])
    return index


def load_wt_index(template_id: str) -> Optional[dict[str, dict]]:
    path = os.path.join(WT_DIR, f"{template_id}.json")
    if not os.path.exists(path):
        return None
    with open(path) as f:
        wt = json.load(f)
    root = wt.get("tree") or wt
    return build_wt_index(root)


def wt_path_of(flat_key: str) -> str:
    """
    Convert a flat composition key to its WT id-path.
      'tpl/archetype/event:2/element|magnitude' -> 'tpl/archetype/event/element'
    """
    base = flat_key.split("|")[0]
    parts = [seg.split(":")[0] for seg in base.split("/")]
    return "/".join(parts)


# ── mutation ───────────────────────────────────────────────────────────────────

_PROTECTED_SEGMENTS = frozenset({
    "category", "context", "language", "territory", "composer",
    "_work_flow_id", "_guideline_id", "_instruction_details", "ism_transition",
})


def _is_protected(base: str) -> bool:
    """True if any path segment (index-stripped) is in the protected set."""
    return bool(
        _PROTECTED_SEGMENTS & {seg.split(":")[0] for seg in base.split("/")}
    )


def _jitter_datetime(value: str, rm_type: str) -> str:
    """Jitter a date/time string within ±15% of one day (86 400 s)."""
    delta_s = random.uniform(-0.15 * 86400, 0.15 * 86400)

    if rm_type == "DV_DATE_TIME":
        for fmt in ("%Y-%m-%dT%H:%M:%S", "%Y-%m-%dT%H:%M:%SZ"):
            try:
                parsed = dt.datetime.strptime(value, fmt)
                return (parsed + dt.timedelta(seconds=delta_s)).strftime("%Y-%m-%dT%H:%M:%S")
            except ValueError:
                pass

    elif rm_type == "DV_DATE":
        try:
            parsed = dt.date.fromisoformat(value)
            return (parsed + dt.timedelta(days=int(delta_s / 86400))).isoformat()
        except ValueError:
            pass

    elif rm_type == "DV_TIME":
        try:
            h, m, s = map(int, value.split(":"))
            total = max(0, min(86399, int(h * 3600 + m * 60 + s + delta_s)))
            return f"{total // 3600:02d}:{(total % 3600) // 60:02d}:{total % 60:02d}"
        except ValueError:
            pass

    return value  # unchanged if parsing failed


def mutate_flat(flat: dict, wt_index: dict[str, dict]) -> dict:
    """
    Return a mutated copy of a flat composition using WT constraints.

    Rules:
      b) Skip paths containing: category, context, language, territory, composer,
         _work_flow_id, _guideline_id
      c) DV_CODED_TEXT with terminology "openehr" → untouched
      d) DV_QUANTITY → jitter |magnitude within WT min/max range, leave |unit alone
      e) DV_DURATION → untouched
      g) DV_CODED_TEXT with terminology "local" and WT list → pick randomly from list
      h) DV_TEXT → prepend random digit 1–5 to existing string
      i) DV_DATE_TIME / DV_DATE / DV_TIME → jitter within ±15% of one day
    """
    out = copy.deepcopy(flat)

    # Group keys by base path (strip |suffix so coded-text triplets are together)
    groups: dict[str, list[str]] = {}
    for key in flat:
        groups.setdefault(key.split("|")[0], []).append(key)

    for base, keys in groups.items():
        if _is_protected(base):
            continue

        wt_node = wt_index.get(wt_path_of(base))
        if wt_node is None:
            continue

        rm_type = wt_node.get("rmType", "")

        # ── DV_QUANTITY ────────────────────────────────────────────────────────
        if rm_type == "DV_QUANTITY":
            inputs = wt_node.get("inputs") or []
            mag_inp = next(
                (i for i in inputs if i.get("suffix") in (None, "", "magnitude")), None
            )
            rng = (mag_inp.get("validation") or {}).get("range") if mag_inp else None
            for key in keys:
                if not key.endswith("|magnitude"):
                    continue
                val = out[key]
                if not isinstance(val, (int, float)):
                    continue
                jittered = float(val) * random.uniform(0.9, 1.1)
                if rng:
                    lo = float(rng.get("min", jittered))
                    hi = float(rng.get("max", jittered))
                    if hi < lo:
                        hi = lo
                    jittered = max(lo, min(hi, jittered))
                out[key] = round(jittered, 2) if isinstance(val, float) else round(jittered)

        # ── DV_CODED_TEXT ──────────────────────────────────────────────────────
        elif rm_type == "DV_CODED_TEXT":
            term_key = next((k for k in keys if k.endswith("|terminology")), None)
            terminology = flat.get(term_key, "") if term_key else ""
            if terminology == "openehr":
                continue
            if terminology == "local":
                inputs = wt_node.get("inputs") or []
                code_inp = next(
                    (i for i in inputs if i.get("suffix") == "code" and i.get("list")),
                    None,
                )
                if code_inp:
                    chosen = random.choice(code_inp["list"])
                    for key in keys:
                        if key.endswith("|code"):
                            out[key] = chosen["value"]
                        elif key.endswith("|value"):
                            out[key] = chosen.get("label", chosen["value"])

        # ── DV_TEXT ────────────────────────────────────────────────────────────
        elif rm_type == "DV_TEXT":
            name_raw = wt_node.get("name") or ""
            base = name_raw.get("value", "") if isinstance(name_raw, dict) else name_raw
            for key in keys:
                if "|" not in key and isinstance(out[key], str):
                    words = (base or out[key]).split()
                    if len(words) > 1:
                        random.shuffle(words)
                        out[key] = " ".join(words)
                    else:
                        out[key] = (base or out[key]) + " " + hex(random.randint(0, 0xFFFF))[2:]

        # ── DV_DATE_TIME / DV_DATE / DV_TIME ──────────────────────────────────
        elif rm_type in ("DV_DATE_TIME", "DV_DATE", "DV_TIME"):
            for key in keys:
                if "|" not in key and isinstance(out[key], str):
                    out[key] = _jitter_datetime(out[key], rm_type)

        # DV_DURATION and everything else → skip

    return out


# ── flat composition helpers ───────────────────────────────────────────────────


def strip_flat_uid(flat: dict) -> dict:
    return {k: v for k, v in flat.items() if k.rsplit("/", 1)[-1] != "_uid"}




def strip_canonical_uid(comp: dict) -> dict:
    comp.pop("uid", None)
    comp.pop("_uid", None)
    return comp


# ── EHRbase REST ───────────────────────────────────────────────────────────────

async def create_ehr(session: aiohttp.ClientSession, url: str) -> str:
    headers = {
        "Accept": "application/json",
        "Prefer": "return=minimal",
        "Content-Type": "application/json",
    }
    async with session.post(f"{url}/ehr", headers=headers) as r:
        if r.status not in (200, 201, 204):
            raise RuntimeError(f"Create EHR failed {r.status}: {await r.text()}")
        loc = r.headers.get("Location", "")
        return loc.rstrip("/").rsplit("/", 1)[-1]


async def create_ehr_pool(
    session: aiohttp.ClientSession, url: str, size: int
) -> list[str]:
    # Create one EHR first to initialize the server-side user record,
    # avoiding a race condition when concurrent requests all try to create it.
    first = await create_ehr(session, url)
    sem = asyncio.Semaphore(10)
    async def one() -> str:
        async with sem:
            return await create_ehr(session, url)
    rest = await asyncio.gather(*[one() for _ in range(size - 1)])
    pool = [first, *rest]
    print(f"[*] Created {size} EHR(s)\n")
    return pool


async def fetch_webtemplate(
    session: aiohttp.ClientSession, url: str, template_id: str
) -> dict:
    async with session.get(
        f"{url}/definition/template/adl1.4/{template_id}",
        headers={"Accept": "application/openehr.wt+json"},
    ) as r:
        if r.status != 200:
            raise RuntimeError(f"WT fetch failed {r.status}: {await r.text()[:200]}")
        return await r.json(content_type=None)



async def fetch_example_flat(
    session: aiohttp.ClientSession, url: str, template_id: str, retries: int = 3, delay: float = 2.0
) -> dict:
    for attempt in range(1, retries + 1):
        async with session.get(
            f"{url}/definition/template/adl1.4/{template_id}/example",
            params={"format": "FLAT"},
            headers={"Accept": "application/json"},
        ) as r:
            if r.status == 200:
                return await r.json(content_type=None)
            err = (await r.text())[:200]
            if r.status == 500 and attempt < retries:
                await asyncio.sleep(delay)
                continue
            raise RuntimeError(f"Flat example fetch failed {r.status}: {err}")
    raise RuntimeError("fetch_example_flat: unreachable")


async def post_canonical(
    session: aiohttp.ClientSession, url: str, ehr_id: str, comp: dict
) -> str:
    headers = {
        "Content-Type": "application/json",
        "Accept": "application/json",
        "Prefer": "return=representation",
    }
    async with session.post(
        f"{url}/ehr/{ehr_id}/composition", json=comp, headers=headers
    ) as r:
        if r.status not in (200, 201, 204):
            raise RuntimeError(f"POST canonical failed {r.status}: {await r.text()[:300]}")
        if r.status == 204:
            loc = r.headers.get("Location", "")
            return loc.rstrip("/").rsplit("/", 1)[-1]
        body = await r.json(content_type=None)
        uid = (body.get("uid") or {}).get("value") or body.get("_uid")
        if not uid:
            loc = r.headers.get("Location", "")
            uid = loc.rstrip("/").rsplit("/", 1)[-1]
        return uid



async def post_flat(
    session: aiohttp.ClientSession,
    url: str,
    ehr_id: str,
    template_id: str,
    flat: dict,
    prefer_repr: bool = False,
) -> tuple[int, str | dict, str]:
    """Returns (status, body, uid). uid extracted from Location header."""
    endpoint = f"{url}/ehr/{ehr_id}/composition?format=FLAT&templateId={template_id}"
    headers = {
        "Content-Type": "application/json",
        "Accept": "application/json",
        "Prefer": "return=representation" if prefer_repr else "return=minimal",
    }
    async with session.post(endpoint, json=flat, headers=headers) as r:
        uid = r.headers.get("Location", "").rstrip("/").rsplit("/", 1)[-1]
        if prefer_repr and r.status in (200, 201):
            return r.status, await r.json(content_type=None), uid
        return r.status, await r.text(), uid


# ── OPT helpers ────────────────────────────────────────────────────────────────

_OPT_NS = "http://schemas.openehr.org/v1"

def extract_opt_template_id(xml_text: str) -> Optional[str]:
    """Extract <template_id><value> from OPT XML (with or without namespace)."""
    try:
        root = ET.fromstring(xml_text)
        for tag in (f"{{{_OPT_NS}}}template_id", "template_id"):
            elem = root.find(f"{tag}/{tag.replace('template_id', 'value')}")
            # handle mixed ns: try both ns and bare value child
            if elem is None:
                parent = root.find(tag)
                if parent is not None:
                    elem = parent.find(f"{{{_OPT_NS}}}value") or parent.find("value")
            if elem is not None and elem.text:
                return elem.text.strip()
    except Exception:
        pass
    return None


# ── mode 3: upload OPTs ────────────────────────────────────────────────────────

async def upload_opts(session: aiohttp.ClientSession, url: str) -> None:
    for f in os.listdir(WT_DIR):
        os.remove(os.path.join(WT_DIR, f))
    opt_files = [f for f in os.listdir(OPT_DIR) if f.endswith(".opt")]
    if not opt_files:
        print(f"[!] No .opt files found in {OPT_DIR}")
        return

    print(f"[*] Uploading {len(opt_files)} OPT(s) ...")
    ok = failed = skipped = 0
    uploaded_ids: list[str] = []

    for fname in sorted(opt_files):
        with open(os.path.join(OPT_DIR, fname), "r", encoding="utf-8") as f:
            xml = f.read()
        try:
            async with session.post(
                f"{url}/definition/template/adl1.4",
                headers={"Content-Type": "application/xml", "Accept": "application/xml"},
                data=xml.encode("utf-8"),
            ) as r:
                if r.status in (200, 201):
                    ok += 1
                    loc = r.headers.get("Location", "")
                    tid = urllib.parse.unquote(loc.rstrip("/").rsplit("/", 1)[-1]) if loc else fname[:-4]
                    uploaded_ids.append(tid)
                    print(f"  [+] {fname} -> {tid}")
                elif r.status == 409:
                    skipped += 1
                    tid = extract_opt_template_id(xml)
                    if tid:
                        uploaded_ids.append(tid)
                        print(f"  [~] Already exists: {fname} -> fetching WT for {tid!r}")
                    else:
                        print(f"  [~] Already exists: {fname} (could not extract template_id from OPT)")
                else:
                    failed += 1
                    err_body = await r.text()
                    print(f"  [!] {fname} ({r.status}): {err_body[:200]}")
                    print(f"      Refer to openEHR specs to fix this template.")
                    opt_path = os.path.join(OPT_DIR, fname)
                    answer = input(f"  Rename to {fname}_invalid to skip in future runs? [y/n]: ").strip().lower()
                    if answer == "y":
                        os.rename(opt_path, opt_path + "_invalid")
                        print(f"  Renamed to {fname}_invalid.")
        except Exception as e:
            failed += 1
            print(f"  [!] {fname}: {e}")

    print(f"\n[*] OPTs uploaded (ok:{ok} | skipped:{skipped} | fail:{failed} | total:{len(opt_files)})")

    if not uploaded_ids:
        return

    print(f"\n[*] Fetching webtemplates for {len(uploaded_ids)} uploaded template(s) -> {WT_DIR}")
    wt_ok = wt_fail = 0
    sem = asyncio.Semaphore(10)

    async def fetch_one(tid: str) -> None:
        nonlocal wt_ok, wt_fail
        async with sem:
            try:
                wt = await fetch_webtemplate(session, url, tid)
                with open(os.path.join(WT_DIR, f"{tid}.json"), "w") as f:
                    json.dump(wt, f, indent=2)
                wt_ok += 1
            except Exception as e:
                wt_fail += 1
                print(f"  [!] WT fetch failed for {tid!r}: {e}")

    await asyncio.gather(*[fetch_one(tid) for tid in uploaded_ids])
    print(f"[*] Webtemplates saved (success:{wt_ok} | fail:{wt_fail} | total:{len(uploaded_ids)})")


# ── mode 3 step 2: fetch flat skeletons ───────────────────────────────────────

async def run_setup(session: aiohttp.ClientSession, url: str) -> None:
    # ── clear stale flat skeletons ────────────────────────────────────────────
    for f in os.listdir(FLAT_DIR):
        os.remove(os.path.join(FLAT_DIR, f))

    # ── re-fetch webtemplates ─────────────────────────────────────────────────
    wt_files = sorted(f for f in os.listdir(WT_DIR) if f.endswith(".json"))
    if not wt_files:
        print("[!] No webtemplates found after upload — nothing to set up.")
        return

    print(f"[*] {len(wt_files)} webtemplate(s) found. Fetching flat examples -> {FLAT_DIR}")
    ok = 0
    failures: list[tuple[str, str, str]] = []  # (fname, wt_path, tid)
    sem = asyncio.Semaphore(10)

    async def one(fname: str) -> None:
        nonlocal ok
        async with sem:
            tid: str = fname[:-5]  # fallback: strip .json
            try:
                with open(os.path.join(WT_DIR, fname)) as f:
                    wt = json.load(f)
                tid = wt.get("templateId") or tid
                if not wt.get("templateId"):
                    raise ValueError(f"No templateId in {fname}")
                flat = await fetch_example_flat(session, url, tid)
                envelope = {"template_id": tid, "flat_comp": flat}
                with open(os.path.join(FLAT_DIR, f"{tid}.json"), "w") as f:
                    json.dump(envelope, f, indent=2)
                ok += 1
            except Exception as e:
                failures.append((fname, os.path.join(WT_DIR, fname), tid))
                print(f"  [!] {fname}: {e}")

    await asyncio.gather(*[one(f) for f in wt_files])
    print(f"[*] Flat examples fetched (success:{ok} | fail:{len(failures)} | total:{len(wt_files)})")

    for fname, wt_path, tid in failures:
        opt_path = os.path.join(OPT_DIR, f"{tid}.opt")
        answer = input(f"  Mark {tid}.opt as invalid and delete its webtemplate? [y/n]: ").strip().lower()
        if answer == "y":
            if os.path.exists(opt_path):
                os.rename(opt_path, opt_path + "_invalid")
                print(f"  Renamed {tid}.opt -> {tid}.opt_invalid.")
            else:
                print(f"  [!] OPT not found at {opt_path} — skipping rename.")
            os.remove(wt_path)
            print(f"  Deleted webtemplate {fname}.")


# ── mode 1: duplicate canonical compositions ───────────────────────────────────

async def run_duplicate(
    dest: str,
    count: int,
    session: Optional[aiohttp.ClientSession] = None,
    url: str = "",
    ehr_pool: list[str] = [],
    packaging: str = "a",
) -> None:
    comp_files = sorted(f for f in os.listdir(USER_COMPS_DIR) if f.endswith(".json"))
    if not comp_files:
        print(f"[!] No compositions in {USER_COMPS_DIR}.")
        return

    send_cdr   = dest == "b" and session is not None
    save_local = dest == "a"
    if save_local:
        for f in os.listdir(DIST_DIR):
            os.remove(os.path.join(DIST_DIR, f))
    ok = failed = 0
    first_errors: dict[str, str] = {}
    counters: dict[str, int] = {}
    sem = asyncio.Semaphore(10)

    zf = (
        zipfile.ZipFile(os.path.join(DIST_DIR, "compositions.zip"), "w", zipfile.ZIP_DEFLATED)
        if save_local and packaging == "b" else None
    )

    total     = count * len(comp_files)
    tick_size = max(1, total // 10)
    last_tick = 0

    def _tick() -> None:
        nonlocal last_tick
        current = min(10, (ok + failed) // tick_size)
        if current > last_tick:
            last_tick = current
            bar = "X" * last_tick + " " * (10 - last_tick)
            print(f"\r[{bar}]", end="", flush=True)

    async def one(fname: str) -> None:
        nonlocal ok, failed
        async with sem:
            try:
                with open(os.path.join(USER_COMPS_DIR, fname)) as f:
                    comp = json.load(f)
                for _ in range(count):
                    clean = strip_canonical_uid(copy.deepcopy(comp))
                    if send_cdr:
                        await post_canonical(session, url, random.choice(ehr_pool), clean)
                    if save_local:
                        n = counters.get(fname, 0)
                        counters[fname] = n + 1
                        out_name = f"{fname[:-5]}_{n:06d}.json"
                        data = json.dumps(clean, indent=2)
                        if zf:
                            zf.writestr(out_name, data)
                        else:
                            with open(os.path.join(DIST_DIR, out_name), "w") as f2:
                                f2.write(data)
                    ok += 1
                    _tick()
            except Exception as e:
                failed += 1
                if fname not in first_errors:
                    first_errors[fname] = str(e)
                _tick()

    if total > 0:
        print("[          ]", end="", flush=True)
    try:
        await asyncio.gather(*[one(f) for f in comp_files])
        print(f"\r[XXXXXXXXXX] {ok + failed:,} done")
    finally:
        if zf:
            zf.close()
    print(f"[*] OK: {ok} | Failed: {failed}")
    for fname, err in first_errors.items():
        print(f"  [!] {fname}: {err}")


# ── mode 2: jitter flat compositions ──────────────────────────────────────────

async def run_generate(
    dest: str,
    count: int,
    session: Optional[aiohttp.ClientSession] = None,
    url: str = "",
    ehr_pool: list[str] = [],
    fmt: str = "a",
    packaging: str = "a",
) -> None:
    flat_files = sorted(f for f in os.listdir(FLAT_DIR) if f.endswith(".json"))
    if not flat_files:
        print("[!] No example skeleton compositions are found; run Setup (mode 3) first.")
        return

    canonical  = fmt == "b"
    send_cdr   = (dest == "b" or canonical) and session is not None
    save_local = dest == "a"

    if save_local:
        for f in os.listdir(DIST_DIR):
            os.remove(os.path.join(DIST_DIR, f))
    ok = failed = 0
    first_errors: dict[str, str] = {}
    counters: dict[str, int] = {}
    uid_records: list[tuple[str, str, str]] = []  # (out_name, ehr_id, uid)
    sem = asyncio.Semaphore(10)

    zf = (
        zipfile.ZipFile(os.path.join(DIST_DIR, "compositions.zip"), "w", zipfile.ZIP_DEFLATED)
        if save_local and packaging == "b" else None
    )

    total     = count * len(flat_files)
    tick_size = max(1, total // 10)
    last_tick = 0

    def _tick() -> None:
        nonlocal last_tick
        current = min(10, (ok + failed) // tick_size)
        if current > last_tick:
            last_tick = current
            bar = "X" * last_tick + " " * (10 - last_tick)
            print(f"\r[{bar}]", end="", flush=True)

    async def one(fname: str) -> None:
        nonlocal ok, failed
        async with sem:
            try:
                with open(os.path.join(FLAT_DIR, fname)) as f:
                    envelope = json.load(f)

                template_id = envelope.get("template_id")
                skeleton = envelope.get("flat_comp")
                if not template_id or not skeleton:
                    raise ValueError("Missing template_id or flat_comp in envelope")

                wt_index = load_wt_index(template_id)
                if wt_index is None:
                    raise ValueError(f"No webtemplate found for {template_id}")

                stripped = strip_flat_uid(skeleton)
                for _ in range(count):
                    flat = copy.deepcopy(stripped)
                    flat = mutate_flat(flat, wt_index)
                    ehr_id = random.choice(ehr_pool) if ehr_pool else ""
                    n = counters.get(fname, 0)
                    counters[fname] = n + 1
                    out_name = f"{fname[:-5]}_{n:06d}.json"
                    if send_cdr:
                        status, response, uid = await post_flat(
                            session, url, ehr_id, template_id, flat
                        )
                        if status not in (200, 201, 204):
                            raise RuntimeError(f"{status} {str(response)[:600]}")
                        if canonical and uid:
                            uid_records.append((out_name, ehr_id, uid))
                    if save_local and not canonical:
                        data = json.dumps(flat, indent=2)
                        if zf:
                            zf.writestr(out_name, data)
                        else:
                            with open(os.path.join(DIST_DIR, out_name), "w") as f2:
                                f2.write(data)
                    ok += 1
                    _tick()
            except Exception as e:
                failed += 1
                if fname not in first_errors:
                    first_errors[fname] = str(e)
                _tick()

    if total > 0:
        print(f"[*] Posting {total:,} compositions ...")
        print("[          ]", end="", flush=True)
    try:
        t0 = time.monotonic()
        await asyncio.gather(*[one(f) for f in flat_files])
        print(f"\r[XXXXXXXXXX] {ok + failed:,} done")
        print(f"[*] OK: {ok} | Failed: {failed}")
        for fname, err in first_errors.items():
            print(f"  [!] {fname}: {err}")
        _elapsed = int(time.monotonic() - t0)
        _mins, _secs = divmod(_elapsed, 60)
        print(f"[*] Time: {_mins}m {_secs}s" if _mins else f"[*] Time: {_secs}s")
        if canonical and uid_records:
            await fetch_canonical_aql(session, url, uid_records, zf)
    finally:
        if zf:
            zf.close()


# ── canonical fetch via AQL ────────────────────────────────────────────────────

async def fetch_canonical_aql(
    session: aiohttp.ClientSession,
    url: str,
    uid_records: list[tuple[str, str, str]],
    zf: Optional[zipfile.ZipFile],
) -> None:
    """Fetch canonical compositions per EHR via AQL and save to disk."""
    if not uid_records:
        print("[!] No UID records — nothing to fetch via AQL.")
        return

    # Group by EHR. Track exact count separately — don't derive it from bucket
    # size, since the bare-UUID fallback key may or may not be added (only when
    # the UID contains '::'), making len(bucket) unreliable as a count.
    ehr_buckets: dict[str, dict[str, str]] = {}  # ehr_id -> {uid_key: out_name}
    ehr_counts:  dict[str, int]            = {}  # ehr_id -> n compositions posted
    for out_name, ehr_id, uid in uid_records:
        bucket = ehr_buckets.setdefault(ehr_id, {})
        bucket[uid] = out_name
        bare = uid.split("::")[0]
        if bare != uid:
            bucket.setdefault(bare, out_name)  # fallback only when actually different
        ehr_counts[ehr_id] = ehr_counts.get(ehr_id, 0) + 1

    total = len(uid_records)
    ok = failed = resolved = 0
    tick_size = max(1, total // 10)
    last_tick = 0
    sem = asyncio.Semaphore(10)

    def _tick(n: int = 1) -> None:
        nonlocal last_tick, resolved
        resolved += n
        current = min(10, resolved // tick_size)
        if current > last_tick:
            last_tick = current
            bar = "X" * last_tick + " " * (10 - last_tick)
            print(f"\r[{bar}]", end="", flush=True)

    async def one_ehr(ehr_id: str, bucket: dict[str, str], expected: int) -> None:
        nonlocal ok, failed
        async with sem:
            ehr_ok = 0
            offset = 0
            try:
                while True:
                    query = (
                        f"SELECT c FROM EHR e[ehr_id/value='{ehr_id}'] "
                        f"CONTAINS COMPOSITION c LIMIT {_AQL_PAGE} OFFSET {offset}"
                    )
                    async with session.post(
                        f"{url}/query/aql",
                        json={"q": query},
                        headers={"Content-Type": "application/json", "Accept": "application/json"},
                    ) as r:
                        if r.status != 200:
                            raise RuntimeError(f"AQL {r.status}: {await r.text()[:200]}")
                        rows = (await r.json(content_type=None)).get("rows", [])
                        for row in rows:
                            comp = row[0] if row else None
                            if not isinstance(comp, dict):
                                continue
                            uid_val = (comp.get("uid") or {}).get("value") or comp.get("_uid", "")
                            out_name = bucket.get(uid_val)
                            if out_name is None and "::" in uid_val:
                                out_name = bucket.get(uid_val.split("::")[0])
                            if out_name is None:
                                continue  # composition belongs to this EHR but not this run
                            data = json.dumps(comp, indent=2)
                            if zf:
                                zf.writestr(out_name, data)
                            else:
                                with open(os.path.join(DIST_DIR, out_name), "w") as f:
                                    f.write(data)
                            ehr_ok += 1
                            ok += 1
                            _tick()
                        if len(rows) < _AQL_PAGE or ehr_ok >= expected:
                            break
                        offset += _AQL_PAGE
                not_found = max(0, expected - ehr_ok)
                if not_found:
                    failed += not_found
                    _tick(not_found)
            except Exception as e:
                shortfall = max(0, expected - ehr_ok)
                failed += shortfall
                _tick(shortfall)
                print(f"\n  [!] AQL failed for EHR {ehr_id[:8]}…: {e}")

    n_ehrs = len(ehr_buckets)
    print(f"[*] Fetching canonical compositions for {n_ehrs} EHR(s) via AQL ...")
    print("[          ]", end="", flush=True)
    t0 = time.monotonic()
    await asyncio.gather(*[one_ehr(eid, bkt, ehr_counts[eid]) for eid, bkt in ehr_buckets.items()])
    print(f"\r[XXXXXXXXXX] {ok + failed:,} done")
    print(f"[*] OK: {ok} | Failed: {failed}")
    _elapsed = int(time.monotonic() - t0)
    _mins, _secs = divmod(_elapsed, 60)
    print(f"[*] Time: {_mins}m {_secs}s" if _mins else f"[*] Time: {_secs}s")


# ── entry point ────────────────────────────────────────────────────────────────

def prompt_api() -> tuple[str, aiohttp.BasicAuth]:
    """Prompt for API credentials (Mode 3 only). Saves to config file."""
    saved: dict = {}
    if os.path.exists(CONFIG_FILE):
        with open(CONFIG_FILE) as f:
            saved = json.load(f)

    default_url  = saved.get("url",      "http://localhost:8080/ehrbase/rest/openehr/v1")
    default_user = saved.get("user",     "ehrbase-user")
    default_pass = saved.get("password", "SuperSecretPassword")

    raw  = input(f"API URL [{default_url}]: ").strip().rstrip("/")
    url  = raw or default_url
    user = input(f"User [{default_user}]: ").strip() or default_user
    pwd  = input(f"Pass [{'*' * len(default_pass)}]: ").strip() or default_pass

    with open(CONFIG_FILE, "w") as f:
        json.dump({"url": url, "user": user, "password": pwd}, f, indent=2)

    return url, aiohttp.BasicAuth(user, pwd)


def load_api() -> Optional[tuple[str, aiohttp.BasicAuth]]:
    """Load saved API credentials silently. Returns None if config missing."""
    if not os.path.exists(CONFIG_FILE):
        print("[!] No API config found. Run Setup (mode 3) first.")
        return None
    with open(CONFIG_FILE) as f:
        cfg = json.load(f)
    return cfg["url"], aiohttp.BasicAuth(cfg["user"], cfg["password"])


async def main() -> None:
    print("--- openEHR Synthetic Data Generator ---")
    print("1. Generate compositions from existing compositions (duplicate)")
    print("2. Generate compositions from templates and jitter")
    print("3. Setup: upload opts and set up modelling environment")
    mode = input("Select mode: ").strip()

    start = time.monotonic()
    try:
        if mode == "3":
            url, auth = prompt_api()
            async with aiohttp.ClientSession(auth=auth) as session:
                await upload_opts(session, url)
                await run_setup(session, url)
            return

        if mode == "1":
            comp_files = sorted(f for f in os.listdir(USER_COMPS_DIR) if f.endswith(".json"))
            if not comp_files:
                print(f"[!] No compositions in {USER_COMPS_DIR}.")
                return
            count = int(
                input(f"Compositions found: {len(comp_files)}. Count per composition [1]: ").strip() or "1"
            )
            print("  (a) Save to local disk (dist/compositions/)")
            print("  (b) Send to openEHR CDR")
            dest = input("  Destination [a/b]: ").strip().lower()
            packaging = "a"
            if dest == "a":
                total = count * len(comp_files)
                if total > 10000:
                    pkg = input(f"  {total:,} compositions to save: (a) Individual files / (b) Zip [default]: ").strip().lower()
                    packaging = "a" if pkg == "a" else "b"
            if dest == "b":
                api = load_api()
                if not api:
                    return
                url, auth = api
                async with aiohttp.ClientSession(auth=auth) as session:
                    pool_size = max(1, (count * len(comp_files)) // 100)
                    print(f"\n[*] Creating {pool_size} EHR(s) ...")
                    ehr_pool = await create_ehr_pool(session, url, pool_size)
                    await run_duplicate(dest, count, session, url, ehr_pool, packaging)
            else:
                await run_duplicate(dest, count, packaging=packaging)
            return

        if mode == "2":
            flat_files = sorted(f for f in os.listdir(FLAT_DIR) if f.endswith(".json"))
            if not flat_files:
                print("[!] No example skeleton compositions are found; run Setup (mode 3) first.")
                return
            count = int(
                input(f"Skeletons found: {len(flat_files)}. Count per skeleton [1]: ").strip() or "1"
            )
            print("  (a) Save to local disk (dist/compositions/)")
            print("  (b) Send to openEHR CDR")
            dest = input("  Destination [a/b]: ").strip().lower()
            fmt = "a"
            packaging = "a"
            if dest == "a":
                fmt_raw = input("  Format: (a) Flat [default] / (b) Canonical (via AQL-note this requires POST to CDR): ").strip().lower()
                fmt = fmt_raw if fmt_raw in ("a", "b") else "a"
                total = count * len(flat_files)
                if total > 10000:
                    pkg = input(f"  {total:,} compositions to save: (a) Individual files / (b) Zip [default]: ").strip().lower()
                    packaging = "a" if pkg == "a" else "b"
            needs_cdr = dest == "b" or fmt == "b"
            if needs_cdr:
                api = load_api()
                if not api:
                    return
                url, auth = api
                async with aiohttp.ClientSession(auth=auth) as session:
                    pool_size = max(1, (count * len(flat_files)) // 100)
                    print(f"\n[*] Creating {pool_size} EHR(s) ...")
                    ehr_pool = await create_ehr_pool(session, url, pool_size)
                    await run_generate(dest, count, session, url, ehr_pool, fmt, packaging)
            else:
                await run_generate(dest, count, packaging=packaging)
            return

        print("[!] Unknown mode.")
    finally:
        elapsed = time.monotonic() - start
        mins, secs = divmod(int(elapsed), 60)
        print(f"[*] Total time: {mins}m {secs}s" if mins else f"[*] Total time: {secs}s")


if __name__ == "__main__":
    asyncio.run(main())
