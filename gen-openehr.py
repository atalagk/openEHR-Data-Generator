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
import aiohttp
from typing import Optional

# ── directories ────────────────────────────────────────────────────────────────

BASE           = "source_models"
OPT_DIR        = os.path.join(BASE, "opts")
WT_DIR         = os.path.join(BASE, "opt_webtemplates")
USER_COMPS_DIR = os.path.join(BASE, "user_compositions")
FLAT_DIR       = os.path.join(BASE, "flat_composition_skeletons")
DIST_DIR       = os.path.join("dist", "compositions")

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
    # The WT tree root 'id' may be the COMPOSITION archetype id (e.g. 'encounter')
    # rather than the template_id. Use the authoritative 'templateId' field from
    # the WT so paths align with normalized flat keys.
    tid = wt.get("templateId") or template_id
    root = dict(root, id=tid)
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
            for key in keys:
                if "|" not in key and isinstance(out[key], str):
                    words = out[key].split()
                    if len(words) > 1:
                        random.shuffle(words)
                        out[key] = " ".join(words)
                    else:
                        out[key] = out[key] + " " + hex(random.randint(0, 0xFFFF))[2:]

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
    session: aiohttp.ClientSession, url: str, template_id: str
) -> dict:
    async with session.get(
        f"{url}/definition/template/adl1.4/{template_id}/example",
        params={"format": "FLAT"},
        headers={"Accept": "application/json"},
    ) as r:
        if r.status != 200:
            raise RuntimeError(f"Flat example fetch failed {r.status}: {await r.text()[:200]}")
        return await r.json(content_type=None)


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
) -> tuple[int, str]:
    endpoint = f"{url}/ehr/{ehr_id}/composition?format=FLAT&templateId={template_id}"
    headers = {"Content-Type": "application/json", "Accept": "application/json"}
    async with session.post(endpoint, json=flat, headers=headers) as r:
        return r.status, await r.text()


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
                    print(f"  [!] {fname} ({r.status}): {await r.text()[:120]}")
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


# ── mode 4: setup ──────────────────────────────────────────────────────────────

async def run_setup(session: aiohttp.ClientSession, url: str) -> None:
    # ── step 1: check webtemplates ────────────────────────────────────────────
    wt_files = sorted(f for f in os.listdir(WT_DIR) if f.endswith(".json"))
    if not wt_files:
        print("[!] No webtemplates found in source_models/opt_webtemplates/")
        print("    Run Mode 3 first to upload OPTs and fetch webtemplates.")
        return

    print(f"[*] Step 1: {len(wt_files)} webtemplate(s) found in {WT_DIR}")

    # ── step 2: fetch flat example compositions ───────────────────────────────
    print(f"\n[*] Step 2: Fetch flat example compositions -> {FLAT_DIR}")
    ok = failed = 0
    sem = asyncio.Semaphore(10)

    async def one(fname: str) -> None:
        nonlocal ok, failed
        async with sem:
            try:
                with open(os.path.join(WT_DIR, fname)) as f:
                    wt = json.load(f)
                tid = wt.get("templateId") or fname[:-5]
                flat = await fetch_example_flat(session, url, tid)
                envelope = {"template_id": tid, "flat_comp": flat}
                with open(os.path.join(FLAT_DIR, f"{tid}.json"), "w") as f:
                    json.dump(envelope, f, indent=2)
                ok += 1
            except Exception as e:
                failed += 1
                print(f"  [!] {fname}: {e}")

    await asyncio.gather(*[one(f) for f in wt_files])
    print(f"  Flat examples fetched (success:{ok} | fail:{failed} | total:{len(wt_files)})")


# ── mode 1: duplicate canonical compositions ───────────────────────────────────

async def run_duplicate(
    dest: str,
    count: int,
    session: Optional[aiohttp.ClientSession] = None,
    url: str = "",
    ehr_id: str = "",
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

    async def one(fname: str) -> None:
        nonlocal ok, failed
        async with sem:
            try:
                with open(os.path.join(USER_COMPS_DIR, fname)) as f:
                    comp = json.load(f)
                for _ in range(count):
                    clean = strip_canonical_uid(copy.deepcopy(comp))
                    if send_cdr:
                        await post_canonical(session, url, ehr_id, clean)
                    if save_local:
                        n = counters.get(fname, 0)
                        counters[fname] = n + 1
                        out_name = f"{fname[:-5]}_{n:06d}.json"
                        with open(os.path.join(DIST_DIR, out_name), "w") as f2:
                            json.dump(clean, f2, indent=2)
                    ok += 1
            except Exception as e:
                failed += 1
                if fname not in first_errors:
                    first_errors[fname] = str(e)

    await asyncio.gather(*[one(f) for f in comp_files])
    print(f"\n[*] Done. OK: {ok} | Failed: {failed}")
    for fname, err in first_errors.items():
        print(f"  [!] {fname}: {err}")


# ── mode 2: jitter flat compositions ──────────────────────────────────────────

async def run_generate(
    dest: str,
    count: int,
    session: Optional[aiohttp.ClientSession] = None,
    url: str = "",
    ehr_id: str = "",
) -> None:
    flat_files = sorted(f for f in os.listdir(FLAT_DIR) if f.endswith(".json"))
    if not flat_files:
        print(f"[!] No flat skeletons in {FLAT_DIR}. Run mode 4 first.")
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

                for _ in range(count):
                    flat = strip_flat_uid(copy.deepcopy(skeleton))
                    flat = mutate_flat(flat, wt_index)
                    if send_cdr:
                        status, txt = await post_flat(session, url, ehr_id, template_id, flat)
                        if status not in (200, 201, 204):
                            raise RuntimeError(f"{status} {txt[:600]}")
                    if save_local:
                        n = counters.get(fname, 0)
                        counters[fname] = n + 1
                        out_name = f"{fname[:-5]}_{n:06d}.json"
                        with open(os.path.join(DIST_DIR, out_name), "w") as f2:
                            json.dump(flat, f2, indent=2)
                    ok += 1
            except Exception as e:
                failed += 1
                if fname not in first_errors:
                    first_errors[fname] = str(e)

    await asyncio.gather(*[one(f) for f in flat_files])
    print(f"\n[*] Done. OK: {ok} | Failed: {failed}")
    for fname, err in first_errors.items():
        print(f"  [!] {fname}: {err}")


# ── entry point ────────────────────────────────────────────────────────────────

def prompt_api() -> tuple[str, aiohttp.BasicAuth]:
    raw = input("API URL [http://localhost:8080/ehrbase/rest/openehr/v1]: ").strip().rstrip("/")
    url = raw or "http://localhost:8080/ehrbase/rest/openehr/v1"
    user = input("User [ehrbase-admin]: ").strip() or "ehrbase-admin"
    pwd = input("Pass [ehrbase]: ").strip() or "ehrbase"
    return url, aiohttp.BasicAuth(user, pwd)


async def main() -> None:
    print("--- openEHR Synthetic Data Generator ---")
    print("1. Generate compositions from existing compositions (duplicate)")
    print("2. Generate compositions from templates and jitter")
    print("3. Upload OPTs to openEHR CDR (run this when you want to add new opts)")
    print("4. Setup modelling environment")
    mode = input("Select mode: ").strip()

    if mode == "3":
        url, auth = prompt_api()
        async with aiohttp.ClientSession(auth=auth) as session:
            await upload_opts(session, url)
        return

    if mode == "4":
        url, auth = prompt_api()
        async with aiohttp.ClientSession(auth=auth) as session:
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
        if dest == "b":
            url, auth = prompt_api()
            async with aiohttp.ClientSession(auth=auth) as session:
                print("\n[*] Creating EHR ...")
                ehr_id = await create_ehr(session, url)
                print(f"[*] EHR: {ehr_id}\n")
                await run_duplicate(dest, count, session, url, ehr_id)
        else:
            await run_duplicate(dest, count)
        return

    if mode == "2":
        flat_files = sorted(f for f in os.listdir(FLAT_DIR) if f.endswith(".json"))
        if not flat_files:
            print(f"[!] No flat skeletons in {FLAT_DIR}. Run mode 4 first.")
            return
        count = int(
            input(f"Skeletons found: {len(flat_files)}. Count per skeleton [1]: ").strip() or "1"
        )
        print("  (a) Save to local disk (dist/compositions/)")
        print("  (b) Send to openEHR CDR")
        dest = input("  Destination [a/b]: ").strip().lower()
        if dest == "b":
            url, auth = prompt_api()
            async with aiohttp.ClientSession(auth=auth) as session:
                print("\n[*] Creating EHR ...")
                ehr_id = await create_ehr(session, url)
                print(f"[*] EHR: {ehr_id}\n")
                await run_generate(dest, count, session, url, ehr_id)
        else:
            await run_generate(dest, count)
        return

    print("[!] Unknown mode.")


if __name__ == "__main__":
    asyncio.run(main())
