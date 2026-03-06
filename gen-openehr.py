#!/usr/bin/env python3
"""
openEHR Synthetic Data Generator
"""

import os
import re
import json
import copy
import random
import asyncio
import aiohttp
from typing import Optional, Tuple

# ── directories ────────────────────────────────────────────────────────────────

BASE      = "source_models"
OPT_DIR   = os.path.join(BASE, "opts")
WT_DIR    = os.path.join(BASE, "opt_webtemplates")
COMPS_DIR = os.path.join(BASE, "opt_compositions")
FLAT_DIR  = os.path.join(BASE, "flat_composition_skeletons")

for d in (OPT_DIR, WT_DIR, COMPS_DIR, FLAT_DIR):
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


# ── jitter ─────────────────────────────────────────────────────────────────────

_COMPOSITION_PROPS  = frozenset({"archetype_details", "language", "territory", "category", "composer", "context"})
_PROTECTED_SEGMENTS = frozenset({"_work_flow_id", "_guideline_id", "_ism_transition"})


def _is_protected(key: str) -> bool:
    parts = key.split("/")
    if len(parts) >= 2 and parts[1].split("|")[0].split(":")[0] in _COMPOSITION_PROPS:
        return True
    return any(p.split("|")[0].split(":")[0] in _PROTECTED_SEGMENTS for p in parts)


def jitter_flat(flat: dict, wt_index: dict[str, dict]) -> dict:
    """
    Return a copy of flat with DV_QUANTITY magnitudes jittered within WT range.

    Rules:
      - Only |magnitude keys are mutated
      - Protected keys (COMPOSITION props, _work_flow_id, _guideline_id, _ism_transition) skipped
      - |unit is never touched
      - DV_CODED_TEXT with openehr terminology is untouched (has no |magnitude)
      - ±10 % jitter, clamped to WT validation range when present
    """
    out = copy.deepcopy(flat)

    for key, val in flat.items():
        if not key.endswith("|magnitude"):
            continue
        if not isinstance(val, (int, float)):
            continue
        if _is_protected(key):
            continue

        wt_node = wt_index.get(wt_path_of(key))
        if wt_node is None or wt_node.get("rmType") != "DV_QUANTITY":
            continue

        inputs = wt_node.get("inputs") or []
        mag_inp = next(
            (i for i in inputs if i.get("suffix") in (None, "", "magnitude")), None
        )
        rng = (mag_inp.get("validation") or {}).get("range") if mag_inp else None

        jittered = float(val) * random.uniform(0.9, 1.1)
        if rng:
            lo = float(rng.get("min", jittered))
            hi = float(rng.get("max", jittered))
            if hi < lo:
                hi = lo
            jittered = max(lo, min(hi, jittered))

        out[key] = round(jittered, 2) if isinstance(val, float) else round(jittered)

    return out


# ── flat composition helpers ───────────────────────────────────────────────────

def get_template_id(flat: dict) -> Optional[str]:
    for key in flat:
        return key.split("/")[0]
    return None


def strip_flat_uid(flat: dict) -> dict:
    return {k: v for k, v in flat.items() if k.rsplit("/", 1)[-1] != "_uid"}


def strip_canonical_uid(comp: dict) -> dict:
    comp.pop("uid", None)
    comp.pop("_uid", None)
    return comp


# ── EHRbase REST ───────────────────────────────────────────────────────────────

async def create_ehr(session: aiohttp.ClientSession, url: str) -> str:
    payload = {
        "_type": "EHR_STATUS",
        "archetype_node_id": "openEHR-EHR-EHR_STATUS.generic.v1",
        "name": {"_type": "DV_TEXT", "value": "EHR status"},
        "subject": {"_type": "PARTY_SELF"},
        "is_queryable": True,
        "is_modifiable": True,
    }
    async with session.post(f"{url}/ehr", json=payload) as r:
        if r.status not in (200, 201):
            raise RuntimeError(f"Create EHR failed {r.status}: {await r.text()}")
        try:
            body = await r.json(content_type=None)
            return body["ehr_id"]["value"]
        except Exception:
            loc = r.headers.get("Location", "")
            return loc.rstrip("/").rsplit("/", 1)[-1]


async def list_template_ids(session: aiohttp.ClientSession, url: str) -> list[str]:
    async with session.get(
        f"{url}/definition/template/adl1.4", headers={"Accept": "application/json"}
    ) as r:
        if r.status != 200:
            raise RuntimeError(f"List templates failed {r.status}: {await r.text()[:200]}")
        data = await r.json(content_type=None)
        return [t["template_id"] for t in data]


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


async def fetch_example_canonical(
    session: aiohttp.ClientSession, url: str, template_id: str
) -> dict:
    async with session.get(
        f"{url}/definition/template/adl1.4/{template_id}/example",
        headers={"Accept": "application/json"},
    ) as r:
        if r.status != 200:
            raise RuntimeError(f"Example fetch failed {r.status}: {await r.text()[:200]}")
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
        if r.status not in (200, 201):
            raise RuntimeError(f"POST canonical failed {r.status}: {await r.text()[:300]}")
        body = await r.json(content_type=None)
        uid = (body.get("uid") or {}).get("value") or body.get("_uid")
        if not uid:
            loc = r.headers.get("Location", "")
            uid = loc.rstrip("/").rsplit("/", 1)[-1]
        return uid


async def fetch_flat(
    session: aiohttp.ClientSession, url: str, ehr_id: str, uid: str
) -> dict:
    async with session.get(
        f"{url}/ehr/{ehr_id}/composition/{uid}?format=FLAT",
        headers={"Accept": "application/json"},
    ) as r:
        if r.status != 200:
            raise RuntimeError(f"FLAT fetch failed {r.status}: {await r.text()[:200]}")
        return await r.json(content_type=None)


async def post_flat(
    session: aiohttp.ClientSession,
    url: str,
    ehr_id: str,
    template_id: str,
    flat: dict,
) -> Tuple[int, str]:
    endpoint = f"{url}/ehr/{ehr_id}/composition?format=FLAT&templateId={template_id}"
    headers = {"Content-Type": "application/json", "Accept": "application/json"}
    async with session.post(endpoint, json=flat, headers=headers) as r:
        return r.status, await r.text()


# ── mode 3: upload OPTs ────────────────────────────────────────────────────────

async def upload_opts(session: aiohttp.ClientSession, url: str) -> None:
    opt_files = [f for f in os.listdir(OPT_DIR) if f.endswith(".opt")]
    if not opt_files:
        print(f"[!] No .opt files found in {OPT_DIR}")
        return

    print(f"[*] Uploading {len(opt_files)} OPT(s) ...")
    ok = failed = skipped = 0
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
                    print(f"  [+] {fname}")
                elif r.status == 409:
                    skipped += 1
                    print(f"  [~] Already exists: {fname}")
                else:
                    failed += 1
                    print(f"  [!] {fname} ({r.status}): {await r.text()[:120]}")
        except Exception as e:
            failed += 1
            print(f"  [!] {fname}: {e}")

    print(f"\n[*] OPTs uploaded (ok:{ok} | skipped:{skipped} | fail:{failed} | total:{len(opt_files)})")


# ── mode 4: setup ──────────────────────────────────────────────────────────────

async def setup_fetch_webtemplates(session: aiohttp.ClientSession, url: str) -> None:
    """Step 1: Fetch webtemplates from CDR -> opt_webtemplates/"""
    try:
        template_ids = await list_template_ids(session, url)
    except Exception as e:
        print(f"  [!] Could not list templates: {e}")
        return

    ok = failed = 0
    sem = asyncio.Semaphore(10)

    async def one(tid: str) -> None:
        nonlocal ok, failed
        async with sem:
            try:
                wt = await fetch_webtemplate(session, url, tid)
                with open(os.path.join(WT_DIR, f"{tid}.json"), "w") as f:
                    json.dump(wt, f, indent=2)
                ok += 1
            except Exception:
                failed += 1

    await asyncio.gather(*[one(tid) for tid in template_ids])
    total = len(template_ids)
    print(f"  Webtemplates fetched from openEHR CDR (success:{ok} | fail:{failed} | total:{total})")


async def setup_fetch_sample_compositions(session: aiohttp.ClientSession, url: str) -> None:
    """Step 2: Fetch /example canonical compositions -> opt_compositions/"""
    template_ids = sorted(f[:-5] for f in os.listdir(WT_DIR) if f.endswith(".json"))
    if not template_ids:
        print("  [!] No webtemplates found — run step 1 first")
        return

    ok = failed = 0
    sem = asyncio.Semaphore(10)

    async def one(tid: str) -> None:
        nonlocal ok, failed
        async with sem:
            try:
                comp = await fetch_example_canonical(session, url, tid)
                with open(os.path.join(COMPS_DIR, f"{tid}.json"), "w") as f:
                    json.dump(comp, f, indent=2)
                ok += 1
            except Exception:
                failed += 1

    await asyncio.gather(*[one(tid) for tid in template_ids])
    total = len(template_ids)
    print(f"  Sample compositions from CDR fetched (success:{ok} | fail:{failed} | total:{total})")


async def setup_post_sample_compositions(
    session: aiohttp.ClientSession, url: str, ehr_id: str
) -> dict[str, str]:
    """Step 3: POST each canonical skeleton -> collect UIDs for flat fetch."""
    comp_files = sorted(f for f in os.listdir(COMPS_DIR) if f.endswith(".json"))
    if not comp_files:
        print("  [!] No sample compositions found — run step 2 first")
        return {}

    ok = failed = 0
    uids: dict[str, str] = {}
    sem = asyncio.Semaphore(5)

    async def one(fname: str) -> None:
        nonlocal ok, failed
        async with sem:
            template_id = fname[:-5]
            try:
                with open(os.path.join(COMPS_DIR, fname)) as f:
                    comp = json.load(f)
                comp = strip_canonical_uid(copy.deepcopy(comp))
                uid = await post_canonical(session, url, ehr_id, comp)
                uids[template_id] = uid
                ok += 1
            except Exception:
                failed += 1

    await asyncio.gather(*[one(f) for f in comp_files])
    total = len(comp_files)
    print(f"  Sample compositions sent to CDR to get flat compositions (success:{ok} | fail:{failed} | total:{total})")
    return uids


async def setup_fetch_flat_compositions(
    session: aiohttp.ClientSession, url: str, ehr_id: str, uids: dict[str, str]
) -> None:
    """Step 4: GET each posted composition as FLAT -> flat_composition_skeletons/"""
    if not uids:
        print("  [!] No composition UIDs available — run step 3 first")
        return

    ok = failed = 0
    sem = asyncio.Semaphore(5)

    async def one(template_id: str, uid: str) -> None:
        nonlocal ok, failed
        async with sem:
            try:
                flat = await fetch_flat(session, url, ehr_id, uid)
                flat = strip_flat_uid(flat)
                with open(os.path.join(FLAT_DIR, f"{template_id}.json"), "w") as f:
                    json.dump(flat, f, indent=2)
                ok += 1
            except Exception:
                failed += 1

    await asyncio.gather(*[one(tid, uid) for tid, uid in uids.items()])
    total = len(uids)
    print(f"  FLAT json compositions fetched (success:{ok} | fail:{failed} | total:{total})")


async def run_setup(session: aiohttp.ClientSession, url: str) -> None:
    print("\n[*] Step 1: Fetch Webtemplates from openEHR CDR -> source_models/opt_webtemplates")
    await setup_fetch_webtemplates(session, url)

    print("\n[*] Step 2: Fetch sample compositions from CDR -> source_models/opt_compositions")
    await setup_fetch_sample_compositions(session, url)

    print("\n[*] Step 3: POST sample compositions to CDR")
    ehr_id = await create_ehr(session, url)
    uids = await setup_post_sample_compositions(session, url, ehr_id)

    print("\n[*] Step 4: Fetch FLAT json compositions -> source_models/flat_composition_skeletons")
    await setup_fetch_flat_compositions(session, url, ehr_id, uids)


# ── modes 1 & 2: generate compositions ────────────────────────────────────────

async def run_generate(
    session: aiohttp.ClientSession,
    url: str,
    ehr_id: str,
    count: int,
    do_jitter: bool,
) -> None:
    flat_files = sorted(f for f in os.listdir(FLAT_DIR) if f.endswith(".json"))
    if not flat_files:
        print(f"[!] No flat skeletons in {FLAT_DIR}. Run mode 4 first.")
        return

    ok = 0
    failed = 0
    first_errors: dict[str, str] = {}
    sem = asyncio.Semaphore(10)

    async def one(fname: str) -> None:
        nonlocal ok, failed
        async with sem:
            try:
                with open(os.path.join(FLAT_DIR, fname)) as f:
                    skeleton = json.load(f)

                template_id = get_template_id(skeleton)
                if not template_id:
                    raise ValueError("Cannot determine template_id from flat keys")

                wt_index = None
                if do_jitter:
                    wt_index = load_wt_index(template_id)
                    if wt_index is None:
                        raise ValueError(f"No webtemplate found for {template_id}")

                for _ in range(count):
                    flat = strip_flat_uid(copy.deepcopy(skeleton))
                    if do_jitter:
                        flat = jitter_flat(flat, wt_index)
                    status, txt = await post_flat(session, url, ehr_id, template_id, flat)
                    if status in (200, 201):
                        ok += 1
                    else:
                        failed += 1
                        if fname not in first_errors:
                            first_errors[fname] = f"{status} {txt[:120]}"
            except Exception as e:
                failed += 1
                if fname not in first_errors:
                    first_errors[fname] = str(e)

    await asyncio.gather(*[one(f) for f in flat_files])

    print(f"\n[*] Done. OK: {ok} | Failed: {failed}")
    for fname, err in first_errors.items():
        print(f"  [!] {fname}: {err}")


# ── entry point ────────────────────────────────────────────────────────────────

def prompt_api() -> Tuple[str, aiohttp.BasicAuth]:
    raw = input("API URL [http://localhost:8080/ehrbase/rest/openehr/v1]: ").strip().rstrip("/")
    url = raw or "http://localhost:8080/ehrbase/rest/openehr/v1"
    user = input("User [ehrbase-admin]: ").strip() or "ehrbase-admin"
    pwd = input("Pass [ehrbase]: ").strip() or "ehrbase"
    return url, aiohttp.BasicAuth(user, pwd)


async def main() -> None:
    print("--- openEHR Synthetic Data Generator ---")
    print("1. Generate compositions from existing compositions (duplicate)")
    print("2. Generate compositions from templates")
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

    if mode not in ("1", "2"):
        print("[!] Unknown mode.")
        return

    flat_files = sorted(f for f in os.listdir(FLAT_DIR) if f.endswith(".json"))
    if not flat_files:
        print(f"[!] No flat skeletons in {FLAT_DIR}. Run mode 4 first.")
        return

    count = int(
        input(f"Skeletons found: {len(flat_files)}. Count per skeleton [1]: ").strip() or "1"
    )
    url, auth = prompt_api()

    async with aiohttp.ClientSession(auth=auth) as session:
        print("\n[*] Creating EHR ...")
        ehr_id = await create_ehr(session, url)
        print(f"[*] EHR: {ehr_id}\n")
        await run_generate(session, url, ehr_id, count, do_jitter=(mode == "2"))


if __name__ == "__main__":
    asyncio.run(main())
