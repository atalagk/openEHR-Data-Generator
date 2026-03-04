#!/usr/bin/env python3
"""
openEHR High-Volume Generator (Hybrid /example + WT reconciliation, indexed)

What this script does:
- Uses EHRbase /example canonical JSON as the primary skeleton (maximum reuse).
- Applies a WT-based reconciliation pass (generic, RM-aware) to fill gaps:
    * min/max occurrences, including AQL predicates (nodeId + optional name/value)
    * ITEM_TREE required items[atXXXX] and required null_flavour (derived from WT subtree)
    * required STRING attributes on ACTIVITY (e.g., action_archetype_id) when WT says min=1
- Never injects ELEMENT into SECTION.items / COMPOSITION.content (ContentItem lists).
- Never touches ism_transition (avoids invalid transitions).

Also includes:
- Mode 7: POST /example "as-is" (no reconciliation, no jitter) to identify broken templates
  so you can skip fixing those.

Modes:
1) POST: GET /example -> reconcile with WT -> POST to EHRbase
2) Transform existing canonical JSON files using WT rules (source_compositions -> generated_compositions)
3) Write files: GET /example -> reconcile with WT -> write to OUTPUT_DIR
4) Upload OPTs
5) Fetch webtemplates from EHRbase
6) Cache /example skeletons
7) Post examples as-is (diagnostic) -> writes broken_templates.json

"""

import os
import json
import uuid
import random
import copy
import time
import asyncio
import aiohttp
import re
import shutil
from datetime import datetime
from typing import Any, Optional, Tuple, Dict, List


# --- Configuration ---
BASE_SOURCE = "source_models"
WT_DIR = os.path.join(BASE_SOURCE, "webtemplates")
OPT_DIR = os.path.join(BASE_SOURCE, "opts")
# COMP_DIR = "source_compositions"
COMP_DIR = os.path.join(BASE_SOURCE, "compositions")
# OUTPUT_DIR = "generated_compositions"
OUTPUT_DIR = "dist/compositions"
SKELETON_DIR = os.path.join(BASE_SOURCE, "canonical_skeletons")
REPORT_DIR = os.path.join(BASE_SOURCE, "reports")

for d in (WT_DIR, OPT_DIR, COMP_DIR, OUTPUT_DIR, SKELETON_DIR, REPORT_DIR):
    os.makedirs(d, exist_ok=True)

processed_count = 0


# -----------------------------
# Utility
# -----------------------------
def now_dt_z() -> str:
    return datetime.now().strftime("%Y-%m-%dT%H:%M:%S.000Z")


def now_date() -> str:
    return datetime.now().strftime("%Y-%m-%d")


def now_time() -> str:
    return datetime.now().strftime("%H:%M:%S")


def scramble_plain_text(text: str) -> str:
    """Light jitter for free-text DV_TEXT only."""
    if not text or len(text) < 6:
        return text
    words = text.split()
    if len(words) < 3:
        return text + " " + "".join(random.choices("abcdefghijklmnopqrstuvwxyz", k=3))
    idx = random.randint(0, len(words) - 1)
    w = words[idx]
    if len(w) >= 3:
        words[idx] = "".join(random.sample(w, len(w)))
    return " ".join(words)


def strip_server_fields(obj):
    """Remove fields that should not be re-posted."""
    if isinstance(obj, dict):
        obj.pop("uid", None)
        obj.pop("composition_uid", None)
        obj.pop("_uid", None)
        for k, v in list(obj.items()):
            obj[k] = strip_server_fields(v)
        return obj
    if isinstance(obj, list):
        return [strip_server_fields(x) for x in obj]
    return obj


# -----------------------------
# WT loading
# -----------------------------
def extract_wt_tree(wt_json: dict) -> dict:
    """
    Supports common WT wrappers:
      - {"tree": {...}}
      - {"webTemplate": {"tree": {...}}}
      - {"web_template": {"tree": {...}}}
      - {"webtemplate": {"tree": {...}}}
    """
    if isinstance(wt_json.get("tree"), dict):
        return wt_json["tree"]
    for k in ("webTemplate", "web_template", "webtemplate"):
        if isinstance(wt_json.get(k), dict) and isinstance(wt_json[k].get("tree"), dict):
            return wt_json[k]["tree"]
    raise ValueError("Cannot find WT tree in JSON")


import os
import json

def load_wt_for_template(template_id: str):
    path = os.path.join("source_models", "webtemplates", f"{template_id}.json")

    if not os.path.exists(path):
        print(f"[!] Skipping template {template_id}: webtemplate file not found: {path}")
        return None  # <-- key change

    with open(path, "r", encoding="utf-8") as f:
        return json.load(f)


def list_template_ids_from_wt_dir():
    """
    Uses WT_DIR filenames:
      - valid: <template_id>.json
      - invalid: <template_id>.json.invalid  (ignored)
    """
    template_ids = []
    for f in os.listdir(WT_DIR):
        # accept only exact *.json, reject *.json.invalid
        if f.endswith(".json") and not f.endswith(".json.invalid"):
            template_ids.append(f[:-5])
    return sorted(set(template_ids))


# -----------------------------
# WT index (for robust matching)
# -----------------------------
def build_wt_index(wt_root: dict):
    """
    Indexes to locate WT nodes when traversal matching is imperfect.
      - by_nodeid_rm[(nodeId, rmType)] -> [nodes]
      - by_id[id] -> [nodes]
      - all_nodes -> [nodes]
    """
    by_nodeid_rm: Dict[Tuple[str, str], List[dict]] = {}
    by_id: Dict[str, List[dict]] = {}
    all_nodes: List[dict] = []

    stack = [wt_root]
    while stack:
        n = stack.pop()
        all_nodes.append(n)

        nid = n.get("nodeId")
        rm = n.get("rmType")
        if nid and rm:
            by_nodeid_rm.setdefault((nid, rm), []).append(n)

        i = n.get("id")
        if i:
            by_id.setdefault(i, []).append(n)

        for ch in (n.get("children") or []):
            stack.append(ch)

    return {"by_nodeid_rm": by_nodeid_rm, "by_id": by_id, "all_nodes": all_nodes}


# -----------------------------
# WT choice parsing for DV_TEXT / DV_CODED_TEXT
# -----------------------------
def wt_lists(wt_node: dict):
    for inp in (wt_node.get("inputs") or []):
        lst = inp.get("list")
        if lst:
            yield inp, lst


def pick_wt_item(wt_node: dict):
    for inp, lst in wt_lists(wt_node):
        return inp, random.choice(lst)
    return None, None


def item_label(item: dict) -> Optional[str]:
    return item.get("label") or item.get("value")


def is_coded_list_input(inp: dict, sample_item: dict) -> bool:
    if inp.get("terminology"):
        return True
    if (inp.get("type") or "").upper() == "CODED_TEXT":
        v = sample_item.get("value")
        return isinstance(v, str) and len(v) > 0
    return False


def build_dv_text(value: str) -> dict:
    return {"_type": "DV_TEXT", "value": value}


def build_dv_coded_text(label: str, code: str, terminology: str) -> dict:
    return {
        "_type": "DV_CODED_TEXT",
        "value": label,
        "defining_code": {
            "_type": "CODE_PHRASE",
            "terminology_id": {"_type": "TERMINOLOGY_ID", "value": terminology},
            "code_string": str(code),
        },
    }


DV_LEAF_TYPES = {
    "DV_TEXT", "DV_CODED_TEXT", "DV_ORDINAL", "DV_BOOLEAN",
    "DV_DATE_TIME", "DV_DATE", "DV_TIME", "DV_QUANTITY", "DV_COUNT", "DV_IDENTIFIER",
}


def apply_leaf_value_wt(leaf: dict, wt_node: Optional[dict]) -> dict:
    """
    WT-driven leaf fill/jitter.

    Critical:
    - Never touch ism_transition.* (avoids invalid transitions)
    - WT rmType DV_CODED_TEXT => payload DV_CODED_TEXT
    - WT rmType DV_TEXT => may upgrade to DV_CODED_TEXT subtype if coded list exists
    """
    if not isinstance(leaf, dict) or "_type" not in leaf or not wt_node:
        return leaf
    if leaf.get("_type") not in DV_LEAF_TYPES:
        return leaf

    aql = wt_node.get("aqlPath") or ""
    # Never touch ism_transition.* (avoids invalid transitions)
    # Never touch name/value (template-constrained; changing it triggers "not in template" errors)
    if isinstance(aql, str) and ("/ism_transition" in aql or "/name/value" in aql):
        return leaf

    wt_rm = wt_node.get("rmType")
    cur_t = leaf.get("_type")

    if wt_rm in ("DV_TEXT", "DV_CODED_TEXT"):
        inp, item = pick_wt_item(wt_node)

        if wt_rm == "DV_CODED_TEXT":
            if cur_t != "DV_CODED_TEXT":
                leaf = build_dv_coded_text(label="local-coded", code="at0000", terminology="local")
                cur_t = "DV_CODED_TEXT"

            if item and inp:
                terminology = inp.get("terminology")
                code = item.get("value")
                label = item_label(item) or ""
                if terminology and code is not None:
                    return build_dv_coded_text(str(label), str(code), str(terminology))
                if label:
                    leaf["value"] = str(label)
            return leaf

        if wt_rm == "DV_TEXT":
            if cur_t == "DV_CODED_TEXT":
                if item and inp:
                    label = item_label(item) or ""
                    terminology = inp.get("terminology")
                    code = item.get("value")
                    if is_coded_list_input(inp, item) and terminology and code is not None:
                        return build_dv_coded_text(str(label), str(code), str(terminology))
                    leaf["value"] = str(label)
                return leaf

            if item and inp:
                label = item_label(item) or ""
                terminology = inp.get("terminology")
                code = item.get("value")
                if is_coded_list_input(inp, item) and terminology and code is not None:
                    return build_dv_coded_text(str(label), str(code), str(terminology))
                return build_dv_text(str(label))  # text-only enum

            # free text
            if cur_t != "DV_TEXT":
                leaf = build_dv_text(str(leaf.get("value", "")))
            if isinstance(leaf.get("value"), str):
                leaf["value"] = scramble_plain_text(leaf["value"])
            return leaf

    # other DV_* safe jitter
    t = leaf.get("_type")
    if t == "DV_DATE_TIME" and "value" in leaf:
        leaf["value"] = now_dt_z()
    elif t == "DV_DATE" and "value" in leaf:
        leaf["value"] = now_date()
    elif t == "DV_TIME" and "value" in leaf:
        leaf["value"] = now_time()
    elif t == "DV_QUANTITY" and isinstance(leaf.get("magnitude"), (int, float)):
        leaf["magnitude"] = round(float(leaf["magnitude"]) * random.uniform(0.9, 1.1), 1)
    elif t == "DV_COUNT" and isinstance(leaf.get("magnitude"), int):
        leaf["magnitude"] = max(0, leaf["magnitude"] + random.choice([-1, 0, 1]))
    elif t == "DV_BOOLEAN" and isinstance(leaf.get("value"), bool):
        if random.random() < 0.15:
            leaf["value"] = not leaf["value"]
    elif t == "DV_IDENTIFIER":
        leaf["id"] = f"ID-{uuid.uuid4().hex[:8].upper()}"

    return leaf


# ============================================================
# HYBRID WT reconciliation pass (indexed, min+max)
# ============================================================
_RX_AQL_SEL = re.compile(r"\[(?P<nodeid>[^ \]]+)(?:\s+and\s+name/value='(?P<name>[^']+)')?\]")


def _parse_selector(aql: str) -> Tuple[Optional[str], Optional[str]]:
    if not isinstance(aql, str):
        return (None, None)
    m = _RX_AQL_SEL.search(aql)
    if not m:
        return (None, None)
    return (m.group("nodeid"), m.group("name"))


def _canon_name_value(obj: dict) -> Optional[str]:
    name = (obj or {}).get("name")
    if isinstance(name, dict):
        return name.get("value")
    return None


def _is_item_tree(node: dict) -> bool:
    return isinstance(node, dict) and node.get("_type") == "ITEM_TREE" and isinstance(node.get("items"), list)


def _is_dv_leaf(node: dict) -> bool:
    return isinstance(node, dict) and isinstance(node.get("_type"), str) and node["_type"].startswith("DV_")


def _make_minimal_item_tree(archetype_node_id: str, name: str = "Tree") -> dict:
    return {
        "_type": "ITEM_TREE",
        "archetype_node_id": archetype_node_id,
        "name": {"_type": "DV_TEXT", "value": name},
        "items": [],
    }


def _make_minimal_element(at_code: str) -> dict:
    return {
        "_type": "ELEMENT",
        "archetype_node_id": at_code,
        "name": {"_type": "DV_TEXT", "value": at_code},
        "value": {"_type": "DV_TEXT", "value": ""},
    }


def _make_minimal_history() -> dict:
    return {
        "_type": "HISTORY",
        "archetype_node_id": "at0001",
        "name": {"_type": "DV_TEXT", "value": "History"},
        "origin": {"_type": "DV_DATE_TIME", "value": now_dt_z()},
        "events": [],
    }


def _make_minimal_point_event() -> dict:
    return {
        "_type": "POINT_EVENT",
        "archetype_node_id": "at0002",
        "name": {"_type": "DV_TEXT", "value": "Event"},
        "time": {"_type": "DV_DATE_TIME", "value": now_dt_z()},
        "data": _make_minimal_item_tree("at0003", "Data"),
    }


def factory_from_rmtype(rm_type: str, nodeid: str, namev: Optional[str]) -> Optional[dict]:
    """
    Generic minimal RM factories for common rmTypes.
    Used only when no prototype exists.

    IMPORTANT:
    - Never fabricate ELEMENT/CLUSTER unless inside ITEM_TREE.items (handled elsewhere).
    - ACTION is intentionally NOT fabricated (ism_transition semantics); rely on /example prototype.
    """
    nm = namev or nodeid

    if rm_type == "SECTION":
        return {
            "_type": "SECTION",
            "archetype_node_id": nodeid,
            "name": {"_type": "DV_TEXT", "value": nm},
            "items": [],
        }

    if rm_type == "OBSERVATION":
        # Minimal OBSERVATION with a history + 1 event (safe)
        return {
            "_type": "OBSERVATION",
            "archetype_node_id": nodeid,
            "name": {"_type": "DV_TEXT", "value": nm},
            "language": {"_type": "CODE_PHRASE", "terminology_id": {"_type": "TERMINOLOGY_ID", "value": "ISO_639-1"}, "code_string": "en"},
            "encoding": {"_type": "CODE_PHRASE", "terminology_id": {"_type": "TERMINOLOGY_ID", "value": "IANA_character-sets"}, "code_string": "UTF-8"},
            "subject": {"_type": "PARTY_SELF"},
            "data": _make_minimal_history(),
        }

    if rm_type == "HISTORY":
        return _make_minimal_history()

    if rm_type == "POINT_EVENT":
        return _make_minimal_point_event()

    if rm_type == "EVALUATION":
        return {
            "_type": "EVALUATION",
            "archetype_node_id": nodeid,
            "name": {"_type": "DV_TEXT", "value": nm},
            "language": {"_type": "CODE_PHRASE", "terminology_id": {"_type": "TERMINOLOGY_ID", "value": "ISO_639-1"}, "code_string": "en"},
            "encoding": {"_type": "CODE_PHRASE", "terminology_id": {"_type": "TERMINOLOGY_ID", "value": "IANA_character-sets"}, "code_string": "UTF-8"},
            "subject": {"_type": "PARTY_SELF"},
            "data": _make_minimal_item_tree("at0001", "Data"),
        }

    if rm_type == "INSTRUCTION":
        return {
            "_type": "INSTRUCTION",
            "archetype_node_id": nodeid,
            "name": {"_type": "DV_TEXT", "value": nm},
            "language": {"_type": "CODE_PHRASE", "terminology_id": {"_type": "TERMINOLOGY_ID", "value": "ISO_639-1"}, "code_string": "en"},
            "encoding": {"_type": "CODE_PHRASE", "terminology_id": {"_type": "TERMINOLOGY_ID", "value": "IANA_character-sets"}, "code_string": "UTF-8"},
            "subject": {"_type": "PARTY_SELF"},
            "narrative": {"_type": "DV_TEXT", "value": "narrative"},
            "activities": [],
        }

    if rm_type == "ACTIVITY":
        # action_archetype_id is required by some templates; leave placeholder, reconciliation may fill
        return {
            "_type": "ACTIVITY",
            "archetype_node_id": nodeid,
            "name": {"_type": "DV_TEXT", "value": nm},
            "timing": {"_type": "DV_PARSABLE", "value": "R1/2020-01-01T00:00:00Z/P1D", "formalism": "timing"},
            "description": _make_minimal_item_tree("at0001", "Description"),
            "action_archetype_id": "openEHR-EHR-ACTION.procedure.v1",
        }

    if rm_type == "ADMIN_ENTRY":
        return {
            "_type": "ADMIN_ENTRY",
            "archetype_node_id": nodeid,
            "name": {"_type": "DV_TEXT", "value": nm},
            "language": {"_type": "CODE_PHRASE", "terminology_id": {"_type": "TERMINOLOGY_ID", "value": "ISO_639-1"}, "code_string": "en"},
            "encoding": {"_type": "CODE_PHRASE", "terminology_id": {"_type": "TERMINOLOGY_ID", "value": "IANA_character-sets"}, "code_string": "UTF-8"},
            "subject": {"_type": "PARTY_SELF"},
            "data": _make_minimal_item_tree("at0001", "Data"),
        }

    if rm_type == "ITEM_TREE":
        return _make_minimal_item_tree(nodeid, nm)

    return None


# -----------------------------
# COMPOSITION.category validity helper
# -----------------------------
def pick_composition_category_from_wt(wt_root: dict) -> Optional[dict]:
    """
    Try to find a WT node representing COMPOSITION.category and pick from its list if present.
    Fallback to standard openEHR 'event' category.
    """
    # common approach: search WT for rmType DV_CODED_TEXT with aqlPath ending in /category
    stack = [wt_root]
    while stack:
        n = stack.pop()
        aql = n.get("aqlPath") or ""
        if n.get("rmType") in ("DV_CODED_TEXT", "DV_TEXT") and isinstance(aql, str) and aql.endswith("/category"):
            inp, item = pick_wt_item(n)
            if inp and item:
                terminology = inp.get("terminology")
                code = item.get("value")
                label = item_label(item) or "event"
                if terminology and code is not None:
                    return build_dv_coded_text(str(label), str(code), str(terminology))
            # no list => fall through
        for ch in (n.get("children") or []):
            stack.append(ch)
    return None


def ensure_composition_category(comp: dict, wt_root: Optional[dict] = None) -> None:
    """
    Ensure COMPOSITION.category exists and is valid for EHRbase.
    Default: openehr::433 |event|
    """
    if not isinstance(comp, dict) or comp.get("_type") != "COMPOSITION":
        return
    cat = comp.get("category")
    if isinstance(cat, dict) and cat.get("_type") == "DV_CODED_TEXT":
        # assume OK
        return
    # try WT-driven pick
    if wt_root:
        picked = pick_composition_category_from_wt(wt_root)
        if picked:
            comp["category"] = picked
            return
    # safe default
    comp["category"] = build_dv_coded_text("event", "433", "openehr")


# -----------------------------
# Reconcile list cardinality
# -----------------------------
def reconcile_list_field(parent_obj: dict, field: str, wt_children: List[dict], wt_index: dict):
    """
    Ensure parent_obj[field] list satisfies min/max occurrences implied by WT children selectors.

    IMPORTANT special case:
    - INSTRUCTION.activities MUST contain ACTIVITY only. Some WTs nest SECTION-like nodes under
      activity description; we must NOT insert SECTION objects directly into activities[].
    """
    if field not in parent_obj or not isinstance(parent_obj.get(field), list):
        return

    # If this is INSTRUCTION.activities, only consider WT children whose rmType is ACTIVITY
    if parent_obj.get("_type") == "INSTRUCTION" and field == "activities":
        wt_children = [c for c in wt_children if c.get("rmType") == "ACTIVITY"]

    arr = parent_obj[field]

    # Count required occurrences per selector
    requirements: List[Tuple[str, str, Optional[str], int, Optional[int]]] = []
    for ch in wt_children:
        aql = ch.get("aqlPath") or ""
        nodeid, namev = _parse_selector(aql)
        rm = ch.get("rmType")
        occ = ch.get("occurrences") or {}
        # WT sometimes uses min/max at child level
        mino = int(occ.get("min", 0) or 0)
        maxo = occ.get("max", None)
        maxo_i = None
        if maxo not in (None, "", "*"):
            try:
                maxo_i = int(maxo)
            except Exception:
                maxo_i = None
        if nodeid and rm:
            requirements.append((nodeid, rm, namev, mino, maxo_i))

    for nodeid, rm, namev, mino, maxo in requirements:
        # current matches
        matches = []
        for x in arr:
            if not isinstance(x, dict):
                continue
            if x.get("archetype_node_id") != nodeid:
                continue
            if x.get("_type") != rm:
                continue
            if namev is not None:
                if _canon_name_value(x) != namev:
                    continue
            matches.append(x)

        # Enforce min
        while len(matches) < mino:
            # Try prototype from existing match
            proto = matches[0] if matches else None
            if proto:
                new_obj = copy.deepcopy(proto)
            else:
                new_obj = factory_from_rmtype(rm, nodeid, namev)
                if new_obj is None:
                    break
            # DO NOT randomize name/value; name stays template-constrained by default
            arr.append(new_obj)
            matches.append(new_obj)

        # Enforce max
        if maxo is not None and len(matches) > maxo:
            # remove extras from the end
            for _ in range(len(matches) - maxo):
                victim = matches.pop()
                try:
                    arr.remove(victim)
                except ValueError:
                    pass


# -----------------------------
# ITEM_TREE.items enforcement
# -----------------------------
def ensure_item_tree_min_items(item_tree: dict, wt_node: dict):
    """
    For ITEM_TREE, ensure children with occurrences.min >= 1 exist as ELEMENT/CLUSTER in items[].
    Also ensures null_flavour exists if WT indicates it (under DV leaf typically).
    """
    if not _is_item_tree(item_tree):
        return
    items = item_tree.get("items", [])

    wt_children = wt_node.get("children") or []
    for ch in wt_children:
        occ = ch.get("occurrences") or {}
        mino = int(occ.get("min", 0) or 0)
        if mino <= 0:
            continue

        # In ITEM_TREE.items, expected rmType is ELEMENT/CLUSTER
        rm = ch.get("rmType")
        aql = ch.get("aqlPath") or ""
        nodeid, namev = _parse_selector(aql)
        if not nodeid or rm not in ("ELEMENT", "CLUSTER"):
            continue

        # find match
        found = []
        for it in items:
            if not isinstance(it, dict):
                continue
            if it.get("_type") != rm:
                continue
            if it.get("archetype_node_id") != nodeid:
                continue
            if namev is not None and _canon_name_value(it) != namev:
                continue
            found.append(it)

        while len(found) < mino:
            if rm == "ELEMENT":
                new_it = _make_minimal_element(nodeid)
            else:
                new_it = {
                    "_type": "CLUSTER",
                    "archetype_node_id": nodeid,
                    "name": {"_type": "DV_TEXT", "value": namev or nodeid},
                    "items": [],
                }
            items.append(new_it)
            found.append(new_it)

    item_tree["items"] = items


# -----------------------------
# Recursive reconciliation
# -----------------------------
def reconcile_with_wt(canon_json: dict, wt_root: dict, wt_index: dict) -> dict:
    """
    Walk canonical JSON, using WT structure to:
    - enforce COMPOSITION.category validity
    - reconcile list cardinality where WT specifies occurrences
    - ensure ITEM_TREE has required items
    - set required ACTIVITY.action_archetype_id if missing
    - apply leaf fills/jitter based on WT list inputs (but never touch name/value, ism_transition)
    """
    if not isinstance(canon_json, dict):
        return canon_json

    # Ensure category early if root
    if canon_json.get("_type") == "COMPOSITION":
        ensure_composition_category(canon_json, wt_root)

    def walk(obj: Any, wt_node: Optional[dict]):
        if isinstance(obj, dict):
            # leaf application
            if _is_dv_leaf(obj):
                return apply_leaf_value_wt(obj, wt_node)

            # ACTIVITY required attribute
            if obj.get("_type") == "ACTIVITY":
                if obj.get("action_archetype_id") in (None, ""):
                    # keep a plausible default; template may constrain further
                    obj["action_archetype_id"] = "openEHR-EHR-ACTION.procedure.v1"

            # ITEM_TREE enforcement
            if _is_item_tree(obj) and wt_node and wt_node.get("rmType") == "ITEM_TREE":
                ensure_item_tree_min_items(obj, wt_node)

            # list cardinality enforcement for known list fields
            if wt_node:
                wt_children = wt_node.get("children") or []
                for field in ("content", "items", "events", "activities"):
                    if isinstance(obj.get(field), list):
                        reconcile_list_field(obj, field, wt_children, wt_index)

            # Recurse into children
            for k, v in list(obj.items()):
                # Find matching WT child for this attribute by rmType/nodeId where possible.
                wt_child = None
                if wt_node:
                    if isinstance(v, list):
                        # list: keep same wt_node for now; list reconciliation handled above
                        wt_child = None
                    else:
                        # heuristic: match by rmType + selector nodeId if present
                        if isinstance(v, dict) and "_type" in v:
                            t = v.get("_type")
                            nid = v.get("archetype_node_id")
                            candidates = []
                            if nid and t:
                                candidates = wt_index["by_nodeid_rm"].get((nid, t), [])
                            if candidates:
                                wt_child = candidates[0]
                obj[k] = walk(v, wt_child)

            return obj

        if isinstance(obj, list):
            return [walk(x, wt_node) for x in obj]

        return obj

    return walk(canon_json, wt_root)


# -----------------------------
# EHRbase REST helpers
# -----------------------------
import re

async def create_ehr(session, url):
    payload = {
        "_type": "EHR_STATUS",
        "archetype_node_id": "openEHR-EHR-EHR_STATUS.generic.v1",
        "name": {"_type": "DV_TEXT", "value": "EHR status"},
        "subject": {"_type": "PARTY_SELF"},
        "is_queryable": True,
        "is_modifiable": True,
    }

    async with session.post(f"{url}/ehr", json=payload) as resp:
        # EHRbase sometimes returns 201 with no JSON content-type/body.
        if resp.status not in (200, 201):
            txt = await resp.text()
            raise RuntimeError(f"Create EHR failed {resp.status}: {txt}")

        # 1) Try Location header (most reliable)
        loc = resp.headers.get("Location") or resp.headers.get("location")
        if loc:
            # expected .../ehr/<ehr_id>
            m = re.search(r"/ehr/([^/?#]+)", loc)
            if m:
                return m.group(1)

        # 2) Try body as text (could be JSON or plain)
        body = await resp.text()
        if body:
            # Try JSON if it looks like JSON
            if body.lstrip().startswith("{"):
                try:
                    data = json.loads(body)
                    if isinstance(data, dict):
                        if isinstance(data.get("ehr_id"), dict) and data["ehr_id"].get("value"):
                            return data["ehr_id"]["value"]
                        if data.get("ehrId"):
                            return data["ehrId"]
                        if data.get("ehr_id"):
                            return data["ehr_id"]
                except Exception:
                    pass

            # Sometimes server returns the id as plain text
            # e.g. "b3f6..." or similar
            if len(body.strip()) >= 8 and " " not in body.strip():
                return body.strip()

        # 3) If we got here, we created it but couldn't extract the id
        raise RuntimeError("Create EHR succeeded but could not extract ehr_id (no Location header and no usable body).")

async def fetch_example_composition(session, url, template_id):
    async with session.get(f"{url}/definition/template/adl1.4/{template_id}/example") as resp:
        txt = await resp.text()
        if resp.status != 200:
            raise RuntimeError(f"Fetch example failed {resp.status}: {txt}")
        return json.loads(txt)


async def post_canonical_composition(session, url, ehr_id, composition_json):
    async with session.post(f"{url}/ehr/{ehr_id}/composition", json=composition_json) as resp:
        txt = await resp.text()
        return resp.status, txt


async def upload_all_opts(session, url, opt_dir):
    files = [f for f in os.listdir(opt_dir) if f.endswith(".opt")]
    if not files:
        print(f"[*] No OPT files in {opt_dir}")
        return
    for f in files:
        path = os.path.join(opt_dir, f)
        with open(path, "rb") as fh:
            data = fh.read()
        async with session.post(f"{url}/definition/template/adl1.4", data=data, headers={"Content-Type": "application/xml"}) as resp:
            txt = await resp.text()
            if resp.status in (200, 201, 204):
                print(f"  [+] Uploaded OPT: {f}")
            else:
                print(f"  [!] Failed OPT {f}: {resp.status} {txt}")


async def fetch_webtemplates(session, url, opt_dir, wt_dir):
    """
    Fetch webtemplates from EHRbase definition endpoint for all uploaded templates.
    This assumes you already uploaded OPTs (mode 4).
    """
    # naive approach: list OPT filenames to infer template ids
    template_ids = []
    for f in os.listdir(opt_dir):
        if f.endswith(".opt"):
            template_ids.append(os.path.splitext(f)[0])

    ok = 0
    for tid in template_ids:
        async with session.get(f"{url}/definition/template/adl1.4/{tid}") as resp:
            txt = await resp.text()
            if resp.status != 200:
                print(f"  [!] WT fetch failed {tid}: {resp.status} {txt}")
                continue
            out = os.path.join(wt_dir, f"{tid}.json")
            with open(out, "w", encoding="utf-8") as f:
                f.write(txt)
            ok += 1
            print(f"  [+] Saved WT: {tid}")
    print(f"[*] Done. Webtemplates saved: {ok}/{len(template_ids)}")


async def ensure_skeleton_cached(session, url, template_id):
    """
    Cache /example for each template as the baseline skeleton to mutate.
    """
    out = os.path.join(SKELETON_DIR, f"{template_id}.json")
    if os.path.exists(out):
        with open(out, "r", encoding="utf-8") as f:
            return json.load(f)
    example = await fetch_example_composition(session, url, template_id)
    example = strip_server_fields(example)
    with open(out, "w", encoding="utf-8") as f:
        json.dump(example, f, indent=2)
    return example


# -----------------------------
# Mode 2 helper (get template_id from canonical)
# -----------------------------
def extract_template_id_from_comp(comp):
    """
    Returns template_id string if present, else None.
    Handles comp being dict OR list (some files may wrap the composition in an array).
    """

    # If comp is a list/array, try to unwrap or find a dict item
    if isinstance(comp, list):
        # common case: [ {composition...} ]
        if len(comp) == 1 and isinstance(comp[0], dict):
            comp = comp[0]
        else:
            # try to find the first dict that has archetype_details
            found = None
            for item in comp:
                if isinstance(item, dict) and "archetype_details" in item:
                    found = item
                    break
            if found is None:
                return None
            comp = found

    # If it's still not a dict, can't extract
    if not isinstance(comp, dict):
        return None

    ad = comp.get("archetype_details") or {}
    if not isinstance(ad, dict):
        return None

    template_id = (
        (ad.get("template_id") or {}).get("value")
        if isinstance(ad.get("template_id"), dict)
        else None
    )

    return template_id


# -----------------------------
# Mode 7 helper: post examples as-is
# -----------------------------
async def post_example_as_is(session, url, template_id, ehr_id) -> Tuple[bool, str]:
    """
    Fetch /example and POST without reconciliation/jitter.
    We strip server fields that should not be re-posted.
    """
    try:
        example = await fetch_example_composition(session, url, template_id)
    except Exception as e:
        return False, f"example_fetch_failed: {e}"

    payload = strip_server_fields(copy.deepcopy(example))
    status, txt = await post_canonical_composition(session, url, ehr_id, payload)
    if status in (200, 201, 204):
        return True, "ok"
    return False, f"{status} {txt}"


# -----------------------------
# Task execution
# -----------------------------

async def run_generation_for_templates(session, url, ehr_id, template_ids, per_template, mode, concurrency=20):
    """
    Logging:
      - Prints ONE line per template, when that template has finished ALL its instances.
      - Therefore ok + fail == per_template in the printed line.
      - No per-instance spam.
    """
    sem = asyncio.Semaphore(concurrency)

    # stats[template_id] = {"ok": int, "fail": int, "first_error": str|None}
    stats = {tid: {"ok": 0, "fail": 0, "first_error": None} for tid in template_ids}
    done_count = {tid: 0 for tid in template_ids}

    printed = set()
    lock = asyncio.Lock()

    def _short_err(txt: str, status: int | None = None) -> str:
        s = (txt or "").strip().replace("\n", " ")
        s = s[:220]
        return f"{status} {s}" if status is not None else s

    async def maybe_print_template_done(template_id: str):
        async with lock:
            if template_id in printed:
                return
            if done_count[template_id] < per_template:
                return

            printed.add(template_id)
            ok = stats[template_id]["ok"]
            fail = stats[template_id]["fail"]

            # Should always add up now
            total = ok + fail
            if total != per_template:
                print(f"  [!] {template_id}: DONE ok={ok}, fail={fail} (total={total} != {per_template})")
                return

            if fail == 0:
                print(f"  [+] {template_id}: DONE ok={ok}, fail={fail}")
            else:
                err = stats[template_id]["first_error"] or ""
                print(f"  [!] {template_id}: DONE ok={ok}, fail={fail} | first_error={err}")

    async def one_instance(template_id: str):
        global processed_count
        async with sem:
            try:
                wt_root = load_wt_for_template(template_id)
                wt_index = build_wt_index(wt_root)

                skeleton = await ensure_skeleton_cached(session, url, template_id)
                payload = copy.deepcopy(skeleton)
                payload = strip_server_fields(payload)

                payload = reconcile_with_wt(payload, wt_root, wt_index)
                payload = strip_server_fields(payload)

                if mode == "1":
                    status, txt = await post_canonical_composition(session, url, ehr_id, payload)
                    if status in (200, 201, 204):
                        stats[template_id]["ok"] += 1
                    else:
                        stats[template_id]["fail"] += 1
                        if stats[template_id]["first_error"] is None:
                            stats[template_id]["first_error"] = _short_err(txt, status)
                else:
                    out_file = os.path.join(OUTPUT_DIR, f"{template_id}_{uuid.uuid4().hex[:6]}.json")
                    with open(out_file, "w", encoding="utf-8") as f:
                        json.dump(payload, f, indent=2)
                    stats[template_id]["ok"] += 1

            except Exception as e:
                stats[template_id]["fail"] += 1
                if stats[template_id]["first_error"] is None:
                    stats[template_id]["first_error"] = _short_err(str(e), None)

            finally:
                # Count this instance as done regardless of outcome
                done_count[template_id] += 1
                processed_count += 1
                await maybe_print_template_done(template_id)

    tasks = []
    for tid in template_ids:
        for _ in range(per_template):
            tasks.append(asyncio.create_task(one_instance(tid)))

    if tasks:
        await asyncio.gather(*tasks)

async def clean_up_invalid_templates(session, url, template_ids):
    """
    Clean up invalid Templates:
    - Creates a fresh EHR
    - For each template_id: POST /example as-is
    - If it fails, rename WT file:
        WT_DIR/<template_id>.json  -> WT_DIR/<template_id>.json.invalid
    - Writes a report to REPORT_DIR/broken_templates.json
    """
    print("\n[*] Initializing Patient Record (EHR_STATUS) for diagnostic run...")
    ehr_id = await create_ehr(session, url)
    print(f"[*] Diagnostic EHR ID: {ehr_id}\n")

    broken = []
    ok = 0

    for tid in template_ids:
        good, msg = await post_example_as_is(session, url, tid, ehr_id)
        if good:
            ok += 1
            print(f"  [+] Example OK   | {tid}")
            continue

        print(f"  [!] Example FAIL | {tid} | {msg}")
        broken.append({"template_id": tid, "error": msg})

        # Rename the WT file to mark it invalid
        src = os.path.join(WT_DIR, f"{tid}.json")
        dst = os.path.join(WT_DIR, f"{tid}.json.invalid")
        try:
            if os.path.exists(src):
                # If dst already exists, overwrite it (idempotent)
                if os.path.exists(dst):
                    os.remove(dst)
                os.rename(src, dst)
        except Exception as e:
            print(f"     [!] Could not rename WT file for {tid}: {e}")

    report_path = os.path.join(REPORT_DIR, "broken_templates.json")
    with open(report_path, "w", encoding="utf-8") as f:
        json.dump({"ok": ok, "failed": len(broken), "broken": broken}, f, indent=2)

    print(f"\n[*] Clean up complete. OK={ok}, INVALID={len(broken)}")
    print(f"[*] Wrote: {report_path}")

# -----------------------------
# Main
# -----------------------------
async def main():
    global processed_count

    print("--- openEHR High-Volume Generator (Hybrid /example + WT reconciliation) ---")
    print("1. API Upload (GET /example -> reconcile with WT -> POST)")
    print("2. WT-transform Existing JSONs (source_compositions -> generated_compositions)")
    print("3. Stored (GET /example -> reconcile with WT -> write to file)")
    print("4. Upload OPTs from source_models/opts to openEHR CDR")
    print("5. Create webtemplates (Fetch authoritative from EHRbase)")
    print("6. Bootstrap canonical skeleton cache (/example) for all templates in WT_DIR")
    print("7. DIAGNOSTIC: POST /example AS-IS for each template; write broken_templates.json")

    mode = input("Select Mode: ").strip()

    if mode in ("2", "3"):
        if os.path.exists(OUTPUT_DIR):
            shutil.rmtree(OUTPUT_DIR)
        os.makedirs(OUTPUT_DIR, exist_ok=True)

    start_time = time.time()

    url = None
    auth = None

    if mode in ("1", "4", "5", "6", "7"):
        # hardcoded for debugging
        # url = "http://localhost:8080/ehrbase/rest/openehr/v1"
        # user = "ehrbase"
        # pswd = "ehrbase"
        
        url = input("API URL (e.g., http://localhost:8080/ehrbase/rest/openehr/v1): ").strip().rstrip("/")
        user = input("User: ")
        pswd = input("Pass: ")
        auth = aiohttp.BasicAuth(user, pswd)

    async with aiohttp.ClientSession(auth=auth) as session:
        if mode == "4":
            await upload_all_opts(session, url, OPT_DIR)
            return

        if mode == "5":
            await fetch_webtemplates(session, url, OPT_DIR, WT_DIR)
            return

        if mode == "6":
            template_ids = list_template_ids_from_wt_dir()
            if not template_ids:
                print(f"[*] No templates found in {WT_DIR} (expected <template_id>.json). Run mode 5 first.")
                return
            print(f"[*] Bootstrapping skeleton cache for {len(template_ids)} templates...")
            ok = 0
            for tid in template_ids:
                try:
                    await ensure_skeleton_cached(session, url, tid)
                    ok += 1
                    print(f"  [+] Cached skeleton: {tid}")
                except Exception as e:
                    print(f"  [!] Failed skeleton: {tid} | {e}")
            print(f"[*] Done. Cached: {ok}/{len(template_ids)}")
            return

        if mode == "7":
            template_ids = list_template_ids_from_wt_dir()
            if not template_ids:
                print(f"[*] No valid templates found in {WT_DIR} (expected <template_id>.json).")
                return

            await clean_up_invalid_templates(session, url, template_ids)
            return

        if mode in ("1", "3"):
            template_ids = list_template_ids_from_wt_dir()
            if not template_ids:
                print(f"[*] No templates found in {WT_DIR} (expected <template_id>.json). Run mode 5 first.")
                return

            per_template = int(input(f"Templates: {len(template_ids)}. Count per template: ").strip())
            concurrency = 20
            try:
                concurrency_in = input("Concurrency (default 20): ").strip()
                if concurrency_in:
                    concurrency = max(1, int(concurrency_in))
            except Exception:
                concurrency = 20

            ehr_id = None
            if mode == "1":
                print("\n[*] Initializing Patient Record (EHR_STATUS)...")
                ehr_id = await create_ehr(session, url)
                print(f"[*] Success! Assigned new EHR ID: {ehr_id}\n")

            total = len(template_ids) * per_template
            print(f"[*] Executing {total} tasks...")
            await run_generation_for_templates(
                session, url, ehr_id, template_ids, per_template, mode, concurrency=concurrency
            )
            print(f"\n[*] Done. Processed: {processed_count} | Time: {time.time() - start_time:.2f}s")
            return

        if mode == "2":
            files = [f for f in os.listdir(COMP_DIR) if f.endswith(".json")]
            if not files:
                print(f"[*] No .json files in {COMP_DIR}")
                return
            count = int(input(f"Files: {len(files)}. Count per file: ").strip())
            print(f"[*] Generating {len(files) * count} WT-transformed compositions to {OUTPUT_DIR} ...")

            for fname in files:
                with open(os.path.join(COMP_DIR, fname), "r", encoding="utf-8") as f:
                    base_json = json.load(f)

                tid = extract_template_id_from_comp(base_json)
                if not tid:
                    print(f"[!] Skipping {fname}: cannot extract template_id (unexpected JSON shape)")
                    continue

                wt_root = load_wt_for_template(tid)
                if wt_root is None:
                    # optional: increment a skip counter here
                    continue
                wt_index = build_wt_index(wt_root)

                for _ in range(count):
                    payload = reconcile_with_wt(copy.deepcopy(base_json), wt_root, wt_index)
                    payload = strip_server_fields(payload)
                    out_file = os.path.join(OUTPUT_DIR, f"{fname.replace('.json','')}_{uuid.uuid4().hex[:6]}.json")
                    with open(out_file, "w", encoding="utf-8") as out:
                        json.dump(payload, out, indent=2)
                    processed_count += 1

            print(f"[*] Done. Processed: {processed_count} | Time: {time.time() - start_time:.2f}s")
            return

        print("[!] Unknown mode.")


if __name__ == "__main__":
    asyncio.run(main())