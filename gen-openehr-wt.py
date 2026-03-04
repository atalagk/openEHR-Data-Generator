import os
import json
import uuid
import random
import copy
import time
import asyncio
import aiohttp
import re
from datetime import datetime

# --- Configuration ---
BASE_SOURCE = "source_models"
WT_DIR = os.path.join(BASE_SOURCE, "webtemplates")
OPT_DIR = os.path.join(BASE_SOURCE, "opts")
COMP_DIR = "source_compositions"
OUTPUT_DIR = "generated_compositions"

if not os.path.exists(WT_DIR): os.makedirs(WT_DIR)
if not os.path.exists(OPT_DIR): os.makedirs(OPT_DIR)
if not os.path.exists(COMP_DIR): os.makedirs(COMP_DIR)
if not os.path.exists(OUTPUT_DIR): os.makedirs(OUTPUT_DIR)

processed_count = 0

# --- Text Jitter ---
def scramble_plain_text(text: str) -> str:
    if not text or len(text) < 4: return text
    words = text.split()
    if len(words) < 2: return text + " " + "".join(random.choices("abcdefghijklmnopqrstuvwxyz", k=3))
    idx = random.randint(0, len(words) - 1)
    words[idx] = "".join(random.sample(words[idx], len(words[idx])))
    return " ".join(words)

# --- Existing JSON Jitter ---
def jitter_existing_json(obj):
    if isinstance(obj, dict):
        if "_type" in obj:
            t = obj["_type"]
            if t == "DV_TEXT" and "value" in obj:
                obj["value"] = scramble_plain_text(obj["value"])
            elif t == "DV_DATE_TIME" and "value" in obj:
                obj["value"] = datetime.now().strftime("%Y-%m-%dT%H:%M:%S.000Z")
            elif t == "DV_QUANTITY" and "magnitude" in obj:
                obj["magnitude"] = round(obj["magnitude"] * random.uniform(0.9, 1.1), 1)
        for k, v in obj.items():
            if k not in ["archetype_node_id", "name", "archetype_details"]:
                obj[k] = jitter_existing_json(v)
    elif isinstance(obj, list):
        return [jitter_existing_json(i) for i in obj]
    return obj

# --- Dynamic Web Template Synthesis ---

class WebTemplateGenerator:
    def __init__(self, template_json, filename="unknown"):
        self.template = template_json
        self.template_id = template_json.get("templateId", filename.replace(".json", ""))
        self.base_time = datetime.now().strftime("%Y-%m-%dT%H:%M:%S.000Z")
        self.default_language = (template_json.get("defaultLanguage") or "en").lower()

    # -----------------------------
    # Helpers
    # -----------------------------
    def _get_val(self, node, key_snake):
        camel_map = {"rm_type": "rmType", "node_id": "nodeId"}
        val = node.get(key_snake)
        if val is None and key_snake in camel_map:
            val = node.get(camel_map[key_snake])
        return val

    def _get_first_aql(self, node):
        if node.get("aqlPath"):
            return node["aqlPath"]
        for c in node.get("children", []) or []:
            r = self._get_first_aql(c)
            if r:
                return r
        return ""

    def _extract_node_id(self, node, default_id="at0000"):
        nid = node.get("nodeId") or node.get("node_id")
        if nid:
            return nid
        aql = node.get("aqlPath", "") or ""
        if aql:
            aql_clean = re.sub(r"/[a-zA-Z_]+$", "", aql)
            matches = re.findall(r"\[(at[\d\.]+)\]", aql_clean)
            if matches:
                return matches[-1]
        return default_id

    def _is_locatable_name_path(self, aql: str) -> bool:
        if not aql:
            return False
        return aql.endswith("/name") or aql.endswith("/name/value")

    def _mk_loc_name(self, text: str, at_code: str):
        # DV_CODED_TEXT is DV_TEXT subtype; satisfies templates that *expect* DV_CODED_TEXT for name.
        if not at_code or not str(at_code).startswith("at"):
            at_code = "at0000"
        return {
            "_type": "DV_CODED_TEXT",
            "value": str(text or "Unknown"),
            "defining_code": {
                "_type": "CODE_PHRASE",
                "terminology_id": {"_type": "TERMINOLOGY_ID", "value": "local"},
                "code_string": str(at_code),
            },
        }

    def _repeat_count(self, node):
        """Produce at least min occurrences; otherwise 1 if allowed."""
        mx = node.get("max", 1)
        if mx == 0:
            return 0
        mn = node.get("min", 0)
        try:
            mn = int(mn or 0)
        except Exception:
            mn = 0
        if mn > 0:
            return mn
        return 1 if (mx == -1 or mx >= 1) else 0

    def _aql_is_rm_attr_leaf(self, aql: str) -> str | None:
        """
        Detect DV leaf nodes that are RM attributes, NOT ELEMENT values.
        Returns the RM attribute name if recognized.
        """
        if not aql:
            return None
        for attr in ("/time", "/width", "/math_function", "/timing", "/ism_transition", "/start_time"):
            if aql.endswith(attr):
                return attr.strip("/")
        return None

    # -----------------------------
    # Public
    # -----------------------------
    def generate(self):
        root = self.template.get("tree", self.template)
        comp = self._build_node(root)

        if not comp:
            comp = {
                "_type": "COMPOSITION",
                "archetype_node_id": self._extract_node_id(root, "openEHR-EHR-COMPOSITION.report.v1"),
                "name": self._mk_loc_name(self.template_id, "at0000"),
                "content": [],
            }

        comp["_type"] = "COMPOSITION"
        comp.setdefault("archetype_node_id", self._extract_node_id(root, "openEHR-EHR-COMPOSITION.report.v1"))
        comp.setdefault("name", self._mk_loc_name(root.get("name") or self.template_id, self._extract_node_id(root, "at0000")))

        comp["archetype_details"] = {
            "_type": "ARCHETYPED",
            "archetype_id": {"_type": "ARCHETYPE_ID", "value": comp["archetype_node_id"]},
            "template_id": {"_type": "TEMPLATE_ID", "value": self.template_id},
            "rm_version": "1.0.4",
        }

        # Only set defaults if missing (do NOT hardcode category to 433 anymore)
        comp.setdefault(
            "language",
            {
                "_type": "CODE_PHRASE",
                "terminology_id": {"_type": "TERMINOLOGY_ID", "value": "ISO_639-1"},
                "code_string": self.default_language or "en",
            },
        )
        comp.setdefault(
            "territory",
            {
                "_type": "CODE_PHRASE",
                "terminology_id": {"_type": "TERMINOLOGY_ID", "value": "ISO_3166-1"},
                "code_string": "NZ",
            },
        )
        comp.setdefault("composer", {"_type": "PARTY_IDENTIFIED", "name": "Generator-Bot"})

        # Safe default only if template did not include category
        comp.setdefault(
            "category",
            {
                "_type": "DV_CODED_TEXT",
                "value": "event",
                "defining_code": {
                    "_type": "CODE_PHRASE",
                    "terminology_id": {"_type": "TERMINOLOGY_ID", "value": "openehr"},
                    "code_string": "433",
                },
            },
        )

        comp.setdefault(
            "context",
            {
                "_type": "EVENT_CONTEXT",
                "start_time": {"_type": "DV_DATE_TIME", "value": self.base_time},
                "setting": {
                    "_type": "DV_CODED_TEXT",
                    "value": "other care",
                    "defining_code": {
                        "_type": "CODE_PHRASE",
                        "terminology_id": {"_type": "TERMINOLOGY_ID", "value": "openehr"},
                        "code_string": "238",
                    },
                },
            },
        )

        return comp

    # -----------------------------
    # Core builder
    # -----------------------------
    def _build_node(self, node):
        rm_type = self._get_val(node, "rm_type")
        if not rm_type or node.get("max") == 0:
            return None

        aql = node.get("aqlPath", "") or ""
        node_id = self._extract_node_id(node)

        # EVENT typing: detect interval vs point
        if rm_type == "EVENT":
            child_ids = {str(c.get("id", "")).lower() for c in (node.get("children", []) or [])}
            rm_type = "INTERVAL_EVENT" if ("width" in child_ids or "math_function" in child_ids) else "POINT_EVENT"

        # DV leaf
        if rm_type.startswith("DV_") or rm_type.startswith("DV_INTERVAL"):
            return self._populate_leaf(node)

        # Minimal CODE_PHRASE / PARTY
        if rm_type == "PARTY_PROXY":
            return {"_type": "PARTY_SELF"}

        if rm_type == "CODE_PHRASE":
            # language/territory/encoding
            if aql.endswith("/language"):
                return {
                    "_type": "CODE_PHRASE",
                    "terminology_id": {"_type": "TERMINOLOGY_ID", "value": "ISO_639-1"},
                    "code_string": self.default_language or "en",
                }
            if aql.endswith("/territory"):
                return {
                    "_type": "CODE_PHRASE",
                    "terminology_id": {"_type": "TERMINOLOGY_ID", "value": "ISO_3166-1"},
                    "code_string": "NZ",
                }
            if aql.endswith("/encoding"):
                return {
                    "_type": "CODE_PHRASE",
                    "terminology_id": {"_type": "TERMINOLOGY_ID", "value": "IANA_character-sets"},
                    "code_string": "UTF-8",
                }
            return {
                "_type": "CODE_PHRASE",
                "terminology_id": {"_type": "TERMINOLOGY_ID", "value": "local"},
                "code_string": "at0000",
            }

        # Base LOCATABLE
        obj = {
            "_type": rm_type,
            "archetype_node_id": node_id,
            "name": self._mk_loc_name(node.get("name", "Unknown"), node_id if str(node_id).startswith("at") else "at0000"),
        }

        # Entry-level common fields
        if rm_type in ["OBSERVATION", "EVALUATION", "INSTRUCTION", "ACTION", "ADMIN_ENTRY"]:
            obj.setdefault("subject", {"_type": "PARTY_SELF"})
            obj.setdefault(
                "language",
                {
                    "_type": "CODE_PHRASE",
                    "terminology_id": {"_type": "TERMINOLOGY_ID", "value": "ISO_639-1"},
                    "code_string": self.default_language or "en",
                },
            )
            obj.setdefault(
                "encoding",
                {
                    "_type": "CODE_PHRASE",
                    "terminology_id": {"_type": "TERMINOLOGY_ID", "value": "IANA_character-sets"},
                    "code_string": "UTF-8",
                },
            )

        # ISM_TRANSITION must satisfy invariants in some templates: try lists first
        if rm_type == "ISM_TRANSITION":
            built = {"_type": "ISM_TRANSITION"}
            for c in node.get("children", []) or []:
                cid = str(c.get("id", "")).lower()
                if cid in ("current_state", "careflow_step"):
                    v = self._populate_leaf(c)
                    if v:
                        built[cid] = v
            built.setdefault(
                "current_state",
                {
                    "_type": "DV_CODED_TEXT",
                    "value": "completed",
                    "defining_code": {
                        "_type": "CODE_PHRASE",
                        "terminology_id": {"_type": "TERMINOLOGY_ID", "value": "openehr"},
                        "code_string": "532",
                    },
                },
            )
            built.setdefault(
                "careflow_step",
                {
                    "_type": "DV_CODED_TEXT",
                    "value": "completed",
                    "defining_code": {
                        "_type": "CODE_PHRASE",
                        "terminology_id": {"_type": "TERMINOLOGY_ID", "value": "local"},
                        "code_string": "at0000",
                    },
                },
            )
            return built

        # Child handling
        children = node.get("children", []) or []

        # Buckets
        items = []
        content = []
        events = []
        activities = []
        protocol_items = []
        state_items = []
        desc_items = []

        # Direct RM attribute ids we attach as-is
        direct_ids = {
            "data", "protocol", "state", "description",
            "time", "width", "math_function", "timing",
            "ism_transition",
            "context", "category", "language", "territory", "composer", "encoding", "subject",
            "start_time", "setting"
        }

        for child in children:
            child_rm = self._get_val(child, "rm_type")
            child_id = str(child.get("id", "")).lower()
            child_aql = child.get("aqlPath", "") or ""

            # IMPORTANT FIX: only override LOCATABLE.name if the AQL is actually /name or /name/value
            if child_id == "name" and self._is_locatable_name_path(child_aql):
                nm = self._populate_leaf(child)
                if nm:
                    if nm.get("_type") == "DV_TEXT":
                        obj["name"] = self._mk_loc_name(nm.get("value", node.get("name", "Unknown")), node_id if str(node_id).startswith("at") else "at0000")
                    else:
                        obj["name"] = nm
                continue

            # Build required occurrences
            rep = self._repeat_count(child)
            for _ in range(rep):
                # RM-attribute DV leaf detection by aql suffix (prevents wrapping /time into ELEMENT etc.)
                rm_attr = self._aql_is_rm_attr_leaf(child_aql)

                # direct attach if id is known direct attr OR aql says it’s an RM attribute
                if (child_id in direct_ids) or (rm_attr in ("time", "width", "math_function", "timing", "start_time")):
                    val = self._build_node(child)
                    if val:
                        obj[child_id if child_id in direct_ids else rm_attr] = val
                    continue

                built = self._build_node(child)
                if not built:
                    continue

                btype = built.get("_type")

                # Classify by type / aql path
                if btype in ("POINT_EVENT", "INTERVAL_EVENT"):
                    events.append(built)
                elif btype == "ACTIVITY":
                    activities.append(built)
                else:
                    if "/protocol" in child_aql:
                        protocol_items.append(built)
                    elif "/state" in child_aql:
                        state_items.append(built)
                    elif "/description" in child_aql:
                        desc_items.append(built)
                    else:
                        # composition content vs items
                        if rm_type == "COMPOSITION":
                            content.append(built)
                        else:
                            items.append(built)

        # Assemble
        if rm_type == "COMPOSITION":
            obj["content"] = content

        elif rm_type in ["SECTION", "CLUSTER", "ITEM_TREE", "ITEM_TABLE", "ITEM_LIST", "ITEM_SINGLE"]:
            # keep empty only if min > 0; else drop
            if not items and (node.get("min") or 0) == 0:
                return None
            obj["items"] = items

        elif rm_type == "OBSERVATION":
            # HISTORY container ids derived from aql if possible
            hist_id = self._extract_node_id({"aqlPath": aql + "/data[at0001]"}, "at0001")  # safe
            # Build HISTORY with events if WT didn’t provide explicit HISTORY node
            if "data" not in obj:
                if not events:
                    # create one POINT_EVENT only if template clearly expects events container
                    evt_id = self._extract_node_id({"aqlPath": aql + "/data[at0001]/events[at0002]"}, "at0002")
                    tree_id = self._extract_node_id({"aqlPath": aql + f"/data[at0001]/events[{evt_id}]/data[at0003]"}, "at0003")
                    events = [{
                        "_type": "POINT_EVENT",
                        "archetype_node_id": evt_id,
                        "name": self._mk_loc_name("Any event", evt_id),
                        "time": {"_type": "DV_DATE_TIME", "value": self.base_time},
                        "data": {
                            "_type": "ITEM_TREE",
                            "archetype_node_id": tree_id,
                            "name": self._mk_loc_name("Tree", tree_id),
                            "items": items
                        }
                    }]
                obj["data"] = {
                    "_type": "HISTORY",
                    "archetype_node_id": hist_id,
                    "name": self._mk_loc_name("History", hist_id),
                    "origin": {"_type": "DV_DATE_TIME", "value": self.base_time},
                    "events": events
                }

            if protocol_items and "protocol" not in obj:
                prot_id = self._extract_node_id({"aqlPath": aql + "/protocol[at0004]"}, "at0004")
                obj["protocol"] = {
                    "_type": "ITEM_TREE",
                    "archetype_node_id": prot_id,
                    "name": self._mk_loc_name("Tree", prot_id),
                    "items": protocol_items
                }

        elif rm_type in ("POINT_EVENT", "INTERVAL_EVENT"):
            # event.data/state
            if "data" not in obj:
                tree_id = self._extract_node_id({"aqlPath": aql + "/data[at0003]"}, "at0003")
                obj["data"] = {
                    "_type": "ITEM_TREE",
                    "archetype_node_id": tree_id,
                    "name": self._mk_loc_name("Tree", tree_id),
                    "items": items
                }
            if state_items and "state" not in obj:
                st_id = self._extract_node_id({"aqlPath": aql + "/state[at0001]"}, "at0001")
                obj["state"] = {
                    "_type": "ITEM_TREE",
                    "archetype_node_id": st_id,
                    "name": self._mk_loc_name("Tree", st_id),
                    "items": state_items
                }
            obj.setdefault("time", {"_type": "DV_DATE_TIME", "value": self.base_time})

            if rm_type == "INTERVAL_EVENT":
                obj.setdefault("width", {"_type": "DV_DURATION", "value": "PT24H"})
                obj.setdefault(
                    "math_function",
                    {
                        "_type": "DV_CODED_TEXT",
                        "value": "mean",
                        "defining_code": {
                            "_type": "CODE_PHRASE",
                            "terminology_id": {"_type": "TERMINOLOGY_ID", "value": "openehr"},
                            "code_string": "146",
                        },
                    },
                )

        elif rm_type in ["EVALUATION", "ADMIN_ENTRY"]:
            if "data" not in obj:
                tree_id = self._extract_node_id({"aqlPath": aql + "/data[at0001]"}, "at0001")
                obj["data"] = {
                    "_type": "ITEM_TREE",
                    "archetype_node_id": tree_id,
                    "name": self._mk_loc_name("Tree", tree_id),
                    "items": items
                }
            if protocol_items and "protocol" not in obj:
                prot_id = self._extract_node_id({"aqlPath": aql + "/protocol[at0004]"}, "at0004")
                obj["protocol"] = {
                    "_type": "ITEM_TREE",
                    "archetype_node_id": prot_id,
                    "name": self._mk_loc_name("Tree", prot_id),
                    "items": protocol_items
                }

        elif rm_type == "ACTION":
            if "description" not in obj:
                desc_id = self._extract_node_id({"aqlPath": aql + "/description[at0001]"}, "at0001")
                obj["description"] = {
                    "_type": "ITEM_TREE",
                    "archetype_node_id": desc_id,
                    "name": self._mk_loc_name("Tree", desc_id),
                    "items": (desc_items or items)
                }
            if protocol_items and "protocol" not in obj:
                prot_id = self._extract_node_id({"aqlPath": aql + "/protocol[at0004]"}, "at0004")
                obj["protocol"] = {
                    "_type": "ITEM_TREE",
                    "archetype_node_id": prot_id,
                    "name": self._mk_loc_name("Tree", prot_id),
                    "items": protocol_items
                }
            obj.setdefault("time", {"_type": "DV_DATE_TIME", "value": self.base_time})
            obj.setdefault(
                "ism_transition",
                {
                    "_type": "ISM_TRANSITION",
                    "current_state": {
                        "_type": "DV_CODED_TEXT",
                        "value": "completed",
                        "defining_code": {
                            "_type": "CODE_PHRASE",
                            "terminology_id": {"_type": "TERMINOLOGY_ID", "value": "openehr"},
                            "code_string": "532",
                        },
                    },
                    "careflow_step": {
                        "_type": "DV_CODED_TEXT",
                        "value": "completed",
                        "defining_code": {
                            "_type": "CODE_PHRASE",
                            "terminology_id": {"_type": "TERMINOLOGY_ID", "value": "local"},
                            "code_string": "at0000",
                        },
                    },
                },
            )

        elif rm_type == "INSTRUCTION":
            # Use WT-derived activities if any; else keep empty unless min forces it (then add minimal)
            if activities:
                obj["activities"] = activities
            else:
                # only create fallback if INSTRUCTION node min suggests it's required (rare, but happens)
                if (node.get("min") or 0) > 0:
                    obj["activities"] = [{
                        "_type": "ACTIVITY",
                        "archetype_node_id": "at0001",
                        "name": self._mk_loc_name("Activity", "at0001"),
                        "description": {
                            "_type": "ITEM_TREE",
                            "archetype_node_id": "at0002",
                            "name": self._mk_loc_name("Tree", "at0002"),
                            "items": items
                        },
                        "timing": {"_type": "DV_PARSABLE", "value": "PT24H", "formalism": "ISO8601"},
                        "action_archetype_id": ".*"
                    }]
            obj.setdefault("narrative", {"_type": "DV_TEXT", "value": "Clinical instruction"})

        elif rm_type == "ACTIVITY":
            obj.setdefault("timing", {"_type": "DV_PARSABLE", "value": "PT24H", "formalism": "ISO8601"})
            obj.setdefault("action_archetype_id", ".*")
            if "description" not in obj:
                did = self._extract_node_id({"aqlPath": aql + "/description[at0001]"}, "at0001")
                obj["description"] = {
                    "_type": "ITEM_TREE",
                    "archetype_node_id": did,
                    "name": self._mk_loc_name("Tree", did),
                    "items": items
                }

        # ELEMENT must have value or null_flavour
        if rm_type == "ELEMENT":
            if not obj.get("value"):
                obj.pop("value", None)
                obj.setdefault(
                    "null_flavour",
                    {
                        "_type": "DV_CODED_TEXT",
                        "value": "no information",
                        "defining_code": {
                            "_type": "CODE_PHRASE",
                            "terminology_id": {"_type": "TERMINOLOGY_ID", "value": "openehr"},
                            "code_string": "271",
                        },
                    },
                )

        return obj

    # -----------------------------
    # Leaf generator
    # -----------------------------
    def _populate_leaf(self, node):
        rm_type = self._get_val(node, "rm_type")
        if not rm_type:
            return None
        inputs = node.get("inputs", []) or []

        # DV_INTERVAL<T>
        if rm_type.startswith("DV_INTERVAL"):
            inner = "DV_QUANTITY"
            if "<" in rm_type and ">" in rm_type:
                inner = rm_type.split("<", 1)[1].split(">", 1)[0].strip() or "DV_QUANTITY"
            bound = self._populate_leaf({"rm_type": inner, "inputs": inputs, "name": node.get("name")})
            if not bound:
                return None
            return {
                "_type": "DV_INTERVAL",
                "lower": bound,
                "upper": copy.deepcopy(bound),
                "lower_included": True,
                "upper_included": True,
            }

        # Upgrade DV_TEXT→DV_CODED_TEXT if there’s a list/terminology
        if rm_type == "DV_TEXT":
            if any(i.get("list") or i.get("terminology") for i in inputs):
                rm_type = "DV_CODED_TEXT"

        # DV_CODED_TEXT
        if rm_type == "DV_CODED_TEXT":
            for inp in inputs:
                lst = inp.get("list") or []
                if lst:
                    valid = [c for c in lst if str(c.get("value")) not in ["253", "271", "272", "273", "unknown", "no information"]]
                    if valid:
                        c = random.choice(valid)
                        lbl = c.get("label") or c.get("localizedName") or c.get("value") or "Unknown"
                        term = str(inp.get("terminology") or c.get("terminology") or "local")
                        return {
                            "_type": "DV_CODED_TEXT",
                            "value": str(lbl),
                            "defining_code": {
                                "_type": "CODE_PHRASE",
                                "terminology_id": {"_type": "TERMINOLOGY_ID", "value": term},
                                "code_string": str(c.get("value")),
                            },
                        }
            # IMPORTANT FIX: do NOT emit code_string=0000 (causes "does not match any option")
            return None

        # DV_ORDINAL
        if rm_type == "DV_ORDINAL":
            for inp in inputs:
                lst = inp.get("list") or []
                if lst:
                    valid = [c for c in lst if str(c.get("value")) not in ["253", "271", "272", "273", "unknown", "no information"]]
                    if valid:
                        c = random.choice(valid)
                        term = str(inp.get("terminology") or c.get("terminology") or "local")
                        return {
                            "_type": "DV_ORDINAL",
                            "value": int(c.get("ordinal", 0)),
                            "symbol": {
                                "_type": "DV_CODED_TEXT",
                                "value": str(c.get("label") or c.get("localizedName") or "Unknown"),
                                "defining_code": {
                                    "_type": "CODE_PHRASE",
                                    "terminology_id": {"_type": "TERMINOLOGY_ID", "value": term},
                                    "code_string": str(c.get("value")),
                                },
                            },
                        }
            return None

        # DV_QUANTITY
        if rm_type == "DV_QUANTITY":
            rng = next((i for i in inputs if (i.get("suffix") in (None, "", "magnitude"))), {}).get("validation", {}).get("range", {}) or {}
            min_v = float(rng.get("min", 1.0))
            max_v = float(rng.get("max", min_v + 10.0))
            if max_v < min_v:
                max_v = min_v + 10.0

            unit_inp = next((i for i in inputs if i.get("suffix") == "unit"), {})
            if unit_inp.get("list"):
                units = str(random.choice(unit_inp["list"]).get("value"))
            else:
                # if units are constrained but we can’t see them, better return None than "1" (causes "must be kg" etc.)
                if any(i.get("suffix") == "unit" for i in inputs):
                    return None
                units = "1"

            return {"_type": "DV_QUANTITY", "magnitude": round(random.uniform(min_v, max_v), 1), "units": units}

        # DV_DURATION (try to respect constraints if list exists; else PT24H)
        if rm_type == "DV_DURATION":
            # if any input has a list of durations, use it
            for inp in inputs:
                lst = inp.get("list") or []
                if lst:
                    v = random.choice(lst).get("value")
                    if v:
                        return {"_type": "DV_DURATION", "value": str(v)}
            return {"_type": "DV_DURATION", "value": "PT24H"}

        # DV_DATE_TIME / DV_DATE / DV_TIME
        if rm_type == "DV_DATE_TIME":
            return {"_type": "DV_DATE_TIME", "value": datetime.now().strftime("%Y-%m-%dT%H:%M:%S.000Z")}
        if rm_type == "DV_DATE":
            return {"_type": "DV_DATE", "value": datetime.now().strftime("%Y-%m-%d")}
        if rm_type == "DV_TIME":
            return {"_type": "DV_TIME", "value": datetime.now().strftime("%H:%M:%S")}

        # DV_TEXT
        if rm_type == "DV_TEXT":
            return {"_type": "DV_TEXT", "value": scramble_plain_text(f"Clinical record for {node.get('name', 'item')}")}

        # DV_BOOLEAN
        if rm_type == "DV_BOOLEAN":
            return {"_type": "DV_BOOLEAN", "value": random.choice([True, False])}

        # DV_COUNT
        if rm_type == "DV_COUNT":
            rng = next((i for i in inputs if not i.get("suffix")), {}).get("validation", {}).get("range", {}) or {}
            min_v = int(rng.get("min", 1))
            max_v = int(rng.get("max", min_v + 10))
            if max_v < min_v:
                max_v = min_v + 10
            return {"_type": "DV_COUNT", "magnitude": random.randint(min_v, max_v)}

        # DV_IDENTIFIER
        if rm_type == "DV_IDENTIFIER":
            return {
                "_type": "DV_IDENTIFIER",
                "id": f"ID-{uuid.uuid4().hex[:8].upper()}",
                "issuer": "GEN",
                "assigner": "SYS",
                "type": "SERIAL",
            }

        # DV_URI / DV_EHR_URI
        if rm_type in ("DV_URI", "DV_EHR_URI"):
            return {"_type": rm_type, "value": f"ehr://{uuid.uuid4()}"}

        # DV_MULTIMEDIA
        if rm_type == "DV_MULTIMEDIA":
            return {
                "_type": "DV_MULTIMEDIA",
                "media_type": {
                    "_type": "CODE_PHRASE",
                    "terminology_id": {"_type": "TERMINOLOGY_ID", "value": "IANA_media-types"},
                    "code_string": "text/plain",
                },
                "size": random.randint(1024, 2048),
                "data": "SGVsbG8gT3BlbkVIUg==",
            }

        # DV_PARSABLE
        if rm_type == "DV_PARSABLE":
            return {"_type": "DV_PARSABLE", "value": "Generated parseable string", "formalism": "text/plain"}

        return None

# --- Task Execution Engine ---
async def process_task(session, payload, template_id, mode, url, ehrs):
    global processed_count
    if not payload: return

    if mode == '1':
        ehr_id = random.choice(ehrs)
        headers = {'Prefer': 'return=representation', 'Accept': 'application/json', 'Content-Type': 'application/json'}
        try:
            async with session.post(f"{url}/ehr/{ehr_id}/composition", headers=headers, json=payload) as r:
                resp_text = await r.text()
                if r.status in [200, 201, 204]:
                    print(f"  [+] Success (201) | Template: {template_id}")
                else:
                    print(f"  [!] API Error {r.status} | Template: {template_id}\n      {resp_text}")
        except Exception as e:
            print(f"  [!] Request failed: {e}")
            
    elif mode in ['2', '3']:
        out_file = os.path.join(OUTPUT_DIR, f"{template_id}_{uuid.uuid4().hex[:6]}.json")
        with open(out_file, 'w', encoding='utf-8') as f:
            json.dump(payload, f, indent=2)

    processed_count += 1

# --- Helpers: OPT Upload & WebTemplate Fetch ---
async def upload_all_opts(session, url, opt_dir):
    if not os.path.exists(opt_dir):
        print(f"[*] Directory {opt_dir} not found.")
        return
    opt_files = [f for f in os.listdir(opt_dir) if f.endswith(".opt")]
    if not opt_files:
        print(f"[*] No .opt files found in {opt_dir}.")
        return
        
    print(f"\n[*] Found {len(opt_files)} OPT files. Uploading templates to EHRbase...")
    for opt_file in opt_files:
        opt_path = os.path.join(opt_dir, opt_file)
        with open(opt_path, 'r', encoding='utf-8') as f:
            opt_xml = f.read()
        
        headers = {'Content-Type': 'application/xml', 'Accept': 'application/xml'}
        try:
            async with session.post(f"{url}/definition/template/adl1.4", headers=headers, data=opt_xml.encode('utf-8')) as r:
                if r.status in [200, 201, 204]:
                    print(f"  [+] Uploaded: {opt_file}")
                elif r.status == 409:
                    print(f"  [~] Already exists: {opt_file}")
                else:
                    print(f"  [!] Failed {opt_file} (Status {r.status}): {await r.text()}")
        except Exception as e:
            print(f"  [!] Connection error uploading {opt_file}: {e}")

async def fetch_webtemplates(session, url, opt_dir, wt_dir):
    if not os.path.exists(wt_dir): os.makedirs(wt_dir)
    opt_files = [f for f in os.listdir(opt_dir) if f.endswith(".opt")]
    
    print(f"\n[*] Extracting Template IDs and fetching authoritative Web Templates from EHRbase...")
    for opt_file in opt_files:
        with open(os.path.join(opt_dir, opt_file), 'r', encoding='utf-8') as f:
            content = f.read()
        
        match = re.search(r'<template_id>\s*<value>(.*?)</value>', content)
        if not match: continue
        template_id = match.group(1)
        
        # openEHR REST API specifies 'application/openehr.wt+json' for Web Templates
        headers = {'Accept': 'application/openehr.wt+json'}
        try:
            async with session.get(f"{url}/definition/template/adl1.4/{template_id}", headers=headers) as r:
                if r.status == 200:
                    wt_json = await r.json()
                    wt_path = os.path.join(wt_dir, f"{template_id}.json")
                    with open(wt_path, 'w', encoding='utf-8') as out_f:
                        json.dump(wt_json, out_f, indent=2)
                    print(f"  [+] Saved WebTemplate: {template_id}.json")
                else:
                    print(f"  [!] Failed to fetch WT for {template_id} (Status {r.status})")
        except Exception as e:
            print(f"  [!] Connection error fetching {template_id}: {e}")

# --- Main Runtime ---
async def main():
    print("--- Unified openEHR RM Synthesizer ---")
    print("1. API Upload (Synthesize Compositions to EHRbase)")
    print("2. Jitter Existing JSONs")
    print("3. Stored (Source Webtemplates to local JSON)")
    print("4. Upload OPTs from source_models/opts to openEHR CDR")
    print("5. Create webtemplates (Fetch authoritative from EHRbase)")
    
    mode = input("Select Mode: ").strip()

    if mode in ['2', '3']:
        if os.path.exists(OUTPUT_DIR): shutil.rmtree(OUTPUT_DIR)
        os.makedirs(OUTPUT_DIR)

    tasks, start_time = [], time.time()
    url, ehrs = None, []

    async with aiohttp.ClientSession() as session:
        if mode in ['1', '4', '5']:
            #url = input("API URL (e.g., http://localhost:8080/ehrbase/rest/openehr/v1): ").strip().rstrip("/")
            #user = input("User: ")
            #pswd = input("Pass: ")
            
            url = "http://localhost:8080/ehrbase/rest/openehr/v1"
            user = "ehrbase"
            pswd = "ehrbase"
            
            session._auth = aiohttp.BasicAuth(user, pswd)

        if mode == '4':
            await upload_all_opts(session, url, OPT_DIR)
            return

        elif mode == '5':
            await fetch_webtemplates(session, url, OPT_DIR, WT_DIR)
            return

        elif mode == '1':
            print("\n[*] Initializing Patient Record (EHR_STATUS)...")
            try:
                headers = {'Prefer': 'return=representation', 'Accept': 'application/json', 'Content-Type': 'application/json'}
                ehr_payload = {"_type": "EHR_STATUS", "archetype_node_id": "openEHR-EHR-EHR_STATUS.generic.v1", "name": {"_type": "DV_TEXT", "value": "EHR Status"}, "subject": {"_type": "PARTY_SELF"}, "is_queryable": True, "is_modifiable": True}
                async with session.post(f"{url}/ehr", headers=headers, json=ehr_payload) as r:
                    if r.status in [200, 201]:
                        ehr_data = await r.json()
                        valid_ehr_id = ehr_data.get("ehr_id", {}).get("value")
                        ehrs = [valid_ehr_id]
                        print(f"[*] Success! Assigned new EHR ID: {valid_ehr_id}\n")
                    else:
                        print(f"[!] Critical Failure: Server rejected EHR creation. Status: {r.status}")
                        return 
            except Exception as e:
                print(f"[!] Connection failed: {e}")
                return

            files = [f for f in os.listdir(WT_DIR) if f.endswith(".json")]
            count = int(input("Count per template: "))
            for f in files:
                with open(os.path.join(WT_DIR, f), 'r', encoding='utf-8') as file:
                    gen = WebTemplateGenerator(json.load(file), f)
                    for i in range(count):
                        tasks.append(process_task(session, gen.generate(), gen.template_id, mode, url, ehrs))
                    
        elif mode == '2':
            files = [f for f in os.listdir(COMP_DIR) if f.endswith(".json")]
            count = int(input(f"Files: {len(files)}. Count per file: "))
            for f in files:
                with open(os.path.join(COMP_DIR, f), 'r', encoding='utf-8') as file:
                    base_json = json.load(file)
                    for i in range(count): tasks.append(process_task(None, jitter_existing_json(copy.deepcopy(base_json)), f.replace('.json',''), mode, None, None))
                    
        elif mode == '3':
            files = [f for f in os.listdir(WT_DIR) if f.endswith(".json")]
            count = int(input(f"Templates: {len(files)}. Count per template: "))
            for f in files:
                with open(os.path.join(WT_DIR, f), 'r', encoding='utf-8') as file:
                    gen = WebTemplateGenerator(json.load(file), f)
                    for i in range(count): tasks.append(process_task(None, gen.generate(), gen.template_id, mode, None, None))

        if tasks:
            print(f"[*] Executing {len(tasks)} tasks...")
            await asyncio.gather(*tasks)
            print(f"\n[*] Done. Processed: {processed_count} | Time: {time.time()-start_time:.2f}s")

if __name__ == "__main__":
    asyncio.run(main())