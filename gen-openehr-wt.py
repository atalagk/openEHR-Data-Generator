import os
import json
import random
import asyncio
import aiohttp
import uuid
import copy
import string
import time
import shutil
import re
from datetime import datetime, timedelta

import nltk
from nltk.corpus import wordnet

# --- NLTK Setup ---
nltk.download('punkt_tab', quiet=True)
nltk.download('averaged_perceptron_tagger_eng', quiet=True)
nltk.download('wordnet', quiet=True)

# --- Configuration ---
MAX_CONCURRENT_REQUESTS = 50
semaphore = asyncio.Semaphore(MAX_CONCURRENT_REQUESTS)
BASE_SOURCE = "source_models"
COMP_DIR = os.path.join(BASE_SOURCE, "compositions")
WT_DIR = os.path.join(BASE_SOURCE, "webtemplates")
OUTPUT_DIR = os.path.join("dist", "compositions")

processed_count = 0

# ---------------- Logic: NLP & Jitter ----------------

def scramble_plain_text(text):
    if not text or not isinstance(text, str): return text
    try:
        words = nltk.word_tokenize(text)
        tagged = nltk.pos_tag(words)
        new_words = []
        for word, tag in tagged:
            if (tag.startswith('NN') or tag.startswith('JJ')) and word not in string.punctuation:
                syns = {l.name().replace('_', ' ') for s in wordnet.synsets(word) for l in s.lemmas()}
                new_words.append(random.choice(list(syns)) if syns and random.random() < 0.3 else word)
            else: new_words.append(word)
        return " ".join(new_words).replace(" ,", ",").replace(" .", ".")
    except: return text

def jitter_existing_json(node):
    if isinstance(node, dict):
        t = node.get("_type")
        if t == "DV_TEXT" and "value" in node:
            node["value"] = scramble_plain_text(node["value"])
        elif t == "DV_QUANTITY" and "magnitude" in node:
            node["magnitude"] = round(float(node["magnitude"]) * random.uniform(0.85, 1.15), 2)
        elif t == "DV_DATE_TIME" and "value" in node:
            dt = datetime.now() - timedelta(days=random.randint(0, 730))
            node["value"] = dt.strftime("%Y-%m-%dT%H:%M:%S.000Z")
        for v in node.values(): jitter_existing_json(v)
    elif isinstance(node, list):
        for v in node: jitter_existing_json(v)
    return node

# ---------------- Logic: Dynamic Web Template Synthesis ----------------

class WebTemplateGenerator:
    def __init__(self, template_json, filename="unknown"):
        self.template = template_json
        self.template_id = template_json.get("templateId", filename.replace(".json", ""))
        self.base_time = datetime.now().strftime("%Y-%m-%dT%H:%M:%S.000Z")

    def _get_val(self, node, key_snake):
        camel_map = {"rm_type": "rmType", "node_id": "nodeId"}
        val = node.get(key_snake)
        if val is None and key_snake in camel_map:
            val = node.get(camel_map[key_snake])
        return val

    def _get_first_aql(self, node):
        if node.get("aqlPath"): return node["aqlPath"]
        for c in node.get("children", []):
            res = self._get_first_aql(c)
            if res: return res
        return ""

    def _extract_aql_node_id(self, children, pattern):
        for child in children:
            aql = self._get_first_aql(child)
            if aql:
                matches = re.findall(pattern, aql)
                if matches: return matches[0]
        return None

    def generate(self):
        root = self.template.get("tree", self.template)
        comp = self._build_node(root)
        
        comp["_type"] = "COMPOSITION"
        comp["archetype_details"] = {
            "_type": "ARCHETYPED",
            "archetype_id": {"_type": "ARCHETYPE_ID", "value": comp.get("archetype_node_id", "openEHR-EHR-COMPOSITION.report.v1")},
            "rm_version": "1.0.2"
        }
        comp["language"] = {"_type": "CODE_PHRASE", "terminology_id": {"_type": "TERMINOLOGY_ID", "value": "ISO_639-1"}, "code_string": "en"}
        comp["territory"] = {"_type": "CODE_PHRASE", "terminology_id": {"_type": "TERMINOLOGY_ID", "value": "ISO_3166-1"}, "code_string": "NZ"}
        comp["category"] = {"_type": "DV_CODED_TEXT", "value": "event", "defining_code": {"_type": "CODE_PHRASE", "terminology_id": {"_type": "TERMINOLOGY_ID", "value": "openehr"}, "code_string": "433"}}
        comp["composer"] = {"_type": "PARTY_IDENTIFIED", "name": "Generator-Bot"}
        
        if "context" not in comp:
            comp["context"] = {
                "_type": "EVENT_CONTEXT",
                "start_time": {"_type": "DV_DATE_TIME", "value": self.base_time},
                "setting": {"_type": "DV_CODED_TEXT", "value": "other care", "defining_code": {"_type": "CODE_PHRASE", "terminology_id": {"_type": "TERMINOLOGY_ID", "value": "openehr"}, "code_string": "238"}}
            }
        return comp

    def _build_node(self, node):
        rm_type = self._get_val(node, "rm_type")
        if not rm_type or node.get("max") == 0: return None
        
        if rm_type == "EVENT": rm_type = "POINT_EVENT"
        
        node_id_key = node.get("id", "").lower()
        if node_id_key in ["language", "encoding", "subject"]: return None

        obj = {
            "_type": rm_type,
            "name": {"_type": "DV_TEXT", "value": node.get("name", "Unknown")},
            "archetype_node_id": self._get_val(node, "node_id") or "at0000"
        }

        if rm_type in ["OBSERVATION", "EVALUATION", "INSTRUCTION", "ACTION", "ADMIN_ENTRY"]:
            obj["language"] = {"_type": "CODE_PHRASE", "terminology_id": {"_type": "TERMINOLOGY_ID", "value": "ISO_639-1"}, "code_string": "en"}
            obj["encoding"] = {"_type": "CODE_PHRASE", "terminology_id": {"_type": "TERMINOLOGY_ID", "value": "IANA_character-sets"}, "code_string": "UTF-8"}
            obj["subject"] = {"_type": "PARTY_SELF"}

        if rm_type.startswith("DV_"): return self._populate_leaf(node)

        children = node.get("children", [])
        
        # 1. Strict Bucketing System
        content_list, items_list, events_list, activities_list = [], [], [], []
        
        for child in children:
            child_rm = self._get_val(child, "rm_type")
            child_id = child.get("id", "").lower()
            if child_id in ["language", "encoding", "subject"]: continue
            
            child_built = self._build_node(child)
            if not child_built: continue

            c_type = child_built.get("_type")
            
            # Bucketing Logic
            if c_type in ["POINT_EVENT", "INTERVAL_EVENT"]:
                child_built["time"] = {"_type": "DV_DATE_TIME", "value": self.base_time}
                if c_type == "INTERVAL_EVENT":
                    child_built["width"] = {"_type": "DV_DURATION", "value": "PT1H"}
                    child_built["math_function"] = {"_type": "DV_CODED_TEXT", "value": "mean", "defining_code": {"_type": "CODE_PHRASE", "terminology_id": {"_type": "TERMINOLOGY_ID", "value": "openehr"}, "code_string": "146"}}
                events_list.append(child_built)
                
            elif c_type == "ACTIVITY":
                activities_list.append(child_built)
                
            elif c_type in ["SECTION", "OBSERVATION", "EVALUATION", "INSTRUCTION", "ACTION", "ADMIN_ENTRY", "GENERIC_ENTRY"]:
                content_list.append(child_built)
                
            elif c_type in ["CLUSTER", "ELEMENT"]:
                items_list.append(child_built)
                
            elif c_type.startswith("DV_"):
                # 2. Automatically wrap stray DV_ nodes in ELEMENTs
                if rm_type == "ELEMENT":
                    obj["value"] = child_built
                else:
                    items_list.append({
                        "_type": "ELEMENT",
                        "name": {"_type": "DV_TEXT", "value": child.get("name", "Unknown")},
                        "archetype_node_id": self._get_val(child, "node_id") or "at0000",
                        "value": child_built
                    })

        # 3. Secure RM Type Assembly
        if rm_type == "COMPOSITION":
            # Emergency wrapper if elements exist but no observations were found
            if items_list and not content_list:
                content_list = [{
                    "_type": "OBSERVATION", "name": {"_type": "DV_TEXT", "value": "Generated Observation"}, "archetype_node_id": "at0000",
                    "language": {"_type": "CODE_PHRASE", "terminology_id": {"_type": "TERMINOLOGY_ID", "value": "ISO_639-1"}, "code_string": "en"},
                    "encoding": {"_type": "CODE_PHRASE", "terminology_id": {"_type": "TERMINOLOGY_ID", "value": "IANA_character-sets"}, "code_string": "UTF-8"},
                    "subject": {"_type": "PARTY_SELF"},
                    "data": {"_type": "HISTORY", "name": {"_type": "DV_TEXT", "value": "History"}, "archetype_node_id": "at0001", "origin": {"_type": "DV_DATE_TIME", "value": self.base_time},
                             "events": [{"_type": "POINT_EVENT", "name": {"_type": "DV_TEXT", "value": "Event"}, "archetype_node_id": "at0002", "time": {"_type": "DV_DATE_TIME", "value": self.base_time},
                                         "data": {"_type": "ITEM_TREE", "archetype_node_id": "at0003", "name": {"_type": "DV_TEXT", "value": "Tree"}, "items": items_list}}]}
                }]
            obj["content"] = content_list
            
        elif rm_type == "SECTION":
            obj["items"] = content_list
            
        elif rm_type in ["CLUSTER", "ITEM_TREE", "ITEM_LIST", "ITEM_TABLE", "ITEM_SINGLE"]:
            obj["items"] = items_list
            
        elif rm_type == "OBSERVATION":
            hist_node = self._extract_aql_node_id(children, r'/data\[(at\d+)\]') or "at0001"
            if not events_list:
                events_list = [{"_type": "POINT_EVENT", "name": {"_type": "DV_TEXT", "value": "Any event"}, "archetype_node_id": "at0002", "time": {"_type": "DV_DATE_TIME", "value": self.base_time}, "data": {"_type": "ITEM_TREE", "archetype_node_id": "at0003", "name": {"_type": "DV_TEXT", "value": "Tree"}, "items": items_list}}]
            obj["data"] = {"_type": "HISTORY", "name": {"_type": "DV_TEXT", "value": "History"}, "archetype_node_id": hist_node, "origin": {"_type": "DV_DATE_TIME", "value": self.base_time}, "events": events_list}

        elif rm_type in ["POINT_EVENT", "INTERVAL_EVENT", "EVALUATION", "ADMIN_ENTRY"]:
            # Events default to at0003 for data, Evaluations default to at0001
            fallback_node = "at0003" if rm_type.endswith("EVENT") else "at0001"
            tree_node = self._extract_aql_node_id(children, r'/data\[(at\d+)\]') or fallback_node
            
            obj["data"] = {
                "_type": "ITEM_TREE", 
                "name": {"_type": "DV_TEXT", "value": "Tree"}, 
                "archetype_node_id": tree_node, 
                "items": items_list
            }            
            
        elif rm_type == "ACTION":
            desc_node = self._extract_aql_node_id(children, r'/description\[(at\d+)\]') or "at0001"
            obj["description"] = {"_type": "ITEM_TREE", "archetype_node_id": desc_node, "name": {"_type": "DV_TEXT", "value": "Tree"}, "items": items_list}
            obj["ism_transition"] = {"_type": "ISM_TRANSITION", "current_state": {"_type": "DV_CODED_TEXT", "value": "completed", "defining_code": {"_type": "CODE_PHRASE", "terminology_id": {"_type": "TERMINOLOGY_ID", "value": "openehr"}, "code_string": "245"}}}
            obj["time"] = {"_type": "DV_DATE_TIME", "value": self.base_time}
            
        elif rm_type == "ACTIVITY":
            obj["description"] = {"_type": "ITEM_TREE", "name": {"_type": "DV_TEXT", "value": "Tree"}, "archetype_node_id": "at0001", "items": items_list}
            obj["timing"] = {"_type": "DV_PARSABLE", "value": "PT24H", "formalism": "ISO8601"}
            obj["action_archetype_id"] = ".*"
            
        elif rm_type == "INSTRUCTION":
            if not activities_list:
                activities_list = [{"_type": "ACTIVITY", "name": {"_type": "DV_TEXT", "value": "Generated Activity"}, "archetype_node_id": "at0001", "description": {"_type": "ITEM_TREE", "name": {"_type": "DV_TEXT", "value": "Tree"}, "archetype_node_id": "at0002", "items": items_list}, "timing": {"_type": "DV_PARSABLE", "value": "PT24H", "formalism": "ISO8601"}, "action_archetype_id": ".*"}]
            obj["activities"] = activities_list
            obj["narrative"] = {"_type": "DV_TEXT", "value": "Generated narrative"}

        return obj

    def _populate_leaf(self, node):
        rm_type = self._get_val(node, "rm_type")
        inputs = node.get("inputs", [])
        obj = {"_type": rm_type}

        if rm_type.startswith("DV_INTERVAL<"):
            inner_type = rm_type[12:-1] 
            obj["lower"] = self._populate_leaf({"rm_type": inner_type, "inputs": inputs, "name": node.get("name")})
            obj["upper"] = self._populate_leaf({"rm_type": inner_type, "inputs": inputs, "name": node.get("name")})
            obj["lower_included"], obj["upper_included"] = True, True
            return obj

        if rm_type == "DV_CODED_TEXT":
            for inp in inputs:
                if "list" in inp and inp["list"]:
                    choice = random.choice(inp["list"])
                    obj["value"] = choice.get("label")
                    obj["defining_code"] = {"_type": "CODE_PHRASE", "terminology_id": {"_type": "TERMINOLOGY_ID", "value": inp.get("terminology", "local")}, "code_string": str(choice.get("value"))}
                    return obj
            obj["value"], obj["defining_code"] = "N/A", {"_type": "CODE_PHRASE", "terminology_id": {"_type": "TERMINOLOGY_ID", "value": "local"}, "code_string": "000"}

        elif rm_type == "DV_ORDINAL":
            for inp in inputs:
                if "list" in inp and inp["list"]:
                    choice = random.choice(inp["list"])
                    raw_val = choice.get("value")
                    try:
                        obj["value"] = int(raw_val)
                    except:
                        obj["value"] = int(choice.get("ordinal", 0))
                    obj["symbol"] = {"_type": "DV_CODED_TEXT", "value": choice.get("label", "N/A"), "defining_code": {"_type": "CODE_PHRASE", "terminology_id": {"_type": "TERMINOLOGY_ID", "value": "local"}, "code_string": str(raw_val)}}
                    return obj

        elif rm_type == "DV_QUANTITY":
            mag_input = next((i for i in inputs if i.get("suffix") == "magnitude"), {})
            v_range = mag_input.get("validation", {}).get("range", {})
            min_v, max_v = v_range.get("min", 0.0), v_range.get("max", 1000.0)
            obj["magnitude"] = round(random.uniform(min_v, max_v), 2)
            unit_inp = next((i for i in inputs if i.get("suffix") == "unit"), {})
            obj["units"] = random.choice(unit_inp["list"]).get("value") if unit_inp.get("list") else "1"

        elif rm_type == "DV_PROPORTION":
            num_input = next((i for i in inputs if i.get("suffix") == "numerator"), {})
            v_range = num_input.get("validation", {}).get("range", {})
            min_v, max_v = v_range.get("min", 0.0), v_range.get("max", 100.0)
            obj["numerator"] = round(random.uniform(min_v, max_v), 1)
            den_input = next((i for i in inputs if i.get("suffix") == "denominator"), {})
            d_range = den_input.get("validation", {}).get("range", {})
            obj["denominator"] = d_range.get("min", 100.0)
            p_types = node.get("proportionTypes", ["percent"])
            obj["type"] = 1 if "percent" in p_types else 2

        elif rm_type == "DV_TIME":
            obj["value"] = datetime.now().strftime("%H:%M:%S.000Z")
        elif rm_type == "DV_DURATION":
            obj["value"] = f"PT{random.randint(15, 60)}M"
        elif rm_type in ["DV_URI", "DV_EHR_URI"]:
            obj["value"] = f"ehr://{uuid.uuid4()}"
        elif rm_type == "DV_MULTIMEDIA":
            obj["media_type"] = {"_type": "CODE_PHRASE", "terminology_id": {"_type": "TERMINOLOGY_ID", "value": "IANA_media-types"}, "code_string": "text/plain"}
            obj["size"] = random.randint(1024, 2048)
            obj["data"] = "SGVsbG8gT3BlbkVIUg==" 
        elif rm_type == "DV_PARSABLE":
            obj["value"] = "Generated parseable string"
            obj["formalism"] = "text/plain"
        elif rm_type == "DV_IDENTIFIER":
            obj["id"], obj["issuer"], obj["assigner"], obj["type"] = f"ID-{uuid.uuid4().hex[:8].upper()}", "GEN", "SYS", "SERIAL"
        elif rm_type == "DV_BOOLEAN":
            obj["value"] = random.choice([True, False])
        elif rm_type == "DV_COUNT":
            v_range = next((i for i in inputs if not i.get("suffix")), {}).get("validation", {}).get("range", {})
            min_v, max_v = v_range.get("min", 0), v_range.get("max", 10)
            obj["magnitude"] = random.randint(int(min_v), int(max_v))
        elif rm_type == "DV_TEXT":
            obj["value"] = scramble_plain_text(f"Clinical record for {node.get('name')}")
        elif rm_type in ["DV_DATE_TIME", "DV_DATE"]:
            obj["value"] = datetime.now().strftime("%Y-%m-%d" if rm_type == "DV_DATE" else "%Y-%m-%dT%H:%M:%S.000Z")

        return obj

# ---------------- Execution Shell ----------------

async def process_task(session, comp_json, name, mode, url, ehrs):
    global processed_count
    if mode in ['2', '3']:
        path = os.path.join(OUTPUT_DIR, f"{name}_{uuid.uuid4().hex[:4]}.json")
        with open(path, 'w', encoding='utf-8') as f:
            json.dump(comp_json, f, indent=2, ensure_ascii=False)
    else:
        async with semaphore:
            target = random.choice(ehrs)
            headers = {'Content-type': 'application/json', 'Prefer': 'return=representation'}
            try:
                async with session.post(f"{url}/ehr/{target}/composition", json=comp_json, headers=headers) as r:
                    if r.status % 100 != 2: print(f"API Error {r.status}: {await r.text()}")
            except Exception as e: print(f"Upload failed: {e}")
    
    processed_count += 1
    if processed_count % 100 == 0: print(f"[*] Progress: {processed_count} generated...")

async def main():
    print("--- Unified openEHR RM Synthesizer ---")
    print("1. API Upload\n2. Jitter Existing\n3. Stored (Source Webtemplates)")
    mode = input("Select Mode: ")

    if mode in ['2', '3']:
        if os.path.exists(OUTPUT_DIR): shutil.rmtree(OUTPUT_DIR)
        os.makedirs(OUTPUT_DIR)

    tasks, start_time = [], time.time()
    url, ehrs = None, [str(uuid.uuid4())]

    async with aiohttp.ClientSession() as session:
        if mode == '1':
            url, user, pswd = input("API URL: "), input("User: "), input("Pass: ")
            session._auth = aiohttp.BasicAuth(user, pswd)
            files = [f for f in os.listdir(WT_DIR) if f.endswith(".json")]
            count = int(input("Count per template: "))
            for f in files:
                with open(os.path.join(WT_DIR, f), 'r', encoding='utf-8') as file:
                    gen = WebTemplateGenerator(json.load(file), f)
                    for i in range(count): tasks.append(process_task(session, gen.generate(), gen.template_id, mode, url, ehrs))
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