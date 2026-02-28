import time
import requests
import asyncio
import aiohttp
from requests.auth import HTTPBasicAuth
import json
import random
import os
import re
import copy
import string
from datetime import datetime, timedelta
from getpass import getpass

import nltk
from nltk.corpus import wordnet

# --- Global Configuration & Rate Limiting ---
# Limits concurrent API requests to prevent "Connection Timeout" or "Connector is closed"
MAX_CONCURRENT_REQUESTS = 50 
semaphore = asyncio.Semaphore(MAX_CONCURRENT_REQUESTS)
OUTPUT_DIR = "stored_compositions"

# Download necessary offline NLTK datasets
nltk.download('punkt', quiet=True)
nltk.download('averaged_perceptron_tagger_eng', quiet=True) 
nltk.download('wordnet', quiet=True)
nltk.download('punkt_tab', quiet=True)

header = {'Content-type': 'application/json;', 'Accept': 'application/json;', 'charset': 'utf-8;',
          'Prefer': 'return=representation'}
headerOpts = {'Content-type': 'application/xml;', 'charset': 'utf-8;',
              'Prefer': 'return=representation'}

ehr_ids = []
successful_composition_list = {}
successful_uploads_counter = 0
upload_counter = 0

base_url = ""
username = ""
password = ""

CLINICAL_SYNONYMS = {
    "24336-0": ["Gas panel - Arterial blood", "Arterial blood gas", "ABG panel"],
    "2019-8": ["CO2 (BldA) [Partial pressure]", "pCO2 Arterial", "Arterial CO2 tension"],
    "2703-7": ["Oxygen (BldA) [Partial pressure]", "pO2 Arterial"],
    "2744-1": ["pH (BldA)", "Arterial pH"],
    "at0012": ["final", "completed", "done"],
    "at0013": ["registered", "initial", "preliminary"]
}

class Composition:
    def __init__(self, amount, composition_json, filename):
        self.amount = amount
        self.composition_json = composition_json
        self.filename = filename

# ---------------- Canonical RM JSON Fixups ----------------
_ISO_BASIC_RE = re.compile(
    r"^(?P<Y>\d{4})(?P<m>\d{2})(?P<d>\d{2})T(?P<H>\d{2})(?P<M>\d{2})(?P<S>\d{2})(?:[.,](?P<ms>\d+))?(?P<tz>Z|[+-]\d{2}:?\d{2})$"
)

def _as_list(x):
    return [] if x is None else (x if isinstance(x, list) else [x])

def _normalize_dv_datetime(value: str) -> str:
    if not isinstance(value, str): return value
    m = _ISO_BASIC_RE.match(value.strip())
    if not m: return value
    parts = m.groupdict()
    ms = (parts["ms"] or "0" + "000")[:3]
    tz = parts["tz"]
    if tz != "Z" and len(tz) == 5 and tz[3] != ":":
        tz = tz[:3] + ":" + tz[3:]
    return f'{parts["Y"]}-{parts["m"]}-{parts["d"]}T{parts["H"]}:{parts["M"]}:{parts["S"]}.{ms}{tz}'

def canonicalize_rm_json(root, filename="Unknown"):
    def walk(node):
        if isinstance(node, dict):
            t = node.get("_type")
            if t == "ITEM_TABLE":
                node["_type"] = "ITEM_TREE"
                if "rows" in node: node["items"] = _as_list(node.pop("rows"))
            for list_attr in ["links", "participations"]:
                if list_attr in node: node[list_attr] = _as_list(node.get(list_attr))
            if t == "COMPOSITION" and "content" in node: node["content"] = _as_list(node.get("content"))
            elif t == "SECTION" and "items" in node: node["items"] = _as_list(node.get("items"))
            elif t == "DV_DATE_TIME" and isinstance(node.get("value"), str):
                node["value"] = _normalize_dv_datetime(node["value"])
            for v in node.values(): walk(v)
        elif isinstance(node, list):
            for v in node: walk(v)
    walk(root)
    return root

# ---------------- Synthetic Data Scramblers ----------------

def scramble_plain_text(text):
    if not text or not isinstance(text, str): return text
    try:
        words = nltk.word_tokenize(text)
        tagged_words = nltk.pos_tag(words)
        new_words = []
        for word, tag in tagged_words:
            if (tag.startswith('NN') or tag.startswith('JJ')) and word not in string.punctuation:
                synonyms = set()
                for syn in wordnet.synsets(word):
                    for lemma in syn.lemmas():
                        synonyms.add(lemma.name().replace('_', ' '))
                if synonyms and random.random() < 0.30:
                    new_words.append(random.choice(list(synonyms)))
                else: new_words.append(word)
            else: new_words.append(word)
        return " ".join(new_words).replace(" ,", ",").replace(" .", ".")
    except: return text

def generate_varied_composition(base_json):
    variant = copy.deepcopy(base_json)
    def walk(node):
        if isinstance(node, dict):
            t = node.get("_type")
            if t == "DV_TEXT" and "value" in node:
                node["value"] = scramble_plain_text(node["value"])
            elif t == "DV_CODED_TEXT" and "defining_code" in node:
                code = node["defining_code"].get("code_string")
                if code in CLINICAL_SYNONYMS: node["value"] = random.choice(CLINICAL_SYNONYMS[code])
            elif t == "DV_QUANTITY" and "magnitude" in node:
                node["magnitude"] = round(float(node["magnitude"]) * random.uniform(0.85, 1.15), 2)
            elif t == "DV_DATE_TIME" and "value" in node:
                new_date = datetime.now() - timedelta(days=random.randint(0, 730), minutes=random.randint(0, 1440))
                node["value"] = new_date.strftime("%Y-%m-%dT%H:%M:%S.000Z")
            for v in node.values(): walk(v)
        elif isinstance(node, list):
            for v in node: walk(v)
    walk(variant)
    return variant

# ---------------- Async / File Logic ----------------

async def do_ehr_posts(session, execution_counter):
    async with session.post(base_url + "/ehr", data="", headers=header) as response:
        data = await response.json()
        print(f"-> Created EHR: {data['ehr_id']['value']}")
        ehr_ids.append(data["ehr_id"]["value"])

async def do_composition_task(session, composition, index, mode):
    global upload_counter, successful_uploads_counter
    unique_payload = generate_varied_composition(composition.composition_json)
    
    if mode == '2': # FILE MODE
        file_path = os.path.join(OUTPUT_DIR, f"{os.path.splitext(composition.filename)[0]}_{index}.json")
        with open(file_path, 'w', encoding='utf-8') as f:
            json.dump(unique_payload, f, ensure_ascii=False, indent=2)
        upload_counter += 1
        if upload_counter % 100 == 0: print(f"Generated {upload_counter} files...")
        return

    # API MODE
    async with semaphore:
        target_ehr = random.choice(ehr_ids)
        try:
            async with session.post(f"{base_url}/ehr/{target_ehr}/composition", 
                                    json=unique_payload, headers=header, timeout=60) as resp:
                if resp.ok:
                    successful_uploads_counter += 1
                    successful_composition_list[composition.filename] += 1
                upload_counter += 1
                if upload_counter % 50 == 0: print(f"Uploaded {upload_counter} compositions...")
        except Exception as e:
            print(f"Failed {composition.filename}: {e}")

async def run_main_logic(auth, compositions, mode, ehr_count):
    connector = aiohttp.TCPConnector(limit=MAX_CONCURRENT_REQUESTS)
    async with aiohttp.ClientSession(auth=auth, connector=connector) as session:
        if mode == '1':
            print(f"--- Creating {ehr_count} EHRs ---")
            await asyncio.gather(*[do_ehr_posts(session, i) for i in range(ehr_count)])
        
        print(f"--- Processing Compositions ---")
        tasks = []
        for comp in compositions:
            for i in range(comp.amount):
                tasks.append(do_composition_task(session, comp, i, mode))
        await asyncio.gather(*tasks)

# ---------------- Initialization ----------------

def check_connection(u, p, url):
    try:
        r = requests.get(url + "/definition/template/adl1.4", auth=HTTPBasicAuth(u, p), timeout=10)
        return r.status_code == 200
    except: return False

def load_compositions(total_needed):
    compositions = []
    files = []
    for r, d, f in os.walk("compositions"):
        for name in f: 
            if name.endswith(".json"): files.append(os.path.join(r, name))
    
    if not files: return []
    per_file = total_needed // len(files)
    remainder = total_needed % len(files)

    for i, path in enumerate(files):
        with open(path, encoding='utf-8') as f:
            count = per_file + (1 if i < remainder else 0)
            fname = os.path.basename(path)
            compositions.append(Composition(count, canonicalize_rm_json(json.load(f), fname), fname))
            successful_composition_list[fname] = 0
    return compositions

def main():
    global base_url, username, password
    print("--- openEHR Data Generator ---")
    print("1. API Upload\n2. File Generation")
    mode = input("Select Mode: ")

    if mode == '1':
        base_url = input("URL: ")
        username = input("User: ")
        password = getpass("Pass: ")
        if not check_connection(username, password, base_url):
            print("Connection failed!"); return
        auth = aiohttp.BasicAuth(username, password)
        ehr_count = int(input("EHR Count: "))
    else:
        if not os.path.exists(OUTPUT_DIR): os.makedirs(OUTPUT_DIR)
        auth = None
        ehr_count = 0

    comp_count = int(input("Total Compositions: "))
    comps = load_compositions(comp_count)
    
    asyncio.run(run_main_logic(auth, comps, mode, ehr_count))
    print(f"\nFinished. Processed {upload_counter} items.")

if __name__ == "__main__":
    main()