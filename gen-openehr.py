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

# Download necessary offline NLTK datasets (runs silently if already downloaded)
nltk.download('punkt', quiet=True)
nltk.download('averaged_perceptron_tagger', quiet=True)
nltk.download('averaged_perceptron_tagger_eng', quiet=True)  # <-- ADD THIS LINE
nltk.download('wordnet', quiet=True)
# Fallback for punkt if punkt_tab is required by newer nltk versions
nltk.download('punkt_tab', quiet=True)

# base_url = "http://localhost:8080/ehrbase/rest/openehr/v1"
# username = "myuser"
# password = "myPassword432"

header = {'Content-type': 'application/json;', 'Accept': 'application/json;', 'charset': 'utf-8;',
          'Prefer': 'return=representation'}
headerOpts = {'Content-type': 'application/xml;', 'charset': 'utf-8;',
              'Prefer': 'return=representation'}
ehr_ids = []
successful_composition_list = {}
successful_uploads_counter = 0
upload_counter = 0

global base_url
global username
global password

# --- Local Terminology Dictionary for DV_CODED_TEXT ---
# Add your known LOINC, SNOMED, or local codes here to cycle through synonyms
CLINICAL_SYNONYMS = {
    "24336-0": ["Gas panel - Arterial blood", "Arterial blood gas", "ABG panel"],
    "2019-8": ["CO2 (BldA) [Partial pressure]", "pCO2 Arterial", "Arterial CO2 tension"],
    "2703-7": ["Oxygen (BldA) [Partial pressure]", "pO2 Arterial"],
    "2744-1": ["pH (BldA)", "Arterial pH"],
    "at0012": ["final", "completed", "done"],
    "at0013": ["registered", "initial", "preliminary"]
}

class Composition:
    def __init__(self, amount, composition_json, ehr_id, filename):
        self.amount = amount
        self.composition_json = composition_json
        self.ehr_id = ehr_id
        self.filename = filename

# ---------------- Canonical RM JSON Fixups ----------------
_ISO_BASIC_RE = re.compile(
    r"^(?P<Y>\d{4})(?P<m>\d{2})(?P<d>\d{2})T(?P<H>\d{2})(?P<M>\d{2})(?P<S>\d{2})(?:[.,](?P<ms>\d+))?(?P<tz>Z|[+-]\d{2}:?\d{2})$"
)

def _as_list(x):
    return [] if x is None else (x if isinstance(x, list) else [x])

def _normalize_dv_datetime(value: str) -> str:
    if not isinstance(value, str):
        return value
    m = _ISO_BASIC_RE.match(value.strip())
    if not m:
        return value
    parts = m.groupdict()
    ms = (parts["ms"] or "0" + "000")[:3]
    tz = parts["tz"]
    if tz != "Z" and len(tz) == 5 and tz[3] != ":":
        tz = tz[:3] + ":" + tz[3:]
    return f'{parts["Y"]}-{parts["m"]}-{parts["d"]}T{parts["H"]}:{parts["M"]}:{parts["S"]}.{ms}{tz}'

def canonicalize_rm_json(root, filename="Unknown"):
    """In-place canonicalization and legacy ITEM_TABLE migration."""
    def walk(node):
        if isinstance(node, dict):
            t = node.get("_type")

            if t == "ITEM_TABLE":
                print(f"[*] Warning: Deprecated ITEM_TABLE found in {filename}. Auto-migrating to ITEM_TREE.")
                node["_type"] = "ITEM_TREE"
                if "rows" in node:
                    node["items"] = _as_list(node.pop("rows"))
                t = "ITEM_TREE"

            for list_attr in ["links", "participations"]:
                if list_attr in node: node[list_attr] = _as_list(node.get(list_attr))

            if t == "COMPOSITION" and "content" in node: node["content"] = _as_list(node.get("content"))
            elif t == "SECTION" and "items" in node: node["items"] = _as_list(node.get("items"))
            elif t == "INSTRUCTION" and "activities" in node: node["activities"] = _as_list(node.get("activities"))
            elif t in ("ITEM_TREE", "CLUSTER", "ITEM_LIST") and "items" in node: node["items"] = _as_list(node.get("items"))
            elif t == "HISTORY" and "events" in node: node["events"] = _as_list(node.get("events"))
            elif t in ("DV_TEXT", "DV_CODED_TEXT") and "mappings" in node: node["mappings"] = _as_list(node.get("mappings"))

            if t == "DV_DATE_TIME" and isinstance(node.get("value"), str):
                node["value"] = _normalize_dv_datetime(node["value"])
            if t == "DV_QUANTITY" and "magnitude" in node:
                mag = node["magnitude"]
                if isinstance(mag, str):
                    try:
                        f = float(mag)
                        node["magnitude"] = int(f) if f.is_integer() else f
                    except ValueError: pass

            for v in node.values(): walk(v)
        elif isinstance(node, list):
            for v in node: walk(v)
    walk(root)
    return root

# ---------------- Synthetic Data Scramblers ----------------

def scramble_plain_text(text):
    """Uses NLTK/WordNet to replace nouns and adjectives with synonyms."""
    if not text or not isinstance(text, str): return text
    
    words = nltk.word_tokenize(text)
    tagged_words = nltk.pos_tag(words)
    new_words = []
    
    for word, tag in tagged_words:
        if (tag.startswith('NN') or tag.startswith('JJ')) and word not in string.punctuation:
            synonyms = set()
            for syn in wordnet.synsets(word):
                for lemma in syn.lemmas():
                    synonym_word = lemma.name().replace('_', ' ')
                    if synonym_word.lower() != word.lower():
                        synonyms.add(synonym_word)
            
            if synonyms and random.random() < 0.30:  # 30% chance to swap
                new_words.append(random.choice(list(synonyms)))
            else:
                new_words.append(word)
        else:
            new_words.append(word)
            
    return " ".join(new_words).replace(" ,", ",").replace(" .", ".")

def generate_varied_composition(base_json):
    """Deep copies the canonical JSON and injects clinical/NLP jitter."""
    variant = copy.deepcopy(base_json)
    
    def walk(node):
        if isinstance(node, dict):
            t = node.get("_type")
            
            # 1. NLP Plain Text Scrambler (DV_TEXT)
            if t == "DV_TEXT" and "value" in node:
                node["value"] = scramble_plain_text(node["value"])
            
            # 2. Terminology Dictionary Swap (DV_CODED_TEXT)
            elif t == "DV_CODED_TEXT" and "defining_code" in node:
                code = node["defining_code"].get("code_string")
                if code in CLINICAL_SYNONYMS:
                    node["value"] = random.choice(CLINICAL_SYNONYMS[code])

            # 3. Clinical Value Jitter (DV_QUANTITY +/- 15%)
            elif t == "DV_QUANTITY" and "magnitude" in node:
                variance = random.uniform(0.85, 1.15) 
                node["magnitude"] = round(float(node["magnitude"]) * variance, 2)
            
            # 4. Timeline Scatter (DV_DATE_TIME randomly within last 2 years)
            elif t == "DV_DATE_TIME" and "value" in node:
                random_days = random.randint(0, 730)
                random_mins = random.randint(0, 1440)
                new_date = datetime.now() - timedelta(days=random_days, minutes=random_mins)
                node["value"] = new_date.strftime("%Y-%m-%dT%H:%M:%S.000Z")

            for v in node.values(): walk(v)
        elif isinstance(node, list):
            for v in node: walk(v)

    walk(variant)
    return variant

# ---------------- Async Upload Logic ----------------

async def send_ehrs(auth, ehr_count):
    async with aiohttp.ClientSession(auth=auth) as session:
        post_tasks = [do_ehr_posts(session, i) for i in range(ehr_count)]
        await asyncio.gather(*post_tasks)

async def do_ehr_posts(session, execution_counter):
    async with session.post(base_url + "/ehr", data="", headers=header) as response:
        data = await response.text()
        print("-> Created EHRs %d" % execution_counter)
        json_object = json.loads(data)
        ehr_ids.append(json_object["ehr_id"]["value"])

def check_connection():
    auth_request = HTTPBasicAuth(username, password)
    response = requests.get(base_url + "/definition/template/adl1.4", auth=auth_request)
    if response.status_code == 200:
        print("Connected successfully")
        return True
    else:
        print("Connection error:" + str(response.content) + " retry!")
        return False

def enter_login():
    global base_url, username, password
    base_url = input("Enter openEHR API URL (e.g. http://localhost:8080/ehrbase/rest/openehr/v1): ")
    username = input("Enter your username: ")
    password = getpass("Enter your password: ")

def create_ehrs(auth, ehr_count):
    loop = asyncio.get_event_loop()
    loop.run_until_complete(send_ehrs(auth, ehr_count))

async def send_compositions(auth, compositions):
    async with aiohttp.ClientSession(auth=auth) as session:
        post_tasks = []
        for comp in compositions:
            post_tasks.append(send_composition(session, comp))
        await asyncio.gather(*post_tasks)

async def send_composition(session, composition):
    post_tasks = [do_composition_posts(session, composition, i) for i in range(composition.amount)]
    await asyncio.gather(*post_tasks)

def manage_success(composition, execution_counter):
    global successful_uploads_counter
    print("-> Created composition " + composition.filename + " number %d" % execution_counter)
    successful_composition_list[composition.filename] += 1
    successful_uploads_counter += 1

async def do_composition_posts(session, composition, execution_counter):
    global upload_counter
    
    # Generate a deeply copied, randomized variant of the base template
    unique_payload = generate_varied_composition(composition.composition_json)
    
    # Dynamically select a random target EHR for *this specific* upload
    target_ehr = random.choice(ehr_ids)
    
    data = json.dumps(unique_payload, ensure_ascii=False)
    
    async with session.post(base_url + "/ehr/" + target_ehr + "/composition", data=data.encode("utf-8"),
                            headers=header) as response:
        response_text = await response.text()
        upload_counter += 1
        print("Compositions uploaded: " + str(upload_counter))
        if response.ok:
            manage_success(composition, execution_counter)
        else:
            print("ERROR: ", response.status)
            print("Filename: ", composition.filename)
            print("Error message: ", response_text)

def create_compositions(auth, composition_count):
    compositions = load_compositions(composition_count)
    loop = asyncio.get_event_loop()
    try:
        loop.run_until_complete(send_compositions(auth, compositions))
    finally:
        loop.close()

def generate_random_amounts(composition_file_count, composition_count):
    while composition_file_count >= composition_count:
        print(f"Amount of compositions needs to be bigger than: {composition_file_count}.")
        composition_count = int(input("How many compositions do you want: "))
    dividers = sorted(random.sample(range(1, composition_count), composition_file_count - 1))
    return [a - b for a, b in zip(dividers + [composition_count], [0] + dividers)]

def count_composition_files():
    files = sum(len(filenames) for _, _, filenames in os.walk('compositions/'))
    return files

def load_compositions(composition_count):
    composition_file_count = count_composition_files()
    amounts = generate_random_amounts(composition_file_count, composition_count)
    compositions = []
    print("Loading Composition files ...")
    for root, dirs, files in os.walk("compositions", topdown=False):
        for filename in files:
            with open(os.path.join(root, filename), encoding='utf-8') as file:
                raw_json = json.load(file)
                # Canonicalize the base JSON once upon loading
                canonical_json = canonicalize_rm_json(raw_json, filename=filename)
                
                compositions.append(Composition(int(amounts.pop(0)), canonical_json, None, filename))
                successful_composition_list[filename] = 0
    return compositions

def send_opts(filename, basic_auth_requests):
    with open(filename, encoding='utf-8') as f:
        data = f.read().encode('utf8')
    response = requests.post(base_url + "/definition/template/adl1.4", data=data,
                             headers=headerOpts, auth=basic_auth_requests)
    print("Upload " + filename + ": " + str(response.status_code))

def load_opts(basic_auth_requests):
    print("---------Uploading OPTS-------------")
    for root, dirs, files in os.walk("opts", topdown=False):
        for name in files:
            send_opts(os.path.join(root, name), basic_auth_requests)

def main():
    enter_login()
    status_code = check_connection()
    while not status_code:
        enter_login()
        status_code = check_connection()
        
    basic_auth_requests = HTTPBasicAuth(username, password)
    auth = aiohttp.BasicAuth(login=username, password=password, encoding='utf-8')
    
    load_opts(basic_auth_requests)
    
    print("---------Configure Amount-------------")
    ehr_count = int(input("How many EHRs do you want: "))
    composition_count = int(input("How many compositions do you want: "))
    
    print("---------Creating EHRs-------------")
    create_ehrs(auth, ehr_count)
    
    print("---------Creating Compositions-------------")
    create_compositions(auth, composition_count)
    
    print("Successfully created: " + str(successful_composition_list))
    print("Sum of successfully created: " + str(successful_uploads_counter))

if __name__ == "__main__":
    main()