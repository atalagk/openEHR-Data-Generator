from fastapi import FastAPI, Body, Request, Response
import json
import os
import uuid
from datetime import datetime

app = FastAPI(title="openEHR Dummy & JSON Sink")

STORAGE_DIR = "stored_compositions"
os.makedirs(STORAGE_DIR, exist_ok=True)

# 1. Mock Connection Check
# genkidata.py calls GET /definition/template/adl1.4 to verify the server is alive.
@app.get("/definition/template/adl1.4")
async def check_connection():
    return {"status": "connected"}

# 2. Mock Template (OPT) Upload
# genkidata.py calls POST /definition/template/adl1.4 with XML data.
@app.post("/definition/template/adl1.4")
async def receive_opts(request: Request):
    # We accept the XML and return 200 OK so the script continues.
    return Response(status_code=200)

# 3. Mock EHR Creation
# genkidata.py calls POST /ehr to generate EHRs.
@app.post("/ehr")
async def create_ehr():
    new_ehr_id = str(uuid.uuid4())
    # The script expects this exact JSON structure to extract the ehr_id
    return {"ehr_id": {"value": new_ehr_id}}

# 4. The Actual File Sink (Composition Upload)
# genkidata.py calls POST /ehr/{ehr_id}/composition with the JSON data.
@app.post("/ehr/{ehr_id}/composition")
async def receive_composition(ehr_id: str, payload: dict = Body(...)):
    # Append a microsecond timestamp to avoid overwriting files
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S%f")
    filename = f"{ehr_id}_{timestamp}.json"
    file_path = os.path.join(STORAGE_DIR, filename)

    try:
        with open(file_path, "w", encoding="utf-8") as f:
            json.dump(payload, f, indent=4)
    except Exception as e:
        # Returning a 500 triggers the script's "ERROR" print statement
        return Response(content=str(e), status_code=500)

    # The script looks for response.ok (2xx status), so this succeeds.
    return {"status": "success", "file": filename}

if __name__ == "__main__":
    import uvicorn
    # Enforcing 127.0.0.1 (localhost) and port 8088 to fix the WinError 10049
    uvicorn.run(app, host="127.0.0.1", port=8088)