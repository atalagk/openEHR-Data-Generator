# openEHR Data Generator
This is a fork of Berlin-Institute-of-Health / Genkidata (https://github.com/Berlin-Institute-of-Health/Genkidata)

It has significant changes:
- fixed quite a few validation issues
- instead of just duplicating existing Compositions, it uses:
    - An NLP library to change text to synonyms for DV_TEXT. While not perfect from a clinical semantics point of view, it's much better than lorem ipsum stuff!
    - For quantities it changes values randomly between -15 <> +15 percent so it's likely to be clinically plausable.
- in addition a new feature to create canonical Compositions from Webtemplates

Compositions are taken from test data from https://github.com/ehrbase/openEHR_SDK so they pretty much cover all possible variations.

The amount of Compositions and EHRs is defined by user input. 

## Requirements:
* Running openEHR server (e.g. ehrbase) or openEHR v1 API wrapper to store em as canonical json.
* python3.12

### Assumptions:
- Project is located on a LOCAL drive (not Google Drive / OneDrive)
- Python 3.12 is installed
- requirements.txt exists
- gen-openehr-wt.py is the entry point

### Setup

1. Open Terminal (Bash / PowerShell)
2. Set current location to project directory
3. Verify Python 3.12 is available; if not install

#### Windows

> `py -3.12 -V`

#### Linux

> `python3.12 -V`

   Expected: Python 3.12.x; if not found install

   **Install Python 3.12**

#### Windows

> Download and install from Web

#### Linux

> sudo apt update

> sudo apt install python3.12 python3.12-venv


4. Create a new virtual environment using Python 3.12

#### Windows
> py -3.12 -m venv venv

#### Linux
> python3.12 -m venv venv


5. Activate the virtual environment

#### Windows

>  .\venv\Scripts\Activate.ps1

#### Linux
> source venv/bin/activate

Expected prompt prefix: (venv)

6. Upgrade pip and install dependencies

>  python -m pip install --upgrade pip setuptools wheel

>  python -m pip install -r requirements.txt


## Run the application
>  python gen-openehr-wt.py

It will prompt 3 options:
1. API Upload
2. Jitter Existing
3. Stored (Source Webtemplates) 

Note: option 2 uses existing Compositions (\source_models\compositions) but rather than just duplicating in the original app it creates new values)

Important rules:
- Do NOT move the venv directory after creation
- Do NOT store venvs in Google Drive / OneDrive
- If project folder is moved, delete and recreate venv
- Treat venv as disposable; source files are the asset

If running in an IDE the getpass won't work, use static variable then as contained in the code.

## Saving Generated Compositions as Canonical JSON

Run the api2file app

    python api2file.py

## Original App
genkidata.py is also available in repo for reference.