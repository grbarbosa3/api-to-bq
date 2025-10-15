# API to BigQuery – OpenSky (SJC Airspace)

## Project about flights on my city São José dos Campos.  
**Stack**: Python, BigQuery (MERGE), logging, validações, CLI.

## How to Run
```bash
python -m venv .venv && . .venv/bin/activate
pip install -r requirements.txt
export GOOGLE_APPLICATION_CREDENTIALS=keys/sa.json
python -m src.main --project <proj> --dataset de_demo --table opensky_states
