
import requests, time

BBOX = (-24.2, -47.0, -22.0, -44.0)  # sul, oeste, norte, leste

def _call():
    r = requests.get(
        "https://opensky-network.org/api/states/all",
        params={"lamin": BBOX[0], "lomin": BBOX[1], "lamax": BBOX[2], "lomax": BBOX[3]},
        timeout=30,
    )
    if r.status_code in (429, 503):
        raise RuntimeError(f"transient status {r.status_code}")
    r.raise_for_status()
    return r.json()

def fetch_page(page:int):
  
    backoff = 1.0
    for _ in range(5):
        try:
            data = _call()
            states = data.get("states") or []
            rows = []
            for s in states:
                rows.append({
                    "icao24": s[0],
                    "callsign": (s[1] or "").strip(),
                    "origin_country": s[2],
                    "time_position": s[3],
                    "last_contact": s[4],
                    "lon": s[5],
                    "lat": s[6],
                    "baro_altitude": s[7],
                    "on_ground": s[8],
                    "velocity": s[9],
                    "heading": s[10],
                })
            return rows
        except Exception:
            time.sleep(backoff)
            backoff = min(backoff * 2, 8)
    return []  

