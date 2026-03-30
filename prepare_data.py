"""
prepare_data.py
---------------
Teisendab Remix AVL ekspordi (4.0.3 Detailne liinide väljumine)
profiilide dashboardi jaoks sobivaks JSON-iks.

Kasutus:
    python prepare_data.py andmed.csv            # -> data/data_<periood>.json + manifest
    python prepare_data.py andmed.csv minu.json  # -> minu.json (manifest ei uuene)

Eeldab: pandas  (pip install pandas)
"""

import sys
import json
import os
import re
import glob
import pandas as pd


DATA_DIR = "data"  # alamkaust kuhu JSON-id kirjutatakse


def load_csv(path: str) -> pd.DataFrame:
    for enc in ("utf-8", "utf-8-sig", "cp1257", "latin-1"):
        try:
            df = pd.read_csv(path, skiprows=3, encoding=enc, low_memory=False)
            return df
        except UnicodeDecodeError:
            continue
    raise ValueError(f"Ei suuda faili lugeda: {path}")


def extract_period_from_header(path: str) -> str:
    """Loe periood CSV esimesest reast (veerud 24-25)."""
    for enc in ("utf-8", "utf-8-sig", "cp1257", "latin-1"):
        try:
            header = pd.read_csv(path, nrows=1, header=None, encoding=enc)
            start = str(header.iloc[0, 24]).strip()[:10] if len(header.columns) > 24 else ""
            end   = str(header.iloc[0, 25]).strip()[:10] if len(header.columns) > 25 else ""
            if re.match(r"\d{4}-\d{2}-\d{2}", start) and re.match(r"\d{4}-\d{2}-\d{2}", end):
                return f"{start}_{end}"
            break
        except UnicodeDecodeError:
            continue
    # Kui päisest ei leia, tuleta failinimest
    m = re.search(r"(\d{4}-\d{2}-\d{2})[^\d]*(\d{4}-\d{2}-\d{2})", os.path.basename(path))
    if m:
        return f"{m.group(1)}_{m.group(2)}"
    return "periood_tundmatu"


def build_raw(df: pd.DataFrame) -> dict:
    df = df.rename(columns={
        "Peatuse jrk":           "jrk",
        "Liin":                  "liin",
        "Veoots":                "veoots",
        "Peatus":                "peatus",
        "Valideerimisi":         "board",
        "Väljumisi":             "alight",
        "Pardal":                "pardal",
        "Planeeritud väljumine": "planned_dep",
    })

    df["jrk"] = pd.to_numeric(df["jrk"], errors="coerce")
    df = df[df["jrk"].notna()].copy()
    df["jrk"] = df["jrk"].astype(int)

    df["liin"]   = df["liin"].astype(str).str.strip()
    df["veoots"] = df["veoots"].astype(str).str.strip()
    df = df[df["liin"].str.len() > 0]
    df = df[df["liin"].str.lower() != "nan"]
    df = df[df["veoots"].str.len() > 0]
    df = df[df["veoots"].str.lower() != "nan"]

    df["board"]       = pd.to_numeric(df["board"],       errors="coerce").fillna(0)
    df["alight"]      = pd.to_numeric(df["alight"],      errors="coerce").fillna(0)
    df["pardal"]      = pd.to_numeric(df["pardal"],      errors="coerce").fillna(0)
    df["planned_dep"] = pd.to_datetime(df["planned_dep"], errors="coerce")

    print(f"  Kehtivaid ridu: {len(df):,}")

    # Unikaalne väljumine = veoots + kuupäev (sama veoots käib iga päev!)
    df["dep_date"] = df["planned_dep"].dt.normalize()

    RAW = {}
    for liin, lgrp in df.groupby("liin"):
        trips = []
        for (veoots, dep_date), tgrp in lgrp.groupby(["veoots", "dep_date"]):
            tgrp = tgrp.sort_values("jrk")
            dep = tgrp.iloc[0]["planned_dep"]
            date_str = dep.strftime("%d.%m.%Y") if pd.notna(dep) else ""
            time_str = dep.strftime("%H:%M")     if pd.notna(dep) else ""
            stops = [
                {
                    "jrk":    int(r["jrk"]),
                    "peatus": str(r["peatus"]),
                    "board":  round(float(r["board"]),  2),
                    "alight": round(float(r["alight"]), 2),
                    "pardal": round(float(r["pardal"]), 2),
                }
                for _, r in tgrp.iterrows()
            ]
            trips.append({"veoots": veoots, "date": date_str, "time": time_str, "stops": stops})

        stop_acc: dict = {}
        for t in trips:
            for s in t["stops"]:
                k = s["jrk"]
                if k not in stop_acc:
                    stop_acc[k] = {"jrk": k, "peatus": s["peatus"], "b": [], "a": [], "p": []}
                stop_acc[k]["b"].append(s["board"])
                stop_acc[k]["a"].append(s["alight"])
                stop_acc[k]["p"].append(s["pardal"])

        avg = sorted(
            [
                {
                    "jrk":    v["jrk"],
                    "peatus": v["peatus"],
                    "board":  round(sum(v["b"]) / len(v["b"]), 2),
                    "alight": round(sum(v["a"]) / len(v["a"]), 2),
                    "pardal": round(sum(v["p"]) / len(v["p"]), 2),
                }
                for v in stop_acc.values()
            ],
            key=lambda x: x["jrk"],
        )
        RAW[liin] = {"n_trips": len(trips), "avg": avg, "trips": trips}

    return RAW


def update_manifest(data_dir: str) -> None:
    """Skanni data/ kaust ja kirjuta manifest.json."""
    files = sorted(glob.glob(os.path.join(data_dir, "data_*.json")))
    entries = []
    for f in files:
        fname = os.path.basename(f)
        # Tuleta periood failinimest: data_2025-02-01_2025-07-31.json
        m = re.search(r"data_(\d{4}-\d{2}-\d{2})_(\d{4}-\d{2}-\d{2})\.json", fname)
        if m:
            label = f"{m.group(1)} – {m.group(2)}"
        else:
            label = fname.replace("data_", "").replace(".json", "")
        entries.append({"file": f"data/{fname}", "label": label})

    manifest_path = os.path.join(data_dir, "manifest.json")
    with open(manifest_path, "w", encoding="utf-8") as f:
        json.dump(entries, f, ensure_ascii=False, indent=2)
    print(f"  manifest.json uuendatud: {len(entries)} perioodi")
    for e in entries:
        print(f"    {e['label']}  ->  {e['file']}")


def main():
    if len(sys.argv) < 2:
        print("Kasutus: python prepare_data.py andmed.csv")
        sys.exit(1)

    csv_path = sys.argv[1]

    # Kui kasutaja annab valjundfaili ise ette, kasuta seda (vana kasutusviis)
    custom_out = sys.argv[2] if len(sys.argv) > 2 else None

    print(f"Loen: {csv_path}")
    df = load_csv(csv_path)
    print(f"  Ridu: {len(df):,}  |  Veerge: {len(df.columns)}")

    raw = build_raw(df)
    liine   = len(raw)
    valja   = sum(d["n_trips"] for d in raw.values())
    peatusi = sum(len(d["avg"]) for d in raw.values())
    print(f"  Liine: {liine}  |  Valjumisi: {valja:,}  |  Peatusi (keskmised): {peatusi:,}")

    if custom_out:
        json_path = custom_out
    else:
        os.makedirs(DATA_DIR, exist_ok=True)
        period    = extract_period_from_header(csv_path)
        json_path = os.path.join(DATA_DIR, f"data_{period}.json")

    with open(json_path, "w", encoding="utf-8") as f:
        json.dump(raw, f, ensure_ascii=False, separators=(",", ":"))

    size_kb = os.path.getsize(json_path) / 1024
    print(f"Kirjutatud: {json_path}  ({size_kb:.0f} KB)")

    if not custom_out:
        print()
        update_manifest(DATA_DIR)


if __name__ == "__main__":
    main()
