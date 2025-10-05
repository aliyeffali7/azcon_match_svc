# # azcon_match/data_loader.py  (drop-in)
# import time
# from typing import Any, Tuple, List
# import pandas as pd

# from . import config, preprocessing as pp
# from .preprocessing import extract_material

# # ---------- Normalisers ----------
# UNIT_MAP = {
#     "m²": "m(2)", "m2": "m(2)", "m(2)": "m(2)",
#     "m": "m", "metr": "m", "pm": "m",
#     "əd": "ədəd", "ed": "ədəd", "eded": "ədəd", "ədəd": "ədəd",
#     "ton": "ton",
# }
# def normalize_unit(u: Any) -> str:
#     if not isinstance(u, str) and pd.isna(u):
#         return ""
#     u = str(u).strip().lower()
#     return UNIT_MAP.get(u, u)

# FLAG_MAP = {
#     "məhsul": "məhsul", "mehsul": "məhsul", "product": "məhsul",
#     "xidmət": "xidmət", "xidmet": "xidmət", "service": "xidmət",
#     "mix": "mix",
# }
# def normalize_flag(f: Any) -> str:
#     if not isinstance(f, str):
#         return ""
#     f = f.strip().lower()
#     return FLAG_MAP.get(f, f)

# # ---------- Master loader ----------
# def load_master(path: str | None = None) -> pd.DataFrame:
#     """Read master Excel and compute canon/tokens/material."""
#     path = path or config.MASTER_PATH
#     print("⏳ Loading master …")
#     t0 = time.time()

#     cols = [config.MASTER_TEXT_COL, config.MASTER_FLAG_COL, config.PRICE_COL, config.UNIT_COL]
#     df = (
#         pd.read_excel(path, engine="openpyxl")[cols]
#         .dropna(subset=[config.MASTER_TEXT_COL])
#     )

#     # normalise
#     df[config.MASTER_FLAG_COL] = df[config.MASTER_FLAG_COL].map(normalize_flag)
#     df[config.UNIT_COL]        = df[config.UNIT_COL].map(normalize_unit)
#     df[config.PRICE_COL]       = pd.to_numeric(df[config.PRICE_COL], errors="coerce")

#     # canon/tokens/material
#     df["canon"]    = df[config.MASTER_TEXT_COL].map(pp.canon)
#     df["tokens"]   = df["canon"].str.split().map(set)
#     df["material"] = df[config.MASTER_TEXT_COL].astype(str).str.lower().apply(extract_material)

#     print(f"Master rows: {len(df)}  ({time.time() - t0:.2f}s)\n")
#     return df

# # keep the old helper name for compatibility
# load_master_path = load_master

# # ---------- Query loader ----------
# def load_queries(path: str | None = None) -> List[Tuple[str, str, str]]:
#     """Return a list of (text, flag, unit) tuples from the query Excel."""
#     path = path or config.QUERY_PATH
#     qdf = pd.read_excel(path, engine="openpyxl")

#     need = [config.QUERY_TEXT_COL, config.QUERY_FLAG_COL, config.UNIT_COL]
#     missing = [c for c in need if c not in qdf.columns]
#     if missing:
#         raise ValueError(f"Query sheet missing required columns: {missing}")

#     # normalise at source
#     qdf[config.QUERY_FLAG_COL] = qdf[config.QUERY_FLAG_COL].map(normalize_flag)
#     qdf[config.UNIT_COL]       = qdf[config.UNIT_COL].map(normalize_unit)

#     return (
#         qdf[need]
#         .dropna(subset=[config.QUERY_TEXT_COL])
#         .values
#         .tolist()
#     )

# azcon_match/data_loader.py
# Robust master loader: header autodetect + computed columns (canon/tokens/material)

import time
from typing import Any, Tuple, List, Dict
import pandas as pd

from . import config, preprocessing as pp
from .preprocessing import extract_material
from . import config, preprocessing as pp



import sqlalchemy
# ---------- Normalisers ----------
UNIT_MAP = {
    "m²": "m(2)", "m2": "m(2)", "m(2)": "m(2)",
    "m": "m", "metr": "m", "pm": "m",
    "əd": "ədəd", "ed": "ədəd", "eded": "ədəd", "ədəd": "ədəd",
    "ton": "ton",
}
def normalize_unit(u: Any) -> str:
    if not isinstance(u, str) and pd.isna(u):
        return ""
    u = str(u).strip().lower()
    return UNIT_MAP.get(u, u)

FLAG_MAP = {
    "məhsul": "məhsul", "mehsul": "məhsul", "product": "məhsul",
    "xidmət": "xidmət", "xidmet": "xidmət", "service": "xidmət",
    "mix": "mix",
}
def normalize_flag(f: Any) -> str:
    if not isinstance(f, str):
        return ""
    f = f.strip().lower()
    return FLAG_MAP.get(f, f)

# ---------- Helpers ----------
def _norm(s: str) -> str:
    return (s or "").strip().lower().translate(pp.TRANSLIT)

def _pick_col(cols: List[str], prefer: List[str], contains_any: List[str]) -> str | None:
    """Pick best matching column by exact normalized name or substring heuristics."""
    nmap: Dict[str, str] = {c: _norm(c) for c in cols}

    # 1) exact preferred names
    for want in prefer:
        wantn = _norm(want)
        for c, cn in nmap.items():
            if cn == wantn:
                return c

    # 2) substring heuristics (all tokens in contains_any)
    for c, cn in nmap.items():
        if any(tok in cn for tok in contains_any):
            return c

    return None

# ---------- Master loader ----------
def load_master(path: str | None = None) -> pd.DataFrame:
    """
    Read master Excel and ensure required columns exist:
      text: config.MASTER_TEXT_COL
      flag: config.MASTER_FLAG_COL
      price: config.PRICE_COL
      unit: config.UNIT_COL
    Also compute: canon, tokens, material
    """
    path = path or config.MASTER_PATH
    print("⏳ Loading master …")
    t0 = time.time()

    raw = pd.read_excel(path, engine="openpyxl")
    cols = list(raw.columns)

    text_col = _pick_col(
        cols,
        [config.MASTER_TEXT_COL, "Malların (işlərin və xidmətlərin) adı", "Ad"],
        ["ad", "mallarin", "xidmet", "mehsul", "name", "description"]
    )
    flag_col = _pick_col(
        cols,
        [config.MASTER_FLAG_COL, "Tip", "Növ", "Type"],
        ["tip", "type", "nov", "kateqor"]
    )
    price_col = _pick_col(
        cols,
        [config.PRICE_COL, "Qiymət", "Qiymeti", "Price", "Birim qiymət"],
        ["qiym", "price"]
    )
    unit_col = _pick_col(
        cols,
        [config.UNIT_COL, "Ölçü vahidi", "Vahid", "Unit"],
        ["vahid", "unit", "olcu"]
    )

    # Build df with expected API column names (create missing with NaN)
    df = pd.DataFrame()
    if text_col:
        df[config.MASTER_TEXT_COL] = raw[text_col]
    else:
        df[config.MASTER_TEXT_COL] = raw.iloc[:, 0]  # last resort: first column

    if flag_col:
        df[config.MASTER_FLAG_COL] = raw[flag_col]
    else:
        df[config.MASTER_FLAG_COL] = ""

    if price_col:
        df[config.PRICE_COL] = raw[price_col]
    else:
        df[config.PRICE_COL] = pd.NA  # missing prices → still works, only scores

    if unit_col:
        df[config.UNIT_COL] = raw[unit_col]
    else:
        df[config.UNIT_COL] = ""

    # normalise
    df[config.MASTER_FLAG_COL] = df[config.MASTER_FLAG_COL].map(normalize_flag)
    df[config.UNIT_COL]        = df[config.UNIT_COL].map(normalize_unit)
    df[config.PRICE_COL]       = pd.to_numeric(df[config.PRICE_COL], errors="coerce")

    # canon/tokens/material
    df["canon"]    = df[config.MASTER_TEXT_COL].map(pp.canon)
    df["tokens"]   = df["canon"].str.split().map(set)
    # if text_col is weird, compute material from canon/text robustly
    df["material"] = df[config.MASTER_TEXT_COL].astype(str).str.lower().apply(extract_material)

    # drop rows where text is missing after all
    df = df.dropna(subset=[config.MASTER_TEXT_COL])

    print(f"Master rows: {len(df)}  ({time.time() - t0:.2f}s)\n")
    return df

# keep the old helper name for compatibility
load_master_path = load_master

# ---------- Query loader (unchanged) ----------
def load_queries(path: str | None = None) -> List[Tuple[str, str, str]]:
    path = path or config.QUERY_PATH
    qdf = pd.read_excel(path, engine="openpyxl")

    need = [config.QUERY_TEXT_COL, config.QUERY_FLAG_COL, config.UNIT_COL]
    missing = [c for c in need if c not in qdf.columns]
    if missing:
        raise ValueError(f"Query sheet missing required columns: {missing}")

    qdf[config.QUERY_FLAG_COL] = qdf[config.QUERY_FLAG_COL].map(normalize_flag)
    qdf[config.UNIT_COL]       = qdf[config.UNIT_COL].map(normalize_unit)

    return (
        qdf[need]
        .dropna(subset=[config.QUERY_TEXT_COL])
        .values
        .tolist()
    )


def load_master_from_db() -> pd.DataFrame:
    engine = sqlalchemy.create_engine(config.MASTER_DB_URL)  # məsələn sqlite:///azcon.db
    df = pd.read_sql_table("calculating", engine)

    # uyğun sütun adlarını maplə
    df = df.rename(columns={
        "name": config.MASTER_TEXT_COL,
        "type": config.MASTER_FLAG_COL,
        "unit": config.UNIT_COL,
        "price": config.PRICE_COL,
    })

    df[config.MASTER_FLAG_COL] = df[config.MASTER_FLAG_COL].map(normalize_flag)
    df[config.UNIT_COL]        = df[config.UNIT_COL].map(normalize_unit)
    df[config.PRICE_COL]       = pd.to_numeric(df[config.PRICE_COL], errors="coerce")

    df["canon"]    = df[config.MASTER_TEXT_COL].map(pp.canon)
    df["tokens"]   = df["canon"].str.split().map(set)
    df["material"] = df[config.MASTER_TEXT_COL].astype(str).str.lower().apply(extract_material)

    return df