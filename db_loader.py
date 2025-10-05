# # db_loader.py
# from __future__ import annotations
# import os
# import pandas as pd
# from sqlalchemy import create_engine, text

# from azcon_match import config as CFG
# from azcon_match import preprocessing as pp
# from azcon_match.data_loader import normalize_flag, normalize_unit
# from azcon_match.preprocessing import extract_material


# def _env(name: str, default: str = "") -> str:
#     return os.getenv(name, default).strip()


# def _pick_col(df_cols, *candidates):
#     """
#     df_cols içindən namizədlərə görə uyğun real sütun adını qaytarır (case-insensitive).
#     Məs: _pick_col(df.columns, "Qiyməti", "price", "cost") → 'price'
#     """
#     cols_lc = {c.lower(): c for c in df_cols}
#     for cand in candidates:
#         if cand and cand.lower() in cols_lc:
#             return cols_lc[cand.lower()]
#     return None


# # def _build_primary_db_url() -> str:
# #     """
# #     FRIEND_DB_URL verilməyibsə, .env-dəki DB_* dəyişənlərindən MySQL URL yığır.
# #     """
# #     engine = _env("DB_ENGINE", "mysql+pymysql")
# #     host   = _env("DB_HOST", "127.0.0.1")
# #     port   = _env("DB_PORT", "3306")
# #     name   = _env("DB_NAME", "")
# #     user   = _env("DB_USER", "")
# #     pwd    = _env("DB_PASSWORD", "")
# #     if not all([engine, host, port, name, user]):
# #         return ""
# #     return f"{engine}://{user}:{pwd}@{host}:{port}/{name}?charset=utf8mb4"
# def _build_primary_db_url() -> str:
#     """
#     1) Əgər MASTER_DB_URL var → onu qaytar
#     2) Yoxdursa DB_* ilə URL yığ:
#        - mysql+pymysql://user:pass@host:port/name?charset=utf8mb4
#        - sqlite:///path.db  (sqlite üçün)
#     """
#     master_url = _env("MASTER_DB_URL")
#     if master_url:
#         return master_url

#     engine = _env("DB_ENGINE", "mysql+pymysql").lower()
#     name   = _env("DB_NAME", "")
#     if engine.startswith("sqlite"):
#         # sqlite üçün təkcə fayl adı kifayətdir
#         if not name:
#             return ""  # natamam
#         # 3 slash = nisbi/absolyut path üçün
#         return f"sqlite:///{name}"

#     # mysql/postgres və s.
#     host = _env("DB_HOST", "127.0.0.1")
#     port = _env("DB_PORT", "3306")
#     user = _env("DB_USER", "")
#     pwd  = _env("DB_PASSWORD", "")
#     if not all([engine, host, port, name, user]):
#         return ""
#     return f"{engine}://{user}:{pwd}@{host}:{port}/{name}?charset=utf8mb4"


# def load_master_from_db() -> pd.DataFrame:
#     """
#     DB-dən master cədvəli oxuyur, sütunları (env → auto-detect) map edir və
#     alqoritmin gözlədiyi adlarla (CFG.*) qaytarır.

#     Prioritet:
#       1) FRIEND_DB_URL + FRIEND_TABLE/FRIEND_SQL (əgər verilibsə)
#       2) DB_* + MASTER_TABLE/MASTER_SQL (əgər FRIEND_* yoxdursa)
#     """
#     # 1) Config – URL və cədvəl/sql
#     friend_url = _env("FRIEND_DB_URL")
#     friend_tbl = _env("FRIEND_TABLE")
#     friend_sql = _env("FRIEND_SQL")

#     primary_url = _build_primary_db_url()
#     master_tbl  = _env("MASTER_TABLE") or _env("FRIEND_TABLE")   # geriyə uyğunluq
#     master_sql  = _env("MASTER_SQL") or _env("FRIEND_SQL")

#     # Hansı istifadə olunacaq?
#     if friend_url:
#         db_url     = friend_url
#         table      = friend_tbl
#         custom_sql = friend_sql
#     else:
#         db_url     = primary_url
#         table      = master_tbl
#         custom_sql = master_sql

#     if not db_url:
#         raise RuntimeError("DB bağlantısı üçün nə FRIEND_DB_URL, nə də DB_* dəyişənləri tamamlanmayıb")

#     if not table and not custom_sql:
#         raise RuntimeError("Cədvəl/Sorğu təyin edilməyib: MASTER_TABLE (və ya FRIEND_TABLE) və ya MASTER_SQL (və ya FRIEND_SQL) .env-də olmalıdır")

#     # 2) Bağlantı və oxu
#     engine = create_engine(db_url, pool_pre_ping=True, future=True)

#     if custom_sql:
#         query = text(custom_sql)
#     else:
#         query = text(f"SELECT * FROM {table}")

#     with engine.connect() as conn:
#         df_raw = pd.read_sql(query, conn)

#     # 3) Sütun adlarını müəyyən et (əvvəl .env, yoxdursa auto-detect aliaslardan)
#     #   Sənin cədvəldə: id, name, type, amount, unit, price
#     col_text = _env("COL_TEXT") or _pick_col(
#         df_raw.columns,
#         "Malların (işlərin və xidmətlərin) adı", "mallarin", "ad", "name", "title"
#     )
#     col_flag = _env("COL_FLAG") or _pick_col(
#         df_raw.columns,
#         "Tip", "tip", "type", "flag", "category"
#     )
#     col_amount = _env("COL_AMOUNT") or _pick_col(
#         df_raw.columns,
#         "Miqdarı", "miqdarı", "miqdar", "amount", "qty", "quantity"
#     )
#     col_unit = _env("COL_UNIT") or _pick_col(
#         df_raw.columns,
#         "Ölçü vahidi", "olcu vahidi", "vahid", "unit"
#     )
#     col_price = _env("COL_PRICE") or _pick_col(
#         df_raw.columns,
#         "Qiyməti", "qiyməti", "qiymet", "price", "cost"
#     )

#     required = {"text": col_text, "flag": col_flag, "amount": col_amount, "unit": col_unit, "price": col_price}
#     missing = [k for k, v in required.items() if not v]
#     if missing:
#         raise ValueError(
#             "Sütun tapılmadı: " + ", ".join(missing)
#             + "\nMövcud sütunlar: " + ", ".join(map(str, df_raw.columns))
#         )

#     # 4) Map → alqoritmin istədiyi adlar
#     df = pd.DataFrame()
#     df[CFG.MASTER_TEXT_COL] = df_raw[col_text].astype(str)
#     df[CFG.MASTER_FLAG_COL] = df_raw[col_flag]
#     df[CFG.UNIT_COL]        = df_raw[col_unit]

#     # Rəqəmsal çevirmələr
#     df["amount"]            = pd.to_numeric(df_raw[col_amount], errors="coerce")
#     df[CFG.PRICE_COL]       = pd.to_numeric(df_raw[col_price],  errors="coerce")

#     # Normalizasiya
#     df[CFG.MASTER_FLAG_COL] = df[CFG.MASTER_FLAG_COL].map(normalize_flag)
#     df[CFG.UNIT_COL]        = df[CFG.UNIT_COL].map(normalize_unit)

#     # Törəmə sütunlar
#     df["canon"]    = df[CFG.MASTER_TEXT_COL].map(pp.canon)
#     df["tokens"]   = df["canon"].str.split().map(set)
#     df["material"] = df[CFG.MASTER_TEXT_COL].str.lower().map(extract_material)

#     # Boş adları atırıq
#     df = df.dropna(subset=[CFG.MASTER_TEXT_COL])

#     return df
# db_loader.py (ROOT-da)
# from __future__ import annotations
# import os
# import pandas as pd
# from sqlalchemy import create_engine, text

# from azcon_match import config as CFG, preprocessing as pp
# # from azcon_match.data_loader import normalize_flag, normalize_unit, extract_material
# from azcon_match.data_loader import normalize_flag, normalize_unit
# from azcon_match.preprocessing import extract_material

# # -------- helpers --------
# def _env(name: str, default: str = "") -> str:
#     return os.getenv(name, default).strip()

# def _pick_col(df_cols, *candidates):
#     cols_lc = {str(c).lower().strip(): c for c in df_cols}
#     for cand in candidates:
#         if not cand:
#             continue
#         k = str(cand).lower().strip()
#         if k in cols_lc:
#             return cols_lc[k]
#     return None

# def _build_db_url() -> str:
#     """
#     1) MASTER_DB_URL varsa → birbaşa onu istifadə et (məs: sqlite:///azcon.db)
#     2) Yoxdursa DB_* ilə URL yığ (mysql/postgres və s.)
#     """
#     direct = _env("MASTER_DB_URL")
#     if direct:
#         return direct

#     engine = _env("DB_ENGINE", "mysql+pymysql").lower()
#     name   = _env("DB_NAME", "")
#     if engine.startswith("sqlite"):
#         if not name:
#             return ""
#         return f"sqlite:///{name}"

#     host = _env("DB_HOST", "127.0.0.1")
#     port = _env("DB_PORT", "3306")
#     user = _env("DB_USER", "")
#     pwd  = _env("DB_PASSWORD", "")
#     if not all([engine, host, port, name, user]):
#         return ""
#     return f"{engine}://{user}:{pwd}@{host}:{port}/{name}?charset=utf8mb4"

# # -------- main loader --------
# def load_master_from_db() -> pd.DataFrame:
#     """
#     Master cədvəli DB-dən oxuyur, sütunları map edir və azcon_match-in
#     gözlədiyi sahələri (text/flag/unit/price + canon/tokens/material) hazırlayır.
#     PRIORITET:
#       - MASTER_DB_URL (+ MASTER_TABLE/MASTER_SQL)
#       - əks halda DB_* + MASTER_TABLE/MASTER_SQL
#     """
#     db_url = _build_db_url()
#     if not db_url:
#         raise RuntimeError("DB URL tapılmadı: MASTER_DB_URL və ya DB_* dəyişənlərini doldur.")

#     table      = _env("MASTER_TABLE") or _env("FRIEND_TABLE") or "calculating"
#     custom_sql = _env("MASTER_SQL")   or _env("FRIEND_SQL")

#     engine = create_engine(db_url, pool_pre_ping=True, future=True)

#     # Oxu
#     if custom_sql:
#         q = text(custom_sql)
#         with engine.connect() as conn:
#             df_raw = pd.read_sql(q, conn)
#     else:
#         # table oxuyuruq (sqlite/mysql hər ikisində işləyir)
#         with engine.connect() as conn:
#             try:
#                 df_raw = pd.read_sql(text(f"SELECT * FROM {table}"), conn)
#             except Exception:
#                 # bəzi driver-larda read_sql_table rahatdır
#                 df_raw = pd.read_sql_table(table, conn)

#     # ---- sütunları müəyyən et (env override → auto-detect) ----
#     col_text = _env("COL_TEXT") or _pick_col(
#         df_raw.columns,
#         "Malların (işlərin və xidmətlərin) adı", "ad", "name", "title", "description", "item"
#     )
#     col_flag = _env("COL_FLAG") or _pick_col(
#         df_raw.columns,
#         "Tip", "type", "növ", "flag", "category"
#     )
#     col_unit = _env("COL_UNIT") or _pick_col(
#         df_raw.columns,
#         "Ölçü vahidi", "vahid", "unit", "uom"
#     )
#     col_price = _env("COL_PRICE") or _pick_col(
#         df_raw.columns,
#         "Qiyməti", "qiymət", "price", "cost"
#     )
#     # amount master üçün şərti; lazım olsa saxlayarıq
#     col_amount = _env("COL_AMOUNT") or _pick_col(
#         df_raw.columns,
#         "Miqdarı", "miqdar", "amount", "qty", "quantity"
#     )

#     # text mütləqdir; digərləri optional
#     if not col_text:
#         raise ValueError(f"Master cədvəldə text/name sütunu tapılmadı. Mövcud: {list(df_raw.columns)}")

#     # ---- map → azcon_match gözləyən adlar ----
#     df = pd.DataFrame()
#     df[CFG.MASTER_TEXT_COL] = df_raw[col_text].astype(str)

#     if col_flag:
#         df[CFG.MASTER_FLAG_COL] = df_raw[col_flag]
#     else:
#         df[CFG.MASTER_FLAG_COL] = ""

#     if col_unit:
#         df[CFG.UNIT_COL] = df_raw[col_unit]
#     else:
#         df[CFG.UNIT_COL] = ""

#     if col_price:
#         df[CFG.PRICE_COL] = pd.to_numeric(df_raw[col_price], errors="coerce")
#     else:
#         df[CFG.PRICE_COL] = pd.NA

#     if col_amount and col_amount in df_raw.columns:
#         df["amount"] = pd.to_numeric(df_raw[col_amount], errors="coerce")

#     # ---- normalizasiya ----
#     df[CFG.MASTER_FLAG_COL] = df[CFG.MASTER_FLAG_COL].map(normalize_flag)
#     df[CFG.UNIT_COL]        = df[CFG.UNIT_COL].map(normalize_unit)

#     # ---- törəmə sütunlar ----
#     df["canon"]    = df[CFG.MASTER_TEXT_COL].map(pp.canon)
#     df["tokens"]   = df["canon"].str.split().map(set)
#     df["material"] = df[CFG.MASTER_TEXT_COL].astype(str).str.lower().map(extract_material)

#     # boş adları at
#     df = df.dropna(subset=[CFG.MASTER_TEXT_COL])

#     return df

# # --- read pooling/echo from env
# def _env_bool(x: str, default=False) -> bool:
#     v = os.getenv(x, "")
#     if not v:
#         return default
#     return str(v).strip().lower() in ("1","true","yes","on")

# echo = _env_bool("SQL_ECHO", False)
# pool_size = int(os.getenv("POOL_SIZE", "5"))
# pool_recycle = int(os.getenv("POOL_RECYCLE", "280"))
# max_overflow = int(os.getenv("MAX_OVERFLOW", "10"))   # optional

# # SQLite üçün xüsusi connect_args (multi-thread oxunuşda error olmasın deyə)
# connect_args = {}
# if db_url.startswith("sqlite:///"):
#     connect_args = {"check_same_thread": False}

# engine = create_engine(
#     db_url,
#     echo=echo,
#     pool_pre_ping=True,
#     pool_size=pool_size,
#     pool_recycle=pool_recycle,
#     max_overflow=max_overflow,
#     future=True,
#     connect_args=connect_args
# )

# db_loader.py (ROOT)
from __future__ import annotations
import os
import pandas as pd
from sqlalchemy import create_engine, text

from azcon_match import config as CFG, preprocessing as pp
from azcon_match.data_loader import normalize_flag, normalize_unit
from azcon_match.preprocessing import extract_material

# -------- helpers --------
def _env(name: str, default: str = "") -> str:
    return os.getenv(name, default).strip()

def _env_bool(x: str, default: bool = False) -> bool:
    v = os.getenv(x, "")
    if not v:
        return default
    return str(v).strip().lower() in ("1", "true", "yes", "on")

def _pick_col(df_cols, *candidates):
    cols_lc = {str(c).lower().strip(): c for c in df_cols}
    for cand in candidates:
        if not cand:
            continue
        k = str(cand).lower().strip()
        if k in cols_lc:
            return cols_lc[k]
    return None

def _build_db_url() -> str:
    """
    1) MASTER_DB_URL varsa → onu istifadə et (məs: sqlite:///azcon.db)
    2) Yoxdursa DB_* ilə URL yığ (mysql/postgres və s.)
    """
    direct = _env("MASTER_DB_URL")
    if direct:
        return direct

    engine = _env("DB_ENGINE", "mysql+pymysql").lower()
    name   = _env("DB_NAME", "")
    if engine.startswith("sqlite"):
        if not name:
            return ""
        return f"sqlite:///{name}"

    host = _env("DB_HOST", "127.0.0.1")
    port = _env("DB_PORT", "3306")
    user = _env("DB_USER", "")
    pwd  = _env("DB_PASSWORD", "")
    if not all([engine, host, port, name, user]):
        return ""
    return f"{engine}://{user}:{pwd}@{host}:{port}/{name}?charset=utf8mb4"

# -------- main loader --------
def load_master_from_db() -> pd.DataFrame:
    """
    Master cədvəli DB-dən oxuyur, sütunları map edir və azcon_match-in
    gözlədiyi sahələri (text/flag/unit/price + canon/tokens/material) hazırlayır.
    PRIORITET:
      - MASTER_DB_URL (+ MASTER_TABLE/MASTER_SQL)
      - əks halda DB_* + MASTER_TABLE/MASTER_SQL
    """
    db_url = _build_db_url()
    if not db_url:
        raise RuntimeError("DB URL tapılmadı: MASTER_DB_URL və ya DB_* dəyişənlərini doldur.")

    table      = _env("MASTER_TABLE") or _env("FRIEND_TABLE") or "calculating"
    custom_sql = _env("MASTER_SQL")   or _env("FRIEND_SQL")

    # --- pool/echo parametrlərini .env-dən al ---
    echo         = _env_bool("SQL_ECHO", False)
    pool_size    = int(os.getenv("POOL_SIZE", "5"))
    pool_recycle = int(os.getenv("POOL_RECYCLE", "280"))
    max_overflow = int(os.getenv("MAX_OVERFLOW", "10"))

    # SQLite üçün xüsusi connect_args; MySQL/Postgres üçün lazım deyil
    connect_args = {}
    if db_url.startswith("sqlite:///"):
        connect_args = {"check_same_thread": False}

    engine = create_engine(
        db_url,
        echo=echo,
        pool_pre_ping=True,
        pool_size=pool_size,
        pool_recycle=pool_recycle,
        max_overflow=max_overflow,
        future=True,
        connect_args=connect_args,
    )

    # ---- Oxu ----
    if custom_sql:
        q = text(custom_sql)
        with engine.connect() as conn:
            df_raw = pd.read_sql(q, conn)
    else:
        with engine.connect() as conn:
            try:
                df_raw = pd.read_sql(text(f"SELECT * FROM {table}"), conn)
            except Exception:
                df_raw = pd.read_sql_table(table, conn)

    # ---- sütunları müəyyən et (env override → auto-detect) ----
    col_text = _env("COL_TEXT") or _pick_col(
        df_raw.columns,
        "Malların (işlərin və xidmətlərin) adı", "ad", "name", "title", "description", "item"
    )
    col_flag = _env("COL_FLAG") or _pick_col(
        df_raw.columns,
        "Tip", "type", "növ", "flag", "category"
    )
    col_unit = _env("COL_UNIT") or _pick_col(
        df_raw.columns,
        "Ölçü vahidi", "vahid", "unit", "uom"
    )
    col_price = _env("COL_PRICE") or _pick_col(
        df_raw.columns,
        "Qiyməti", "qiymət", "price", "cost"
    )
    col_amount = _env("COL_AMOUNT") or _pick_col(
        df_raw.columns,
        "Miqdarı", "miqdar", "amount", "qty", "quantity"
    )

    if not col_text:
        raise ValueError(f"Master cədvəldə text/name sütunu tapılmadı. Mövcud: {list(df_raw.columns)}")

    # ---- map → azcon_match gözləyən adlar ----
    df = pd.DataFrame()
    df[CFG.MASTER_TEXT_COL] = df_raw[col_text].astype(str)
    df[CFG.MASTER_FLAG_COL] = df_raw[col_flag] if col_flag else ""
    df[CFG.UNIT_COL]        = df_raw[col_unit] if col_unit else ""
    df[CFG.PRICE_COL]       = pd.to_numeric(df_raw[col_price], errors="coerce") if col_price else pd.NA

    if col_amount and col_amount in df_raw.columns:
        df["amount"] = pd.to_numeric(df_raw[col_amount], errors="coerce")

    # ---- normalizasiya ----
    df[CFG.MASTER_FLAG_COL] = df[CFG.MASTER_FLAG_COL].map(normalize_flag)
    df[CFG.UNIT_COL]        = df[CFG.UNIT_COL].map(normalize_unit)

    # ---- törəmə sütunlar ----
    df["canon"]    = df[CFG.MASTER_TEXT_COL].map(pp.canon)
    df["tokens"]   = df["canon"].str.split().map(set)
    df["material"] = df[CFG.MASTER_TEXT_COL].astype(str).str.lower().map(extract_material)

    # boş adları at
    df = df.dropna(subset=[CFG.MASTER_TEXT_COL])

    return df


