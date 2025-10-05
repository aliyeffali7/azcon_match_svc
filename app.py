# # app.py
# from __future__ import annotations
# from fastapi import FastAPI, UploadFile, File, Form, HTTPException, Body
# from fastapi.responses import StreamingResponse, JSONResponse
# from typing import Optional, Dict, Any, List

# import os, io, base64, pandas as pd, requests
# from dotenv import load_dotenv
# from sqlalchemy import text  # optional: used in helpers
# load_dotenv()

# from azcon_match import config as CFG
# from azcon_match.api import find_matches
# from db_loader import load_master_from_db
# from azcon_match.output import build_excel_from_results

# app = FastAPI(title="azcon_match microservice")

# # ---------- helpers ----------
# def _env(name: str, default: str = "") -> str:
#     return os.getenv(name, default).strip()

# def _auth(kind: str, prefix: str):
#     kind = (kind or "none").lower()
#     if kind == "bearer":
#         return {"Authorization": f"Bearer {_env(prefix+'AUTH_BEARER')}"}, None
#     if kind == "apikey":
#         return {_env(prefix+"APIKEY_HEADER","X-API-Key"): _env(prefix+"APIKEY_VALUE")}, None
#     if kind == "basic":
#         return {}, (_env(prefix+"BASIC_USER"), _env(prefix+"BASIC_PASS"))
#     return {}, None


# def _auth_from_env(prefix: str):
#     return _auth(_env(prefix + "AUTH_TYPE", "none"), prefix)

# def _build_output_excel(rows: List[Dict[str, Any]]) -> bytes:
#     df = pd.DataFrame(rows)
#     buf = io.BytesIO()
#     with pd.ExcelWriter(buf, engine="openpyxl") as w:
#         df.to_excel(w, index=False, sheet_name="Matches")
#     buf.seek(0)
#     return buf.getvalue()

# # ---- NEW: auto-detect & rename incoming query columns to CFG targets ----
# def _pick_col(df_cols, *candidates):
#     cols_lc = {str(c).lower().strip(): c for c in df_cols}
#     for cand in candidates:
#         if not cand:
#             continue
#         k = str(cand).lower().strip()
#         if k in cols_lc:
#             return cols_lc[k]
#     return None

# def _normalize_query_df_to_cfg(df_in: pd.DataFrame) -> pd.DataFrame:
#     """
#     Fayldakı başlıqları auto-detect edib CFG-də gözlənilən sütunlara rename edir.
#     Tip sütunu optional-dır: yoxdursa, 'query_flag' boş qalacaq.
#     """
#     ov_text = _env("Q_COL_TEXT")
#     ov_flag = _env("Q_COL_FLAG")
#     ov_unit = _env("Q_COL_UNIT")

#     text_src = ov_text or _pick_col(
#         df_in.columns,
#         "Malların (işlərin və xidmətlərin) adı", "ad", "mal adı", "name", "title", "item", "description"
#     )
#     flag_src = ov_flag or _pick_col(
#         df_in.columns,
#         "Tip", "növ", "type", "flag", "category"
#     )
#     unit_src = ov_unit or _pick_col(
#         df_in.columns,
#         "Ölçü vahidi", "vahid", "unit", "uom"
#     )

#     missing = []
#     if not text_src: missing.append(f"text (məs: 'name' → {CFG.QUERY_TEXT_COL})")
#     if not unit_src: missing.append(f"unit (məs: 'unit' → {CFG.UNIT_COL})")

#     if missing:
#         raise ValueError(
#             "Query sheet missing required columns: "
#             + str(missing)
#             + f". Mövcud sütunlar: {list(df_in.columns)}"
#         )

#     rename_map = {
#         text_src: CFG.QUERY_TEXT_COL,
#         unit_src: CFG.UNIT_COL,
#     }
#     if flag_src:
#         rename_map[flag_src] = CFG.QUERY_FLAG_COL

#     df = df_in.rename(columns=rename_map)

#     # əgər Tip/flag sütunu ümumiyyətlə yoxdursa, boş column yaradırıq
#     if CFG.QUERY_FLAG_COL not in df.columns:
#         df[CFG.QUERY_FLAG_COL] = ""

#     return df


# def _process_queries_df(qdf: pd.DataFrame, master_df: pd.DataFrame, top_n: int = 5) -> bytes:
#     # auto normalize incoming columns
#     qdf = _normalize_query_df_to_cfg(qdf)

#     need = [CFG.QUERY_TEXT_COL, CFG.UNIT_COL]  # flag optional, text+unit məcburi
#     missing = [c for c in need if c not in qdf.columns]
#     if missing:
#         raise ValueError(f"Query sheet missing required columns: {missing}")

#     from azcon_match.data_loader import normalize_flag, normalize_unit
#     if CFG.QUERY_FLAG_COL in qdf.columns:
#         qdf[CFG.QUERY_FLAG_COL] = qdf[CFG.QUERY_FLAG_COL].map(normalize_flag)
#     qdf[CFG.UNIT_COL] = qdf[CFG.UNIT_COL].map(normalize_unit)

#     rows_out: List[Dict[str, Any]] = []
#     for q_text, q_flag, q_unit in qdf[[CFG.QUERY_TEXT_COL, CFG.QUERY_FLAG_COL, CFG.UNIT_COL]].itertuples(index=False, name=None):
#         res = find_matches(q_text, q_flag, q_unit, master_df)
#         hits = res.get("priced_hits") or res.get("hits") or []
#         if hits:
#             for (t, sc, pr, u) in hits[:top_n]:
#                 rows_out.append({
#                     "query_text": q_text,
#                     "query_flag": q_flag,
#                     "query_unit": q_unit,
#                     "match_text": t,
#                     "score": sc,
#                     "price": pr,
#                     "unit": u,
#                 })
#         else:
#             rows_out.append({
#                 "query_text": q_text,
#                 "query_flag": q_flag,
#                 "query_unit": q_unit,
#                 "match_text": "",
#                 "score": 0,
#                 "price": None,
#                 "unit": "",
#             })
#     return _build_output_excel(rows_out)

# def _post_result(excel_bytes: bytes, url: str, mode: str, headers: Dict[str,str], basic_auth):
#     try:
#         if mode == "json":
#             payload = {
#                 "filename": "estimate_result.xlsx",
#                 "file_b64": base64.b64encode(excel_bytes).decode("ascii"),
#                 "meta": {"source": "azcon_match_svc"}
#             }
#             headers = {"Content-Type": "application/json", **(headers or {})}
#             r = requests.post(url, json=payload, headers=headers, auth=basic_auth, timeout=60)
#         else:
#             files = {"file": ("estimate_result.xlsx", excel_bytes,
#                               "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet")}
#             r = requests.post(url, files=files, headers=headers or None, auth=basic_auth, timeout=60)
#         r.raise_for_status()
#         return f"ok:{r.status_code}"
#     except requests.RequestException as e:
#         return f"failed:{e}"

# # ---------- endpoints ----------
# @app.post("/health")
# def health(): 
#     return {"ok": True}

# @app.post("/estimates/upload")
# async def upload_estimate(
#     file: UploadFile = File(...),
#     post_to_friend: Optional[bool] = Form(False)
# ):
#     try:
#         # read queries
#         qdf = pd.read_excel(io.BytesIO(await file.read()))
#         # load master from DB
#         master_df = load_master_from_db()
#         # run
#         excel_bytes = _process_queries_df(qdf, master_df, top_n=CFG.TOP_N if hasattr(CFG, "TOP_N") else 5)

#         # optional POST
#         x_post = "no"
#         if post_to_friend:
#             post_url = _env("RESULT_POST_URL")
#             if post_url:
#                 post_mode = _env("RESULT_POST_MODE", "multipart").lower()
#                 headers, basic = _auth_from_env("POST_")
#                 x_post = _post_result(excel_bytes, post_url, post_mode, headers, basic)
#             else:
#                 x_post = "skipped:no RESULT_POST_URL"

#         return StreamingResponse(
#             io.BytesIO(excel_bytes),
#             media_type="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
#             headers={
#                 "Content-Disposition": 'attachment; filename="estimate_result.xlsx"',
#                 "X-Post-To-Friend": x_post,
#             },
#         )
#     except Exception as e:
#         raise HTTPException(status_code=500, detail=str(e))

# @app.post("/estimates/process")
# def process_from_url(payload: Dict[str, Any] = Body(...)):
#     """
#     JSON body:
#     {
#       "source_url": "https://friend/api/export.xlsx",
#       "source_auth": {"type":"bearer|apikey|basic|none", ...},   # optional
#       "post_url": "https://friend/api/receive",                  # optional (env fallback)
#       "post_mode": "multipart|json",                             # optional (env fallback)
#       "post_auth": {"type":"bearer|apikey|basic|none", ...}      # optional (env fallback)
#     }
#     """
#     try:
#         src = (payload.get("source_url") or "").strip()
#         if not src.startswith(("http://","https://")):
#             raise ValueError("source_url must start with http:// or https://")

#         # GET auth
#         s_auth = payload.get("source_auth") or {}
#         if s_auth:
#             t = (s_auth.get("type") or "none").lower()
#             if t == "bearer":
#                 g_headers, g_basic = {"Authorization": f"Bearer {s_auth.get('token','')}"}, None
#             elif t == "apikey":
#                 g_headers, g_basic = {s_auth.get("header","X-API-Key"): s_auth.get("key","")}, None
#             elif t == "basic":
#                 g_headers, g_basic = {}, (s_auth.get("user",""), s_auth.get("pass",""))
#             else:
#                 g_headers, g_basic = {}, None
#         else:
#             g_headers, g_basic = _auth_from_env("GET_")

#         # download queries Excel
#         r = requests.get(src, headers=g_headers or None, auth=g_basic, timeout=60)
#         r.raise_for_status()
#         qdf_raw = pd.read_excel(io.BytesIO(r.content))
#         qdf = _normalize_query_df_to_cfg(qdf_raw)

#         # master from DB
#         master_df = load_master_from_db()

#         # run
#         excel_bytes = _process_queries_df(qdf, master_df, top_n=CFG.TOP_N if hasattr(CFG, "TOP_N") else 5)

#         # post back
#         post_url = (payload.get("post_url") or _env("RESULT_POST_URL"))
#         if post_url:
#             post_mode = (payload.get("post_mode") or _env("RESULT_POST_MODE","multipart")).lower()
#             p_auth = payload.get("post_auth") or {}
#             if p_auth:
#                 t = (p_auth.get("type") or "none").lower()
#                 if t == "bearer":
#                     p_headers, p_basic = {"Authorization": f"Bearer {p_auth.get('token','')}"}, None
#                 elif t == "apikey":
#                     p_headers, p_basic = {p_auth.get("header","X-API-Key"): p_auth.get("key","")}, None
#                 elif t == "basic":
#                     p_headers, p_basic = {}, (p_auth.get("user",""), p_auth.get("pass",""))
#                 else:
#                     p_headers, p_basic = {}, None
#             else:
#                 p_headers, p_basic = _auth_from_env("POST_")

#             post_status = _post_result(excel_bytes, post_url, post_mode, p_headers, p_basic)
#         else:
#             post_status = "skipped:no post_url"

#         return JSONResponse({
#             "ok": True,
#             "bytes_in": len(r.content),
#             "bytes_out": len(excel_bytes),
#             "post_status": post_status
#         })
#     except requests.RequestException as e:
#         raise HTTPException(status_code=502, detail=f"network error: {e}")
#     except Exception as e:
#         raise HTTPException(status_code=500, detail=str(e))


# app.py
from __future__ import annotations
from fastapi import FastAPI, UploadFile, File, Form, HTTPException, Body
from fastapi.responses import StreamingResponse, JSONResponse
from typing import Optional, Dict, Any, List
import os, io, base64, pandas as pd, requests

from dotenv import load_dotenv
load_dotenv()

from azcon_match import config as CFG
def _override_cfg_from_env():
    import os
    def _get_int(name, default=None):
        v = os.getenv(name)
        if v is None or str(v).strip() == "":
            return default
        try:
            return int(str(v).strip())
        except ValueError:
            return default

    topn = _get_int("TOP_N", None)
    if topn is not None:
        CFG.TOP_N = topn

    thr = _get_int("THRESHOLD", None)
    if thr is not None:
        CFG.THRESHOLD = thr

    # istəsən digər paramları da env-dən üstələ:
    # PRICE_AVG_MIN_SCORE, MIN_COVER və s.

_override_cfg_from_env()
from azcon_match.output import build_excel_from_results
from db_loader import load_master_from_db

app = FastAPI(
    title="azcon_match microservice",
    docs_url="/docs",
    redoc_url="/redoc",
    openapi_url="/openapi.json"
)

# ---------- helpers ----------
def _env(name: str, default: str = "") -> str:
    return os.getenv(name, default).strip()

def _auth(kind: str, prefix: str):
    kind = (kind or "none").lower()
    if kind == "bearer":
        return {"Authorization": f"Bearer {_env(prefix+'AUTH_BEARER')}"}, None
    if kind == "apikey":
        return {_env(prefix+"APIKEY_HEADER","X-API-Key"): _env(prefix+"APIKEY_VALUE")}, None
    if kind == "basic":
        return {}, (_env(prefix+"BASIC_USER"), _env(prefix+"BASIC_PASS"))
    return {}, None

def _auth_from_env(prefix: str):
    return _auth(_env(prefix + "AUTH_TYPE", "none"), prefix)

def _pick_col(df_cols, *candidates):
    cols_lc = {str(c).lower().strip(): c for c in df_cols}
    for cand in candidates:
        if not cand: 
            continue
        k = str(cand).lower().strip()
        if k in cols_lc:
            return cols_lc[k]
    return None

def _normalize_query_df_to_cfg(df_in: pd.DataFrame) -> pd.DataFrame:
    """
    Yüklənən Excel başlıqlarını auto-detect edir və CFG adlarına rename edir.
    Yalnız TEXT məcburidir; FLAG/UNIT optionaldır (yoxdursa boş sütun yaradırıq).
    """
    ov_text = _env("Q_COL_TEXT")
    ov_flag = _env("Q_COL_FLAG")
    ov_unit = _env("Q_COL_UNIT")

    text_src = ov_text or _pick_col(
        df_in.columns,
        "Malların (işlərin və xidmətlərin) adı", "ad", "mal adı",
        "name", "title", "item", "description", "mətn"
    )
    flag_src = ov_flag or _pick_col(
        df_in.columns,
        "Tip", "növ", "type", "flag", "category"
    )
    unit_src = ov_unit or _pick_col(
        df_in.columns,
        "Ölçü vahidi", "vahid", "unit", "uom"
    )

    missing = []
    if not text_src: missing.append(f"text (məs: 'name' → {CFG.QUERY_TEXT_COL})")
    if missing:
        raise ValueError(
            "Query sheet missing required columns: "
            + str(missing)
            + f". Mövcud sütunlar: {list(df_in.columns)}"
        )

    rename_map = {text_src: CFG.QUERY_TEXT_COL}
    if flag_src:
        rename_map[flag_src] = CFG.QUERY_FLAG_COL
    if unit_src:
        rename_map[unit_src] = CFG.UNIT_COL

    df = df_in.rename(columns=rename_map)

    # optionalları yoxdursa boş sütun yarat
    if CFG.QUERY_FLAG_COL not in df.columns:
        df[CFG.QUERY_FLAG_COL] = ""
    if CFG.UNIT_COL not in df.columns:
        df[CFG.UNIT_COL] = ""

    return df

def _post_result(excel_bytes: bytes, url: str, mode: str, headers: Dict[str,str], basic_auth):
    try:
        if mode == "json":
            payload = {
                "filename": "estimate_result.xlsx",
                "file_b64": base64.b64encode(excel_bytes).decode("ascii"),
                "meta": {"source": "azcon_match_svc"}
            }
            headers = {"Content-Type": "application/json", **(headers or {})}
            r = requests.post(url, json=payload, headers=headers, auth=basic_auth, timeout=60)
        else:
            files = {"file": ("estimate_result.xlsx", excel_bytes,
                              "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet")}
            r = requests.post(url, files=files, headers=headers or None, auth=basic_auth, timeout=60)
        r.raise_for_status()
        return f"ok:{r.status_code}"
    except requests.RequestException as e:
        return f"failed:{e}"

# ---------- endpoints ----------
@app.get("/health")
def health():
    return {"ok": True}

@app.get("/debug/master-size")
def master_size():
    df = load_master_from_db()
    return {"master_rows": len(df), "cols": list(map(str, df.columns[:12]))}

@app.post("/debug/preview")
async def debug_preview(file: UploadFile = File(...)):
    qdf_raw = pd.read_excel(io.BytesIO(await file.read()))
    qdf = _normalize_query_df_to_cfg(qdf_raw)
    return {
        "rows_raw": len(qdf_raw),
        "rows_after_normalize": len(qdf),
        "columns_raw": list(map(str, qdf_raw.columns)),
        "columns_after": list(map(str, qdf.columns)),
        "nonempty_text": int(qdf[CFG.QUERY_TEXT_COL].astype(str).str.strip().ne("").sum()),
        "nonempty_unit": int(qdf[CFG.UNIT_COL].astype(str).str.strip().ne("").sum()),
        "nonempty_flag": int(qdf[CFG.QUERY_FLAG_COL].astype(str).str.strip().ne("").sum()),
    }

@app.post("/estimates/upload")
async def upload_estimate(
    file: UploadFile = File(...),
    post_to_friend: Optional[bool] = Form(False)
):
    try:
        # 1) queries
        qdf_raw = pd.read_excel(io.BytesIO(await file.read()))
        qdf = _normalize_query_df_to_cfg(qdf_raw)

        # 2) master (DB-dən)
        master_df = load_master_from_db()

        # 3) run → Excel (Summary + Details) azcon_match/output.py
        excel_bytes = build_excel_from_results(
            qdf[[CFG.QUERY_TEXT_COL, CFG.QUERY_FLAG_COL, CFG.UNIT_COL]],
            master_df,
            top_n=getattr(CFG, "TOP_N", 5)
        )

        # 4) optional POST
        x_post = "no"
        if post_to_friend:
            post_url = _env("RESULT_POST_URL")
            if post_url:
                post_mode = _env("RESULT_POST_MODE", "multipart").lower()
                headers, basic = _auth_from_env("POST_")
                x_post = _post_result(excel_bytes, post_url, post_mode, headers, basic)
            else:
                x_post = "skipped:no RESULT_POST_URL"

        return StreamingResponse(
            io.BytesIO(excel_bytes),
            media_type="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
            headers={
                "Content-Disposition": 'attachment; filename="estimate_result.xlsx"',
                "X-Post-To-Friend": x_post,
            },
        )
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@app.post("/estimates/process")
def process_from_url(payload: Dict[str, Any] = Body(...)):
    """
    JSON:
    {
      "source_url": "https://friend/api/export.xlsx",
      "source_auth": {"type":"bearer|apikey|basic|none", ...},   # optional
      "post_url": "https://friend/api/receive",                  # optional (env fallback)
      "post_mode": "multipart|json",                             # optional (env fallback)
      "post_auth": {"type":"bearer|apikey|basic|none", ...}      # optional (env fallback)
    }
    """
    try:
        src = (payload.get("source_url") or "").strip()
        if not src.startswith(("http://","https://")):
            raise ValueError("source_url must start with http:// or https://")

        # GET auth
        s_auth = payload.get("source_auth") or {}
        if s_auth:
            t = (s_auth.get("type") or "none").lower()
            if t == "bearer":
                g_headers, g_basic = {"Authorization": f"Bearer {s_auth.get('token','')}"}, None
            elif t == "apikey":
                g_headers, g_basic = {s_auth.get("header","X-API-Key"): s_auth.get("key","")}, None
            elif t == "basic":
                g_headers, g_basic = {}, (s_auth.get("user",""), s_auth.get("pass",""))
            else:
                g_headers, g_basic = {}, None
        else:
            g_headers, g_basic = _auth_from_env("GET_")

        # 1) download
        r = requests.get(src, headers=g_headers or None, auth=g_basic, timeout=60)
        r.raise_for_status()
        qdf_raw = pd.read_excel(io.BytesIO(r.content))
        qdf = _normalize_query_df_to_cfg(qdf_raw)

        # 2) master
        master_df = load_master_from_db()

        # 3) run
        excel_bytes = build_excel_from_results(
            qdf[[CFG.QUERY_TEXT_COL, CFG.QUERY_FLAG_COL, CFG.UNIT_COL]],
            master_df,
            top_n=getattr(CFG, "TOP_N", 5)
        )

        # 4) post back
        post_url = (payload.get("post_url") or _env("RESULT_POST_URL"))
        if post_url:
            post_mode = (payload.get("post_mode") or _env("RESULT_POST_MODE","multipart")).lower()
            p_auth = payload.get("post_auth") or {}
            if p_auth:
                t = (p_auth.get("type") or "none").lower()
                if t == "bearer":
                    p_headers, p_basic = {"Authorization": f"Bearer {p_auth.get('token','')}"}, None
                elif t == "apikey":
                    p_headers, p_basic = {p_auth.get("header","X-API-Key"): p_auth.get("key","")}, None
                elif t == "basic":
                    p_headers, p_basic = {}, (p_auth.get("user",""), p_auth.get("pass",""))
                else:
                    p_headers, p_basic = {}, None
            else:
                p_headers, p_basic = _auth_from_env("POST_")

            post_status = _post_result(excel_bytes, post_url, post_mode, p_headers, p_basic)
        else:
            post_status = "skipped:no post_url"

        return JSONResponse({
            "ok": True,
            "bytes_in": len(r.content),
            "bytes_out": len(excel_bytes),
            "post_status": post_status
        })
    except requests.RequestException as e:
        raise HTTPException(status_code=502, detail=f"network error: {e}")
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


