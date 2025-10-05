# azcon_match/api.py
from __future__ import annotations

from typing import Dict, Any, List, Tuple, Optional
from pathlib import Path
import importlib
import logging

from . import config

logger = logging.getLogger(__name__)

# ---------------------------------------------------------
# Aşağı qatdakı matcher modulunu ağıllı import et
# ---------------------------------------------------------
def _import_matcher_module():
    for name in ("azcon_match.matcher_v2", "azcon_match.matcher"):
        try:
            return importlib.import_module(name)
        except ModuleNotFoundError:
            continue
    raise ModuleNotFoundError("Matcher modulunu tapa bilmədim (matcher_v2/matcher).")

_matcher = _import_matcher_module()

# ---------------------------------------------------------
# Master yükləyici
# ---------------------------------------------------------
def load_master(path: Optional[str | Path] = None):
    """
    Master Excel-i oxuyur və alqoritmin gözlədiyi DataFrame qaytarır.
    path verilibsə onu, verilməyibsə data_loader.load_master() default konfiqi istifadə edir.
    """
    try:
        from . import data_loader
        if path is not None:
            return data_loader.load_master(path=str(path))
        return data_loader.load_master()
    except Exception as e:
        # Fallback: birbaşa pandas
        import pandas as pd
        _p = str(path) if path else getattr(config, "MASTER_PATH", None)
        if not _p:
            raise RuntimeError("Master yolunu tapa bilmədim. load_master(path=...) ötür.") from e
        logger.warning("data_loader.load_master alınmadı, pandas ilə oxuyuram: %s (%s)", _p, e)
        return pd.read_excel(_p)

# ---------------------------------------------------------
# Nəticəni sabit formata çevirən helper
# ---------------------------------------------------------
def _normalize_result(raw_res: Any) -> Dict[str, Any]:
    """
    Çıxış formatı (sabit):
      {
        "priced_hits": List[Tuple[text:str, score:int, price:float|None, unit:str]],
        "why": [...],
        "stats": {...}
      }
    """
    out: Dict[str, Any] = {"priced_hits": [], "why": [], "stats": {}}

    if raw_res is None:
        return out

    if isinstance(raw_res, dict) and "priced_hits" in raw_res:
        out["priced_hits"] = raw_res.get("priced_hits") or []
        out["why"] = raw_res.get("why") or []
        out["stats"] = raw_res.get("stats") or {}
        return out

    if isinstance(raw_res, list):
        hits = []
        for it in raw_res:
            if isinstance(it, (list, tuple)):
                t = it[0] if len(it) > 0 else ""
                sc = it[1] if len(it) > 1 else 0
                pr = it[2] if len(it) > 2 else None
                un = it[3] if len(it) > 3 else ""
                hits.append((t, sc, pr, un))
            elif isinstance(it, dict):
                t = it.get("name") or it.get("text") or ""
                sc = it.get("score") or it.get("similarity") or 0
                pr = it.get("price") or it.get("avg_price")
                un = it.get("unit") or it.get("uom") or ""
                hits.append((t, sc, pr, un))
        out["priced_hits"] = hits
        return out

    if isinstance(raw_res, dict):
        container = raw_res.get("items") or raw_res.get("results") or raw_res.get("entries")
        if isinstance(container, list):
            hits = []
            for it in container:
                if isinstance(it, dict):
                    t = it.get("name") or it.get("text") or ""
                    sc = it.get("score") or it.get("similarity") or 0
                    pr = it.get("price") or it.get("avg_price")
                    un = it.get("unit") or it.get("uom") or ""
                    hits.append((t, sc, pr, un))
                elif isinstance(it, (list, tuple)):
                    t = it[0] if len(it) > 0 else ""
                    sc = it[1] if len(it) > 1 else 0
                    pr = it[2] if len(it) > 2 else None
                    un = it[3] if len(it) > 3 else ""
                    hits.append((t, sc, pr, un))
            out["priced_hits"] = hits
            out["why"] = raw_res.get("why") or []
            out["stats"] = raw_res.get("stats") or {}
            return out

    return out

# ---------------------------------------------------------
# Public API – views.py yalnız bunları çağıracaq
# ---------------------------------------------------------
def find_matches(q_raw: str, q_flag: str, q_unit: str, master_df) -> Dict[str, Any]:
    """
    Altda funksiyanın adı/signature-ı fərqli ola bilər.
    Burda bir neçə mümkün variantı cəhd edirik, nəticəni normalize edirik.
    İstənilən səhvdə BOŞ struktur qaytarırıq (None YOX!).
    """
    candidates = [
        ("find_matches", (q_raw, q_flag, q_unit, master_df), {}),
        ("process",      (q_raw, q_flag, q_unit, master_df), {}),
        ("run",          (), {"query": q_raw, "flag": q_flag, "unit": q_unit, "master": master_df}),
        ("match",        (), {"query": q_raw, "flag": q_flag, "unit": q_unit, "master": master_df}),
    ]

    for fname, args, kwargs in candidates:
        func = getattr(_matcher, fname, None)
        if callable(func):
            try:
                raw = func(*args, **kwargs)
                return _normalize_result(raw)
            except TypeError:
                # signature uyğun gəlməyibsə növbəti varianta keç
                continue
            except Exception as e:
                logger.error("matcher.%s çağırışında xəta: %s", fname, e, exc_info=True)
                return {"priced_hits": [], "why": [f"error:{fname}:{e}"], "stats": {}}

    CandidateClass = getattr(_matcher, "Matcher", None) or getattr(_matcher, "Engine", None)
    if CandidateClass:
        try:
            obj = CandidateClass()
            for mname in ("predict", "match", "run"):
                m = getattr(obj, mname, None)
                if callable(m):
                    try:
                        raw = m(q_raw, q_flag, q_unit, master_df)
                    except TypeError:
                        raw = m(query=q_raw, flag=q_flag, unit=q_unit, master=master_df)
                    return _normalize_result(raw)
        except Exception as e:
            logger.error("Matcher/Engine obyekti ilə xəta: %s", e, exc_info=True)
            return {"priced_hits": [], "why": [f"error:class:{e}"], "stats": {}}

    # Son çarə – BOŞ (None yox!)
    return {"priced_hits": [], "why": [], "stats": {}}
