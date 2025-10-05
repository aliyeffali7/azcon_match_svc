# # azcon_match/output.py
# import io
# import pandas as pd
# from azcon_match import matcher

# def build_excel_from_results(qdf: pd.DataFrame, master_df: pd.DataFrame, top_n: int = 5) -> bytes:
#     """
#     qdf: canonical kolonlarla gəlməlidir: [QUERY_TEXT_COL, QUERY_FLAG_COL, UNIT_COL]
#     master_df: load_master_from_db() nəticəsi
#     """
#     summary_lines, detail_rows = [], []

#     for q_text, q_flag, q_unit in qdf.itertuples(index=False, name=None):
#         res = matcher.find_matches(q_text, q_flag, q_unit, master_df)
#         summary_lines.extend(matcher.summarise(res).splitlines())
#         summary_lines.append("")  # sorğular arasında boş sətir

#         priced = res.get("priced_hits") or []
#         hits = priced if priced else (res.get("hits") or [])
#         if hits:
#             for (t, sc, pr, u) in hits[:top_n]:
#                 detail_rows.append({
#                     "query_text": q_text,
#                     "query_flag": q_flag,
#                     "query_unit": q_unit,
#                     "match_text": t,
#                     "score": sc,
#                     "price": pr,
#                     "unit": u,
#                 })
#         else:
#             detail_rows.append({
#                 "query_text": q_text,
#                 "query_flag": q_flag,
#                 "query_unit": q_unit,
#                 "match_text": "",
#                 "score": 0,
#                 "price": None,
#                 "unit": "",
#             })

#     buf = io.BytesIO()
#     with pd.ExcelWriter(buf, engine="openpyxl") as w:
#         pd.DataFrame({"Summary": summary_lines}).to_excel(w, index=False, sheet_name="Summary")
#         pd.DataFrame(detail_rows).to_excel(w, index=False, sheet_name="Details")
#     buf.seek(0)
#     return buf.getvalue()

# azcon_match/output.py
import io
import pandas as pd
from azcon_match import matcher
import numpy as np

def _fmt_line(t, sc, pr, u):
    # “3-lü keçid 32 – 0.7 ₼ / ədəd (score 86)”
    price_str = f"{pr:.1f} ₼" if pd.notna(pr) else "—"
    unit_str  = (u or "").strip() or ""
    unit_part = f" / {unit_str}" if unit_str else ""
    return f"{t} – {price_str}{unit_part} (score {int(sc)})"

def build_excel_from_results(qdf: pd.DataFrame, master_df: pd.DataFrame, top_n: int = 5) -> bytes:
    """
    ÇIXIŞ: analyzed_Book1 stilində tək 'Sheet1'
      Sütunlar: Sual | Qiymət | Ölçü vahidi | Uyğunluq dərəcəsi | Uyğun gələn sətrlər
    qdf → sütunları bu sırayla olmalıdır: [QUERY_TEXT_COL, QUERY_FLAG_COL, UNIT_COL]
    """
    rows = []
    for q_text, q_flag, q_unit in qdf.itertuples(index=False, name=None):
        res = matcher.find_matches(q_text, q_flag, q_unit, master_df)

        priced = res.get("priced_hits") or []
        hits   = priced if priced else (res.get("hits") or [])

        # sıralama: əvvəlcə score ↓, sonra price ↑ (qiymət eyni deyilsə)
        def sort_key(x):
            t, sc, pr, u = x
            # NaN qiymətləri sonda saxla
            pr_key = float(pr) if pd.notna(pr) else float("inf")
            return (-int(sc), pr_key)

        hits_sorted = sorted(hits, key=sort_key)
        top_hits    = hits_sorted[:top_n]   # limitsiz istəyirsənsə: top_hits = hits_sorted

        if top_hits:
            # --- Qiymətlərin ədədi ortası ---
            prices = [float(pr) for (_, _, pr, _) in top_hits if pd.notna(pr)]
            mean_price = round(float(np.mean(prices)), 2) if prices else None

            # --- Vahid: ən çox təkrarlanan (mode), yoxdursa top hit vahidi ---
            units = [ (u or "").strip() for (_, _, _, u) in top_hits if (u or "").strip() ]
            if units:
                # mode
                counts = {}
                for uu in units:
                    counts[uu] = counts.get(uu, 0) + 1
                vahid = max(counts, key=counts.get)
            else:
                # fallback top hit unit
                _, sc0, pr0, u0 = top_hits[0]
                vahid = (u0 or "").strip()

            # --- Score: top hit-in score-u (istəsən orta score da yaza bilərik) ---
            _, sc0, _, _ = top_hits[0]
            score0 = int(sc0)

            # --- Uyğun gələn sətrlər (vizual siyahı) ---
            lines  = "\n".join(_fmt_line(t, sc, pr, u) for (t, sc, pr, u) in top_hits)

            qiymet = mean_price
        else:
            qiymet = None
            vahid  = ""
            score0 = 0
            lines  = "— tapılmadı"

        rows.append({
            "Sual": q_text,
            "Qiymət": qiymet,
            "Ölçü vahidi": vahid,
            "Uyğunluq dərəcəsi": score0,
            "Uyğun gələn sətrlər": lines,
        })

    out = io.BytesIO()
    with pd.ExcelWriter(out, engine="openpyxl") as w:
        pd.DataFrame(rows).to_excel(w, index=False, sheet_name="Sheet1")
    out.seek(0)
    return out.getvalue()
