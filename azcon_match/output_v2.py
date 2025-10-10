# output_v2.py
import io
from typing import List, Dict, Any, Optional
import pandas as pd

V2_COLS = ["ad", "amount", "unit", "price", "total_price", "total_with_edv"]

def _to_float(x) -> Optional[float]:
    try:
        if x is None:
            return None
        if isinstance(x, str):
            x = x.strip().replace(",", ".")
            if x == "":
                return None
        return float(x)
    except Exception:
        return None

def _mode(values: List[Any]) -> Optional[Any]:
    vals = [v for v in values if v not in (None, "", "nan")]
    if not vals:
        return None
    s = pd.Series(vals)
    return s.mode().iloc[0] if not s.mode().empty else None

def _mean_float(values: List[Any]) -> Optional[float]:
    nums = [v for v in (_to_float(x) for x in values) if v is not None]
    if not nums:
        return None
    return float(sum(nums) / len(nums))

def build_output_df_v2(
    qdf: pd.DataFrame,
    matches: List[Dict[str, Any]],
    *,
    text_col: str,
    amount_col: Optional[str] = None,
) -> pd.DataFrame:
    rows = []
    n = len(qdf)

    for i in range(n):
        q_text = str(qdf.iloc[i][text_col]) if text_col in qdf.columns else ""

        amount = 1.0
        if amount_col and amount_col in qdf.columns:
            amount = _to_float(qdf.iloc[i][amount_col]) or 1.0

        hits = matches[i] if i < len(matches) else []
        price_list, unit_list = [], []
        for h in hits:
            if isinstance(h, (list, tuple)) and len(h) >= 4:
                price_list.append(h[2])
                unit_list.append(h[3])
            elif isinstance(h, dict):
                price_list.append(h.get("price"))
                unit_list.append(h.get("unit"))

        mean_price = _mean_float(price_list)
        unit = _mode(unit_list)

        total_price = None
        total_with_edv = None
        if mean_price is not None:
            total_price = round((amount or 0) * mean_price, 4)
            total_with_edv = round(total_price * 1.18, 4)

        rows.append({
            "ad": q_text,
            "amount": amount,
            "unit": unit,
            "price": mean_price,
            "total_price": total_price,
            "total_with_edv": total_with_edv,
        })

    df = pd.DataFrame(rows, columns=V2_COLS)

    # Alt cəm sətri (ədədi)
    sum_total = float(df["total_price"].fillna(0).sum())
    sum_total_vat = float(df["total_with_edv"].fillna(0).sum())
    total_row = {
        "ad": "", "amount": "", "unit": "", "price": "",
        "total_price": round(sum_total, 4),
        "total_with_edv": round(sum_total_vat, 4),
    }
    df = pd.concat([df, pd.DataFrame([total_row])], ignore_index=True)
    return df

def build_excel_bytes_v2(df: pd.DataFrame, sheet_name: str = "ResultV2") -> bytes:
    bio = io.BytesIO()
    with pd.ExcelWriter(bio, engine="xlsxwriter") as writer:
        df.to_excel(writer, index=False, sheet_name=sheet_name)
        try:
            wb = writer.book
            ws = writer.sheets[sheet_name]
            bold = wb.add_format({"bold": True})
            last_row = len(df)  # 1-based excel row index (header + data)
            ws.set_row(last_row, None, bold)  # son sətir qalın
        except Exception:
            pass
    return bio.getvalue()
