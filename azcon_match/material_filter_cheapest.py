
# import pandas as pd
# from .preprocessing import extract_material
# def choose_cheapest_subset(query_canon:str, cand_df:pd.DataFrame)->pd.DataFrame:
#     q_mat=extract_material(query_canon)
#     if q_mat: return cand_df[cand_df.get("material").fillna("").str.lower()==q_mat]
#     return cand_df  # No prune if no material in query
# azcon_match/material_filter_cheapest.py
import pandas as pd
from .preprocessing import extract_material

def choose_cheapest_subset(query_canon: str, cand_df: pd.DataFrame) -> pd.DataFrame:
    q_mat = extract_material(query_canon)
    if not q_mat:
        return cand_df  # No prune if no material in query

    # robust: if 'material' column is missing, act as if none matches
    if "material" not in cand_df.columns:
        return cand_df  # nothing to filter by, keep all

    mat_series = cand_df["material"].fillna("").str.lower()
    return cand_df[mat_series == q_mat]
