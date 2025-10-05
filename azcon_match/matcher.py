
import statistics
from typing import List, Tuple, Dict, Any
import pandas as pd
from rapidfuzz import fuzz
from . import preprocessing as pp, numeric
Match = Tuple[str,int,float,str]
def _normalize_unit(u:str)->str:
    if not isinstance(u,str): return ""
    u=u.strip().lower()
    return {"m²":"m(2)","m2":"m(2)","m(2)":"m(2)","m":"m","metr":"m","pm":"m","əd":"ədəd","ed":"ədəd","eded":"ədəd","ədəd":"ədəd","ton":"ton"}.get(u,u)
def score_row(q_tok:set,s_tok:set,q:str,s:str)->int:
    score=fuzz.token_set_ratio(q,s)
    if any((c in q_tok)^(c in s_tok) for c in pp.CRITICAL): score=int(score*0.80)
    return score
def find_matches(query_raw:str, query_flag:str, query_unit:str, master_df:pd.DataFrame)->Dict[str,Any]:
    from .data_loader import normalize_flag, normalize_unit
    q_can=pp.canon(query_raw); q_tokens=set(q_can.split())
    q_nums=numeric.extract(query_raw); has_qnum=bool(q_nums)
    q_flag=normalize_flag(query_flag); q_unit=normalize_unit(query_unit)
    from .material_filter_cheapest import choose_cheapest_subset
    cand=choose_cheapest_subset(q_can, master_df)
    if q_flag in {"məhsul","xidmət","mix"}: cand=cand[cand["Tip"].map(normalize_flag)==q_flag]
    if q_unit: cand=cand[cand["Ölçü vahidi"].map(normalize_unit)==q_unit]
    hits:List[Match]=[]
    for s_text,s_flag,price,unit,s_can,s_tokens in cand[["Malların (işlərin və xidmətlərin) adı","Tip","Qiyməti","Ölçü vahidi","canon","tokens"]].itertuples(index=False, name=None):
        if not (q_tokens & (s_tokens - pp.GENERIC)): continue
        if any(c in q_tokens and c not in s_tokens for c in pp.CRITICAL): continue
        if pp.coverage(q_tokens,s_tokens) < 0.50: continue
        penal=1.0
        if has_qnum:
            c_nums=numeric.extract(s_text)
            if c_nums and not any(q==c for q in q_nums for c in c_nums): continue
            if not c_nums: penal=0.80
        score=int(score_row(q_tokens,s_tokens,q_can,s_can)*penal)
        if score<80: continue
        hits.append((s_text,score,price,unit))
    priced=[(t,sc,pr,u) for (t,sc,pr,u) in hits if (sc>=8 and pd.notna(pr))]
    prices=[pr for _,_,pr,_ in priced]
    return {"raw": query_raw, "canonical": q_can, "unit": q_unit or "?", "hits": hits, "priced_hits": priced, "prices": prices}
def summarise(res:Dict[str,Any])->str:
    lines=[f"Query: {res['raw']}  (unit:{res['unit']})"]
    if res["prices"]:
        med=statistics.median(res["prices"]); avg=sum(res["prices"])/len(res["prices"])
        u=res["priced_hits"][0][3] if res["priced_hits"] else "?"
        lines.append(f"   → Median: {med:.2f} ₼ / {u} | Mean: {avg:.2f} ₼ (n={len(res['prices'])})")
        for t,sc,pr,u in res["priced_hits"]: lines.append(f"      • {t} – {pr} ₼ / {u}  (score {sc})")
    else:
        lines.append("   – no priced matches ≥ 8 –")
        for t,sc,pr,u in sorted(res["hits"], key=lambda x: x[1], reverse=True)[:5]:
            lines.append(f"      · {t} – {('—' if pd.isna(pr) else pr)} / {u}  (score {sc})")
    return "\n".join(lines)
