
from dataclasses import dataclass, asdict
from typing import List, Dict, Set
import re
from . import preprocessing as pp
@dataclass
class CanonTrace:
    raw:str; lowered:str; phrase_replaced:str; cleaned:str
    tokens:List[str]; tokens_nostop:List[str]; norm_tokens:List[str]; norm_set:Set[str]
    def as_dict(self)->Dict: return asdict(self)
def trace(text:str)->"CanonTrace":
    raw=text or ""; lowered=raw.lower().translate(pp.TRANSLIT)
    phrase_replaced=lowered
    for p,r in pp.SYN.items():
        if " " in p: phrase_replaced=phrase_replaced.replace(p,r)
    cleaned=re.sub(r"[^\w\s]"," ",phrase_replaced); cleaned=re.sub(r"\s+"," ",cleaned).strip()
    tokens=cleaned.split(); tokens_nostop=[t for t in tokens if t not in pp.STOP_AZ]
    norm_tokens=[pp.norm_token(t) for t in tokens_nostop]; norm_set=set(norm_tokens)
    return CanonTrace(raw,lowered,phrase_replaced,cleaned,tokens,tokens_nostop,norm_tokens,norm_set)
def compare(a:str,b:str)->dict:
    ta,tb=trace(a),trace(b); overlap=ta.norm_set & tb.norm_set
    return {"overlap":overlap,"only_in_a":ta.norm_set-overlap,"only_in_b":tb.norm_set-overlap,
            "coverage_a":pp.coverage(ta.norm_set,tb.norm_set),"coverage_b":pp.coverage(tb.norm_set,ta.norm_set),
            "critical_mismatch":any((c in ta.norm_set)^(c in tb.norm_set) for c in pp.CRITICAL)}
# --- add this in diagnostics.py ---
from typing import Dict, Any
from . import preprocessing as pp, numeric, config

def explain_candidate(q_raw: str, row: Dict[str, Any]) -> str:
    # row must contain: text, canon, tokens
    q_can = pp.canon(q_raw); q_tok = set(q_can.split())
    s_text, s_can, s_tokens = row["text"], row["canon"], row["tokens"]

    # adaptive coverage for short queries
    qt = len(q_tok); min_cover = config.MIN_COVER
    if qt <= 2: min_cover = 0.25
    elif qt == 3: min_cover = 0.35

    if not (q_tok & (s_tokens - pp.GENERIC)): return "fail: no non-generic token overlap"
    if any(c in q_tok and c not in s_tokens for c in pp.CRITICAL): return "fail: missing critical term(s)"
    if pp.coverage(q_tok, s_tokens) < min_cover: return "fail: coverage"
    # numeric check (current exact match rule)
    qn = numeric.extract(q_raw); cn = numeric.extract(s_text)
    if qn and cn and not any(q == c for q in qn for c in cn): return "fail: numeric mismatch"
    return "pass"


