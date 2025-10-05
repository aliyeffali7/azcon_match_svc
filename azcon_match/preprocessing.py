
import json, pathlib, re
from typing import Set
import advertools as adv
import pandas as pd
TRANSLIT = str.maketrans("ğiçşöüə", "gıcsoue")
STOP_AZ  = adv.stopwords["azerbaijani"]
SUFFIXES = ["lanması","lənməsi","lanma","lənmə","nması","nməsi","ması","məsi","ların","lərin","ları","ləri","lar","lər"]; SUFFIXES.sort(key=len, reverse=True)
def _base_norm(tok:str)->str:
    tok=tok.lower().translate(TRANSLIT)
    for suf in SUFFIXES:
        if tok.endswith(suf): tok=tok[:-len(suf)]; break
    return tok
VOCAB_PATH = pathlib.Path(__file__).with_name("vocab.json"); _v=json.load(open(VOCAB_PATH,encoding="utf-8"))
SYN={_base_norm(k):_base_norm(v) for k,v in _v.get("synonyms",{}).items()}
SYN.update({k.translate(TRANSLIT):v.translate(TRANSLIT) for k,v in list(SYN.items()) if k.translate(TRANSLIT)!=k})
GENERIC:set[str] = {_base_norm(t) for t in _v.get("generic",[])}
CRITICAL:set[str]= {_base_norm(t) for t in _v.get("critical",[])}
_PHRASE_SYN={k:v for k,v in SYN.items() if " " in k}
def norm_token(tok:str)->str: 
    base=_base_norm(tok); return SYN.get(base,base)
def canon(text:str)->str:
    if not isinstance(text,str): return ""
    lowered=text.lower().translate(TRANSLIT)
    for p,r in _PHRASE_SYN.items(): lowered=lowered.replace(p,r)
    cleaned=re.sub(r"[^\w\s]"," ",lowered)
    tokens=[norm_token(t) for t in cleaned.split() if t not in STOP_AZ]
    return " ".join(tokens)
MATERIAL_REGEX=re.compile(r"\b(pvc|mdf|şüşə|suse|alüminium|aluminum|taxta|laminat|beton|daş|polikarbonat)\b",re.I)
def extract_material(text:str)->str|None:
    m=MATERIAL_REGEX.search(text or ""); return m.group(1).lower() if m else None
def coverage(a:Set[str],b:Set[str])->float: return len(a & b)/max(1,len(a))
# --- Add near other helpers in preprocessing.py ---
def is_generic_only(text: str) -> bool:
    """
    Return True if, after canon(), the query contains no discriminative tokens
    (i.e., all tokens are in the GENERIC set).
    """
    if not isinstance(text, str) or not text.strip():
        return True
    toks = set(canon(text).split())
    return len(toks - GENERIC) == 0

def non_generic_tokens(text: str) -> set[str]:
    """
    Convenience helper: the set of discriminative tokens in the query.
    """
    toks = set(canon(text).split())
    return toks - GENERIC
