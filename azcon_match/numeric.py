
import re
from typing import List, Tuple
UNIT_RE=r"(mm(?:\(?2\)?|2)?|cm(?:\(?2\)?|2)?|m(?:\(?2\)?|2)?|sm|qr|kv|a|v|w|ton)"
PATTERN=re.compile(rf"(?:d\s*=\s*)?(\d+(?:[.,]\d+)?)\s*{UNIT_RE}",re.I)
def extract(text:str)->List[Tuple[float,str]]:
    out=[]; 
    if not text: return out
    for num,unit in PATTERN.findall(text): out.append((float(num.replace(",",".")),unit.lower()))
    return out
