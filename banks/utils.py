import unicodedata

def to_number(s):
    s = unicodedata.normalize("NFKD", str(s))
    s=s.replace(' ','').replace(',','.')
    try:
        s = float(s)
    except:
        s = 0
    return s
