"""
Deterministic salary parser — prototype (measurement pass, CAO 10).
Reads first-LLM wage_information blocks, emits SalaryRow dicts matching
schema/salary_schema.py. NO LLM. Heuristic column-role classification +
include/exclude. READ-ONLY on inputs.

Goal of this version: reproduce the *standard* wage tables (the ones that
survive into llm_analysis) so we can measure parser-vs-LLM agreement.
"""
import json, re, glob, os, collections

# ----------------------------------------------------------------------
# regexes / vocab
# ----------------------------------------------------------------------
DATE_RE = re.compile(r'(\d{1,2})[/-](\d{1,2})[/-](\d{2,4})')
NUM_RE  = re.compile(r'^-?\d[\d.,]*\d$|^-?\d$')
PCT_RE  = re.compile(r'(\d+(?:[.,]\d+)?)\s*%')

UNIT_MAP = [
    ('4-week','4-week'), ('4 week','4-week'), ('vierwek','4-week'), ('periode','period'),
    ('uur','hourly'), ('hour','hourly'),
    ('week','weekly'),
    ('maand','monthly'), ('month','monthly'),
    ('jaar','annual'), ('annual','annual'), ('year','annual'),
]
# unit-preference order (TABLE SELECTION rule)
UNIT_PREF = {'monthly':0,'hourly':1,'4-week':2,'weekly':3,'annual':4,'period':5,None:9}

KEY_AGE   = ('age','leeftijd')
KEY_STEP  = ('trede','periodiek','periode','increment','volgnr','inpas','anciennit',
             'dienstjaar','staffel','step','half-year','half year','halfjaar','max. duration',
             'maximum duration','maximale duur','max. duur','max duur')
KEY_GROUP = ('functiegroep','function group','function level','functieniveau',
             'function years','scale','schaal','salaris-functie','groep','group')

# Exclusion-driven inclusion: a wage TABLE (grid) is in scope UNLESS its
# description signals a non-standard / youth / allowance table. This generalises
# far better across CAOs than a worker-name whitelist (Dutch & English alike).
EXCLUDE_DESC = (
    # apprentices / trainees
    'apprentice','leerling','trainee','intern ','stagiair',
    'beroepsbegeleidende','beroepsopleidende','vocational training',
    'following a training','following training','practical training','bbl-','bbl ','bol-',
    # youth / under-adult tables (EN + NL). bare 'youth'/'jeugd' intentionally OMITTED
    # (collide with "(youth) wage" WML refs); youth ROWS are dropped at row level.
    '16 to 20','16-20','16 t/m','t/m 20','t/m 21','up to 21','up to 23','younger than',
    'jonger dan','tot 21','tot 23','under 23','under 21',
    'jeugdschaal','jeugdschalen','jeugdloonschaal','youth scale','youth wage table',
    # NOTE: entry/'aanloop'/start scales are INCLUDED and tagged is_entry (Hanna Phase-0
    # decision, implemented 2026-07-03) -- see ENTRY_DESC / is_entry_row below.
    # trainee teachers / subsidised through-flow schemes / reference-only tables
    'teacher in training','in training','in opleiding','lio',
    'through-flow','doorstroombanen','in- and through','in- en doorstroom',
    '15-year outlook','outlook','guaranteed scale',
    # allowances / bonuses / reimbursements (not base pay)
    'allowance','bonus','toeslag','incentive','distance','driving','accident-free',
    'reiskosten','vergoeding','premie','overtime','irregular','consignatie',
    # the scaffolding ALLOWANCE table in this sector (not a base scale)
    'scaffolding allowance','steiger',
    # profit-share / leave-buy-back supplements are not base pay (Haiku wave, CAO 759)
    'tantième','tantieme','verlofsparen','leave buy-back','leave-buy-back',
    # merit/growth PERCENTAGE matrices (RSP % by performance rating, CAO 163) -- the
    # cells are growth percentages, not wages
    'salarisgroeitabel','salary growth table','groeitabel','merit matrix','merittabel',
)


def doc_effective_date(extract):
    """best-effort YYYY-MM-DD from general_information validity start."""
    months = {'january':1,'februari':2,'february':2,'januari':1,'march':3,'maart':3,'april':4,
              'may':5,'mei':5,'june':6,'juni':6,'july':7,'juli':7,'august':8,'augustus':8,
              'september':9,'october':10,'oktober':10,'november':11,'december':12}
    gi = extract.get('general_information', [])
    flat = []
    for s in gi:
        if isinstance(s, list): flat.extend(x for x in s if isinstance(x,str))
        elif isinstance(s, str): flat.append(s)
    for s in flat:
        sl = s.lower()
        if 'validity start' in sl or 'effective date' in sl or 'contract period' in sl:
            m = re.search(r'(\d{1,2})\s+([a-z]+)\s+(\d{4})', sl)
            if m and m.group(2) in months:
                return '%04d-%02d-%02d' % (int(m.group(3)), months[m.group(2)], int(m.group(1)))
            m2 = DATE_RE.search(sl)
            if m2:
                d,mo,y = m2.groups(); y=int(y); y=y+2000 if y<100 else y
                return '%04d-%02d-%02d' % (y,int(mo),int(d))
    return None


def parse_header_date(h):
    # SEASON header '2016/17' (football/sports CAOs, e.g. 1809 referees): the season is
    # the date axis; unparsed it leaked into jobgroup with date=None (relabel-agent
    # autopsy finding). Mapped to the season START -- Dutch sports seasons run
    # 1 July-30 June (documented convention, same category as the end_date derivation).
    ms = re.fullmatch(r'((?:19|20)\d\d)\s*/\s*(\d{2})', h.strip())
    if ms and int(ms.group(2)) == (int(ms.group(1)) + 1) % 100:
        return '%s-07-01' % ms.group(1)
    m = DATE_RE.search(h)
    if not m: return None
    d,mo,y = m.groups(); y=int(y); y=y+2000 if y<100 else y
    return '%04d-%02d-%02d' % (y,int(mo),int(d))


def parse_num(cell):
    # strip currency symbols and all whitespace (incl. NBSP) -> handles "€4,734", "1 234,50"
    c = re.sub(r'[€$£\s ]', '', cell.strip())
    # footnote markers: '2185.13*' (holistic audit, CAO 1165: starred WML-tied cells were
    # silently dropped). Strip at both ends only -- '*' INSIDE a token still fails NUM_RE.
    c = c.strip('*†')
    if not c or not NUM_RE.match(c): return None
    if ',' in c and '.' in c:
        # last separator is the decimal one
        if c.rfind(',') > c.rfind('.'):   # european: 2.204,92
            c = c.replace('.','').replace(',','.')
        else:                              # anglo: 2,204.92
            c = c.replace(',','')
    elif ',' in c:
        # single comma: decimal if 1-2 trailing digits else thousands
        dec = c.rsplit(',',1)[1]
        c = c.replace(',','.') if len(dec) <= 2 else c.replace(',','')
    elif c.count('.') > 1:
        # DOTTED CODE guard (Haiku wave, CAO 279): '01.04.05'-style classification/article
        # numbers have ALL groups <=2 digits -- a genuine thousands format always has a
        # 3-digit group ('1.653.60' ok). Without this, codes became pseudo-amounts (104.05).
        if all(len(g) <= 2 for g in c.lstrip('-').split('.')):
            return None
        # multi-dot, no comma: '1.234.567' = all-thousands; BUT '1.653.60' uses the LAST
        # dot as the DECIMAL (1-2 trailing digits) -> 1653.60. Haiku gate finding (CAO 827):
        # the old rule read it as 165360, a x100 error invisible to provenance (the audit
        # shares this parser).
        intp, dec = c.rsplit('.', 1)
        if len(dec) <= 2:
            c = intp.replace('.','') + '.' + dec
        else:
            c = c.replace('.','')          # 1.234.567 thousands
    elif '.' in c:
        # single dot, no comma: Dutch THOUSANDS separator if exactly 3 trailing digits
        # ("2.107"->2107, "21.580"->21580), but a real decimal if 1-2 ("15.34", "2.5").
        intp, dec = c.rsplit('.',1)
        if len(dec) == 3 and intp.lstrip('-').isdigit():
            c = c.replace('.','')
    try: return float(c)
    except: return None


# MANGLED-DECIMAL pattern (upstream artifact, CAO 592): '2.184,54' lost its comma ->
# '2.18454'. Single dot, 4-6 decimal digits, NONZERO integer part (excludes genuine
# tiny factors like '0.0018' in multiplier tables). 3 decimals is already handled as
# Dutch thousands by parse_num. Rescue happens in parse_extract WITH table context.
MANGLED_DEC = re.compile(r'^-?[1-9]\d{0,2}\.\d{4,6}$')

GROUPLABEL_PREFIX = re.compile(
    r'^(wage ?group|loongroep|salarisschaal|functiegroep|function ?group|salary ?group|'
    r'scale|schaal|groep|group|niveau|level)[:\s]*', re.I)

def is_grouplabel(h):
    """header cell that is a bare job-group / scale code (matrix column)."""
    s = GROUPLABEL_PREFIX.sub('', h.strip()).strip()
    return bool(re.fullmatch(r'\d{1,2}[a-z]?|[IVX]{1,4}|[A-Za-z]\d?|[A-Za-z]/\d{1,2}', s))

def grouplabel_value(h):
    return GROUPLABEL_PREFIX.sub('', h.strip()).strip()


SCALE_CODE = re.compile(r'^([A-Za-z]{1,3}|\d{1,2})[.\-\s]+(\d{1,2}[a-z]?)$')
def split_scale_code(s):
    """split a '<group><sep><step>' cell into (group, step):
    '1.01'->('1','01'), 'LB.01'->('LB','01'), '18.13'->('18','13'),
    'A 1'->('A','1'), 'A-4'->('A','4'); else (s, None)."""
    m = SCALE_CODE.fullmatch(s.strip())
    if m:
        return m.group(1), m.group(2)
    return s, None


# Header tokens that mark a column as a ROW-AXIS key even when its cells are numeric
# (e.g. "Experience years" cells 0..13). Deliberately EXCLUDES group/scale/level —
# those appear inside VALUE-column headers ("Group 1 Min.") as jobgroup parameters.
# row-axis key headers. NOTE: bare 'year'/'jaar' removed — they collide with value
# columns like "2291.50 hours per year" (an FTE basis carrying monthly pay). Experience
# /function-year STEP columns are caught by content (small integer cells = not money).
KEY_HEADER = ('age','leeftijd','experience','ervaring',
              'trede','periodiek','periode','staffel','anciennit','dienstjaar',
              'increment','volgnr','inpas','step','stap','function years','functiejaren',
              'service','seniorit')

EMBED_JG = re.compile(
    r'(?:group|groep|schaal|scale|functieniveau|function level|niveau|level|'
    r'functiegroep|function group|salarisschaal)\s*([0-9]{1,2}[a-z]?|[IVX]{1,4}|[A-Za-z])\b', re.I)
# step axis encoded in a VALUE-column header, e.g. "Functiejaar 3", "Trede 2", "Year 1"
EMBED_STEP = re.compile(
    r'(?:functiejaar|schaaljaar|dienstjaar|ervaringsjaar|service ?year|'
    r'trede|periodiek|periode|step|stap|jaar|year|anc)\s*\.?\s*([0-9]{1,2})', re.I)
# NUMBER-FIRST step header, e.g. "0 Functional year", "3 functiejaren", "5 dienstjaren"
# (CAO 709: without this all ten "N Functional years" columns collapsed into ONE
# dup-date identity, and 'year' leaked into unit detection as 'annual')
EMBED_STEP_PRE = re.compile(
    r'^\s*([0-9]{1,2})\s*(?:functional\s+years?|functie\s?jaren?|functiejaar|fj|schaaljaren?|'
    r'dienstjaren?|ervaringsjaren?|service\s?years?|treden?|periodieken?)\b', re.I)
# HALF-YEAR / entry progression step header, e.g. "1e halfjaar", "2e half-year", "inloop"
# (CAO 10 Bouw: value columns '1e halfjaar (uur) | 2e halfjaar (uur) | uurloon' are STEPS
# at one date -- without a step label they collapsed into ONE same-date timeline labeled
# '(scale)'). 'halfjaar' must NOT be read as 'jaar'->annual, so it is stripped/matched here.
EMBED_HALFYEAR = re.compile(r'\b(\d)\s*e?\s*half\s*[- ]?\s*(?:jaar|year)\b', re.I)
EMBED_INLOOP = re.compile(r'\b(inloop|aanloop|instap)\b', re.I)
# step-axis phrase to strip BEFORE unit detection (its 'year'/'jaar' is a step word, not pay period)
STEP_PHRASE = re.compile(
    r'\d{1,2}\s*(?:functional\s+years?|functie\s?jaren?|functiejaar|schaaljaren?|'
    r'dienstjaren?|ervaringsjaren?|service\s?years?)|\bhalf\s*[- ]?\s*(?:jaar|year)\b', re.I)

def parse_value_header(h):
    """Parse a VALUE column header: date, unit, min/max, embedded jobgroup, embedded step."""
    hl = h.lower().strip()
    date = parse_header_date(h) or date_from_desc(h)   # numeric OR textual ("1 January 2011")
    # FTE-basis header ("2291.50 hours per year", "AVW (2,149.5 hours/year)" -- CAO 634's
    # slash form, "Annual Hours 2247.70" -- its label-first form) carries base PERIOD pay,
    # not hourly/annual
    if re.search(r'(hours?|uren|uur)\s*(?:per|/)\s*(year|jaar)', hl) \
       or re.search(r'(annual\s+hours|jaaruren)\s*[:=]?\s*[\d.,]+', hl) \
       or re.search(r'per\s+(year|jaar)\b', hl) and re.search(r'hour|uur|uren', hl):
        unit = 'monthly'
    else:
        # strip an FTE EMPLOYMENT-BASIS clause ("38 hours per week", "bij 36 uur per week")
        # BEFORE unit detection: its 'hour'/'week' words describe the FTE basis, not the pay
        # period -- 'Monthly salary (38 hours per week)' must not be read as hourly/weekly
        hu = re.sub(r'\d+(?:[.,]\d+)?\s*(?:hours?|uren|uur)\s*(?:per|/)\s*(?:week|weken|maand|month)', ' ', hl)
        hu = STEP_PHRASE.sub(' ', hu)   # '0 Functional year' must not read as unit=annual
        if 'per uur' in hu or 'base hourly' in hu or 'hourly wage' in hu or re.search(r'\buur\b', hu) or 'per hour' in hu:
            unit = 'hourly'
        else:
            unit = next((u for k,u in UNIT_MAP if k in hu), None)
    # word-boundary: bare 'min'/'max' substrings hit 'terMINal', 'adMINistratie', 'MAXimale duur'.
    # 'van(af)'/'tot' count ONLY as the whole header (the Dutch 'Van | Tot' min/max pair) --
    # inside longer headers they are usually date ranges ('1-1-2015 tot 1-7-2015').
    if re.search(r'(?<![a-z])(?:min|minimum|minimaal)(?![a-z])', hl) and 'duur' not in hl and 'duration' not in hl:
        minmax = 'min'
    elif re.search(r'(?<![a-z])(?:max|maximum|maximaal)(?![a-z])', hl) and 'duur' not in hl and 'duration' not in hl:
        minmax = 'max'
    elif hl.strip() in ('van', 'vanaf'):
        minmax = 'min'
    elif hl.strip() in ('tot', 't/m', 'tot en met'):
        minmax = 'max'
    else:
        minmax = None
    jg = None
    m = EMBED_JG.search(h)
    if m: jg = m.group(1).upper()
    elif is_grouplabel(h): jg = grouplabel_value(h)
    # 'maandloon A' / 'uurloon B' / 'salaris C' (CAO 679 taxi rijdend-personeel: value
    # columns are (unit x scale) pairs) -- the trailing token is the jobgroup/scale.
    # Also 'C (monthly wage)' / 'A and B (monthly wage)' (CAO 1644 transposed jobgroup
    # columns): the unit clause set `unit` and blocked the bare-jobgroup branch below, so
    # the scale label was lost and the age-rows collapsed into one same-date timeline.
    elif unit is not None and minmax is None and jg is None and not date:
        mw = re.fullmatch(r'(?:maand|uur|week|jaar)?(?:loon|salaris|salary|wage)\s*'
                          r'([A-Z]|[IVX]{1,4}|\d{1,2}[a-z]?)', h.strip(), re.I)
        if mw:
            jg = mw.group(1).upper()
        else:
            # strip a parenthetical/trailing unit clause, then test the remainder for a
            # scale label ('C', 'A and B', 'II', 'F/G') -- NOT a step ('1e halfjaar').
            core = re.sub(r'\((?:[^)]*(?:wage|loon|salaris|salary|maand|uur|week|jaar|'
                          r'month|hour|year)[^)]*)\)', ' ', h, flags=re.I)
            core = re.sub(r'\b(?:monthly|hourly|weekly|annual|maandloon|uurloon|weekloon|'
                          r'maandsalaris|per\s+(?:maand|uur|week|jaar|month|hour|year))\b',
                          ' ', core, flags=re.I).strip(' ()-')
            if re.fullmatch(r'[A-Z](?:\s*(?:and|en|/|&|,|-)\s*[A-Z]){0,2}|[IVX]{1,4}',
                            core, re.I) and not EMBED_HALFYEAR.search(h):
                jg = core.upper()
    elif not date and not unit and not minmax:
        # bare job-group label as a value-column header (e.g. 'A/B', 'A/2', 'F-45', 'II')
        h2 = h.strip()
        STOP = ('scale','schaal','groep','group','periodiek','periode','trede','niveau',
                'level','step','stap','functiegroep','salarisschaal','leeftijd','age',
                'min','max','sal','loon','wage','bedrag','amount','euro','uur','week',
                'maand','jaar','year','hour','jr','anc')
        if 0 < len(h2) <= 8 and re.fullmatch(r'[A-Za-z0-9][A-Za-z0-9 /._\-]{0,7}', h2) \
           and re.search(r'[A-Za-z0-9]', h2) \
           and not any(w in h2.lower() for w in STOP) \
           and not EMBED_STEP_PRE.match(h2):     # '2 FJ' is a step header, not a jobgroup
            jg = h2.upper()
        # PER-COLUMN SPLIT for category columns (Haiku finding CAO 449): a LONGER worded
        # header ('Department I Extra', 'Afdeling II') is a sub-category label -- without it,
        # all such columns collapse into ONE identity with dup_date amounts. Same STOP-word
        # guard keeps unit/date/scale headers out ('uur' in 'hour' also blocks FTE variants).
        elif 8 < len(h2) <= 40 and re.fullmatch(r'[A-Za-z][A-Za-z0-9 /.,()_\-]{1,39}', h2) \
           and re.search(r'[A-Za-z]{3}', h2) \
           and not any(w in h2.lower() for w in STOP):
            jg = h2
    step = None
    if not date:                       # don't read "year" out of a date header
        mp = EMBED_STEP_PRE.match(h)
        ms = EMBED_STEP.search(h)
        mh = EMBED_HALFYEAR.search(h)  # '1e halfjaar' -> distinct step (CAO 10 Bouw)
        mk = re.fullmatch(r'\(kolom (\d+)\)', h.strip())
        if mk: step = 'kolom %s' % mk.group(1)   # synthetic positional label (headerless
                                                 # tables): honest 'column k', no claimed
                                                 # periodiek semantics
        elif mp: step = mp.group(1)
        elif ms: step = ms.group(1)
        elif mh: step = '%se halfjaar' % mh.group(1)
        elif EMBED_INLOOP.search(h): step = 'inloop'
        elif re.search(r'aanvang|instap|start|begin', hl): step = 'start'
    return {'date':date,'unit':unit,'minmax':minmax,'jobgroup':jg,'step':step,'header':h}

def has_kw(text, kws):
    """word-boundary keyword check (so 'age' does NOT match inside 'wage group')."""
    t = text.lower()
    return any(re.search(r'(?<![a-z])'+re.escape(k)+r'(?![a-z])', t) for k in kws)


# value-column headers that are NOT base pay (premiums, allowances, one-offs)
NONBASE_KW = ('overtime','overwerk','ploegen','ploegendienst','shift','consignatie',
              'toeslag','surcharge','premie','meeruren','overuren','irregular','ort',
              'allowance','vergoeding','bonus','bijslag','uitkering','gratification',
              'outlier','uitloop','waarneming','acting','eenmalig','one-off','13e','13th',
              # factor / piece-rate columns are not base pay (CAO 3866: 'Multiplier'
              # 1.350 parsed as Dutch-thousands 1350 and emitted as a wage)
              'multiplier','vermenigvuldigingsfactor','per block')
SCALE_WORD = ('schaal','scale','loongroep','salarisschaal','groep','grade','niveau','functiegroep')
def is_surcharge_header(h):
    """value column that is NOT base pay (premium/overtime/allowance) -> exclude.
    e.g. 'Uurloon II (105%)', 'Acting allowance', 'Outlier (107.50%)'.
    EXCEPTION: a base-SCALE tier like 'Schaal 1 (110%)' is base pay -> keep."""
    hl = h.lower()
    # a header that is ONLY a percentage ('5%', '10%') is a progression/modifier column,
    # not base pay -- emitting it duplicates amounts at the same date (Haiku gate, CAO 683)
    if re.fullmatch(r'[\d.,]+\s*%', h.strip()): return True
    # a bare HOURS-BASIS column ('Hours/month', 'uren per week'): FTE hours, never pay
    # (CAO 730: '156,000' hours/month emitted as a 156000 'hourly wage')
    if re.fullmatch(r'(hours?|uren|uur)\s*(?:/|per)\s*(month|maand|week|weken|year|jaar)',
                    h.strip().lower()): return True
    # an INCREMENT-SIZE column ('Standard step' 21-67 EUR next to Minimum|Maximum,
    # CAO 3798; 'omvang periodiek', CAO 709 class): the size of the periodic raise,
    # not base pay
    if re.fullmatch(r'(standard|standaard)\s*(step|stap|periodiek)|omvang\s*periodiek|'
                    r'periodieke?\s*(verhoging|bedrag)|step\s*(size|amount)|'
                    r'(verhoging|increase)\s*per\s*(periodiek|step|trede)',
                    h.strip().lower()): return True
    # word-boundary match: bare substring made 'ort' hit 'transpORT'/'suppORT' and
    # 'shift' hit compound words, silently nuking genuine base-pay columns
    if has_kw(hl, NONBASE_KW): return True
    if not any(s in hl for s in SCALE_WORD):       # '%' tier on a named scale = base, keep
        for m in re.finditer(r'(\d{2,3}(?:[.,]\d+)?)\s*%', h):
            if float(m.group(1).replace(',','.')) > 100: return True   # >100% premium -> exclude
    return False


def _cell_is_money(c, v, yb_money=False):
    """money-looking cell: decimal separator present, or a big integer -- EXCEPT a bare
    calendar year (1900-2035 without separator), which is a key/date token, not pay.
    Salaries in that numeric range are fine when written with cents ('1.995,80').
    yb_money=True (salary-matrix tables) overrides the year guard: a bare 1900-2035
    integer IS a wage there (ZKN/metal Trede grids where scale amounts land in the band)."""
    if v is None: return False
    if not yb_money and re.fullmatch(r'(19\d\d|20[0-3]\d)', c.strip()): return False
    return ('.' in c or ',' in c) or v >= 100


BARE_YEAR = re.compile(r'(19\d\d|20[0-3]\d)$')

def table_yearband_is_salary(rows, ncols):
    """True when this table's bare-integer money STRADDLES the calendar-year band --
    i.e. it has bare-integer amounts both below 1900 and above 2035 on one continuum.
    Then the 1900-2035 bare integers between them are wages, not years (ZKN/metal
    Trede grids). A birth-year cohort column ('1957 | 1958 | ...') has NO such money
    continuum, so it stays year-guarded. Requires the band actually to be populated."""
    below = above = ambig = 0
    for r in rows:
        for c in r.split('|'):
            c = c.strip()
            if '.' in c or ',' in c:      # decimals are already unambiguous money
                continue
            v = parse_num(c)
            if v is None or v < 100:
                continue
            if BARE_YEAR.fullmatch(c):
                ambig += 1
            elif v < 1900:
                below += 1
            elif v > 2035:
                above += 1
    return ambig >= 2 and below >= 2 and above >= 2


def col_value_frac(rows, i, ncols, yb_money=False):
    """fraction of NON-EMPTY cells in column i that look like MONEY (decimal, or >=100).
    Empty / '-' cells are ignored so sparse value columns aren't misread as keys.
    Ragged rows are skipped -- EXCEPT when the table has NO well-formed row at all
    (fully-triangular tables like CAO 2165's 'Age | WML | FG/TREDE 1..9', where every
    data row is shorter than the header): then fall back to LEFT-ALIGNED indexing so
    classification still sees the money. This fallback can only fire on tables that
    previously classified to zero value columns, so it cannot change working tables."""
    def _frac(pool):
        n = money = 0
        for c in pool:
            c = c.strip()
            if c in ('', '-', '—', 'nntb', 'n.n.b.', 'nvt', '-,-'): continue
            n += 1
            if _cell_is_money(c, parse_num(c), yb_money): money += 1
        return money, n
    exact=[r.split('|') for r in rows if len(r.split('|'))==ncols]
    if exact:
        m, n = _frac([cells[i] for cells in exact])
        if n:                       # column has data in full-width rows -> trust it
            return m / n
    # No full-width row carries this column (merged youth+function-year grids: the scale
    # columns are filled only by the NARROWER function-year rows, so exact-width sampling
    # sees them empty and misclassifies them as keys). Fall back to index-aligned full pool.
    m, n = _frac([cells[i] for cells in (r.split('|') for r in rows) if i < len(cells)])
    return m / n if n else 0.0


def _row_numeric_frac(line):
    cells = [c.strip() for c in line.split('|')]
    if not cells: return 0.0
    nums = sum(1 for c in cells if parse_num(c) is not None)
    return nums / len(cells)

# bare "group header" line: "Functiegroep 5", "Functiegroep 5:", "Garantieschaal DTG 1:".
# Trailing colon optional; label limited to 1-3 short tokens so narrative lines don't match.
_GH_KW = (r'(?:functiegroep|salarisschaal|salarischaal|garantieschaal|functieschaal|loonschaal|'
          r'loongroep|schaal|groep|group|scale|wage ?group|niveau|categorie|category|klasse|class)')
GROUP_HDR = re.compile(
    r'^' + _GH_KW +
    r'(?:'
      r'(?:\s+[A-Za-z0-9][\w./\-]{0,6}){1,3}'                                    # 1-3 short tokens
      r'|\s+\d{1,2}[a-z]?\s*(?:tot en met|t/m|t\.?m\.?|to|through|-|–|en)\s*\d{1,2}[a-z]?'  # a RANGE header
    r')\s*:?\s*$', re.I)

def _split_desc_header_rows(line, sep):
    """'desc: header <sep> row <sep> row ...' -> [desc, header, row, row, ...].
    The desc/header boundary is the FIRST ':' whose right-hand side still contains a '|'."""
    segs = [s.strip() for s in line.split(sep)]
    segs = [s for s in segs if s]
    if not segs: return None
    seg0 = segs[0]
    desc, header = '', seg0
    for m in re.finditer(r':', seg0):
        if '|' in seg0[m.end():]:
            desc, header = seg0[:m.start()].strip(), seg0[m.end():].strip()
            break
    out = []
    if desc: out.append(desc)
    out.append(header)
    out.extend(segs[1:])
    return out


def explode_mashed(line):
    """A whole table crammed into ONE string with a non-newline ROW separator:
      '|| ' (double pipe, e.g. CAO 1630) or '; ' (semicolon, e.g. CAO 2948).
    Returns [desc, header, row, ...] or None if the line isn't a mashed table."""
    if not isinstance(line, str) or line.count('|') < 3: return None
    # require >=2 '||' occurrences: a single '||' may just be one empty cell written
    # without spaces, not a row separator
    if line.count('||') >= 2:
        return _split_desc_header_rows(line, '||')
    # semicolon-separated rows: several ';' AND a pipe-bearing tabular structure
    if line.count(';') >= 2 and re.search(r';\s*[^;|]{0,24}\|', line):
        return _split_desc_header_rows(line, ';')
    return None


def _split_embedded_columns(s):
    """Title/header/rows mashed into ONE line:
    - 'Salary scales ... (plus 2.5%): Columns: Periodic | Scale 1 | ...' (CAO 301/234
      class) -- an embedded 'Columns:' marker; without the split the whole line becomes
      the desc and the FIRST DATA ROW gets promoted to header.
    - '... Columns: Nr. | 1-1-2023 | ... Rows: 1 | 1.934,40 | ...; 2 | ...' (CAO 1612
      class) -- the header AND all data behind an embedded 'Rows:' marker; the ';' row
      separators are handled downstream by explode_mashed once 'Rows:' is split off.
    Split only when real tabular content follows each marker (>=2 pipes)."""
    parts = []
    m = re.search(r'\bcolumns\s*:', s, re.I)
    if m and m.start() > 0 and s[m.start():].count('|') >= 2:
        head = s[:m.start()].rstrip(' :')
        if head:
            parts.append(head)
            s = s[m.start():]
    mr = re.search(r'\bRows\s*:\s*', s)          # case-sensitive: upstream marker
    if mr and mr.start() > 0 and s[mr.end():].count('|') >= 2:
        before = s[:mr.start()].rstrip(' .')
        if before:
            parts.append(before)
        parts.append(s[mr.end():])
    elif parts:
        parts.append(s)
    return parts if len(parts) > 1 else None


def flatten_lines(blk):
    """Some extract blocks pack a WHOLE table (desc + 'Columns:' header + every data row)
    into a SINGLE list element. The row separator may be an embedded newline (most common),
    a double-pipe '||', or a semicolon ';'. Split those into real lines so block_to_table
    sees a multi-line table instead of one opaque string. Blank lines are dropped; internal
    '|' cells are preserved."""
    if not isinstance(blk, list): return blk
    out = []
    def _emit(s):
        emb = _split_embedded_columns(s)
        for piece in (emb if emb else [s]):
            ex = explode_mashed(piece)
            out.extend(ex) if ex else out.append(piece)
    for l in blk:
        if isinstance(l, str) and '\n' in l:
            for s in l.split('\n'):
                if not s.strip(): continue
                _emit(s)
        elif isinstance(l, str):
            _emit(l)
        else:
            out.append(l)
    return out


# salary-table TITLE markers: a NON-pipe line carrying one of these (or a date) begins a
# new sub-table when a fresh header follows it. Kept strict so ordinary notes don't split.
TITLE_KW = ('salary scale', 'salary table', 'wage table', 'wage scale', 'pay scale',
            'salarisschaal', 'salaristabel', 'salaristabellen', 'loontabel', 'loonschaal',
            'salarissen per', 'salaris per', 'salarisschalen', 'gross per month',
            'per maand', 'monthly amounts', 'wage tables', 'salary scales')


def _looks_like_header_line(l):
    """a pipe line that is a HEADER, not data: few MONEY-looking cells. (A header of bare
    step numbers -- 'Scale | 1 | 2 | ... | 15' -- looks numeric but carries no euro amounts,
    so test the money fraction, not the numeric fraction.)"""
    if not (isinstance(l, str) and '|' in l): return False
    cells = [c.strip() for c in l.split('|')]
    nonempty = [c for c in cells if c not in ('', '-', '—')]
    if not nonempty: return False
    money = sum(1 for c in nonempty
                for v in [parse_num(c)] if v is not None and (('.' in c or ',' in c) or v >= 100))
    return money / len(nonempty) < 0.4


def split_blocks(blk):
    """A single wage_information block can stack SEVERAL sub-tables, each with its own
    'Columns:' header (e.g. an Age table then a Function-Years table). Split at every
    'Columns:' line so each sub-table is parsed on its own; keep the block description.

    NOTE: an earlier version also split on dated/keyword TITLE lines (to catch multi-table
    blocks like CAO 429 that lack 'Columns:' markers). That over-split ordinary tables and
    net-cost ~1000 points / 5pts coverage across the corpus, so it was reverted. The
    title-boundary split is a candidate to REINTRODUCE only with a much tighter guard."""
    if not isinstance(blk, list) or not blk: return [blk]
    blk = flatten_lines(blk)
    desc = blk[0] if isinstance(blk[0], str) else ''
    idxs = [i for i,l in enumerate(blk) if isinstance(l,str) and l.lower().strip().startswith('columns:')]
    if len(idxs) <= 1: return [blk]
    subs = []
    for k,start in enumerate(idxs):
        end = idxs[k+1] if k+1 < len(idxs) else len(blk)
        subs.append([desc] + blk[start:end])
    return subs


def _unfuse_mega_stream(cells, ncols):
    """CAO 1496: an entire table crammed into ONE 'Columns:' line with the ROW BREAKS
    lost -- each next row's KEY got glued to the previous row's last wage
    ('21,5510' = wage 21,55 + row key 10). BOUNDARY reconstruction: every fused cell
    closes one row (the wage part) and opens the next (the key part). Proof #1 (never a
    guess): >=2 fused boundaries and boundary keys ascending by EXACTLY 1.

    SHORT rows (staircase rows whose empty cells were lost upstream) have an ambiguous
    cell->column mapping (schaal 1-11 vs 2-12). Proof #2, MONOTONICITY DISAMBIGUATION:
    full-width rows are alignment-ANCHORED; a short row is emitted only if EXACTLY ONE
    of its two alignments (trailing-missing vs leading-missing) is consistent with the
    anchored rows' per-column monotonicity (wage never decreases as the step key rises),
    and the other alignment provably violates it. Undecidable rows stay dropped. A final
    whole-table monotonicity check rejects everything on any residual violation."""
    FUSED = re.compile(r'^(\d{1,3},\d{2})(\d{1,2})$')
    segs, cur, keys = [], [], []
    for c in cells:
        m = FUSED.match(c.strip())
        if m:
            cur.append(m.group(1))
            segs.append(cur)
            cur = [m.group(2)]; keys.append(int(m.group(2)))
        else:
            cur.append(c)
    if cur: segs.append(cur)
    if len(keys) < 2: return None
    if any(b - a != 1 for a, b in zip(keys, keys[1:])): return None
    keyed = segs[1:]                                   # segs[0] = unfused preamble -> dropped
    if any(len(r) > ncols for r in keyed): return None
    anchored = [r for r in keyed if len(r) == ncols]
    if len(anchored) < 2: return None
    def _grid(row):    # column index -> parsed wage (None for empty/non-numeric)
        return {i: parse_num(row[i]) for i in range(1, len(row))}
    anchor_cols = []   # (key, {col: val})
    for r in anchored:
        k = parse_num(r[0])
        if k is None: return None
        anchor_cols.append((int(k), _grid(r)))
    def consistent(key, grid):
        """no strict monotonicity violation vs any anchored row (equal wages allowed)."""
        for k2, g2 in anchor_cols:
            if k2 == key: continue
            for i, v in grid.items():
                v2 = g2.get(i)
                if v is None or v2 is None: continue
                if (k2 > key and v2 < v) or (k2 < key and v2 > v):
                    return False
        return True
    out = list(anchored)
    for r in keyed:
        if len(r) == ncols: continue
        pad = ncols - len(r)
        left = r + [''] * pad                          # trailing cells missing
        right = [r[0]] + [''] * pad + r[1:]            # leading cells missing (staircase)
        k = parse_num(r[0])
        if k is None: continue
        ok_l = consistent(int(k), _grid(left))
        ok_r = consistent(int(k), _grid(right))
        if ok_l != ok_r:                               # exactly one alignment survives
            out.append(left if ok_l else right)
        # both pass or both fail -> undecidable -> row stays dropped (honest)
    # final whole-table proof over everything we intend to emit
    final = [(int(parse_num(r[0])), _grid(r)) for r in out]
    for a in range(len(final)):
        for b in range(len(final)):
            ka, ga = final[a]; kb, gb = final[b]
            if kb <= ka: continue
            for i, v in ga.items():
                v2 = gb.get(i)
                if v is not None and v2 is not None and v2 < v:
                    return None
    out.sort(key=lambda r: parse_num(r[0]))
    return [' | '.join(r) for r in out]


def block_to_table(blk):
    if not isinstance(blk,list): return None
    blk = flatten_lines(blk)
    if len(blk)<2: return None
    desc = blk[0] if isinstance(blk[0],str) else ''
    # COMMA-separated header rescue (CAO 80): the header line uses commas, not pipes
    # ('Functiegroep A, Basisloon, Uurloon I (100%), ...') -- it was dropped as prose and
    # the first DATA row became the header (amounts as jobgroups). Convert commas->pipes
    # ONLY when: the line precedes the block's first pipe line, its comma-width equals the
    # modal pipe-row width, every cell is short (<=40), and no cell carries a year (keeps
    # prose titles like 'effective 1 April 2021' out).
    pw = [len(l.split('|')) for l in blk[1:] if isinstance(l,str) and '|' in l]
    if pw:
        from statistics import mode as _m
        modal_w = _m(pw)
        fixed = []; seen_pipe = False
        for l in blk[1:]:
            if isinstance(l,str) and '|' in l: seen_pipe = True
            elif isinstance(l,str) and not seen_pipe and l.count(',') >= 2:
                cells = [c.strip() for c in l.split(',')]
                if (len(cells) == modal_w and all(0 < len(c) <= 40 for c in cells)
                        and not any(re.search(r'\b(19|20)\d\d\b', c) for c in cells)):
                    l = ' | '.join(cells)
                    seen_pipe = True
            fixed.append(l)
        blk = [blk[0]] + fixed
    cl = next((l for l in blk[:3] if isinstance(l,str) and l.lower().strip().startswith('columns:')), None)
    # walk the block, pairing each pipe-row with the most recent bare "group header"
    # line (e.g. "Functiegroep 5:") so the job-group label isn't lost.
    cur_group=None; paired=[]
    for l in blk[1:]:
        if not isinstance(l,str): continue
        ls=l.strip()
        if '|' in ls and not ls.lower().startswith('columns:'):
            paired.append((cur_group,l))
        elif GROUP_HDR.match(ls):
            cur_group = ls.rstrip(':').strip()
    # a MEGA 'Columns:' line (whole table in one line, CAO 1496) has no separate data
    # lines -- let it through to the resegmentation branch below
    if not paired and not (cl and len(cl.split('|')) >= 30): return None
    if cl:
        cols = [c.strip() for c in cl.split(':',1)[1].split('|')]
        rowpairs = paired
        # MEGA 'Columns:' line (CAO 1496): header AND all data cells in one line, row
        # breaks lost. True header = the leading run of non-numeric cells; the numeric
        # remainder is resegmented ONLY if the fused-key arithmetic proof holds.
        if len(cols) >= 30 and not rowpairs:
            nhdr = 0
            while nhdr < len(cols) and parse_num(cols[nhdr]) is None:
                nhdr += 1
            # header fusion ('schaal 1116' = 'schaal 11' + first row key '16'): the last
            # header cell may carry the first data row's key. Defuse ONLY when the split
            # continues the header's own numeric sequence ('schaal 10' -> 'schaal 11').
            first_key = None
            if 3 <= nhdr <= 20:
                mh = re.fullmatch(r'([A-Za-z][A-Za-z ]*?)(\d{1,2})(\d{1,2})', cols[nhdr-1].strip())
                mp = re.fullmatch(r'([A-Za-z][A-Za-z ]*?)(\d{1,2})', cols[nhdr-2].strip())
                if mh and mp and mh.group(1).strip() == mp.group(1).strip() \
                   and int(mh.group(2)) == int(mp.group(2)) + 1:
                    cols[nhdr-1] = '%s%s' % (mh.group(1), mh.group(2))
                    first_key = mh.group(3)
            if 2 <= nhdr <= 20 and len(cols) > 2 * nhdr:
                stream = ([first_key] if first_key else []) + cols[nhdr:]
                seg = _unfuse_mega_stream(stream, nhdr)
                new_cols = cols[:nhdr]
                if not seg:
                    # the LAST column's header label may itself be lost upstream (widest
                    # rows are one wider than the header). Retry with the header extended
                    # by its own proven numeric sequence ('schaal 10, schaal 11' -> 'schaal 12').
                    m2 = re.fullmatch(r'([A-Za-z][A-Za-z ]*?)(\d{1,2})', cols[nhdr-1].strip())
                    m1 = re.fullmatch(r'([A-Za-z][A-Za-z ]*?)(\d{1,2})', cols[nhdr-2].strip())
                    if m1 and m2 and m1.group(1).strip() == m2.group(1).strip() \
                       and int(m2.group(2)) == int(m1.group(2)) + 1:
                        seg = _unfuse_mega_stream(stream, nhdr + 1)
                        if seg:
                            new_cols = cols[:nhdr] + ['%s%d' % (m2.group(1), int(m2.group(2)) + 1)]
                if seg:
                    rowpairs = [(None, s) for s in seg]
                    cols = new_cols
    else:
        # HEADER SELECTION: a block may carry decorative/preamble pipe lines
        # ("Salarisschaal | tot en met 5", "schaal | salaris") BEFORE the real column
        # header (" | 1-8-2014 | 1-8-2015 | ..."). Pick, among the leading NON-data pipe
        # lines, the last one whose width matches the modal width of the numeric data rows;
        # otherwise fall back to the first pipe line (preserves the ragged _rowkey case).
        from statistics import mode as _mode
        # "data" = rows with MONEY, not merely numbers: a numeric header of bare step
        # numbers ('Schaal | 1 | 2 | ... | 15') must stay eligible as a header, otherwise
        # it gets melted as a data row and its step numbers can leak out as amounts.
        numwidths = [len(r.split('|')) for _, r in paired if not _looks_like_header_line(r)]
        modal = _mode(numwidths) if numwidths else len(paired[0][1].split('|'))
        hdr_idx = 0
        for i, (g, r) in enumerate(paired):
            if not _looks_like_header_line(r):
                break                                  # reached the money-bearing data rows
            if len(r.split('|')) == modal:
                hdr_idx = i                             # best header candidate so far
        # HEADERLESS table guard (CAO 234 jeugdzorg): when the block has NO genuine
        # header line (only a 'Schaal |' stub inside the title), the fallback used to
        # eat the FIRST DATA ROW as the header -- salaries became column labels and
        # jobgroups ('1549.89'). If the chosen header is itself a MONEY row, keep it as
        # data and synthesize neutral POSITIONAL labels ('(kolom k)'): content
        # classification still finds the key/value columns, identities stay separate,
        # and the anchors are honest positional names instead of leaked amounts.
        if not _looks_like_header_line(paired[hdr_idx][1]):
            cols = ['(kolom %d)' % i for i in range(modal)]
            rowpairs = paired
        else:
            cols = [c.strip() for c in paired[hdr_idx][1].split('|')]
            # STACKED two-row header ('Groep | A | B | ...' above 'Leeftijd/functiej. | | ...'):
            # the chosen header may have EMPTY cells whose labels live on another leading
            # header line of the same width -- merge cell-wise so no column label is lost.
            for j, (_, r) in enumerate(paired[:hdr_idx]):
                other = [c.strip() for c in r.split('|')]
                if len(other) != len(cols): continue
                cols = [c if c else o for c, o in zip(cols, other)]
            rowpairs = paired[hdr_idx+1:]
    rowpairs = [(g,r) for g,r in rowpairs if _row_numeric_frac(r) > 0]
    if not rowpairs: return None
    if any(g for g,_ in rowpairs):                 # group headers present -> synthetic key column
        cols = ['_group'] + cols
        rows = [ (g if g else '') + ' | ' + r for g,r in rowpairs ]
    else:
        rows = [r for _,r in rowpairs]
    # ragged matrix: data rows have one extra leading cell (unlabeled row-key axis)
    from statistics import mode
    try: modal = mode(len(r.split('|')) for r in rows)
    except Exception: modal = len(cols)
    if modal == len(cols) + 1:
        cols = ['_rowkey'] + cols     # synthetic key column for the row axis
    # NOTE: do NOT drop length-mismatched rows here — keep them so coverage can
    # account for them honestly. The melt skips misaligned rows for emission, but
    # they still count in the coverage denominator (so silent drops are visible).
    return {'desc':desc, 'cols':cols, 'rows':rows}


def count_money_cells(rows):
    """total money-looking cells across ALL rows (coverage denominator, alignment-agnostic)."""
    n=0
    for r in rows:
        for c in r.split('|'):
            c=c.strip()
            if _cell_is_money(c, parse_num(c)): n+=1
    return n


# Explicit youth-RANGE table signals. NOTE: bare 'youth'/'jeugd' removed — they collide
# with WML references like "statutory minimum (youth) wage". Youth ROWS are dropped at
# row level, and a pure youth table's rows all get filtered there anyway.
YOUTH_DESC = ('16 to 20','16-20','16 t/m','t/m 20','t/m 21','up to 21','up to 23',
              'younger than','jonger dan','tot 21','tot 23','under 23','under 21',
              'jeugdschaal','jeugdschalen','jeugdloonschaal','youth scale','youth wage table')
ADULT_DESC = ('adult','volwassen','21 jaar of ouder','of ouder','or older','en ouder',
              '21 and older','21 years and older','21+','22+','23+')

# NON-BASE-PAY SECTION PREFIXES (upstream extractor section labels, CAO 730 finding):
# blocks starting with these hold bonuses/allowances/notes, never base wage tables --
# their money cells were leaking in as junk pay rows (already D-flagged, now excluded).
# 'job classifications' and 'general wage increases' are deliberately NOT here: some
# CAOs embed genuine scale-range / new-rate tables there (kept + flagged instead).
NONBASE_SECTION = ('all bonuses and incentive', 'bonuses and incentive',
                   'job-specific allowances', 'holiday allowance',
                   'seniority or loyalty', 'rules on personal allowances',
                   'qualification or diploma', 'retirement gratuities',
                   'vacation and holiday', 'notes explaining')


def should_include(desc):
    dl = desc.lower()
    if any(dl.startswith(p) for p in NONBASE_SECTION): return False
    # hard exclusions (apprentice/allowance/bonus/start/...) minus the youth-range terms
    if any(x in dl for x in EXCLUDE_DESC if x not in YOUTH_DESC): return False
    # youth-range term: exclude only if it is NOT a combined adults+youth table
    if any(x in dl for x in YOUTH_DESC) and not any(a in dl for a in ADULT_DESC):
        return False
    return True


def age_ok(age_label):
    """keep open-ended adult bands or bands intersecting 21-65; drop pure youth.
    21 (not 23): the Dutch adult minimum wage applies from age 21 (since 2019), so
    '21 jaar' / '22 jaar' rows are full adult wages and must not be dropped."""
    if not age_label: return True
    al = age_label.lower()
    if any(t in al for t in ('or older','of ouder','en ouder','+','adult','volwassen')): return True
    nums = [int(n) for n in re.findall(r'\d+', al)]
    if not nums: return True
    return max(nums) >= 21


MONTHS_TXT = {'january':1,'februari':2,'february':2,'januari':1,'march':3,'maart':3,'april':4,
              'may':5,'mei':5,'june':6,'juni':6,'july':7,'juli':7,'august':8,'augustus':8,
              'september':9,'october':10,'oktober':10,'november':11,'december':12,
              # 3-letter abbreviations ('Aug 1, 2010' headers, CAO 941 -- the relabel agent
              # was right about these dates; the parser simply couldn't read them)
              'jan':1,'feb':2,'mrt':3,'mar':3,'apr':4,'jun':6,'jul':7,'aug':8,
              'sep':9,'sept':9,'okt':10,'oct':10,'nov':11,'dec':12}

# ELIGIBILITY-clause date: 'hired before July 1, 1994', 'in dienst voor/na 1-1-2010',
# 'geboren voor ...' -- an entry CONDITION, not the wage-effective date (CAO 750 catering
# audit: the '1994' eligibility year was mislabeled as the table date). Strip these spans
# before date_from_desc reads a title date.
ELIG_DATE = re.compile(
    r'(?:hired|employed|in\s+(?:dienst|service)|geboren|born|joined|started|aangenomen)'
    r'[^.;,]{0,30}?(?:before|after|voor|na|prior to|since|sinds|vanaf|until|tot)\s+'
    r'[^.;]*?(?:\d{1,2}[- /]\d{1,2}[- /]\d{2,4}|[a-z]+\s+\d{1,2},?\s+\d{4}|\d{1,2}\s+[a-z]+\s+\d{4}|\d{4})',
    re.I)

def date_from_desc(desc):
    """date stated inside a table title: 'effective April 1, 2012' / 'per 1 januari 2012'.
    Eligibility-condition dates ('hired before July 1, 1994') are NOT wage dates -> removed
    first so they cannot be mistaken for the table's effective date."""
    dl = ELIG_DATE.sub(' ', desc.lower())
    # scan ALL 'N <word> YYYY' matches and take the first whose word is a real month --
    # NOT just the first pattern (CAO 759: 'week 1 of 2022 (3 January 2022)' matched
    # '1 of 2022' first, 'of' isn't a month, and the real '3 January 2022' was missed).
    for m in re.finditer(r'(\d{1,2})\s+([a-z]+)\s+(\d{4})', dl):    # 1 januari 2012
        if m.group(2) in MONTHS_TXT:
            return '%04d-%02d-%02d'%(int(m.group(3)),MONTHS_TXT[m.group(2)],int(m.group(1)))
    for m in re.finditer(r'([a-z]+)\s+(\d{1,2}),?\s+(\d{4})', dl):  # april 1, 2012
        if m.group(1) in MONTHS_TXT:
            return '%04d-%02d-%02d'%(int(m.group(3)),MONTHS_TXT[m.group(1)],int(m.group(2)))
    d = parse_header_date(desc)
    return d


def jobgroup_from_desc(desc):
    """scale/group label stated in a table TITLE, e.g. 'Salarisschaal 2 - Period...' -> '2'.
    Used as the jobgroup when the table itself doesn't carry one (rows are steps/ages)."""
    m = re.search(r'\b(?:salarisschaal|salary scale|loonschaal|functieschaal|schaal|'
                  r'functiegroep|function group|loongroep|wage group|fwg)\s+([0-9]{1,2}[a-z]?|[IVX]{1,4}|[A-Z])\b',
                  desc, re.I)
    if m: return m.group(1).upper()
    return None


def looks_like_age(val):
    """True if a cell value denotes an AGE (not a step like 'Trede 0')."""
    v = val.lower()
    if any(t in v for t in ('jaar','jarig','year','jr','ouder','older')): return True
    if re.fullmatch(r'\d{2}\+?', v.strip()) and 13 <= int(re.match(r'\d{2}',v.strip()).group()) <= 67: return True
    return False


def age_from_desc(desc):
    m = re.search(r'(\d{2})\s*(?:years?|jaar)?\s*(?:or older|of ouder|en ouder|\+)', desc.lower())
    if m: return '%s or older' % m.group(1)
    return None


def unit_from_desc(desc):
    dl = desc.lower()
    # Strip FTE EMPLOYMENT-BASIS clauses first: "for a full-time employment of 36 hours per
    # week", "based on 1878 hours per year". The "per week/year" there describes the FTE
    # basis, NOT the pay period -- without this, "euros per MONTH for ... 36 hours per week"
    # is misread as weekly. (CAO 1612 Haiku finding.)
    dl = re.sub(r'\d+(?:[.,]\d+)?\s*(?:hours?|uren|uur)\s+per\s+(?:week|weken|maand|month|year|jaar)', ' ', dl)
    # also strip a bare FTE clause '38-hour workweek' / '38 urige werkweek' (no 'per')
    dl = re.sub(r'\d+\s*-?\s*(?:hours?|uren|uur|urige?)\s*-?\s*(?:work\s?week|werkweek)', ' ', dl)
    # EARLIEST mention wins (CAO 1944: 'Monthly wages ... (hourly wage * 38*52.2/12)' --
    # the title's leading words name the table; later mentions are formula explanations).
    KW = (('hourly', ('per uur','uurloon','hourly','per hour','/uur','/hour','uurbedrag')),
          ('4-week', ('4 weken','4-week','four-week','vierwek','4weken','per 4')),
          ('weekly', ('per week','weekloon','weekly','weekbedrag','weeksalaris')),
          # 4-weekly PERIOD pay ('salarisschaal per periode SNOEP', 'periodesalaris'). Use the
          # SPACE-delimited 'per periode' (not bare 'periode') so it never matches 'periodiek'
          # (= a step/increment, not a pay period). CAO 359 zoetwaren audit: SNOEP/KOEK tables
          # are titled 'per periode' but were falling to unit=None -> magnitude-guessed monthly.
          ('period', ('per periode','periodesalaris','per periode ','periodeloon','per 4-weekse periode')),
          # 'basis van 12 maanden' = a 12-month (annual) sum (CAO 632 Banken audit); it
          # must OUTRANK the bare 'maand' hit inside 'maanden' -- earliest-position rule
          # does that ('basis van 12 ...' starts before its own 'maanden').
          ('annual', ('jaarsalaris','annual','yearly','per jaar','per year','jaarbedrag',
                      'basis van 12 maanden','based on 12 months','basis of 12 months')),
          ('monthly', ('per maand','maandsalaris','maandloon','monthly','per month','maandbedrag','month','maand')))
    best = None
    for unit, kws in KW:
        for k in kws:
            p = dl.find(k)
            if p >= 0 and (best is None or p < best[0]):
                best = (p, unit)
    return best[1] if best else None


def ft_hours_from_desc(desc):
    """weekly full-time-hours basis stated in a title/header: '36 hours per week',
    'bij een 38-urige werkweek', '40-uur per week'. Returns float or None."""
    dl = desc.lower()
    m = re.search(r'(\d{2}(?:[.,]\d{1,2})?)\s*(?:-?\s*(?:hours?|uren|uur|urige))\s*'
                  r'(?:per\s*week|werk\s*week|work\s*week|week)', dl)
    if not m:
        m = re.search(r'(?:werkweek|workweek)\s*van\s*(\d{2}(?:[.,]\d{1,2})?)\s*(?:uur|uren)', dl)
    if m:
        v = parse_num(m.group(1))
        if v and 10 <= v <= 48: return v
    return None


def holiday_incl_from_desc(desc):
    """True/False if the title states holiday allowance is included/excluded, else None."""
    dl = desc.lower()
    # take the LAST incl/excl keyword before the holiday term (word-bounded), so
    # '(excluding vacation days allowance and holiday allowance)' reads the 'excluding'
    HOL = r'(?:vakantietoeslag|vakantiegeld|holiday allowance|holiday pay|vacation allowance)'
    m = re.search(r'\b(incl\w*|excl\w*|including|excluding|met|zonder)\b\.?[^.;|]{0,60}' + HOL, dl)
    if not m:
        m = re.search(HOL + r'[^.;|]{0,30}\b(inbegrepen|included|excluded|niet inbegrepen)\b', dl)
        if not m: return None
        kw = m.group(1)
        return not ('niet' in kw or 'exclu' in kw)
    kw = m.group(1)
    return not ('excl' in kw or kw == 'zonder')


# entry/'aanloop' scale signals -- tables/rows are INCLUDED and tagged is_entry=True
ENTRY_DESC = ('aanloop', 'instap', 'entry-level', 'entry table', 'entry scale',
              'start table', 'start-table', 'startschaal', 'aanvangssalaris',
              'aanvangsalaris')   # single-s spelling: 'Functieaanvangsalaris'
ENTRY_ROW = ('aanloop', 'instap', 'entry', 'aanvang')


def is_entry_row(rowtext, desc_entry):
    return desc_entry or any(k in rowtext for k in ENTRY_ROW)


def worker_from_desc(desc):
    dl = desc.lower()
    if 'uta' in dl: return 'UTA employee'
    if 'office' in dl or 'kantoor' in dl: return 'Office employee'
    if 'construction site' in dl or 'bouwplaats' in dl: return 'Construction site employee'
    # generic: pull a "for <X> employees/workers/medewerkers" phrase if present
    m = re.search(r'for ([a-z][a-z &/-]{2,40}?) (?:employees|workers|staff)', dl)
    if m: return m.group(1).strip().title() + ' employee'
    m = re.search(r'(?:voor|van) ([a-z][a-z &/-]{2,40}?)(?:werknemers|medewerkers|personeel)', dl)
    if m: return m.group(1).strip().title()
    return None


_FAM_STRIP = re.compile(
    r'\d{1,2}[-/ ](?:\d{1,2}|januari|februari|maart|april|mei|juni|juli|augustus|september|'
    r'oktober|november|december|january|february|march|may|june|july|august|october)[-/ ]?\d{0,4}'
    r'|\b(19|20)\d\d\b|\d+([.,]\d+)?\s*%|\d+', re.I)

def table_family(desc):
    """normalized table-desc with dates/years/percentages/numbers stripped -> the table's
    FAMILY. Cross-block timeline merge is allowed ONLY within one family, so 'Corporate
    Catering per 1-1-2020' merges with 'Corporate Catering per 1-7-2020' but NEVER with
    'Institutional Catering ...' (Haiku finding CAO 750: cross-family merges built rows
    mixing amounts from different populations -- relabeling can't fix that corruption)."""
    return ' '.join(_FAM_STRIP.sub(' ', (desc or '').lower()).split())[:80]


def parse_extract(extract):
    """returns (rows, table_diagnostics, docdate).
    Rows with the SAME identity (jobgroup x step x worker x age x min/max) are merged
    ACROSS table blocks within the file, so successive dated tables build ONE timeline.
    Dates come ONLY from columns / titles / a date row-axis -- never the document date."""
    docdate = doc_effective_date(extract)   # reported for reference, NOT used to date rows
    out_rows = []
    diags = []
    global_groups = collections.OrderedDict()
    # FTE-basis split (CAO 725 Groen): a file may state the SAME scale at two work-week
    # bases ('38-hour work week' vs '40-hour', different pay). Only when >=2 distinct bases
    # exist in the file do we add ft to the row identity -- otherwise timelines that mention
    # the basis inconsistently across dated blocks would fragment.
    _fts = set()
    for _b0 in extract.get('wage_information', []):
        for _l in (_b0 if isinstance(_b0, list) else [_b0]):
            if isinstance(_l, str):
                _v = ft_hours_from_desc(_l)
                if _v: _fts.add(_v)
    multi_ft = len(_fts) >= 2
    tidx = -1                               # per-included-table index (role-check instrumentation)
    for blk0 in extract.get('wage_information', []):
      for blk in split_blocks(blk0):
        t = block_to_table(blk)
        if not t: continue
        tidx += 1
        inc = should_include(t['desc'])
        ncols = len(t['cols'])
        # ---- classify columns by CELL CONTENT: value (money) vs key (row axis) ----
        # salary-matrix tables (ZKN/metal Trede grids) have amounts inside the calendar
        # band 1900-2035 -> the year guard would void them and misclassify whole money
        # columns as keys (their amounts then fuse into the step label). Detect once.
        yb_money = table_yearband_is_salary(t['rows'], ncols)
        vfrac = [col_value_frac(t['rows'], i, ncols, yb_money) for i in range(ncols)]
        val_idx, key_idx = [], []
        for i,h in enumerate(t['cols']):
            if is_surcharge_header(h): continue          # premium/overtime column -> ignore (not base pay)
            is_keyhdr = has_kw(h, KEY_HEADER) and not parse_header_date(h)
            # CONTENT WINS over a key-word header: a column that is overwhelmingly money is a
            # VALUE column even when its header is a step word ("Periodiek 3", "Trede 2",
            # "FG/TREDE 1") -- i.e. a TRANSPOSED step-as-columns table. A genuine key column
            # (scale codes, ages, experience years) holds small ints/labels, never euro
            # amounts, so it never reaches this money fraction. parse_value_header() still
            # pulls the step label out of the header (EMBED_STEP), so nothing is lost.
            strong_money = vfrac[i] >= 0.8
            # DATE-HEADED column = value column BY CONSTRUCTION (a date labels a pay
            # period, never a row axis). Needed when salaries fall in the calendar-year
            # band 1900-2035 ('1.01 | 1935' career tables, CAO 1022): the year guard in
            # _cell_is_money voids them and the real pay column classified as a key.
            if not strong_money and not is_keyhdr and parse_header_date(h):
                cells_i = [r.split('|')[i].strip() for r in t['rows'] if len(r.split('|')) == ncols]
                ne = [c for c in cells_i if c not in ('', '-')]
                if ne and sum(1 for c in ne if parse_num(c) is not None) / len(ne) >= 0.6:
                    strong_money = True
            if strong_money or (vfrac[i] >= 0.5 and not is_keyhdr):
                val_idx.append(i)
            else:
                key_idx.append(i)
        # SCALE.STEP-CODE first column ('1.01', '1.02', ... CAO 1022 career tables):
        # parses as tiny "money" (1.01) so the whole table ends up key-less and all
        # identities collapse. If NO key column exists, and column 0 is entirely
        # ascending d.dd codes sitting far below the other columns' money, it is the
        # row axis, not pay.
        if not key_idx and len(val_idx) >= 2 and 0 in val_idx:
            c0 = [r.split('|')[0].strip() for r in t['rows'] if len(r.split('|')) == ncols]
            c0 = [c for c in c0 if c not in ('', '-')]
            if c0 and all(re.fullmatch(r'\d{1,2}\.\d{2}', c) for c in c0):
                v0 = [parse_num(c) for c in c0]
                rest = sorted(v for i in val_idx if i != 0
                              for r in t['rows'] if len(r.split('|')) == ncols
                              for v in [parse_num(r.split('|')[i].strip())] if v is not None)
                if rest and v0 == sorted(v0) and max(v0) < rest[len(rest)//2] / 50:
                    val_idx.remove(0); key_idx.append(0)
        # FTE-basis columns ("N hours per year"): keep ONLY the standard (smallest-hours)
        # one, so the same pay isn't emitted 3x under different FTE definitions.
        # parse_num handles '1878.00' / '2.291,50' correctly (naive replace('.','') turned
        # 1878.00 into 187800 and could flip the smallest-hours choice)
        fte = [(i, parse_num(m.group(1)))
               for i in val_idx
               for m in [re.search(r'([\d.,]+)\s*(?:hours?|uren|uur)\s*(?:per|/)\s*(?:year|jaar)', t['cols'][i].lower())
                         or re.search(r'(?:annual\s+hours|jaaruren)\s*[:=]?\s*([\d.,]+)', t['cols'][i].lower())] if m]
        fte = [(i, v) for i, v in fte if v is not None]
        if len(fte) > 1:
            keep = min(fte, key=lambda z:z[1])[0]
            drop = {i for i,_ in fte if i != keep}
            val_idx = [i for i in val_idx if i not in drop]
        # YEAR-BAND money rescue (holistic audit 2026-07, CAO 214/822/824/233): whole
        # salary columns whose amounts fall in the calendar band 1900-2035 ('1935 |
        # 1938 | 1980 | 2026') are voided by the year guard in _cell_is_money, drop to
        # vfrac~0 and get classified as KEY columns -- their amounts then vanish into
        # row_key ('0 | 1935 | 1935 | ...') and the step label turns to junk. A
        # key-side column is really MONEY when the table already has confirmed money
        # columns, this column is well-filled and fully numeric, its values sit in the
        # same magnitude band as that money, and nothing marks it as an axis (no
        # key-word header, no date header, not an ascending year/step sequence).
        if val_idx and key_idx:
            _mv = [v for i2 in val_idx
                   for r in t['rows'] if len(r.split('|')) == ncols
                   for v in [parse_num(r.split('|')[i2].strip())]
                   if v is not None and v >= 100]
            _med = sorted(_mv)[len(_mv)//2] if len(_mv) >= 5 else None
            if _med:
                for i in sorted(key_idx):
                    h = t['cols'][i]
                    if has_kw(h, KEY_HEADER) or parse_header_date(h): continue
                    cells_i = [r.split('|')[i].strip() for r in t['rows']
                               if len(r.split('|')) == ncols]
                    ne = [c for c in cells_i if c not in ('', '-')]
                    if len(ne) < 3 or len(ne) < 0.6 * max(1, len(cells_i)): continue
                    vs = [parse_num(c) for c in ne]
                    if any(v is None for v in vs): continue
                    # consecutive ascending ints = genuine year/step axis, keep as key
                    if len(set(vs)) >= 3 and all(0 <= b - a <= 1 for a, b in zip(vs, vs[1:])):
                        continue
                    if all(_med / 1.6 <= v <= _med * 1.6 for v in vs):
                        key_idx.remove(i); val_idx.append(i)
                val_idx.sort()
        vheaders = {i:parse_value_header(t['cols'][i]) for i in val_idx}
        dg = {'tidx':tidx,'desc':t['desc'][:80],'included':inc,'n_data_rows':len(t['rows']),
              'cols':[(t['cols'][i], 'value' if i in val_idx else 'key') for i in range(ncols)],
              # role-check instrumentation: token sets per field + amounts this table emits
              'jg_tokens':set(),'step_tokens':set(),'age_tokens':set(),'amounts':set()}
        diags.append(dg)
        if not inc or not val_idx:
            if not inc: continue
            else: continue
        worker = worker_from_desc(t['desc'])
        fam = table_family(t['desc'])
        desc_age = age_from_desc(t['desc'])
        desc_date = date_from_desc(t['desc'])
        desc_unit = unit_from_desc(t['desc'])
        # NOTE: do NOT widen the unit scan to block notes -- a note mentioning 'per uur' (e.g. an
        # overtime/on-call rate) gets mis-applied to a monthly table, flipping real monthly wages
        # to 'hourly' -> magnitude_implausible -> wrongly demoted to D (measured: ~3,100 rows on
        # CAO 1029 etc.). Per-column units (parse_value_header) already handle the safe cases.
        desc_ft = ft_hours_from_desc(t['desc'])
        if desc_ft is None:
            # the workweek is often stated NOT in the title but in a note / column-header
            # BELOW it ('Note: amounts based on 36 hours per week', 'Per Month (38-hour week)').
            # Scan the rest of THIS table's block so the per-table basis is captured (else it
            # falls to the coarser CAO-level fallback in mw_indices). ~16% of workweek-bearing
            # tables carry it in a note, not the title.
            for _l in (blk if isinstance(blk, list) else [blk]):
                if isinstance(_l, str) and _l != t['desc']:
                    _v = ft_hours_from_desc(_l)
                    if _v is not None:
                        desc_ft = _v; break
        desc_hol = holiday_incl_from_desc(t['desc'])
        if desc_hol is None:
            # holiday-allowance incl/excl is very often a FOOTNOTE, not the title
            # ('Note: lonen EXCL 8% vakantietoeslag'). Scan this table's block. Safe: the regex
            # requires an incl/excl word next to vakantietoeslag/holiday allowance (54/56 real).
            for _l in (blk if isinstance(blk, list) else [blk]):
                if isinstance(_l, str) and _l != t['desc']:
                    _h = holiday_incl_from_desc(_l)
                    if _h is not None:
                        desc_hol = _h; break
        desc_entry = any(k in t['desc'].lower() for k in ENTRY_DESC)
        # jobgroup from title (e.g. "Salarisschaal 2 - ...") used only when the table has
        # NO job-group column of its own (no value-header jobgroup, single bare row-key axis).
        has_colgroup = any(vheaders[i]['jobgroup'] for i in val_idx)
        desc_jg = None if has_colgroup else jobgroup_from_desc(t['desc'])
        # mangled-decimal peer context (Hanna-authorized derivation, 2026-07-05): median of
        # this table's CLEAN money cells; a MANGLED_DEC cell sitting ~10^k below it gets a
        # digit-preserving decimal shift ('2.18454' -> 2184.54, never a guessed number).
        # Corpus sweep: fires on exactly 2 cells (CAO 592); '0.0018' factor tables refused.
        _clean = []
        for _r in t['rows']:
            for _c in _r.split('|'):
                _c = _c.strip()
                if MANGLED_DEC.match(re.sub(r'[€$£\s ]', '', _c)): continue
                _v = parse_num(_c)
                if _v is not None and _cell_is_money(_c, _v): _clean.append(_v)
        tbl_med = sorted(_clean)[len(_clean)//2] if len(_clean) >= 5 else None
        seen_cells = 0
        block_points = set()          # distinct (identity, date) this block contributes -> coverage
        ragged = 0; ragged_skipped = 0
        has_minmax = any(vheaders[i]['minmax'] for i in val_idx)
        _hdr_norm = [str(c).strip().lower() for c in t['cols']]
        for r in t['rows']:
            cells = [c.strip() for c in r.split('|')]
            # REPEATED HEADER ROW as data (CAO 776 Papierindustrie: 'Group | 1 | 2 | ... |
            # 11' reappears mid-table and emitted amounts 1,2,...,11). Skip a row whose
            # cells equal the column headers -- it carries no wages, only column indices.
            if len(cells) == ncols and [c.lower() for c in cells] == _hdr_norm:
                continue
            pad_deficit = 0
            if len(cells) != ncols:
                ragged += 1
                # min/max ("staircase") tables have INTERIOR gaps -> right-pad would
                # shift amounts into the wrong group/min-max (min>max). Skip these rows
                # so the loss is VISIBLE via coverage rather than silently mislabeled.
                if has_minmax and len(cells) < ncols:
                    ragged_skipped += 1
                    seen_cells += count_money_cells([r])   # count as unrecovered -> visible in coverage
                    continue
                # simple tables: short rows miss TRAILING cells -> right-pad; long -> truncate.
                # truncated tail may hold money -> count it as UNRECOVERED so coverage stays
                # honest (previously these cells vanished from the denominator entirely)
                if len(cells) > ncols:
                    seen_cells += count_money_cells([' | '.join(cells[ncols:])])
                # HEAVY right-pad (>=2 missing cells) = alignment risk: if the true gaps
                # are LEADING (staircase), the pad shifts values one+ columns left (Haiku
                # audit, CAO 2165: WML age rows confirmed w/ off-by-one column). Tag it
                # so confidence caps such rows at review, never confirm-high.
                pad_deficit = ncols - len(cells)
                cells = (cells + ['']*ncols)[:ncols]
            # row-level exclusion: youth / apprentice / annual-reference rows in mixed tables.
            # 'aanloop'/entry rows are KEPT and tagged is_entry (Phase-0 decision).
            rowtext = ' '.join(cells[i].strip() for i in key_idx).lower()
            if any(k in rowtext for k in ('jeugd','jaarsalaris','leerling','stagiair','instroom')):
                continue
            row_entry = is_entry_row(rowtext, desc_entry)
            # youth row ("15 jaar".."20 jaar"): ages in the TEEN band 13-20 (not 0-12, which
            # are experience/step years, and not 21+). Skip only if all stated ages are teen.
            # guard: '15 jaar dienst' / '15 years of service' is a SERVICE step, not an age
            if not any(_kw in rowtext for _kw in ('ouder','older','adult','+','dienst','service',
                                                  'ervaring','experience','ancienn','anciënn')):
                _ages = [int(n) for n in re.findall(r'(\d{1,2})\s*(?:jaar|jr|year)', rowtext)]
                if _ages and max(_ages) <= 20 and min(_ages) >= 13:
                    continue
            # row-key identity from key columns
            age=None; parts=[]; row_date=None; row_unit=None
            for i in key_idx:
                val=cells[i].strip()
                if val in ('','-'): continue
                # UNIT-IN-ROW-KEY ('0 Hour' / '0 Period' / '0 Month' rows, CAO 1264): the
                # row axis carries the pay period. Strip it into a row-level unit so the
                # hour/month/period variants of one step don't share a false unit bucket.
                mu = re.fullmatch(r'(.*?)[\s/]*\b(hour|uur|month|maand|week|period|periode)\b\.?', val, re.I)
                if mu and mu.group(1).strip(' /|-') != '':
                    row_unit = {'hour':'hourly','uur':'hourly','month':'monthly','maand':'monthly',
                                'week':'weekly','period':'period','periode':'period'}[mu.group(2).lower()]
                    val = mu.group(1).strip(' /|-')
                kd = parse_header_date(val) or date_from_desc(val)
                # date-as-row-axis -> start_date, not jobgroup. Require an EXPLICIT year
                # in the cell: a bare 'd.mm' like '1.01' is a scale.step CODE (CAO 1022
                # career tables), not a yearless date -- misreading it erased the jobgroup.
                if kd and re.search(r'(19|20)\d\d', val): row_date = kd; continue
                # age only if the column is an age column AND the cell looks like an age
                # (so 'Trede 0' in a 'Leeftijd/treden' column is a STEP, not a youth age)
                if has_kw(t['cols'][i], KEY_AGE) and looks_like_age(val): age=val
                # CELL-level age fallback (CAO 2948: '21 jaar'/'23 jaar e.o.' cells under a
                # non-age header landed in jobgroup). Tight guards: explicit 'N jaar/year'
                # with N in adult range, no service/experience wording in the cell, and the
                # column header is not a step/service axis.
                elif (lambda m: m and 13 <= int(m.group(1)) <= 67
                      and not any(k in val.lower() for k in
                                  ('dienst','service','ervaring','ancienn','anciënn'))
                      and not has_kw(t['cols'][i], ('dienstjaar','trede','periodiek','ervaring',
                                                    'experience','service','functiejaren','anc')))(
                      re.match(r'^(\d{1,2})\s*(?:jaar|jarige?n?|years?)\b', val.lower())):
                    age = val
                else: parts.append(val)
            base={}
            if parts:
                jg, st = split_scale_code(parts[0])
                base['rowgroup']=jg
                extra=([st] if st else [])+parts[1:]
                if extra: base['step']=' / '.join(extra)
            if age: base['age_group']=age
            elif desc_age: base.setdefault('age_group', desc_age)
            if 'age_group' in base and not age_ok(base['age_group']): continue
            for vi in val_idx:
                amt = parse_num(cells[vi])
                if amt is None: continue
                # '0.00' placeholder cells (holistic audit, CAO 1165/429): sources print 0
                # where a scale has no such step. A zero amount is never a wage -> skip.
                if amt == 0: continue
                seen_cells += 1
                # mangled-decimal rescue: pattern + ~10^k below peers + rescaled lands in
                # peer range -> shift the decimal, keep every digit. Tagged, tier-capped B,
                # counted separately by the provenance audit (it is a documented derivation).
                rescaled_from = None
                _c2 = re.sub(r'[€$£\s ]', '', cells[vi].strip())
                if tbl_med and MANGLED_DEC.match(_c2) and amt < tbl_med / 50:
                    _cand = amt * (10 ** (len(_c2.split('.')[1]) - 2))
                    if tbl_med / 4 <= _cand <= tbl_med * 4:
                        rescaled_from = cells[vi].strip(); amt = round(_cand, 2)
                vh = vheaders[vi]
                date = vh['date'] or row_date or desc_date   # NO document-date fallback
                ident = dict(base)
                # jobgroup: from value-header (matrix/grouped) else from the row key
                jg_embed = vh['jobgroup']
                if jg_embed is not None:
                    ident['jobgroup'] = jg_embed
                    if 'rowgroup' in ident:   # row key becomes the step axis
                        ident['step'] = (ident.get('step')+' / ' if ident.get('step') else '') + ident.pop('rowgroup')
                elif desc_jg is not None:        # jobgroup stated in the title; row key -> step
                    ident['jobgroup'] = desc_jg
                    if 'rowgroup' in ident:
                        ident['step'] = (ident.get('step')+' / ' if ident.get('step') else '') + ident.pop('rowgroup')
                else:
                    ident['jobgroup'] = ident.pop('rowgroup', '')
                if vh.get('step'):     # step encoded in the value-column header (e.g. "Functiejaar 3")
                    ident['step'] = (ident['step']+' / ' if ident.get('step') else '') + vh['step']
                if vh['minmax']: ident['_mm'] = vh['minmax']
                if worker: ident['_worker'] = worker          # part of identity -> no cross-worker merge
                ident['_family'] = fam                        # merge only within one table family
                if row_entry: ident['_entry'] = True          # entry scale: separate row, tagged
                if 'age_group' not in ident and desc_age: ident['age_group'] = desc_age
                if multi_ft and desc_ft is not None:           # 38h vs 40h -> distinct rows
                    ident['_ft'] = desc_ft
                idk = tuple(sorted((k,v) for k,v in ident.items()))
                block_points.add((idk, date))
                grp = global_groups.setdefault(idk, {'ident':ident,'unit_points':[],'tables':set()})
                if pad_deficit >= 2:
                    grp['ragged_pad'] = max(grp.get('ragged_pad', 0.0),
                                            pad_deficit / max(1, ncols - 1))
                grp['unit_points'].append({'date':date,'amount':amt,
                                           'unit':vh['unit'] or row_unit or desc_unit,
                                           'holiday_incl':desc_hol,
                                           'label':vh['header'] if vh['header']!='_rowkey' else t['desc'][:40],
                                           # per-point CELL ANCHORS (relabel guard G4): the exact
                                           # column header + row-key text this amount came from --
                                           # makes label assignment machine-checkable per point
                                           'col_header':t['cols'][vi],
                                           'row_key':' | '.join(cells[i].strip() for i in key_idx
                                                                if cells[i].strip() not in ('','-')) or None,
                                           'rescaled_from':rescaled_from})
                if desc_ft is not None: grp.setdefault('ft_hours', desc_ft)
                # role-check instrumentation (additive; no effect on row values)
                grp['tables'].add(tidx)
                if ident.get('jobgroup'): dg['jg_tokens'].add(str(ident['jobgroup']))
                if ident.get('step'): dg['step_tokens'].add(str(ident['step']))
                if ident.get('age_group'): dg['age_tokens'].add(str(ident['age_group']))
                dg['amounts'].add(round(amt,2))
        dg['seen_cells'] = seen_cells
        dg['emitted_points'] = len(block_points)
        dg['ragged_rows'] = ragged
        dg['coverage'] = (len(block_points) / seen_cells) if seen_cells else None

    # ---- build SalaryRows from globally-merged groups (timeline across blocks) ----
    from datetime import date as _date, timedelta as _td
    def _day_before(iso):
        try:
            y, m, d = map(int, iso.split('-'))
            return (_date(y, m, d) - _td(days=1)).isoformat()
        except Exception:
            return None
    for idk, grp in global_groups.items():
        ident = grp['ident']; pts = grp['unit_points']
        units = set(p['unit'] for p in pts if p['unit'])
        best = sorted(units, key=lambda u: UNIT_PREF.get(u,9))[0] if units else None
        # MAGNITUDE-BASED unit inference (Hanna, 2026-07-08): when the source states NO pay
        # period anywhere (title/header/row all silent, e.g. CAO 3335 sector scales), infer
        # from the amount magnitude and TAG it (`unit_inferred`) so it is auditable, never
        # silently invented. Hourly (<60) and annual (>=20000) are unambiguous; the monthly
        # band (1500-8000) OVERLAPS 4-week/weekly, so it is a best-guess flagged for review.
        # Mid-gaps stay null (genuinely ambiguous).
        if best is None:
            _av = sorted(p['amount'] for p in pts if p.get('amount'))
            if _av:
                _med = _av[len(_av) // 2]
                _inf = ('hourly' if _med < 60 else
                        'monthly' if 1500 <= _med <= 8000 else
                        'annual' if _med >= 20000 else None)
                if _inf:
                    best = _inf
                    grp['unit_inferred'] = True
        # UNIT SELECTION PER DATE (not per row): keep the preferred unit's observation for
        # each date; an other-unit point survives ONLY for dates the preferred unit doesn't
        # cover (carrying its own unit) -- so a monthly-2019 + hourly-2020 identity keeps
        # BOTH dated observations instead of silently dropping hourly-2020.
        bydate = collections.defaultdict(list)
        for p in pts: bydate[p['date']].append(p)
        chosen = []
        for dt, ps in bydate.items():
            pref = [p for p in ps if p['unit'] == best]
            chosen.extend(pref if pref else
                          [p for p in ps if p['unit'] ==
                           sorted({q['unit'] for q in ps}, key=lambda u: UNIT_PREF.get(u,9))[0]])
        # dedup by (date, amount); keep distinct observations; undated sort last
        seen=set(); tl=[]
        for p in sorted(chosen, key=lambda x:(x['date'] or '9999-99-99')):
            k=(p['date'], round(p['amount'],2))
            if k in seen: continue
            seen.add(k); tl.append(p)
        jg = ident.get('jobgroup','')
        mm = ident.get('_mm')
        if mm: jg = ('%s (%s)' % (jg, 'minimum' if mm=='min' else 'maximum')).strip()
        dts = [p['date'] for p in tl if p['date']]
        # end_date: mechanical derivation = day before the SAME row's next dated point
        # (documented derivation, no invention; last point stays open/null)
        distinct_dates = sorted(set(dts))
        next_start = {a: b for a, b in zip(distinct_dates, distinct_dates[1:])}
        row = {
            # jobgroup falls back to the step, else stays empty. The old '(scale)' literal
            # placeholder was a fake label (CAO 10 Bouw audit) -> null is honest; the row
            # is still identified by (step, worker, age, timeline).
            'jobgroup': jg or ident.get('step','') or None,
            'step': ident.get('step'),
            'worker': ident.get('_worker'),
            'age_group': ident.get('age_group'),
            'unit': best,
            'is_entry': True if ident.get('_entry') else None,
            'ft_hours': grp.get('ft_hours'),
            'timeline': [{'start_date':p['date'],
                          'end_date':_day_before(next_start[p['date']]) if p['date'] in next_start else None,
                          'amount':p['amount'],'unit':p['unit'] or best,
                          'holiday_incl':p.get('holiday_incl'),
                          'table_label':p['label'],
                          'col_header':p.get('col_header'),
                          'row_key':p.get('row_key'),
                          **({'value_source':'rescaled','rescaled_from':p['rescaled_from']}
                             if p.get('rescaled_from') else {})} for p in tl],
        }
        # pure-percentage labels are not valid jobgroup/step NAMES. The % VALUE tells us
        # which kind (Hanna audit 2026-07-08):
        #   <15%      = a step-increment / raise ('Standaard Stap 3%', CAO 249) on a real
        #               (adult) wage row -> keep the row + its real jobgroup, % -> note.
        #   15-99% AND youth-SIZED amount = a YOUTH staffel rate -> DROP (youth excluded).
        #               The amount test is essential: a 70-90% row with a HIGH amount is a
        #               Hay-scale range position on an adult wage (CAO 2050: 70% = EUR4273),
        #               NOT youth -> keep.
        #   >=100%    = adult full rate (100%) or a premium (>100%) -> keep, % -> note.
        _pcts = {}
        for _f in ('jobgroup', 'step'):
            _m = re.fullmatch(r'(\d{1,3}(?:[.,]\d+)?)\s*%', str(row.get(_f) or '').strip())
            if _m: _pcts[_f] = float(_m.group(1).replace(',', '.'))
        if _pcts:
            _med = sorted(p['amount'] for p in tl)[len(tl) // 2] if tl else 0
            _u = best or (tl[0]['unit'] if tl else None)
            _youth_ceil = {'hourly': 11, 'weekly': 450, '4-week': 1700, 'period': 1700,
                           'annual': 24000}.get(_u, 1900)   # default monthly
            if any(15 <= v < 100 for v in _pcts.values()) and _med < _youth_ceil:
                continue                                   # youth staffel row -> drop
            note = []
            for _f, _v in _pcts.items():
                note.append('%s=%s' % (_f, row[_f])); row[_f] = None
            row['_row_note'] = ('increment ' if all(v < 15 for v in _pcts.values())
                                else 'staffel ') + ' '.join(note)
        if grp.get('unit_inferred'): row['_unit_inferred'] = True
        if any(p.get('rescaled_from') for p in tl): row['_rescaled'] = True
        if grp.get('ragged_pad'): row['_ragged_pad'] = round(grp['ragged_pad'], 2)
        if any((p['unit'] or best) != best for p in tl): row['_mixed_unit'] = True
        # quality flag: >1 amount at the same date => columns not fully distinguished
        # (residual surcharge / sub-category / undetected min-max) -> route to review
        if len(dts) != len(set(dts)): row['_dup_date'] = True
        # DROP job-classification mapping rows: a row whose jobgroup OR step is the literal
        # header word 'Scale'/'Schaal' with small-integer "amounts" is a Function-Family->
        # scale-number lookup (CAO 234/3798), not a wage row.
        _labs = {str(row.get(f) or '').strip().lower().strip('()') for f in ('jobgroup', 'step')}
        if _labs & {'scale', 'schaal'} and all(
                p['amount'] == int(p['amount']) and p['amount'] < 20 for p in tl):
            continue
        row['_tables'] = sorted(grp.get('tables', ()))   # contributing table indices (role-check)
        out_rows.append(row)
    # JSON-safety: instrumentation sets -> sorted lists
    for d in diags:
        for k in ('jg_tokens','step_tokens','age_tokens','amounts'):
            if isinstance(d.get(k), set): d[k] = sorted(d[k])
    return out_rows, diags, docdate


if __name__ == '__main__':
    import sys
    f = sys.argv[1] if len(sys.argv)>1 else \
        'outputs/llm_extracted/new_flow/10/Cao Bouw en Infra 2023_extract.json'
    ext = json.load(open(f))
    rows, diags, dd = parse_extract(ext)
    print('doc date:', dd)
    print('parsed rows:', len(rows))
    for r in rows[:8]:
        print(' ', r['jobgroup'], '| worker=',r['worker'],'| age=',r['age_group'],
              '| unit=',r['unit'],'|', [(p['start_date'],p['amount']) for p in r['timeline']])
    print('--- table diagnostics (included?) ---')
    for d in diags:
        print(('[x]' if d['included'] else '[ ]'), d['desc'])
