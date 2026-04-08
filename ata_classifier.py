"""
ata_classifier.py — Intelligent message classifier for construction site messages.

Classifies incoming WhatsApp messages into categories:
    - ata:       Ändrings- och tilläggsarbete (change orders)
    - beslut:    Beslut fattade på plats eller i möte
    - avvikelse: Kvalitetsavvikelse, skada, fel
    - dagbok:    Dagboksanteckning / lägesrapport
    - time_report: Tidrapportering (timmar per projekt)
    - task:      Vanlig uppgift (befintligt flöde)

Also extracts:
    - Estimated amount (from ÄTA messages)
    - Severity (for avvikelse)
    - Project reference

Priority: ÄTA > beslut > avvikelse > time_report > dagbok > task
A message that is both an ÄTA and contains a task verb gets classified as ÄTA
(the task is created as a follow-up inside the ÄTA flow).
"""

import re
from typing import Optional

# ── ÄTA patterns (pre-compiled) ──
ATA_STRONG = [
    # 'äta' and common autocorrect/typo variants (Swedish mobile autocorrect is aggressive)
    re.compile(r'\bäta(?:n|r|s)?\b', re.IGNORECASE),
    re.compile(r'\bätta\b', re.IGNORECASE),           # double-t typo
    re.compile(r'\bäran\b', re.IGNORECASE),           # autocorrect of 'ätan'
    re.compile(r'\bettan\b', re.IGNORECASE),          # autocorrect of 'ätan'
    re.compile(r'\båta(?:n)?\b', re.IGNORECASE),      # wrong diacritic
    re.compile(r'\beta\b(?=.*\b(?:kr|kronor|tim|extra|tillkommer|kostar|arbet|material)\b)', re.IGNORECASE),  # 'eta' only with work/money context
    re.compile(r'\btilläggsarbete', re.IGNORECASE),
    re.compile(r'\bändringsarbete', re.IGNORECASE),
    re.compile(r'\btillägg\b', re.IGNORECASE),
    re.compile(r'\bändring(?:en|ar)?\b.*\b(?:arbete|kostnad|pris)', re.IGNORECASE),
    re.compile(r'\bextra\s+arbete', re.IGNORECASE),
    re.compile(r'\btillkommande\s+(?:arbete|kostnad)', re.IGNORECASE),
    re.compile(r'\bändringsnot', re.IGNORECASE),
    re.compile(r'\bändringsorder', re.IGNORECASE),
]

ATA_MEDIUM = [
    re.compile(r'\btillkommer\b', re.IGNORECASE),
    re.compile(r'\bingår\s+inte\b', re.IGNORECASE),
    re.compile(r'\butöver\s+(?:avtal|kontrakt|offert|entreprenad)', re.IGNORECASE),
    re.compile(r'\bej\s+(?:inkluderat|med\s+i|avtalat)', re.IGNORECASE),
    re.compile(r'\binte\s+(?:med\s+i|inkluderat|avtalat)', re.IGNORECASE),
    re.compile(r'\bmer\s+(?:arbete|jobb|tid)\s+(?:än|mot)', re.IGNORECASE),
    re.compile(r'\böverenskomm(?:it|else)', re.IGNORECASE),
    re.compile(r'\bbeställ(?:are|ning|t)\b.*\b(?:vill|önskar|ändra)', re.IGNORECASE),
]

# ── Beslut patterns (pre-compiled) ──
BESLUT_PATTERNS = [
    re.compile(r'\bbeslut(?:at|ade|ades)?\b', re.IGNORECASE),
    re.compile(r'\bvi\s+(?:har\s+)?(?:bestämt|beslutat|kommit\s+överens|valt|enats)', re.IGNORECASE),
    re.compile(r'\böverenskomm(?:else|it)\b', re.IGNORECASE),
    re.compile(r'\bkom\s+överens\b', re.IGNORECASE),
    re.compile(r'\bgodkän(?:t|de|ner|d)\b', re.IGNORECASE),
    re.compile(r'\bavrop(?:at|ade)?\b', re.IGNORECASE),
    re.compile(r'\bprotokoll(?:fört|förs)?\b', re.IGNORECASE),
    re.compile(r'\bav(?:stäm(?:t|ning|de)|stämde)\b', re.IGNORECASE),
    re.compile(r'\bsamråd\b', re.IGNORECASE),
    re.compile(r'\bvalt\s+(?:att|leverantör|lösning|alternativ)', re.IGNORECASE),
    re.compile(r'\bväljer\s+(?:att|leverantör|lösning|alternativ)', re.IGNORECASE),
]

# ── Avvikelse patterns (pre-compiled) ──
AVVIKELSE_PATTERNS = [
    re.compile(r'\bavvikelse\b', re.IGNORECASE),
    re.compile(r'\bskada(?:d|de|t|r)?\b', re.IGNORECASE),
    re.compile(r'\bdefekt\b', re.IGNORECASE),
    re.compile(r'\bbristfällig', re.IGNORECASE),
    re.compile(r'\bfel(?:aktig|monterad|byggd)?\b', re.IGNORECASE),
    re.compile(r'\breklamation', re.IGNORECASE),
    re.compile(r'\bkvalitetsbrist', re.IGNORECASE),
    re.compile(r'\binte\s+(?:ok|okej|bra|godkänt)', re.IGNORECASE),
    re.compile(r'\bproblem\s+(?:med|på|i)\b', re.IGNORECASE),
    re.compile(r'\bläckage\b', re.IGNORECASE),
    re.compile(r'\bspricka\b', re.IGNORECASE),
    re.compile(r'\bross?t\b', re.IGNORECASE),
    re.compile(r'\bmögel\b', re.IGNORECASE),
    re.compile(r'\bfukt(?:skada)?\b', re.IGNORECASE),
    re.compile(r'\bvattenskada\b', re.IGNORECASE),
    re.compile(r'\bvattenläcka\b', re.IGNORECASE),
    re.compile(r'\böversvämning', re.IGNORECASE),
    re.compile(r'\bsättning(?:sskada)?\b', re.IGNORECASE),
    re.compile(r'\bfel(?:konstruktion|montage|installation)', re.IGNORECASE),
]

AVVIKELSE_SEVERITY = {
    'critical': [
        re.compile(r'\bakut\b', re.IGNORECASE),
        re.compile(r'\bfara\b', re.IGNORECASE),
        re.compile(r'\bsäkerhet', re.IGNORECASE),
        re.compile(r'\bstopp', re.IGNORECASE),
        re.compile(r'\bomedelbar', re.IGNORECASE),
        re.compile(r'\butrym(?:ning|me)\b', re.IGNORECASE),
        re.compile(r'\bras\b', re.IGNORECASE),
        re.compile(r'\bkollaps', re.IGNORECASE),
    ],
    'important': [
        re.compile(r'\bskada\b', re.IGNORECASE),
        re.compile(r'\bläckage\b', re.IGNORECASE),
        re.compile(r'\bvattenskada\b', re.IGNORECASE),
        re.compile(r'\breklamation', re.IGNORECASE),
        re.compile(r'\bbyte\s+(?:av|krävs)\b', re.IGNORECASE),
        re.compile(r'\bfuktskada\b', re.IGNORECASE),
    ],
}

# ── Dagbok patterns (pre-compiled) ──
DAGBOK_PATTERNS = [
    re.compile(r'\bdagbok\b', re.IGNORECASE),
    re.compile(r'\bdagrapport\b', re.IGNORECASE),
    re.compile(r'\bdagens\s+(?:arbete|jobb|insats)', re.IGNORECASE),
    re.compile(r'\bidag\s+(?:har|blev|gjordes|utfördes)', re.IGNORECASE),
    re.compile(r'\bvi\s+(?:har|gjorde|utförde|monterade|rev|satte|la)', re.IGNORECASE),
    re.compile(r'\blägesrapport\b', re.IGNORECASE),
    re.compile(r'\bstatus(?:rapport|uppdatering)\b', re.IGNORECASE),
    re.compile(r'\bpågående\s+arbete', re.IGNORECASE),
    re.compile(r'\bfärdig(?:ställt|t)\b', re.IGNORECASE),
    re.compile(r'\bklart?\s+(?:med|på)\b', re.IGNORECASE),
    re.compile(r'\bdagsrapport\b', re.IGNORECASE),
    re.compile(r'\bvädret\s+(?:stoppade|hindrade|försenade|ställde)', re.IGNORECASE),
    re.compile(r'\bregn(?:ade)?\s+hela', re.IGNORECASE),
    re.compile(r'\bstillestånd', re.IGNORECASE),
    re.compile(r'\bproduktions?stopp', re.IGNORECASE),
    re.compile(r'\binget\s+arbete\s+(?:idag|igår)', re.IGNORECASE),
]

# ── Tidrapport patterns (pre-compiled) ──
TIME_REPORT_STRONG = [
    re.compile(r'\btidrapport', re.IGNORECASE),
    re.compile(r'\btimrapport', re.IGNORECASE),
    re.compile(r'\btid\s*:\s*\d', re.IGNORECASE),          # "tid: 8"
    re.compile(r'\b(\d+[.,]?\d*)\s*(?:h|tim|timmar)\b', re.IGNORECASE),  # "8 timmar", "7.5h", "8tim"
    re.compile(r'\bjobbade\s+\d', re.IGNORECASE),            # "jobbade 8..."
    re.compile(r'\barbetade\s+\d', re.IGNORECASE),           # "arbetade 8..."
    re.compile(r'\brapportera\s+tid', re.IGNORECASE),
]

TIME_REPORT_MEDIUM = [
    re.compile(r'\btimmar\b', re.IGNORECASE),
    re.compile(r'\bheldag\b', re.IGNORECASE),
    re.compile(r'\bhalvdag\b', re.IGNORECASE),
    re.compile(r'\bjobbade\s+(?:hela|halva)\s+dagen', re.IGNORECASE),
    re.compile(r'\bidag\b.*\b(?:h|tim)\b', re.IGNORECASE),  # "idag 8h"
    re.compile(r'\b(?:var|blev)\s+\d+\s*(?:h|tim)', re.IGNORECASE),  # "var 6h"
]

# ── Amount extraction patterns (pre-compiled) ──
# Number body: digits possibly with spaces, then zero-or-more (.ddd) groups, then optional (,d+) decimal
_NUM = r'(\d[\d\s]*(?:\.\d{3})*(?:[,.]\d+)?)'
AMOUNT_PATTERNS = [
    # "150 000 kr", "150000kr", "1.250.000 kr"
    re.compile(_NUM + r'\s*(?:kr|kronor|sek)\b', re.IGNORECASE),
    re.compile(_NUM + r'\s*(?:tkr|tusen)\b', re.IGNORECASE),  # multiply by 1000
    re.compile(_NUM + r'\s*(?:msek|mkr|miljon(?:er)?)\b', re.IGNORECASE),  # multiply by 1_000_000
    # "ca 50 000" preceded by cost context
    re.compile(r'(?:kostar?|pris|belopp|uppskattar?|cirka|ca|uppgår?\s+till)\s+(?:ca\s+)?' + _NUM, re.IGNORECASE),
]


def _extract_hours(text: str) -> Optional[float]:
    """Extract hours from time report message. Returns hours or None."""
    import re as _re
    patterns = [
        _re.compile(r'(\d+[.,]?\d*)\s*(?:h|tim|timmar)\b', _re.IGNORECASE),
        _re.compile(r'\bjobbade\s+(\d+[.,]?\d*)', _re.IGNORECASE),
        _re.compile(r'\barbetade\s+(\d+[.,]?\d*)', _re.IGNORECASE),
        _re.compile(r'\btid\s*:?\s*(\d+[.,]?\d*)', _re.IGNORECASE),
    ]
    for pat in patterns:
        m = pat.search(text)
        if m:
            raw = m.group(1).replace(',', '.')
            try:
                val = float(raw)
                if 0.5 <= val <= 24:
                    return val
            except ValueError:
                continue
    # Heldag / halvdag
    if _re.search(r'\bheldag\b', text, _re.IGNORECASE):
        return 8.0
    if _re.search(r'\bhalvdag\b', text, _re.IGNORECASE):
        return 4.0
    return None


def _extract_amount(text: str) -> Optional[float]:
    """Extract estimated monetary amount from text. Returns SEK or None."""
    import re as _re
    # Swedish thousand-separator pattern: 1-3 digits + (dot + exactly 3 digits)+
    # e.g. "55.000", "1.250", "12.500", "1.250.000" — NOT decimals
    _thousand_sep = _re.compile(r'^\d{1,3}(?:\.\d{3})+$')

    for pattern in AMOUNT_PATTERNS:
        m = pattern.search(text)
        if m:
            raw = m.group(1).replace(' ', '').replace('\xa0', '')
            # If raw matches thousand-separator pattern, strip all dots
            if _thousand_sep.match(raw):
                raw = raw.replace('.', '')
            # Swedish decimal comma: convert to dot
            if ',' in raw and raw.count(',') == 1:
                raw = raw.replace(',', '.')
            try:
                val = float(raw)
            except ValueError:
                continue

            # Detect multiplier from matched context
            full_match = m.group(0).lower()
            if any(x in full_match for x in ['tkr', 'tusen']):
                val *= 1000
            elif any(x in full_match for x in ['msek', 'mkr', 'miljon']):
                val *= 1_000_000

            # Sanity check: 100 to 50M SEK
            if 100 <= val <= 50_000_000:
                return val

    return None


def _assess_severity(text: str) -> str:
    """Assess severity of an avvikelse. Returns critical/important/normal."""
    for level, patterns in AVVIKELSE_SEVERITY.items():
        for pattern in patterns:
            if pattern.search(text):
                return level
    return 'normal'


def _score_patterns(text: str, patterns: list) -> int:
    """Count how many patterns match in text."""
    hits = 0
    for pattern in patterns:
        if pattern.search(text):
            hits += 1
    return hits


def classify_message(text: str, has_image: bool = False) -> dict:
    """Classify a construction site message.

    Returns:
        {
            'type': 'ata' | 'beslut' | 'avvikelse' | 'dagbok' | 'task',
            'confidence': float 0-1,
            'estimated_amount': float | None,  (only for ÄTA)
            'severity': str | None,  (only for avvikelse)
            'signals': list[str],  # matched patterns for transparency
        }
    """
    if not text or len(text.strip()) < 5:
        return {'type': 'task', 'confidence': 0.0, 'estimated_amount': None, 'severity': None, 'signals': []}

    scores = {
        'ata': 0,
        'beslut': 0,
        'avvikelse': 0,
        'time_report': 0,
        'dagbok': 0,
    }
    signals = []

    # ── ÄTA scoring ──
    ata_strong_hits = _score_patterns(text, ATA_STRONG)
    ata_medium_hits = _score_patterns(text, ATA_MEDIUM)
    scores['ata'] = ata_strong_hits * 3 + ata_medium_hits * 1.5
    if ata_strong_hits > 0:
        signals.append(f'ata_strong:{ata_strong_hits}')
    if ata_medium_hits > 0:
        signals.append(f'ata_medium:{ata_medium_hits}')

    amount = _extract_amount(text)
    if amount and scores['ata'] > 0:
        scores['ata'] += 2  # Amount + ÄTA context is very strong
        signals.append(f'amount:{amount:.0f}')

    # ── Beslut scoring ──
    beslut_hits = _score_patterns(text, BESLUT_PATTERNS)
    scores['beslut'] = beslut_hits * 2.5
    if beslut_hits > 0:
        signals.append(f'beslut:{beslut_hits}')

    # ── Avvikelse scoring ──
    avvikelse_hits = _score_patterns(text, AVVIKELSE_PATTERNS)
    scores['avvikelse'] = avvikelse_hits * 2
    if avvikelse_hits > 0:
        signals.append(f'avvikelse:{avvikelse_hits}')
    if has_image and avvikelse_hits > 0:
        scores['avvikelse'] += 1.5  # Photo + avvikelse language
        signals.append('avvikelse_foto')

    # ── Tidrapport scoring ──
    time_strong_hits = _score_patterns(text, TIME_REPORT_STRONG)
    time_medium_hits = _score_patterns(text, TIME_REPORT_MEDIUM)
    scores['time_report'] = time_strong_hits * 3 + time_medium_hits * 1.5
    hours = _extract_hours(text)
    if hours and scores['time_report'] > 0:
        scores['time_report'] += 2  # Hours + time context is very strong
        signals.append(f'hours:{hours}')
    if time_strong_hits > 0:
        signals.append(f'time_strong:{time_strong_hits}')
    if time_medium_hits > 0:
        signals.append(f'time_medium:{time_medium_hits}')

    # ── Dagbok scoring ──
    dagbok_hits = _score_patterns(text, DAGBOK_PATTERNS)
    scores['dagbok'] = dagbok_hits * 2
    if dagbok_hits > 0:
        signals.append(f'dagbok:{dagbok_hits}')

    # ── Determine winner (with documented priority tiebreak) ──
    # Priority: ÄTA > beslut > avvikelse > time_report > dagbok > task
    PRIORITY_ORDER = ['ata', 'beslut', 'avvikelse', 'time_report', 'dagbok']
    best_type = max(scores, key=scores.get)
    best_score = scores[best_type]

    # If a higher-priority type also has a significant score (>= 2),
    # it wins unless the lower-priority type has an overwhelmingly higher score (>3x).
    for ptype in PRIORITY_ORDER:
        if ptype == best_type:
            break  # already the highest-priority winner
        if scores[ptype] >= 2 and scores[ptype] >= best_score * 0.35:
            best_type = ptype
            best_score = scores[ptype]
            break

    # Threshold: minimum score 2 for non-task classification
    if best_score < 2:
        return {
            'type': 'task',
            'confidence': 0.0,
            'estimated_amount': None,
            'severity': None,
            'signals': signals,
        }

    # Confidence: 2→0.5, 3→0.65, 4→0.75, 5→0.85, 6+→0.90+
    confidence = min(0.95, 0.35 + best_score * 0.10)

    result = {
        'type': best_type,
        'confidence': round(confidence, 2),
        'estimated_amount': amount if best_type == 'ata' else None,
        'severity': _assess_severity(text) if best_type == 'avvikelse' else None,
        'hours': _extract_hours(text) if best_type in ('time_report', 'ata') else None,
        'signals': signals,
    }

    return result


# ── Quick test ──
if __name__ == '__main__':
    tests = [
        ("Vi har ett ÄTA på Grimvägen, tillkommande arbete med dränering ca 85 000 kr", True),
        ("Beställaren vill ändra planlösningen på plan 2, tillkommer ca 150 tkr", True),
        ("Beslut taget: vi kör med leverantör X för fasadplåten", False),
        ("Avvikelse: fuktskada i källaren på Rocmore, behöver utredas", False),
        ("Idag har vi monterat ställning på södra gaveln, klart med rivning av befintlig fasad", False),
        ("Mattias kan du kolla upp status på Signalisten?", False),
        ("Grimvägen 8 timmar idag", False),
        ("Jobbade 6h på Lappkärrsberget, brandgata", False),
        ("Tid: 4 tim fasadmontage Enhörna", False),
        ("Extra arbete utöver avtal med isolering av vinden", False),
        ("Vi kom överens om att byta fönsterleverantör", False),
        ("Skada på nylagd marksten vid infarten", True),
    ]

    for text, has_img in tests:
        r = classify_message(text, has_image=has_img)
        print(f"[{r['type']:10s}] conf={r['confidence']:.0%} | {text[:70]}")
        if r['estimated_amount']:
            print(f"           amount: {r['estimated_amount']:,.0f} SEK")
        if r['severity']:
            print(f"           severity: {r['severity']}")
        print()
