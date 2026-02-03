# cleaner.py — ICAIL 2026
# Document Cleaning Pipeline for Apex Court Judgments
#
# Accompanies: "Empirical Analysis of Judicial Writing Across Five Apex Courts"
# For: ICAIL 2026 (International Conference on Artificial Intelligence and Law)
#
# Cleans raw HTML and PDF-extracted text files, removing formatting artefacts,
# boilerplate, and structural noise to produce analysis-ready plain text.
#
# Supports: HTML (UK courts), TXT from PDF extraction (Singapore, Australia courts)

"""
Document cleaning pipeline for legal judgments across jurisdictions.

Supports:
- Formats: HTML (UK courts), TXT extracted from PDF (Singapore, Australia courts)
- Jurisdictions: UK (UKSC, UKHL), Singapore (SGCA, SGHC), Australia (HCA)

Main entry point:
    process_file(filepath, format="auto", jurisdiction="auto")
"""

import os
import re
from pathlib import Path
from typing import Tuple, Optional, Dict, List
from bs4 import BeautifulSoup

# Configuration constants (inline for portability)
class config:
    ENCODING = 'utf-8'
    ENCODING_ERRORS = 'ignore'


# ============================================================================
# COMMON UTILITIES - Used by all formats and jurisdictions
# ============================================================================

def long_path(p: str) -> str:
    """Add Windows long path prefix for paths > 260 chars."""
    if not p.startswith('\\\\?\\'):
        return '\\\\?\\' + os.path.abspath(p)
    return p


def normalize_text(text: str) -> str:
    """
    Standardize whitespace and line breaks.
    Keeps newlines for reflow and digit-stack repair.
    """
    text = text.replace("\r\n", "\n").replace("\r", "\n")
    text = text.replace("\u00a0", " ")  # Non-breaking space
    text = re.sub(r"[ \t]+", " ", text)
    text = re.sub(r"\n{3,}", "\n\n", text)
    return text.strip()


def delete_end_of_document(text: str) -> str:
    """Hard cut: delete 'End of Document' and anything below it."""
    m = re.search(r"(?im)^\s*End of Document\s*$", text)
    if m:
        return text[: m.start()].rstrip()
    m2 = re.search(r"(?i)\bEnd of Document\b", text)
    if m2:
        return text[: m2.start()].rstrip()
    return text


def repair_digit_stacks(text: str) -> str:
    """
    Fix vertical digits caused by PDF artifacts:
      2\n0\n1\n5  -> 2015
      1\n0.      -> 10.
    Must run BEFORE paragraph detection.
    """
    t = text
    # Merge digit-newline-digit repeatedly
    for _ in range(10):
        new = re.sub(r"(?<=\d)\s*\n\s*(?=\d)", "", t)
        if new == t:
            break
        t = new
    # Merge digit-newline-dot
    t = re.sub(r"(?<=\d)\s*\n\s*\.", ".", t)
    # Again after dot merge
    for _ in range(10):
        new = re.sub(r"(?<=\d)\s*\n\s*(?=\d)", "", t)
        if new == t:
            break
        t = new
    return t


def repair_space_separated_digits(text: str) -> str:
    """
    Repair digit sequences separated by single spaces (PDF artifacts).
    E.g., "2 0 1 5" -> "2015", "1 0." -> "10."
    """
    t = text
    for _ in range(10):
        new = re.sub(r'([0-9]) ([0-9])(?![0-9])', r'\1\2', t)
        if new == t:
            break
        t = new
    t = re.sub(r'(?<=[0-9]) (?=\.)', '', t)
    return t


def reflow_lonely_numbered_paras(text: str) -> str:
    """
    If we see:
        1.
        <blank lines>
        This appeal ...
    rewrite to:
        1. This appeal ...
    """
    lines = text.splitlines()
    out = []
    i = 0
    while i < len(lines):
        line = lines[i].strip()
        m = re.match(r"^(\d{1,3})\.\s*$", line)
        if m:
            j = i + 1
            while j < len(lines) and lines[j].strip() == "":
                j += 1
            if j < len(lines):
                nxt = lines[j].strip()
                out.append(f"{m.group(1)}. {nxt}")
                i = j + 1
                continue
        out.append(lines[i])
        i += 1
    return "\n".join(out)


def reflow_lonely_bracket_markers(text: str) -> str:
    """
    Reflow lonely bracket markers like:
        "(2)
        <blank lines>
        In this Chapter ...
    To:
        "(2) In this Chapter ...
    """
    lines = text.splitlines()
    out = []
    i = 0
    marker_re = re.compile(r'^("|")?\(\s*([0-9]{1,3}|[a-zA-Z])\s*\)\s*("|")?\s*$')

    while i < len(lines):
        raw = lines[i]
        line = raw.strip()
        m = marker_re.match(line)
        if m:
            open_q = m.group(1) or ""
            marker = m.group(2)
            close_q = m.group(3) or ""
            j = i + 1
            while j < len(lines) and lines[j].strip() == "":
                j += 1
            if j < len(lines):
                nxt = lines[j].strip()
                out.append(f"{open_q}({marker}){close_q} {nxt}".rstrip())
                i = j + 1
                continue
        out.append(raw)
        i += 1
    return "\n".join(out)


def fix_split_company_names(content: str) -> str:
    """Fix company name suffixes split across lines (Pte Ltd, Sdn Bhd, etc.)."""
    content = re.sub(r'\bPte\.?\s*\n+\s*Ltd\b', 'Pte Ltd', content)
    content = re.sub(r'\bSdn\.?\s*\n+\s*Bhd\b', 'Sdn Bhd', content)
    content = re.sub(r'\b(\w+)\s*\n+\s*Ltd\b', r'\1 Ltd', content)
    content = re.sub(r'\bPrivate\s*\n+\s*Limited\b', 'Private Limited', content)
    content = re.sub(r'\bCo\.?\s*\n+\s*Ltd\b', 'Co Ltd', content)
    content = re.sub(r'\bInc\.?\s*\n+\s*\(', 'Inc (', content)
    return content


def clean_multiple_blanks(content: str) -> str:
    """Clean up multiple blank lines."""
    content = re.sub(r'\n{4,}', '\n\n\n', content)
    content = re.sub(r' +\n', '\n', content)
    content = re.sub(r'  +', ' ', content)
    return content


# ============================================================================
# FORMAT: HTML (UK Courts - UKSC, UKHL)
# ============================================================================

def parse_html(filepath: str) -> BeautifulSoup:
    """Parse HTML file into BeautifulSoup object."""
    path = Path(filepath)
    try:
        content = path.read_text(encoding=config.ENCODING, errors=config.ENCODING_ERRORS)
    except FileNotFoundError:
        if not filepath.startswith("\\\\?\\"):
            extended_path = Path("\\\\?\\" + str(path.resolve()))
            content = extended_path.read_text(encoding=config.ENCODING, errors=config.ENCODING_ERRORS)
        else:
            raise
    return BeautifulSoup(content, "html.parser")


def strip_rubbish_tags(soup: BeautifulSoup) -> BeautifulSoup:
    """Remove script, style, noscript, svg tags from soup."""
    for tag_name in ["script", "style", "noscript", "svg"]:
        for t in soup.find_all(tag_name):
            t.decompose()
    return soup


def extract_date_html(soup: BeautifulSoup) -> Optional[str]:
    """Extract judgment date from HTML content (UK format)."""
    text = soup.get_text()
    patterns = [
        r"JUDGMENT\s+GIVEN\s+ON\s+(\d{1,2}\s+\w+\s+\d{4})",
        r"Judgment\s+given\s+on\s+(\d{1,2}\s+\w+\s+\d{4})",
        r"HEARD\s+ON.*?JUDGMENT\s+(\d{1,2}\s+\w+\s+\d{4})",
        r"(\d{1,2}\s+(?:January|February|March|April|May|June|July|August|September|October|November|December)\s+\d{4})",
    ]
    for pattern in patterns:
        m = re.search(pattern, text, re.IGNORECASE)
        if m:
            return m.group(1).strip()
    return None


# ============================================================================
# FORMAT: TXT (Singapore Courts - SGCA, SGHC from PDF extraction)
# ============================================================================

def delete_note_references(content: str) -> str:
    """Delete [note: X] references and footnote content."""
    content = re.sub(r'\[note:\s*\d+\]', '', content)
    lines = content.split('\n')
    filtered = []
    for line in lines:
        stripped = line.strip()
        if re.match(r'^\d{1,2}\.\s+(?:Appellant|Respondent|Plaintiff|Defendant)\'?s?\s+(?:Case|Skeletal|Submissions|Reply|Core Bundle|Written)', stripped, re.IGNORECASE):
            continue
        if re.match(r'^o\s+\[note:\s*\d+\]', stripped):
            continue
        if re.match(r'^\[note:\s*\d+\]', stripped):
            continue
        filtered.append(line)
    return '\n'.join(filtered)


def remove_version_markers(content: str) -> str:
    """Remove PDF version markers like 'Version No 1: 22 Nov 2024 (10:26 hrs)'."""
    return re.sub(r'Version No \d+:\s*\d+\s+\w+\s+\d{4}\s*\(\d+:\d+\s*hrs?\)', '', content)


def remove_page_numbers(content: str) -> str:
    """Remove standalone page numbers and PDF page markers."""
    lines = content.split('\n')
    filtered = []
    for line in lines:
        stripped = line.strip()
        if re.match(r'^\d{1,3}$', stripped):
            continue
        if re.match(r'^Page\s+\d+\s+of\s+\d+', stripped, re.IGNORECASE):
            continue
        filtered.append(line)
    return '\n'.join(filtered)


def remove_table_of_contents(content: str) -> Tuple[str, bool]:
    """Remove table of contents with dotted leaders and leftover TOC fragments."""
    had_toc = False
    if re.search(r'\.{5,}\s*\d+', content):
        had_toc = True
        content = re.sub(r'[^\n]*\.{5,}\s*\d+[^\n]*\n?', '', content)
    content = re.sub(r'\n\s*(?:Table of )?Contents?\s*\n', '\n', content, flags=re.IGNORECASE)

    # Remove leftover TOC fragments after CORE JUDGMENT
    core_idx = content.find('CORE JUDGMENT')
    if core_idx > 0:
        core_end = content.find('\n', core_idx + 20)
        if core_end > 0:
            after_core = content[core_end:core_end + 3000]
            toc_fragment_pattern = r'\n\s*\([a-z0-9]+\)\s+[A-Z][^.\n]{10,100}(?:\s+\.\.\.\s*\d+|\s+\d+\s*$|\s*$)'
            fragments = re.findall(toc_fragment_pattern, after_core)
            if len(fragments) > 3:
                had_toc = True
                lines = content.split('\n')
                result = []
                in_toc_section = False
                for i, line in enumerate(lines):
                    stripped = line.strip()
                    if 'CORE JUDGMENT' in line:
                        in_toc_section = True
                        result.append(line)
                        continue
                    if in_toc_section:
                        is_toc_entry = False
                        if re.match(r'^\([a-z0-9]+\)\s+[A-Z]', stripped):
                            if len(stripped) < 150 and not stripped.endswith('.'):
                                is_toc_entry = True
                            if re.search(r'\.\.\.\s*\d+\s*$|\s+\d{2,3}\s*$', stripped):
                                is_toc_entry = True
                        if re.match(r'^[A-Z][A-Z\s]+$', stripped) and len(stripped) < 80:
                            is_toc_entry = True
                        if re.match(r'^[A-Z][a-z]+(?:\s+[a-z]+)*$', stripped) and len(stripped) < 50:
                            in_toc_section = False
                        if re.match(r'^\d+\.\s+[A-Z]', stripped) and len(stripped) > 100:
                            in_toc_section = False
                        if is_toc_entry:
                            continue
                    result.append(line)
                content = '\n'.join(result)

    content = re.sub(r'\n\s*\n\s*\n\s*\n', '\n\n\n', content)
    return content, had_toc


def remove_header_footer_citations(content: str, citation_pattern: str = r'\[\d{4}\]\s*SGCA\s*\d+') -> str:
    """Remove random case citations that appear as headers/footers."""
    lines = content.split('\n')
    filtered = []
    prev_blank = False
    for i, line in enumerate(lines):
        stripped = line.strip()
        if re.match(rf'^{citation_pattern}$', stripped):
            next_blank = (i + 1 < len(lines) and not lines[i + 1].strip())
            if prev_blank or next_blank:
                prev_blank = True
                continue
        prev_blank = not stripped
        filtered.append(line)
    return '\n'.join(filtered)


def fix_date_periods(content: str) -> str:
    """Fix dates where period was incorrectly added after day number."""
    months = ['January', 'February', 'March', 'April', 'May', 'June',
              'July', 'August', 'September', 'October', 'November', 'December']
    for month in months:
        content = re.sub(rf'(\d{{1,2}})\.\s*({month})\s+(\d{{4}})',
                         rf'\1 \2 \3', content, flags=re.IGNORECASE)
        content = re.sub(rf'^(\d{{1,2}})\.\s*({month})\s+(\d{{4}})',
                         rf'\1 \2 \3', content, flags=re.MULTILINE | re.IGNORECASE)
    return content


def fix_broken_citations(content: str) -> str:
    """Fix citations split across lines like [2014]\\n\\n4. SLR 723."""
    content = re.sub(r'\[(\d{4})\]\s*\n\s*\n(\d+)\.\s*SLR', r'[\1] \2 SLR', content)
    content = re.sub(r'\[(\d{4})\]\s*\n\s*\n(\d+)\s+SLR', r'[\1] \2 SLR', content)
    return content


def fix_truncated_years(content: str) -> str:
    """
    Fix truncated years from PDF extraction artifacts.

    Examples:
        "January 20." -> "January 20XX" (inferred from context)
        "in 19." -> "in 19XX"
        "married in 19." -> "married in 19XX"

    Uses the case citation year as reference when available.
    """
    # Try to extract case year from citation [YYYY] SGCA/SGHC
    case_year_match = re.search(r'\[(\d{4})\]\s*SG[A-Z]+', content)
    case_year = int(case_year_match.group(1)) if case_year_match else 2020

    months = 'January|February|March|April|May|June|July|August|September|October|November|December'

    # Fix "Month 20." or "Month 19." - truncated year after month
    # Pattern: Month + space + 2 digits + period (but not followed by more digits)
    def fix_month_year(match):
        month = match.group(1)
        year_prefix = match.group(2)  # "19" or "20"
        # Infer full year - use case year's last 2 digits as reference
        if year_prefix == "20":
            # Could be 2000-2029
            suffix = str(case_year)[-2:] if case_year >= 2000 else "20"
            return f"{month} {year_prefix}{suffix}"
        elif year_prefix == "19":
            # Historical date, likely 1990s or earlier
            return f"{month} {year_prefix}90"  # Default to 1990
        return match.group(0)

    content = re.sub(
        rf'({months})\s+(19|20)\.\s*(?!\d)',
        fix_month_year,
        content,
        flags=re.IGNORECASE
    )

    # Fix "in 20." or "of 20." - standalone truncated years
    content = re.sub(
        rf'\b(in|of|from|since|until|by)\s+(19|20)\.\s*(?!\d)',
        lambda m: f"{m.group(1)} {m.group(2)}{str(case_year)[-2:] if m.group(2) == '20' else '90'}",
        content,
        flags=re.IGNORECASE
    )

    # Fix statute citations like "Cap 68, 20. Rev Ed" -> "Cap 68, 2012 Rev Ed"
    content = re.sub(
        r'(Cap\s+\d+,?\s*)(20)\.\s*(Rev\s*Ed)',
        lambda m: f"{m.group(1)}20{str(min(case_year, 2020))[-2:]} {m.group(3)}",
        content,
        flags=re.IGNORECASE
    )

    # Fix "CA 135 of 20." -> "CA 135 of 20XX"
    content = re.sub(
        r'(CA|HC|OS|S|SUM|AD)\s+(\d+)\s+of\s+(20)\.\s*(?!\d)',
        lambda m: f"{m.group(1)} {m.group(2)} of {m.group(3)}{str(case_year)[-2:]}",
        content,
        flags=re.IGNORECASE
    )

    return content


def fix_truncated_numbers(content: str) -> str:
    """
    Fix truncated monetary amounts from PDF extraction.

    Example: "$18,0." is likely truncated - we can't fix the exact value
    but we can remove the trailing period to avoid confusion.

    Pattern: $X,XXX,X. (ends with comma + single digit + period)
    """
    # Remove trailing periods after truncated currency amounts
    # Pattern: $ + digits/commas + single digit + period (not followed by digit)
    content = re.sub(
        r'(\$[\d,]+,\d)\.\s*(?!\d)',
        r'\1[truncated]',
        content
    )

    return content


def fix_money_truncation(content: str) -> str:
    """
    Fix truncated money amounts where decimal was cut off.

    Example: "$6. million" -> "$6 million" (remove orphan period)
    Example: "$1. cm" -> "$1 cm"
    """
    # Pattern: $ + digits + period + space + unit word
    # The period is an artifact from truncated decimal
    content = re.sub(
        r'(\$[\d,]+)\.\s+(million|billion|m\b|b\b|k\b|cm|mm|kg|g\b)',
        r'\1 \2',
        content,
        flags=re.IGNORECASE
    )

    # Also fix standalone truncated decimals like "0. seconds"
    content = re.sub(
        r'(\d+)\.\s+(seconds?|minutes?|hours?|days?|weeks?|months?|years?|cm|mm|metres?|meters?|kg|grams?)',
        r'\1 \2',
        content,
        flags=re.IGNORECASE
    )

    return content


# ============================================================================
# JURISDICTION: UK (UKSC, UKHL)
# ============================================================================

UK_CITATION_PATTERN = re.compile(r"\[\d{4}\]\s+UK(?:SC|HL)\s+\d+")

UK_HEADING_RE = re.compile(r"\(\s*(\d{1,3})\s*\)\s+([A-Z][^()\n]{0,80})")
UK_SUBPARA_RE = re.compile(
    r"\(\s*(\d{1,3})\s*\)\s+(?=(Whether|If|That|To|In|On|The|A|An|As|Where|When|Why|How|[A-Z]))"
)
UK_BAD_HEADING_WORDS = {"section", "subsection", "schedule", "act", "chapter", "paragraph", "para"}


def is_true_heading_uk(num: str, title: str) -> bool:
    """Determine if a numbered section is a true heading vs. regular paragraph (UK)."""
    t = title.strip()
    if len(t.split()) > 10:
        return False
    if any(w in t.lower() for w in UK_BAD_HEADING_WORDS):
        return False
    if t.endswith(",") or t.endswith(";"):
        return False
    return True


def rewrite_core_uk(core_flat: str) -> str:
    """Restructure core text for UK judgments."""
    s = core_flat

    def heading_sub(m):
        num = m.group(1)
        title = m.group(2).strip()
        if is_true_heading_uk(num, title):
            return f"\n\n({num}) {title}\n\n"
        return m.group(0)

    s = UK_HEADING_RE.sub(heading_sub, s)
    s = re.sub(r"\s+(?=(\d{1,3})\.\s+(?=[A-Z\"(]))", "\n\n", s)
    s = re.sub(r"(?<=[^\d\s])(?=(\d{1,3})\.\s+(?=[A-Z\"(]))", "\n\n", s)
    s = UK_SUBPARA_RE.sub(lambda m: f"\n({m.group(1)}) ", s)
    s = re.sub(r"[ \t]+", " ", s)
    s = re.sub(r"\n{3,}", "\n\n", s).strip()
    return s


# ============================================================================
# JURISDICTION: SINGAPORE (SGCA, SGHC)
# ============================================================================

SG_CITATION_PATTERN = re.compile(r"\[\d{4}\]\s+SG(?:CA|HC)\s+\d+")

# Known Singapore judge name mappings
SG_JUDGE_MAPPINGS = {
    'Andrew': 'Andrew Phang Boon Leong JA',
    'Chan': 'Chan Sek Keong CJ',
    'Chao': 'Chao Hick Tin JA',
    'Tay': 'Tay Yong Kwang JCA',
    'Judith': 'Judith Prakash JCA',
    'Sundaresh': 'Sundaresh Menon CJ',
    'V K': 'V K Rajah JA',
    'Steven': 'Steven Chong JCA',
    'Woo': 'Woo Bih Li J',
    'Belinda': 'Belinda Ang Saw Ean JCA',
    'Quentin': 'Quentin Loh J',
}

SG_JUDGE_NAMES = [
    'Chan Sek Keong', 'Andrew Phang', 'Chao Hick Tin', 'Tay Yong Kwang',
    'Judith Prakash', 'Sundaresh Menon', 'Steven Chong', 'Belinda Ang',
    'Quentin Loh', 'Woo Bih Li', 'V K Rajah', 'Lee Seiu Kin'
]

SG_JUDGE_TITLES = ['CJ', 'JCA', 'JA', 'JAD', 'J', 'SJ']


def fix_sg_judge_name_splits(content: str) -> str:
    """Fix Singapore judge names that get split across lines."""
    # Fix V K Rajah specifically
    content = re.sub(r'\bV\s+K\s*\n+\s*Rajah\b', 'V K Rajah', content)
    content = re.sub(r'\bV\s*\n+\s*K\s+Rajah\b', 'V K Rajah', content)
    content = re.sub(r'\bV\s+K\s+Rajah\s*\n+\s*(JA|JAD|J)\b', r'V K Rajah \1', content)

    # General fix for common judge names split at title
    for name in SG_JUDGE_NAMES:
        for title in SG_JUDGE_TITLES:
            pattern = rf'\b{re.escape(name)}\s*\n+\s*{title}\b'
            content = re.sub(pattern, f'{name} {title}', content)
    return content


def fix_truncated_judge_names(content: str) -> str:
    """Fix truncated judge names at CORE JUDGMENT start."""
    for short_name, full_name in SG_JUDGE_MAPPINGS.items():
        pattern = rf'(CORE JUDGMENT\s*\n-+\n\s*\n){short_name}\s+(1\.|Introduction|Background)'
        replacement = rf'\g<1>{full_name} (delivering the judgment of the court):\n\n\2'
        content = re.sub(pattern, replacement, content, flags=re.IGNORECASE)

        pattern2 = rf'(CORE JUDGMENT\s*\n-+\n\s*\n)({short_name})\s+(\d{{1,2}})\.\s+([A-Z])'
        if re.search(pattern2, content):
            content = re.sub(pattern2, rf'\g<1>{full_name} (delivering the judgment of the court):\n\n\3. \4', content)
    return content


def ensure_judge_attribution(content: str) -> str:
    """Ensure proper judge attribution at CORE JUDGMENT start."""
    # Move judge line after divider if it appears before
    judge_before_pattern = re.search(
        r'([A-Z][a-z]+(?:\s+[A-Z][a-z]+)*\s+(?:CJ|JCA|JA|JAD|J|SJ)\s*\([^)]*delivering[^)]*\):?)\s*\n*-+\s*\n*CORE JUDGMENT\s*\n*-+',
        content
    )
    if judge_before_pattern:
        judge_line = judge_before_pattern.group(1).strip()
        if not judge_line.endswith(':'):
            judge_line += ':'
        content = re.sub(
            r'[A-Z][a-z]+(?:\s+[A-Z][a-z]+)*\s+(?:CJ|JCA|JA|JAD|J|SJ)\s*\([^)]*delivering[^)]*\):?\s*\n*(-+\s*\n*CORE JUDGMENT\s*\n*-+)',
            r'\1\n\n' + judge_line,
            content
        )

    # Add attribution if CORE JUDGMENT starts with paragraph number
    match = re.search(r'(CORE JUDGMENT\s*\n-+\n)\s*(\d+)\.\s+([A-Z])', content)
    if match:
        after_divider = content[match.end()-1:match.end()+200]
        if 'delivering' not in after_divider.lower():
            coram_match = re.search(r'Coram\s*:\s*([^\n]+)', content)
            if coram_match:
                judges = coram_match.group(1).strip()
            else:
                header_match = re.search(r'Judges?\s*:\s*([^\n]+)', content)
                judges = header_match.group(1).strip() if header_match else None

            if judges:
                first_judge = judges.split(';')[0].strip()
                first_judge = first_judge.split(',')[0].strip()
                para_num = match.group(2)
                first_char = match.group(3)
                new_start = f"{match.group(1)}\n{first_judge} (delivering the judgment of the court):\n\n{para_num}. {first_char}"
                content = content[:match.start()] + new_start + content[match.end():]
    return content


def remove_case_citation_from_core(content: str) -> str:
    """Remove case citation (header/footer artifact) from CORE JUDGMENT section."""
    case_match = re.search(r'CASE:\s*([^\n]+)', content)
    if not case_match:
        return content

    case_name = case_match.group(1).strip()
    cite_match = re.search(r'\[(\d{4})\]\s*SG(?:CA|HC)\s*(\d+)', case_name)
    if not cite_match:
        return content

    year = cite_match.group(1)
    num = cite_match.group(2)

    core_idx = content.find('CORE JUDGMENT')
    if core_idx < 0:
        return content

    before_core = content[:core_idx]
    core_content = content[core_idx:]

    case_name_parts = re.sub(r'\s*\[\d{4}\]\s*SG(?:CA|HC)\s*\d+\s*', '', case_name).strip()

    patterns = [
        rf'{re.escape(case_name_parts)}\s*\[{year}\]\s*SG(?:CA|HC)\s*{num}\.',
        rf'{re.escape(case_name_parts)}\s*\[{year}\]\s*SG(?:CA|HC)\s*{num}(?=[\s\n])',
        rf'\n\s*\[{year}\]\s*SG(?:CA|HC)\s*{num}\.?\s*\n',
    ]

    for pattern in patterns:
        matches = list(re.finditer(pattern, core_content))
        for match in reversed(matches):
            start = max(0, match.start() - 50)
            before_text = core_content[start:match.start()].lower()
            reference_indicators = [' in [', ' see [', ' at [', 'cited in', 'reported at',
                                    'decision in', 'judgment in', 'case of', 'appeal from',
                                    'reported in', 'affirmed in', 'overruled in']
            if any(word in before_text for word in reference_indicators):
                continue

            end_pos = match.end()
            after_text = core_content[end_pos:end_pos + 50].strip()

            if after_text and not after_text.startswith('\n'):
                core_content = core_content[:match.start()] + ' ' + core_content[match.end():]
            else:
                core_content = core_content[:match.start()] + core_content[match.end():]

    core_content = re.sub(r'  +', ' ', core_content)
    core_content = re.sub(r'\n{3,}', '\n\n', core_content)
    return before_core + core_content


def _extract_sghc_citation_info(content: str):
    """
    Extract the case's SGHC/SGDC/SGMC citation number from the header area.
    Returns (year, court_code, citation_number) or None.
    """
    header = content[:800]
    m = re.search(r'\[(\d{4})\]\s*(SG(?:HC|DC|MC))\s+(\d+)', header)
    if m:
        return m.group(1), m.group(2), m.group(3)
    return None


def remove_sghc_page_headers(content: str, citation_pattern: str = r'\[\d{4}\]\s*SGHC\s*\d+') -> str:
    """
    Remove SGHC/SGDC/SGMC page headers that appear as:
      Line N:   <case short name>     (e.g., "Adri Satryawan Pratama v PP")
      Line N+1: [YYYY] SGHC NNN<page digits><continuation text>

    The citation line has the citation number with page number digits
    fused on, followed by the continuation of the paragraph text.
    We remove the case name line, strip the citation+page prefix from
    the next line, and keep only the continuation text.

    Falls back to the simpler pattern matching when no citation info
    can be extracted from the document header.
    """
    # Try dynamic extraction first (handles fused page numbers)
    info = _extract_sghc_citation_info(content)
    if info:
        year, court_code, cit_num = info
        lines = content.split('\n')
        result = []
        i = 0

        while i < len(lines):
            # Skip near the document start (don't touch headnotes header)
            if i < 15:
                result.append(lines[i])
                i += 1
                continue

            line = lines[i].strip()

            # Check if next line starts with our citation pattern with fused page number
            if i + 1 < len(lines):
                next_line = lines[i + 1].strip()
                # Match: [YYYY] SGxx NNN followed by more digits (page num) then content
                cite_pattern = re.match(
                    rf'^\[{year}\]\s*{court_code}\s*{cit_num}(\d{{1,4}})\s*(.*)',
                    next_line
                )

                if cite_pattern:
                    # Current line should be a case name (short, contains "v", no paragraph number)
                    is_case_name = (
                        re.search(r'\bv\b', line, re.IGNORECASE)
                        and not re.match(r'^\d+\.', line)
                        and len(line) < 120
                        and not re.search(r'[.;:,]$', line)
                        and not re.match(r'^\(', line)
                    )

                    if is_case_name:
                        continuation = cite_pattern.group(2).strip()

                        if continuation:
                            if result and result[-1].strip():
                                prev = result[-1].rstrip()
                                if re.search(r'[a-zA-Z0-9,;:\-\(\"\'\u2018\u201c]$', prev):
                                    result[-1] = prev + ' ' + continuation
                                else:
                                    result.append(continuation)
                            else:
                                result.append(continuation)

                        i += 2
                        continue

            # Also handle standalone citation lines (no case name line before)
            standalone_cite = re.match(
                rf'^\[{year}\]\s*{court_code}\s*{cit_num}(\d{{1,4}})\s*(.*)',
                line
            )
            if standalone_cite and i > 15:
                prev_line = lines[i - 1].strip() if i > 0 else ''
                if not (re.search(r'\bv\b', prev_line, re.IGNORECASE) and len(prev_line) < 120):
                    continuation = standalone_cite.group(2).strip()
                    if continuation:
                        if result and result[-1].strip():
                            prev = result[-1].rstrip()
                            if re.search(r'[a-zA-Z0-9,;:\-\(\"\'\u2018\u201c]$', prev):
                                result[-1] = prev + ' ' + continuation
                            else:
                                result.append(continuation)
                        else:
                            result.append(continuation)
                    i += 1
                    continue

            # Fallback: simple exact-citation-line removal (original logic)
            if re.match(rf'^{citation_pattern}\.?$', line):
                if i > 10:
                    prev_blank = (i == 0 or not lines[i - 1].strip())
                    next_blank = (i + 1 >= len(lines) or not lines[i + 1].strip())
                    if not prev_blank and not next_blank:
                        if i + 1 < len(lines) and not re.match(r'^\s*at\s+\[', lines[i + 1]):
                            i += 1
                            continue
                    elif prev_blank or next_blank:
                        i += 1
                        continue

            result.append(lines[i])
            i += 1

        return '\n'.join(result)

    # No citation info found -- use original simple logic
    lines = content.split('\n')
    filtered = []
    i = 0

    while i < len(lines):
        line = lines[i].strip()

        # Check if this line is a case name followed by a citation on next line
        if i + 1 < len(lines):
            next_line = lines[i + 1].strip()
            if re.match(rf'^{citation_pattern}\.?$', next_line):
                if re.search(r'\bv\b', line, re.IGNORECASE) and not re.search(r'^\d+\.', line):
                    if len(line) < 100 and not re.search(r'[.;:]$', line):
                        i += 2
                        continue

        # Check if this line alone is a standalone citation (page header)
        if re.match(rf'^{citation_pattern}\.?$', line):
            if i > 10:
                prev_blank = (i == 0 or not lines[i - 1].strip())
                next_blank = (i + 1 >= len(lines) or not lines[i + 1].strip())
                if not prev_blank and not next_blank:
                    if i + 1 < len(lines) and not re.match(r'^\s*at\s+\[', lines[i + 1]):
                        i += 1
                        continue
                elif prev_blank or next_blank:
                    i += 1
                    continue

        filtered.append(lines[i])
        i += 1

    return '\n'.join(filtered)


def remove_inline_footnotes(content: str) -> str:
    """
    Remove inline footnote numbers that appear after punctuation.
    E.g., "June 2022.16" -> "June 2022."
          "the contract.20" -> "the contract."
    These are superscript footnote markers from PDF extraction.
    """
    # Pattern: period/comma followed by 1-2 digit number at word boundary
    # Be careful not to remove year numbers like "2022" or citation numbers

    # Remove footnote numbers after periods (most common)
    # But not if followed by more digits (like "10.5" or dates)
    content = re.sub(r'\.(\d{1,2})(?=[\s\n]|$)', '.', content)

    # Remove footnote numbers after closing parenthesis
    content = re.sub(r'\)(\d{1,2})(?=[\s\n]|$)', ')', content)

    # Remove footnote numbers after closing bracket
    content = re.sub(r'\](\d{1,2})(?=[\s\n.,;:])', ']', content)

    # Remove footnote numbers after quotation marks
    content = re.sub(r'"(\d{1,2})(?=[\s\n]|$)', '"', content)

    # Remove footnote references in format like "text24" where it's clearly a superscript
    # This is more aggressive - only use for specific patterns
    content = re.sub(r'([a-z])(\d{1,2})(\s+[A-Z])', r'\1\3', content)

    return content


def remove_orphaned_footnote_markers(content: str) -> str:
    """
    Remove orphaned footnote markers that appear as standalone lines.
    E.g., lines containing just "[1]", "[2]", "[3]" etc.
    These break content flow and should be removed.
    """
    lines = content.split('\n')
    filtered = []

    for line in lines:
        stripped = line.strip()
        # Skip lines that are just footnote markers like [1], [2], [3]
        if re.match(r'^\[\d{1,3}\]$', stripped):
            continue
        # Also skip lines with multiple consecutive markers like "[1] [2] [3]"
        if re.match(r'^(\[\d{1,3}\]\s*)+$', stripped):
            continue
        filtered.append(line)

    return '\n'.join(filtered)


def remove_merged_footnotes(content: str) -> str:
    """
    Remove superscript footnote numbers fused after punctuation.
    E.g., "the contract.20" -> "the contract."
          "June 2022.16" -> "June 2022."

    Conservative: only strip 1-3 digit numbers after sentence-ending
    punctuation when followed by whitespace/newline/EOF.
    Do NOT strip if it looks like a decimal (e.g., "10.5") or section ref.
    """
    # Pattern: lowercase letter + period + 1-3 digits at word boundary
    # This catches "contract.20", "claim.5" etc.
    content = re.sub(r'([a-z])\.(\d{1,3})(?=[\s\n]|$)', r'\1.', content)

    # Pattern: closing parenthesis + digits
    content = re.sub(r'\)(\d{1,3})(?=[\s\n]|$)', ')', content)

    # Pattern: closing bracket + digits
    content = re.sub(r'\](\d{1,3})(?=[\s\n.,;:])', ']', content)

    # Pattern: closing quote + digits
    content = re.sub(r'(["\u201d])(\d{1,3})(?=[\s\n]|$)', r'\1', content)

    # Pattern: lowercase letter + digits + space + uppercase (clearly superscript)
    content = re.sub(r'([a-z])(\d{1,3})(\s+[A-Z])', r'\1\3', content)

    return content


def remove_stray_footnotes(content: str) -> str:
    """
    Remove lines that are clearly footnote content:
    - Evidence references (AEIC, NE, Notes of Evidence, transcript refs)
    - Submission references (AWS, RWS, DCS, PCS, etc.)
    - Record of Appeal references (ROA, AB, BOA)
    - Lines starting with pure footnote number references

    Only removes lines that are short (<200 chars) and look like standalone
    footnote references (not part of main judgment text).
    """
    # Common footnote reference abbreviation patterns
    footnote_abbrevs = (
        r'AEIC|NE|NEs|AWS|RWS|DCS|PCS|DRS|PRS|SOC|DCC|FNBP|BOA|'
        r'PBOD|DBOD|ROA|AB|BA|CB|ACB|RCB|DCB|PCB|'
        r'PBD|DBD|JCB|JAEIC|'
        r'Transcript|Notes?\s+of\s+Evidence'
    )

    footnote_patterns = [
        # Lines starting with abbreviation + "at" + reference
        re.compile(
            rf'^(?:{footnote_abbrevs})\b[^.]*?'
            rf'(?:at\s+(?:pp?\.?\s*\d|para|paras|\[\d|line|pg|page)|'
            rf'dated\s+\d)',
            re.IGNORECASE
        ),
        # Lines that are a full submission citation
        re.compile(
            r'^(?:Appellant|Respondent|Defendant|Plaintiff|Prosecution|Defence|Claimant|'
            r'Applicant|Petitioner|Intervener|1st|2nd|3rd|4th|5th)\S*\s+'
            r'(?:Written\s+)?(?:Submissions?|Skeletal\s+Arguments?|Closing|Reply|Opening)',
            re.IGNORECASE
        ),
        # Lines that are just "NE" or transcript references with page/line numbers
        re.compile(
            rf'^(?:NEs?\s*\(|Notes?\s+of\s+Evidence)',
            re.IGNORECASE
        ),
    ]

    lines = content.split('\n')
    result = []
    in_core = False

    for i, line in enumerate(lines):
        stripped = line.strip()

        if 'CORE JUDGMENT' in stripped:
            in_core = True

        if not in_core or not stripped:
            result.append(line)
            continue

        # Skip lines that are clearly too long to be footnote refs
        if len(stripped) > 200:
            result.append(line)
            continue

        # Check against footnote patterns
        is_footnote = False
        for pat in footnote_patterns:
            if pat.match(stripped):
                is_footnote = True
                break

        # Additional check: lines that are just abbreviation + short reference
        if not is_footnote and len(stripped) < 80:
            if re.match(
                rf'^(?:{footnote_abbrevs})\s+(?:at\s+|of\s+)',
                stripped, re.IGNORECASE
            ):
                is_footnote = True

        # Additional: standalone abbreviation reference lines that end with period
        if not is_footnote and len(stripped) < 120:
            if re.match(
                rf'^(?:\d{{1,4}})?\s*(?:{footnote_abbrevs})\b.*?'
                rf'(?:at\s+(?:pp?\.?\s*\d|para|paras|\[\d|line|pg|page))',
                stripped, re.IGNORECASE
            ):
                is_footnote = True

        if is_footnote:
            continue  # skip this line

        result.append(line)

    return '\n'.join(result)


def ensure_heading_spacing(content: str) -> str:
    """
    Ensure blank lines before and after section headings in CORE JUDGMENT.
    Headings are short lines in Title Case or ALL CAPS without trailing punctuation.
    """
    heading_re = re.compile(
        r'^(?:[A-Z][a-z]+(?:\s+(?:of|the|and|in|on|for|to|at|by|a|an|or|is|as|with|from)\s+)?'
        r'(?:[A-Za-z]+\s*){0,8}'
        r'|[A-Z][A-Z\s]+[A-Z]'
        r'|(?:Issue|Ground|Stage|Phase|Step|Part|Chapter|Section|Annex)\s+\d+'
        r')$'
    )

    lines = content.split('\n')
    result = []
    in_core = False

    for i, line in enumerate(lines):
        stripped = line.strip()

        if 'CORE JUDGMENT' in stripped:
            in_core = True
            result.append(line)
            continue

        if not in_core:
            result.append(line)
            continue

        if not stripped or re.match(r'^[-=]{5,}$', stripped):
            result.append(line)
            continue

        # Check if this is a section heading
        is_heading = (
            3 < len(stripped) < 80 and
            stripped[-1] not in '.,;:!?' and
            not re.match(r'^\d', stripped) and
            not re.match(r'^[\(\[]', stripped) and
            heading_re.match(stripped)
        )

        # Add blank line before heading if previous isn't blank
        if is_heading and result and result[-1].strip():
            result.append('')

        result.append(line)

        # Add blank line after heading if next line exists and isn't blank
        if is_heading:
            if i + 1 < len(lines) and lines[i + 1].strip():
                result.append('')

    text = '\n'.join(result)
    text = re.sub(r'\n{3,}', '\n\n', text)
    return text


def fix_word_concatenation(content: str) -> str:
    """
    Fix words that were concatenated when PDF line breaks were removed.
    E.g., 'claimedinto' -> 'claimed into', 'physicaladdiction' -> 'physical addiction'

    This happens when words at line ends are joined to words at line starts
    without inserting a space.
    """
    # Common short words that often start after concatenation (2-6 chars)
    common_next_words = r'(the|of|in|on|at|to|for|with|by|from|as|or|and|that|this|into|was|is|are|be|been|has|had|have|which|these|those|its|if|but|not|no|so|such|also|only|even|just|more|most|some|any|all|each|both|few|many|much|other|same|own|well|now|then|here|there|when|where|how|why|what|who|whom|whose|an|a|short|long|new|old|first|last|next|high|low|under|over|after|before|during|within|without|between|among|against|through|across|along|around|behind|beyond)'

    # Pattern 1: Common suffixes followed by common short words
    # e.g., "claimedinto" -> "claimed into", "arguingthat" -> "arguing that"
    # Be conservative - only match clear word endings
    content = re.sub(
        rf'([a-z]{{2,}})(ed|ing|ly|al|ous|ive|ful|ment|ness|ion|ble|ant|ent){common_next_words}\b',
        r'\1\2 \3',
        content
    )

    # Pattern 2: Common suffixes followed by word beginning with vowel or common consonant clusters
    # e.g., "previousoperators" -> "previous operators"
    # Only match when followed by 4+ more letters (to avoid false positives)
    content = re.sub(
        r'([a-z]{2,})(ous|ive|ful|ment|ness|ion|ble|ant|ent|ing|ed|ly|al)(a[a-z]{3,}|e[a-z]{3,}|i[a-z]{3,}|o[a-z]{3,}|u[a-z]{3,})',
        r'\1\2 \3',
        content
    )

    # Pattern 3: Word ending in lowercase + uppercase letter (camelCase-like)
    # e.g., "someText" -> "some Text" (but only for clearly merged words)
    content = re.sub(
        r'([a-z]{3,})([A-Z][a-z]{2,})',
        r'\1 \2',
        content
    )

    # Pattern 4: Specific known concatenations
    known_fixes = [
        (r'\bdonot\b', 'do not'),
        (r'\bCoOffenders\b', 'Co-Offenders'),
        (r'\bcooffenders\b', 'co-offenders'),
        (r'\bcareby\b', 'care by'),
        (r'\bprotectMr\b', 'protect Mr'),
        (r'\bprotectMs\b', 'protect Ms'),
        (r'\bprotectMrs\b', 'protect Mrs'),
        (r'\bcannotbe\b', 'cannot be'),
        (r'\bwouldbe\b', 'would be'),
        (r'\bcouldbe\b', 'could be'),
        (r'\bshouldbe\b', 'should be'),
        (r'\bmustbe\b', 'must be'),
        (r'\bwillbe\b', 'will be'),
        (r'\bmaybe\b(?![\s,\.])', 'may be'),  # Avoid "maybe" as standalone word
        (r'\bescrowagreement\b', 'escrow agreement'),
    ]
    for pattern, replacement in known_fixes:
        content = re.sub(pattern, replacement, content, flags=re.IGNORECASE)

    return content


def fix_word_breaks(content: str) -> str:
    """
    Fix words that were broken with spaces inserted mid-word (PDF OCR artifacts).
    E.g., 'cred ibility' -> 'credibility', 'proceed ing' -> 'proceeding'

    This is different from fix_word_concatenation which handles merged words.
    This handles words split by erroneous spaces.
    """
    # Common word-break patterns from PDF extraction
    word_break_fixes = [
        # -ibility/-ability words
        (r'\bcred ibility\b', 'credibility'),
        (r'\bposs ibility\b', 'possibility'),
        (r'\bprob ability\b', 'probability'),
        (r'\bliab ility\b', 'liability'),
        (r'\bsuit ability\b', 'suitability'),
        (r'\bvari ability\b', 'variability'),
        (r'\bavail ability\b', 'availability'),
        (r'\baccept ability\b', 'acceptability'),
        (r'\bcompat ibility\b', 'compatibility'),
        (r'\bfeas ibility\b', 'feasibility'),
        (r'\bvis ibility\b', 'visibility'),
        (r'\bflex ibility\b', 'flexibility'),
        (r'\bstab ility\b', 'stability'),
        (r'\brel iability\b', 'reliability'),

        # -ing words
        (r'\bproceed ing\b', 'proceeding'),
        (r'\bproceed ings\b', 'proceedings'),
        (r'\bunderstand ing\b', 'understanding'),
        (r'\bnotwithstand ing\b', 'notwithstanding'),
        (r'\bwithstand ing\b', 'withstanding'),
        (r'\boutstand ing\b', 'outstanding'),

        # -tion/-sion words
        (r'\beval uation\b', 'evaluation'),
        (r'\bsitu ation\b', 'situation'),
        (r'\bexamin ation\b', 'examination'),
        (r'\bdeterm ination\b', 'determination'),
        (r'\bexplan ation\b', 'explanation'),
        (r'\bappreci ation\b', 'appreciation'),
        (r'\bconsider ation\b', 'consideration'),
        (r'\bprepar ation\b', 'preparation'),
        (r'\bconclus ion\b', 'conclusion'),
        (r'\bdecis ion\b', 'decision'),

        # -ified/-ied/-ifying words
        (r'\bident ified\b', 'identified'),
        (r'\bquant ified\b', 'quantified'),
        (r'\bquant ification\b', 'quantification'),
        (r'\bquant ifying\b', 'quantifying'),
        (r'\bjust ified\b', 'justified'),
        (r'\bjust ification\b', 'justification'),
        (r'\bclass ified\b', 'classified'),
        (r'\bclass ification\b', 'classification'),
        (r'\bspec ified\b', 'specified'),
        (r'\bspec ification\b', 'specification'),
        (r'\bqual ified\b', 'qualified'),
        (r'\bqual ification\b', 'qualification'),
        (r'\bqual ifies\b', 'qualifies'),
        (r'\bqual ify\b', 'qualify'),
        (r'\bsatis fied\b', 'satisfied'),
        (r'\bsatis faction\b', 'satisfaction'),
        (r'\bmodified\b', 'modified'),
        (r'\bverified\b', 'verified'),
        (r'\bverif ication\b', 'verification'),

        # -ally words
        (r'\birration ally\b', 'irrationally'),
        (r'\bessent ially\b', 'essentially'),
        (r'\bsubstant ially\b', 'substantially'),
        (r'\bparticular ly\b', 'particularly'),
        (r'\bspecific ally\b', 'specifically'),
        (r'\bsignific antly\b', 'significantly'),
        (r'\borigin ally\b', 'originally'),
        (r'\badditional ly\b', 'additionally'),
        (r'\bfund amentally\b', 'fundamentally'),

        # -ical words
        (r'\bident ical\b', 'identical'),
        (r'\bpract ical\b', 'practical'),
        (r'\bcrit ical\b', 'critical'),
        (r'\btechn ical\b', 'technical'),
        (r'\bhistor ical\b', 'historical'),
        (r'\bphys ical\b', 'physical'),
        (r'\blogical\b', 'logical'),

        # Other common breaks
        (r'\bdefen dant\b', 'defendant'),
        (r'\bdefen dants\b', 'defendants'),
        (r'\bplaint iff\b', 'plaintiff'),
        (r'\bplaint iffs\b', 'plaintiffs'),
        (r'\bappell ant\b', 'appellant'),
        (r'\bappell ants\b', 'appellants'),
        (r'\brespond ent\b', 'respondent'),
        (r'\brespond ents\b', 'respondents'),
        (r'\bjudg ment\b', 'judgment'),
        (r'\bjudge ment\b', 'judgement'),
        (r'\bagree ment\b', 'agreement'),
        (r'\bstate ment\b', 'statement'),
        (r'\bcommit ment\b', 'commitment'),
        (r'\brequire ment\b', 'requirement'),
        (r'\brequire ments\b', 'requirements'),
        (r'\bdevelop ment\b', 'development'),
        (r'\bgovern ment\b', 'government'),
        (r'\bmanage ment\b', 'management'),
        (r'\bemploy ment\b', 'employment'),
        (r'\benforce ment\b', 'enforcement'),
        (r'\bassess ment\b', 'assessment'),
        (r'\bsettle ment\b', 'settlement'),
        (r'\bestablish ment\b', 'establishment'),

        # Additional common breaks found in SGHC/SGDC files
        (r'\bauthent icity\b', 'authenticity'),
        (r'\bauthent ic\b', 'authentic'),
        (r'\bproced ural\b', 'procedural'),
        (r'\bproced ure\b', 'procedure'),
        (r'\bproced ures\b', 'procedures'),
        (r'\bimmed iately\b', 'immediately'),
        (r'\bimmed iate\b', 'immediate'),
        (r'\breal istic\b', 'realistic'),
        (r'\breal istically\b', 'realistically'),
        (r'\bdisting uish\b', 'distinguish'),
        (r'\bdisting uished\b', 'distinguished'),
        (r'\bdisting uishing\b', 'distinguishing'),
        (r'\banteced ents\b', 'antecedents'),
        (r'\banteced ent\b', 'antecedent'),
        (r'\bdisqual ification\b', 'disqualification'),
        (r'\bdisqual ified\b', 'disqualified'),
        (r'\bintertrochant eric\b', 'intertrochanteric'),
        (r'\bintramed ullary\b', 'intramedullary'),
        (r'\bpreced ents\b', 'precedents'),
        (r'\bpreced ent\b', 'precedent'),
        (r'\bsubsequ ent\b', 'subsequent'),
        (r'\bsubsequ ently\b', 'subsequently'),
        (r'\bconsequ ent\b', 'consequent'),
        (r'\bconsequ ently\b', 'consequently'),
        (r'\bconsequ ences\b', 'consequences'),
        (r'\bconsequ ence\b', 'consequence'),
        (r'\bdelib erately\b', 'deliberately'),
        (r'\bdelib erate\b', 'deliberate'),
        (r'\bsepar ately\b', 'separately'),
        (r'\bsepar ate\b', 'separate'),
        (r'\bsepar ation\b', 'separation'),
        (r'\baccur ately\b', 'accurately'),
        (r'\baccur ate\b', 'accurate'),
        (r'\baccur acy\b', 'accuracy'),
        (r'\bultim ately\b', 'ultimately'),
        (r'\bultim ate\b', 'ultimate'),
        (r'\bapprox imately\b', 'approximately'),
        (r'\bapprox imate\b', 'approximate'),
        (r'\blegit imate\b', 'legitimate'),
        (r'\blegit imately\b', 'legitimately'),
        (r'\bintim ate\b', 'intimate'),
        (r'\bintim ately\b', 'intimately'),
        (r'\bestim ate\b', 'estimate'),
        (r'\bestim ated\b', 'estimated'),
        (r'\bestim ation\b', 'estimation'),

        # -ative words
        (r'\brepresent ative\b', 'representative'),
        (r'\brepresent atives\b', 'representatives'),
        (r'\brepresent ation\b', 'representation'),
        (r'\badministr ative\b', 'administrative'),
        (r'\bquantit ative\b', 'quantitative'),
        (r'\bqualit ative\b', 'qualitative'),
        (r'\bauthorit ative\b', 'authoritative'),

        # -ually words
        (r'\bevent ually\b', 'eventually'),
        (r'\bact ually\b', 'actually'),
        (r'\bcontract ually\b', 'contractually'),
        (r'\bfact ually\b', 'factually'),
        (r'\bmut ually\b', 'mutually'),

        # -ence/-ency words
        (r'\bconting encies\b', 'contingencies'),
        (r'\bconting ency\b', 'contingency'),
        (r'\bdisobed ience\b', 'disobedience'),
        (r'\bobed ience\b', 'obedience'),
        (r'\bconven ience\b', 'convenience'),
        (r'\bexped ience\b', 'expedience'),

        # -able words
        (r'\bobjection able\b', 'objectionable'),
        (r'\breason able\b', 'reasonable'),
        (r'\baction able\b', 'actionable'),
        (r'\bquestion able\b', 'questionable'),
        (r'\bexception able\b', 'exceptionable'),

        # Latin phrases with breaks
        (r'\bint er alia\b', 'inter alia'),
        (r'\binter alia\b', 'inter alia'),
        (r'\bprim a facie\b', 'prima facie'),
        (r'\bult ra vires\b', 'ultra vires'),
        (r'\bpro rata\b', 'pro rata'),

        # -ication words
        (r'\bauthent ication\b', 'authentication'),
        (r'\bauthent icate\b', 'authenticate'),
        (r'\bauthent icated\b', 'authenticated'),
        (r'\bverif ication\b', 'verification'),
        (r'\bcommun ication\b', 'communication'),
        (r'\bcommun icate\b', 'communicate'),
        (r'\bapplic ation\b', 'application'),
        (r'\bimplic ation\b', 'implication'),
        (r'\bpublic ation\b', 'publication'),

        # -ised/-ised words (British spelling)
        (r'\breal ised\b', 'realised'),
        (r'\breal ise\b', 'realise'),
        (r'\breal ises\b', 'realises'),
        (r'\breal ising\b', 'realising'),
        (r'\brecogn ised\b', 'recognised'),
        (r'\brecogn ise\b', 'recognise'),
        (r'\brecogn ises\b', 'recognises'),
        (r'\brecogn ising\b', 'recognising'),
        (r'\bemphas ised\b', 'emphasised'),
        (r'\bemphas ise\b', 'emphasise'),
        (r'\bemphas ises\b', 'emphasises'),
        (r'\bemphas ising\b', 'emphasising'),
        (r'\bsummar ised\b', 'summarised'),
        (r'\bsummar ise\b', 'summarise'),
        (r'\bsummar ises\b', 'summarises'),
        (r'\bsummar ising\b', 'summarising'),
        (r'\bcharacter ised\b', 'characterised'),
        (r'\bcharacter ise\b', 'characterise'),
        (r'\bcharacter ising\b', 'characterising'),
        (r'\butil ised\b', 'utilised'),
        (r'\butil ise\b', 'utilise'),
        (r'\butil ises\b', 'utilises'),
        (r'\butil ising\b', 'utilising'),
        (r'\bminim ised\b', 'minimised'),
        (r'\bminim ise\b', 'minimise'),
        (r'\bminim ising\b', 'minimising'),
        (r'\bmaximised\b', 'maximised'),
        (r'\bmaximise\b', 'maximise'),
        (r'\bmaximising\b', 'maximising'),

        # -ioned/-tion words (additional)
        (r'\baforment ioned\b', 'aforementioned'),
        (r'\baforement ioned\b', 'aforementioned'),
        (r'\bment ioned\b', 'mentioned'),
        (r'\bment ion\b', 'mention'),
        (r'\bment ions\b', 'mentions'),
        (r'\bment ioning\b', 'mentioning'),
        (r'\bmisrepresent ation\b', 'misrepresentation'),
        (r'\bmisrepresent ations\b', 'misrepresentations'),
        (r'\brepresent ations\b', 'representations'),

        # -iated/-iate words
        (r'\bsubstant iated\b', 'substantiated'),
        (r'\bsubstant iate\b', 'substantiate'),
        (r'\bsubstant iates\b', 'substantiates'),
        (r'\bsubstant iating\b', 'substantiating'),
        (r'\bunsubstant iated\b', 'unsubstantiated'),
        (r'\bnegot iated\b', 'negotiated'),
        (r'\bnegot iate\b', 'negotiate'),
        (r'\bnegot iates\b', 'negotiates'),
        (r'\bnegot iating\b', 'negotiating'),
        (r'\bnegot iation\b', 'negotiation'),
        (r'\bnegot iations\b', 'negotiations'),
        (r'\bdifferent iated\b', 'differentiated'),
        (r'\bdifferent iate\b', 'differentiate'),
        (r'\bdifferent iating\b', 'differentiating'),
        (r'\bdifferent iation\b', 'differentiation'),
        (r'\binit iated\b', 'initiated'),
        (r'\binit iate\b', 'initiate'),
        (r'\binit iating\b', 'initiating'),
        (r'\binit iation\b', 'initiation'),
        (r'\binit iative\b', 'initiative'),
        (r'\binit iatives\b', 'initiatives'),
        (r'\binit ial\b', 'initial'),
        (r'\binit ially\b', 'initially'),

        # -ant/-ent words (additional)
        (r'\bexped ient\b', 'expedient'),
        (r'\bexped ients\b', 'expedients'),
        (r'\bexped ition\b', 'expedition'),
        (r'\bexped itions\b', 'expeditions'),
        (r'\bexped itious\b', 'expeditious'),
        (r'\bexped itiously\b', 'expeditiously'),
        (r'\bingred ient\b', 'ingredient'),
        (r'\bingred ients\b', 'ingredients'),
        (r'\bdisobed ient\b', 'disobedient'),
        (r'\bobedient\b', 'obedient'),

        # -age/-aged words
        (r'\badvant age\b', 'advantage'),
        (r'\badvant ages\b', 'advantages'),
        (r'\badvant aged\b', 'advantaged'),
        (r'\badvant ageous\b', 'advantageous'),
        (r'\bdisadvant age\b', 'disadvantage'),
        (r'\bdisadvant ages\b', 'disadvantages'),
        (r'\bdisadvant aged\b', 'disadvantaged'),
        (r'\bdisadvant ageous\b', 'disadvantageous'),
        (r'\bmanage able\b', 'manageable'),

        # -ogue/-ogue words
        (r'\banal ogue\b', 'analogue'),
        (r'\banal ogous\b', 'analogous'),
        (r'\banal ogy\b', 'analogy'),
        (r'\banal ogies\b', 'analogies'),
        (r'\bdial ogue\b', 'dialogue'),
        (r'\bdial ogues\b', 'dialogues'),
        (r'\bcatal ogue\b', 'catalogue'),
        (r'\bcatal ogues\b', 'catalogues'),

        # dis-/ent- prefix words
        (r'\bdisent itle\b', 'disentitle'),
        (r'\bdisent itled\b', 'disentitled'),
        (r'\bdisent itlement\b', 'disentitlement'),
        (r'\bdisent itling\b', 'disentitling'),

        # potent- words
        (r'\bpotent ial\b', 'potential'),
        (r'\bpotent ially\b', 'potentially'),
        (r'\bpotent ials\b', 'potentials'),

        # Other missing patterns found in SGHC files
        (r'\bident ifying\b', 'identifying'),
        (r'\bident ify\b', 'identify'),
        (r'\bident ifies\b', 'identifies'),
        (r'\bident ity\b', 'identity'),
        (r'\bident ities\b', 'identities'),
        (r'\bexecut ion\b', 'execution'),
        (r'\bexecut ive\b', 'executive'),
        (r'\bexecut ed\b', 'executed'),
        (r'\bprosec ution\b', 'prosecution'),
        (r'\bprosec uted\b', 'prosecuted'),
        (r'\bprosec utor\b', 'prosecutor'),
        (r'\bprosec utors\b', 'prosecutors'),
        (r'\bprosec utorial\b', 'prosecutorial'),
        (r'\bconst itution\b', 'constitution'),
        (r'\bconst itutional\b', 'constitutional'),
        (r'\bconst itutionally\b', 'constitutionally'),
        (r'\bconst itute\b', 'constitute'),
        (r'\bconst ituted\b', 'constituted'),
        (r'\bconst itutes\b', 'constitutes'),
        (r'\bconst ituting\b', 'constituting'),
        (r'\binst itution\b', 'institution'),
        (r'\binst itutional\b', 'institutional'),
        (r'\binst itute\b', 'institute'),
        (r'\binst ituted\b', 'instituted'),
        (r'\binst itutes\b', 'institutes'),
        (r'\bsubst itute\b', 'substitute'),
        (r'\bsubst ituted\b', 'substituted'),
        (r'\bsubst itution\b', 'substitution'),
        (r'\brest itution\b', 'restitution'),
        (r'\bdest itute\b', 'destitute'),
        (r'\bcircumst ances\b', 'circumstances'),
        (r'\bcircumst ance\b', 'circumstance'),
        (r'\bcircumst antial\b', 'circumstantial'),
    ]

    for pattern, replacement in word_break_fixes:
        content = re.sub(pattern, replacement, content, flags=re.IGNORECASE)

    # Generic pattern: fix common syllable breaks
    # Pattern: word fragment + space + common suffix
    generic_breaks = [
        (r'(\w{3,})[ ]+(ibility)\b', r'\1\2'),
        (r'(\w{3,})[ ]+(ability)\b', r'\1\2'),
        (r'(\w{3,})[ ]+(tion)\b', r'\1\2'),
        (r'(\w{3,})[ ]+(sion)\b', r'\1\2'),
        (r'(\w{3,})[ ]+(ment)\b', r'\1\2'),
        (r'(\w{3,})[ ]+(ness)\b', r'\1\2'),
        (r'(\w{3,})[ ]+(ally)\b', r'\1\2'),
        (r'(\w{3,})[ ]+(ical)\b', r'\1\2'),
        (r'(\w{3,})[ ]+(ified)\b', r'\1\2'),
        (r'(\w{3,})[ ]+(ings?)\b', r'\1\2'),
        # New generic patterns
        (r'(\w{3,})[ ]+(ative)\b', r'\1\2'),
        (r'(\w{3,})[ ]+(atives)\b', r'\1\2'),
        (r'(\w{3,})[ ]+(ually)\b', r'\1\2'),
        (r'(\w{3,})[ ]+(ence)\b', r'\1\2'),
        (r'(\w{3,})[ ]+(ency)\b', r'\1\2'),
        (r'(\w{3,})[ ]+(encies)\b', r'\1\2'),
        # NOTE: 'able', 'aged', 'ages' handled separately below with exclusion lists
        (r'(\w{3,})[ ]+(ised)\b', r'\1\2'),
        (r'(\w{3,})[ ]+(ises)\b', r'\1\2'),
        (r'(\w{3,})[ ]+(ising)\b', r'\1\2'),
        (r'(\w{3,})[ ]+(ient)\b', r'\1\2'),
        (r'(\w{3,})[ ]+(ients)\b', r'\1\2'),
        (r'(\w{3,})[ ]+(ious)\b', r'\1\2'),
        (r'(\w{3,})[ ]+(iously)\b', r'\1\2'),
        (r'(\w{3,})[ ]+(ogue)\b', r'\1\2'),
        (r'(\w{3,})[ ]+(ogous)\b', r'\1\2'),
        (r'(\w{3,})[ ]+(itled)\b', r'\1\2'),
        (r'(\w{3,})[ ]+(itlement)\b', r'\1\2'),
        (r'(\w{3,})[ ]+(itling)\b', r'\1\2'),
        (r'(\w{3,})[ ]+(ially)\b', r'\1\2'),
        (r'(\w{3,})[ ]+(iated)\b', r'\1\2'),
        (r'(\w{3,})[ ]+(iate)\b', r'\1\2'),
        (r'(\w{3,})[ ]+(iating)\b', r'\1\2'),
        (r'(\w{3,})[ ]+(ution)\b', r'\1\2'),
        (r'(\w{3,})[ ]+(utional)\b', r'\1\2'),
        (r'(\w{3,})[ ]+(uted)\b', r'\1\2'),
        (r'(\w{3,})[ ]+(utes)\b', r'\1\2'),
        (r'(\w{3,})[ ]+(uting)\b', r'\1\2'),
        (r'(\w{3,})[ ]+(ioned)\b', r'\1\2'),
        # Multi-space Latin phrase patterns
        (r'\bint\s+er\s+alia\b', 'inter alia'),
    ]

    for pattern, replacement in generic_breaks:
        content = re.sub(pattern, replacement, content, flags=re.IGNORECASE)

    # Exclusion-aware suffix merging for 'able', 'aged', 'ages'.
    # These suffixes are standalone English words, so "been able" should NOT
    # become "beenable".  Only merge when the preceding fragment is NOT a
    # common standalone word.
    _EXCLUDE_BEFORE_ABLE = {
        'been', 'was', 'were', 'not', 'now', 'his', 'her', 'its', 'the',
        'who', 'how', 'may', 'can', 'all', 'far', 'yet', 'too', 'had',
        'has', 'nor', 'did', 'got', 'but', 'and', 'for', 'per', 'let',
        'set', 'put', 'run', 'cut', 'sat', 'met', 'led', 'won', 'own',
        'due', 'any', 'few', 'one', 'two', 'six', 'ten', 'our', 'more',
        'less', 'also', 'very', 'even', 'just', 'most', 'much', 'best',
        'only', 'well', 'them', 'made', 'seem', 'felt', 'such', 'once',
        'some', 'long', 'ever', 'still', 'half', 'both', 'sure', 'full',
        'hold', 'come', 'find', 'know', 'take', 'give', 'make', 'bear',
        'longer', 'never', 'always', 'whether', 'neither', 'either',
        'rather', 'hardly', 'scarcely', 'barely',
    }
    _EXCLUDE_BEFORE_AGED = {
        'was', 'now', 'who', 'his', 'her', 'not', 'all', 'and', 'the',
        'both', 'once', 'then', 'been', 'were', 'those', 'indeed',
        'persons', 'children', 'people', 'some', 'many', 'most', 'also',
        'ever', 'still', 'they', 'when', 'only', 'each', 'over', 'under',
        'boys', 'girls', 'women', 'being', 'aged',
    }
    _EXCLUDE_BEFORE_AGES = {
        'all', 'the', 'for', 'two', 'ten', 'six', 'his', 'her', 'its',
        'our', 'few', 'old', 'new', 'any', 'both', 'some', 'many', 'most',
        'such', 'over', 'dark', 'middle', 'young', 'from', 'through',
    }

    def _safe_merge(match, exclude_set):
        word = match.group(1)
        if word.lower() in exclude_set:
            return match.group(0)  # don't merge, keep original
        return match.group(1) + match.group(2)

    content = re.sub(
        r'(\w{3,})[ ]+(able)\b',
        lambda m: _safe_merge(m, _EXCLUDE_BEFORE_ABLE),
        content, flags=re.IGNORECASE
    )
    content = re.sub(
        r'(\w{3,})[ ]+(aged)\b',
        lambda m: _safe_merge(m, _EXCLUDE_BEFORE_AGED),
        content, flags=re.IGNORECASE
    )
    content = re.sub(
        r'(\w{3,})[ ]+(ages)\b',
        lambda m: _safe_merge(m, _EXCLUDE_BEFORE_AGES),
        content, flags=re.IGNORECASE
    )

    return content


def fix_date_line_breaks(content: str) -> str:
    """
    Fix dates that are split across lines.
    E.g., "15 April\n2007" -> "15 April 2007"
    Also: "April\n2007" -> "April 2007"
    And with blank lines: "15 April\n\n2007" -> "15 April 2007"
    """
    months = 'January|February|March|April|May|June|July|August|September|October|November|December'

    # Pattern 1: "day Month\nyear" (with optional blank line)
    content = re.sub(
        rf'(\d{{1,2}}\s+(?:{months}))\s*\n\s*\n?\s*(\d{{4}})',
        r'\1 \2',
        content,
        flags=re.IGNORECASE
    )

    # Pattern 2: "Month\nyear" (month at end of line, year on next)
    content = re.sub(
        rf'((?:{months}))\s*\n\s*\n?\s*(\d{{4}})',
        r'\1 \2',
        content,
        flags=re.IGNORECASE
    )

    # Pattern 3: "year\nday Month" (year at end of line, date on next) - less common
    # Only join if the year clearly ends a date context
    content = re.sub(
        rf'(\d{{4}})\s*\n\s*(\d{{1,2}}\s+(?:{months}))',
        r'\1\n\2',
        content,
        flags=re.IGNORECASE
    )

    return content


def fix_mid_sentence_line_breaks(content: str) -> str:
    """
    Fix lines that break mid-sentence before capitalized words.

    Handles cases like:
    - "...plaintiff and Mr\\nTejinder" -> joined (title at end)
    - "...obtained from the Land\\nDealings Approval Unit" -> joined (article near end)
    - "...Mr Tejinder Singh\\n\\nSekhon" -> joined (name split across blank line)
    - "...On\\n5 April 2007" -> joined (preposition at end)
    - "...the 2nd plaintiff told\\nNg that" -> joined (no sentence-ending punct)

    Only joins when it's clearly mid-sentence.
    """
    lines = content.split('\n')
    result = []
    i = 0

    # Titles that when at end of line MUST continue to next line
    titles = {'Mr', 'Mrs', 'Ms', 'Dr', 'Prof', 'Rev', 'Sir', 'Dame', 'Lord', 'Lady',
              'Justice', 'Judge', 'Chief'}

    # Words that can never end a sentence
    must_continue = {
        'the', 'a', 'an', 'of', 'in', 'to', 'for', 'with', 'by', 'as', 'at',
        'on', 'from', 'into', 'upon', 'under', 'over', 'between', 'through',
        'during', 'before', 'after', 'about', 'against', 'without', 'within',
        'and', 'or', 'but', 'nor', 'that', 'which', 'who', 'whom', 'whose',
        'where', 'when', 'while', 'whether', 'if', 'unless', 'although',
        'because', 'since', 'so', 'yet', 'both', 'either', 'neither',
        'not', 'also', 'only', 'than', 'such', 'this', 'these', 'those',
        'its', 'his', 'her', 'their', 'our', 'my', 'your',
    }

    while i < len(lines):
        line = lines[i].rstrip()

        # Skip empty lines, headers, section dividers
        if not line.strip() or re.match(r'^[-=]{5,}', line) or \
           line.strip() in ('HEADNOTES', 'CORE JUDGMENT'):
            result.append(line)
            i += 1
            continue

        stripped = line.strip()
        words = stripped.split()
        if not words:
            result.append(line)
            i += 1
            continue

        last_word = words[-1]
        last_word_clean = last_word.rstrip('.,;:!?')
        last_word_lower = last_word_clean.lower()
        second_last_lower = words[-2].lower().rstrip('.,;:!?') if len(words) >= 2 else ''

        # Determine if this line MUST continue
        should_join = False
        max_blank_lines = 0  # how many blank lines we'll tolerate

        # Rule 1: Line ends with title (Mr, Mrs, etc.) - always join, even across blank line
        if last_word_clean in titles or last_word.rstrip('.') in titles:
            should_join = True
            max_blank_lines = 1

        # Rule 2: Line ends with must-continue word (preposition, article, etc.)
        if last_word_lower in must_continue:
            should_join = True
            max_blank_lines = 1

        # Rule 3: Second-to-last word is must-continue and last word is capitalized
        # E.g., "from the Land\nDealings" - "the" is 2nd-to-last, "Land" is last
        if second_last_lower in must_continue and last_word_clean[0:1].isupper() and \
           not stripped.endswith(('.', '!', '?', ':', ';')):
            should_join = True
            max_blank_lines = 1

        # Rule 4: Line ends with comma
        if stripped.endswith(','):
            should_join = True
            max_blank_lines = 1

        # Rule 5: Line doesn't end with sentence punctuation and next line
        # continues mid-sentence (starts lowercase or with opening paren/quote)
        if not stripped[-1] in '.!?:;' and not should_join:
            # Look ahead for lowercase continuation
            j = i + 1
            blank_count = 0
            while j < len(lines) and not lines[j].strip():
                blank_count += 1
                j += 1
            if j < len(lines) and blank_count <= 1:
                next_content = lines[j].strip()
                if next_content and (next_content[0].islower() or
                                     next_content[0] in '("\''):
                    should_join = True
                    max_blank_lines = blank_count

        # Rule 6: Line doesn't end with sentence punctuation and next line
        # starts with a name followed by parenthetical or comma (likely name continuation)
        # E.g., "Mr Tejinder Singh\n\nSekhon ("Mr Tejinder")"
        if not stripped[-1] in '.!?:;' and not should_join:
            j = i + 1
            blank_count = 0
            while j < len(lines) and not lines[j].strip():
                blank_count += 1
                j += 1
            if j < len(lines) and blank_count <= 1:
                next_content = lines[j].strip()
                # Check if next line starts with Name followed by ( or ,
                if next_content and re.match(r'^[A-Z][a-z]+\s*[\(\[,]', next_content):
                    should_join = True
                    max_blank_lines = blank_count

        # Rule 7: PDF column-width heuristic - if a line is long (>70 chars),
        # doesn't end with sentence punctuation, and next line is also substantial,
        # it's likely a column-boundary break. Join if next line doesn't start
        # with a paragraph number or section header.
        if not should_join and len(stripped) >= 70 and \
           not stripped[-1] in '.!?:;"\u201d)':
            j = i + 1
            blank_count = 0
            while j < len(lines) and not lines[j].strip():
                blank_count += 1
                j += 1
            if j < len(lines) and blank_count == 0:
                next_content = lines[j].strip()
                if next_content and len(next_content) > 10 and \
                   not re.match(r'^\d{1,3}\.\s+[A-Z]', next_content) and \
                   not re.match(r'^[\(\[]', next_content) and \
                   not re.match(r'^[-=]{5,}', next_content) and \
                   next_content not in ('HEADNOTES', 'CORE JUDGMENT'):
                    should_join = True
                    max_blank_lines = 0

        if should_join:
            # Look ahead: skip blank lines and find next content line
            j = i + 1
            blank_count = 0
            while j < len(lines) and not lines[j].strip():
                blank_count += 1
                j += 1

            if j < len(lines) and blank_count <= max_blank_lines:
                next_content = lines[j].strip()
                if next_content and not re.match(r'^\d{1,3}\.\s+[A-Z]', next_content) and \
                   not re.match(r'^[-=]{5,}', next_content) and \
                   next_content not in ('HEADNOTES', 'CORE JUDGMENT'):
                    # Join the lines
                    joined = line.rstrip() + ' ' + next_content
                    lines[j] = joined
                    i = j
                    continue

        result.append(line)
        i += 1

    return '\n'.join(result)


def remove_source_database_boilerplate(content: str) -> str:
    """
    Remove source database navigation boilerplate from SGDC/SGMC files.

    This removes navigation header boilerplate that appears between the case
    title and the actual metadata (Case Number, Suit No, Decision Date, etc.).
    """
    # Pattern to match the source database boilerplate block
    # It starts with case title repeated and ends before Case Number/Suit No

    # Find the HEADNOTES section
    headnotes_match = re.search(r'-{10,}\s*\nHEADNOTES\s*\n-{10,}\s*\n', content)
    if not headnotes_match:
        return content

    headnotes_start = headnotes_match.end()

    # Find where the actual metadata starts (Case Number, Suit No, Decision Date, etc.)
    metadata_patterns = [
        r'\nCase Number:',
        r'\nSuit No:',
        r'\nDecision Date:',
        r'\nTribunal/Court:',
        r'\nCoram:',
        r'\n[A-Z][a-z]+ v [A-Z]',  # Party name pattern (e.g., "Public Prosecutor v Ashwin")
    ]

    # Find the earliest metadata marker after headnotes
    metadata_start = None
    for pattern in metadata_patterns:
        match = re.search(pattern, content[headnotes_start:])
        if match:
            pos = headnotes_start + match.start()
            # Find the start of this line (where actual content begins)
            # Look for a clean party name line before this
            if metadata_start is None or pos < metadata_start:
                metadata_start = pos

    if metadata_start is None:
        return content

    # Extract the boilerplate section
    boilerplate_section = content[headnotes_start:metadata_start]

    # Check if it contains source database markers
    source_db_markers = [
        'Databases',
        'You are here:',
        'Database Search',
        'Name Search',
        'Recent Decisions',
        'District Court of Singapore',
        'Magistrate',
    ]

    has_boilerplate = any(marker in boilerplate_section for marker in source_db_markers)

    if has_boilerplate:
        # Remove the boilerplate section
        # Keep the content before headnotes_start and after metadata_start
        content = content[:headnotes_start] + '\n' + content[metadata_start:].lstrip()

    return content


def reflow_broken_sentences(content: str) -> str:
    """
    Reflow sentences that are broken across lines inappropriately.

    Joins lines where:
    - Line ends without sentence-ending punctuation
    - Next line starts with lowercase or continues a sentence

    This fixes PDF extraction artifacts where paragraphs are split at column
    or page boundaries.
    """
    lines = content.split('\n')
    result = []
    i = 0

    while i < len(lines):
        line = lines[i].rstrip()

        # Skip empty lines
        if not line.strip():
            result.append(line)
            i += 1
            continue

        # Skip section headers and metadata
        if re.match(r'^-{5,}', line) or re.match(r'^={5,}', line):
            result.append(line)
            i += 1
            continue

        # Skip lines that are clearly complete (end with sentence punctuation)
        if line.rstrip().endswith(('.', '!', '?', ':', '"', '"', ')')) and \
           i + 1 < len(lines) and lines[i + 1].strip():
            next_line = lines[i + 1].strip()
            # If next line starts with capital or number, this line is complete
            if re.match(r'^[A-Z0-9\[\("]', next_line):
                result.append(line)
                i += 1
                continue

        # Check if line should be joined with next
        if i + 1 < len(lines):
            next_line = lines[i + 1].strip()

            # Join conditions:
            # 1. Current line doesn't end with sentence punctuation
            # 2. Next line starts with lowercase
            # 3. Current line ends mid-word (ends with letter, next starts with letter)

            should_join = False

            # Condition 1: Line ends without sentence punctuation and next starts lowercase
            if line.strip() and not line.rstrip()[-1] in '.!?:;' and \
               next_line and next_line[0].islower():
                should_join = True

            # Condition 2: Line ends with comma or conjunction, next starts lowercase
            if line.rstrip().endswith(',') and next_line and next_line[0].islower():
                should_join = True

            # Condition 3: Line ends with "and", "or", "the", "a", "of", etc.
            trailing_words = ['and', 'or', 'the', 'a', 'an', 'of', 'in', 'to', 'for', 'with', 'by', 'as']
            for word in trailing_words:
                if line.rstrip().lower().endswith(' ' + word):
                    should_join = True
                    break

            if should_join:
                # Join with next line
                lines[i + 1] = line.rstrip() + ' ' + next_line
                i += 1
                continue

        result.append(line)
        i += 1

    return '\n'.join(result)


def remove_sghc_editorial_notice(content: str) -> str:
    """Remove the standard SGHC editorial notice at the start of judgments."""
    notice_patterns = [
        r'This judgment is subject to final editorial corrections.*?(?:the Singapore Law\s*Reports\.)',
        r'This judgment is subject to final editorial corrections approved by the\s*court.*?for publication.*?Singapore Law\s*Reports\.',
    ]
    for pattern in notice_patterns:
        content = re.sub(pattern, '', content, flags=re.DOTALL | re.IGNORECASE)
    return content


def fix_sghc_date_formatting(content: str) -> str:
    """
    Fix SGHC-specific date formatting issues.
    E.g., "12. December 202413 January 2025" -> separate hearing and judgment dates
    """
    months = 'January|February|March|April|May|June|July|August|September|October|November|December'

    # Fix dates with period after day number
    content = re.sub(rf'(\d{{1,2}})\.\s*({months})\s+(\d{{4}})',
                     r'\1 \2 \3', content, flags=re.IGNORECASE)

    # Fix merged dates like "202413" which should be "2024\n13"
    # This happens when hearing date year runs into judgment day
    content = re.sub(rf'(\d{{4}})(\d{{1,2}})\s+({months})\s+(\d{{4}})',
                     r'\1\n\2 \3 \4', content, flags=re.IGNORECASE)

    return content


def remove_sg_copyright_notice(content: str) -> str:
    """
    Remove Singapore copyright notice and everything after it.
    E.g., "Copyright © Government of Singapore." and all following content.
    """
    # Pattern matches various forms of the copyright notice
    patterns = [
        r'\s*Copyright\s*©\s*Government\s+of\s+Singapore\.?\s*.*$',
        r'\s*Copyright\s+©\s+Government\s+of\s+Singapore\.?\s*.*$',
    ]
    for pattern in patterns:
        content = re.sub(pattern, '', content, flags=re.DOTALL | re.IGNORECASE)
    return content.rstrip()


def remove_sg_footnotes_section(content: str) -> str:
    """
    Remove footnotes section at end of Singapore judgments.
    Footnotes appear as numbered references like [1], [2], [3] followed by text.
    They typically appear after the main judgment text.
    """
    # Find if there's a block of footnotes at the end
    # Pattern: multiple lines starting with [number]
    lines = content.split('\n')

    # Find where consecutive footnote lines start
    footnote_start = None
    consecutive_footnotes = 0

    for i in range(len(lines) - 1, -1, -1):
        line = lines[i].strip()
        # Check if line starts with [number] or is a continuation
        if re.match(r'^\[\d+\]', line):
            consecutive_footnotes += 1
            footnote_start = i
        elif line and consecutive_footnotes > 0:
            # If we hit non-footnote content, check if we have enough footnotes
            if consecutive_footnotes >= 2:
                break
            else:
                # Reset - this wasn't a footnote section
                consecutive_footnotes = 0
                footnote_start = None

    # If we found a footnotes section, remove it
    if footnote_start is not None and consecutive_footnotes >= 2:
        # Also remove any "See generally..." type references before the footnotes
        # Look back a few lines for reference text
        start_removal = footnote_start
        for i in range(max(0, footnote_start - 5), footnote_start):
            line = lines[i].strip()
            if re.match(r'^See\s+', line, re.IGNORECASE) or \
               re.match(r'^At\s+para', line, re.IGNORECASE) or \
               re.match(r'^\[\d+\]', line):
                start_removal = i
                break

        content = '\n'.join(lines[:start_removal])

    return content.rstrip()


def segment_head_core_sghc(full_text: str) -> Tuple[str, str]:
    """
    SGHC-specific segmentation: Split text into headnotes and core judgment.

    For SGHC judgments, the core begins AFTER the date and just BEFORE the judge name.
    The pattern is typically:
        [date] [Judge Name] JC/J/JA: [Introduction/content]

    The headnotes should contain case metadata up to but NOT including the judge's speech.
    The core should begin with the judge's name delivering the judgment.
    """
    # First, find the judge's name pattern that starts the judgment
    # Common patterns: "Andrew Phang Boon Leong JC:", "Sundaresh Menon CJ:"
    judge_pattern = re.compile(
        r'\b([A-Z][a-z]+(?:\s+[A-Z][a-z]+){1,5}\s+(?:CJ|JCA|JA|JAD|JC|J|SJ)\s*:)',
        re.MULTILINE
    )

    match = judge_pattern.search(full_text)

    if match:
        # Found judge name - split here
        split_point = match.start()
        headnotes = full_text[:split_point].strip()
        core = full_text[split_point:].strip()
        return headnotes, core

    # Fallback: Try to find first paragraph "1."
    # But preserve paragraph structure (don't flatten)
    para_match = re.search(r'(?:^|\n)\s*1\.\s+[A-Z]', full_text)
    if para_match:
        # Look backwards from "1." to find the actual start of core
        # (might be judge name on previous line)
        start_pos = para_match.start()
        preceding = full_text[max(0, start_pos - 200):start_pos]

        # Check for judge pattern in preceding text
        judge_in_preceding = judge_pattern.search(preceding)
        if judge_in_preceding:
            # Adjust split point to include judge name
            actual_start = max(0, start_pos - 200) + judge_in_preceding.start()
            headnotes = full_text[:actual_start].strip()
            core = full_text[actual_start:].strip()
        else:
            headnotes = full_text[:start_pos].strip()
            core = full_text[start_pos:].strip()
        return headnotes, core

    # Last resort: return all as core
    return "", full_text


def fix_sghc_paragraph_formatting(content: str) -> str:
    """
    Fix run-on paragraphs in SGHC judgments.
    Ensures proper paragraph breaks between numbered paragraphs.
    """
    # Ensure paragraph numbers get proper line breaks
    # Pattern: text ending with punctuation followed by number.
    content = re.sub(
        r'([.!?])\s+(\d{1,3})\.\s+([A-Z])',
        r'\1\n\n\2. \3',
        content
    )

    # Fix headers merged with following text (e.g., "Introduction The application")
    header_words = [
        'Introduction', 'Background', 'Facts', 'Issues', 'Analysis',
        'Discussion', 'Conclusion', 'Decision', 'Judgment', 'Summary',
        'Preliminary', 'Overview', 'History', 'Submissions', 'Evidence',
    ]

    for header in header_words:
        # Pattern: Header word followed immediately by another capitalized word
        pattern = rf'\b({header})\s+([A-Z][a-z])'
        matches = list(re.finditer(pattern, content))
        for match in reversed(matches):
            # Check this isn't part of a sentence (preceded by punctuation)
            start = match.start()
            if start > 0:
                preceding = content[max(0, start - 5):start].strip()
                if preceding and preceding[-1] in '.!?:':
                    # This is a header - add line breaks
                    replacement = f'{match.group(1)}\n\n{match.group(2)}'
                    content = content[:match.start()] + replacement + content[match.end():]

    return content


def fix_sghc_headnotes_formatting(content: str) -> str:
    """
    Fix SGHC headnotes where metadata fields are split across lines.

    Converts:
        Case Number
        : Suit No 682 of 2018
        Decision Date
        : 22 January 2020

    To:
        Case Number: Suit No 682 of 2018
        Decision Date: 22 January 2020
    """
    # Join metadata field names with their values
    metadata_fields = [
        'Case Number', 'Decision Date', 'Tribunal/Court', 'Coram',
        'Counsel Name\\(s\\)', 'Parties', 'Court', 'Judge', 'Hearing Date'
    ]

    for field in metadata_fields:
        # Pattern: Field name on one line, colon and value on next line(s)
        pattern = rf'({field})\s*\n\s*:\s*'
        content = re.sub(pattern, rf'\1: ', content, flags=re.IGNORECASE)

    # Also fix cases where colon is on same line but value is on next
    content = re.sub(r'(Case Number|Decision Date|Tribunal/Court|Coram|Court|Judge):\s*\n\s*',
                     r'\1: ', content, flags=re.IGNORECASE)

    return content


def fix_sghc_broken_sentences(content: str) -> str:
    """
    Fix sentences broken across lines in SGHC PDFs.

    Example:
        ...in Malaysia in

        June 2012.

    Should be:
        ...in Malaysia in June 2012.
    """
    # Fix month at start of line after a preposition
    months = 'January|February|March|April|May|June|July|August|September|October|November|December'

    # Pattern: preposition + "in" at end of line, followed by blank line(s), then month
    content = re.sub(
        rf'(\b(?:in|on|by|from|until|before|after|during))\s*\n\s*\n\s*({months})',
        r'\1 \2',
        content,
        flags=re.IGNORECASE
    )

    # Fix lowercase word at start of line (continuation of sentence)
    # Pattern: word ending sentence fragment + newline(s) + lowercase continuation
    content = re.sub(
        r'(\b[a-z]+)\s*\n\s*\n\s*([a-z]{2,})',
        r'\1 \2',
        content
    )

    return content


def ensure_paragraph_spacing(content: str) -> str:
    """
    Ensure blank lines between numbered paragraphs and around section headings
    in the CORE JUDGMENT section, so the output has clear visual separation.

    Adds a blank line:
    - Before each numbered paragraph (e.g., "1. The plaintiff...")
    - Before and after section headings (all-caps or title-case lines that
      are short and don't end with sentence punctuation)
    """
    lines = content.split('\n')
    result = []
    in_core = False

    # Common section heading patterns (title case, short, no trailing punct)
    heading_re = re.compile(
        r'^(?:[A-Z][a-z]+(?:\s+[a-z]+)*(?:\s+[A-Z][a-z]+)*'  # Title Case
        r'|[A-Z][A-Z\s]+[A-Z]'  # ALL CAPS
        r'|The\s+\w+(?:\'s)?\s+\w+(?:\s+\w+){0,5}'  # "The plaintiff's case"
        r'|(?:Issue|Ground|Stage|Phase|Step)\s+\d+'  # "Issue 1", "Ground 2"
        r')$'
    )

    for i, line in enumerate(lines):
        stripped = line.strip()

        # Track when we enter CORE JUDGMENT
        if 'CORE JUDGMENT' in stripped:
            in_core = True
            result.append(line)
            continue

        if not in_core:
            result.append(line)
            continue

        # Skip empty lines, dividers
        if not stripped or re.match(r'^[-=]{5,}$', stripped):
            result.append(line)
            continue

        # Check if this is a numbered paragraph start (e.g., "1. The", "23. In")
        is_numbered_para = bool(re.match(r'^\d{1,3}\.\s+[A-Z]', stripped))

        # Check if this is a section heading
        is_heading = (
            len(stripped) < 80 and
            not stripped[-1] in '.,;:!?' and
            not re.match(r'^\d', stripped) and
            not re.match(r'^[\(\[]', stripped) and
            heading_re.match(stripped)
        )

        # Check if this is a sub-item like (a), (b), (i), (ii)
        is_sub_item = bool(re.match(r'^\([a-z]+\)\s', stripped) or
                           re.match(r'^\([ivxlc]+\)\s', stripped))

        # Add blank line before numbered paragraph if previous line isn't blank
        if is_numbered_para and result and result[-1].strip():
            result.append('')

        # Add blank line before heading if previous line isn't blank
        if is_heading and result and result[-1].strip():
            result.append('')

        # Add blank line before sub-items if previous line isn't blank
        # (only if previous line ends with colon or is another sub-item)
        if is_sub_item and result and result[-1].strip():
            prev = result[-1].strip()
            if prev.endswith(':') or re.match(r'^\([a-z]+\)', prev) or \
               re.match(r'^\([ivxlc]+\)', prev):
                pass  # already fine
            else:
                result.append('')

        result.append(line)

        # Add blank line after heading
        if is_heading:
            result.append('')

    # Clean up any triple+ blank lines
    text = '\n'.join(result)
    text = re.sub(r'\n{3,}', '\n\n', text)
    return text


# ============================================================================
# DOCUMENT STRUCTURE - Common across jurisdictions
# ============================================================================

def segment_head_core(full_text: str) -> Tuple[str, str]:
    """
    Split text into headnotes and core judgment.
    Core begins at first "1." (paragraph marker).
    """
    flat = re.sub(r"\s+", " ", full_text)
    flat = repair_space_separated_digits(flat)
    m = re.search(r"\b1\.\s+\S", flat)
    if not m:
        return "", full_text
    start = m.start()
    head_flat = flat[:start].strip()
    core_flat = flat[start:].strip()
    return head_flat, core_flat


def fix_merged_headers(content: str) -> str:
    """Split headers that are merged with paragraph text."""
    header_patterns = [
        r'Introduction\.', r'Background\s+facts?\.', r'Background\.',
        r'The\s+facts?\.', r'Facts\.', r'General\s+principles?\.',
        r'Our\s+decision\.', r'Our\s+view\.', r'Conclusion\.',
        r'Analysis\.', r'Discussion\.', r'Issues?\s+on\s+appeal\.',
        r'The\s+decision\s+below\.', r'Procedural\s+history\.',
        r'Preliminary\s+(?:matters?|issues?|observations?)\.',
    ]

    for pattern in header_patterns:
        regex = rf'^({pattern})\s+([A-Z][a-z]|[A-Z]\s+[a-z])'
        matches = list(re.finditer(regex, content, re.MULTILINE | re.IGNORECASE))
        for match in reversed(matches):
            header = match.group(1).rstrip('.')
            rest_start = match.group(2)
            replacement = f'{header}\n\n{rest_start}'
            content = content[:match.start()] + replacement + content[match.end():]
    return content


def fix_inline_section_headings(content: str) -> str:
    """
    Fix section headings that are merged at the end of paragraphs.
    E.g., "...PP and QQ. The appropriate proportion of the parties' respective share"
    Should become separate heading on new line.
    """
    # Common section heading patterns that appear inline
    heading_patterns = [
        # Multi-word capitalized headings (title case)
        r'The\s+(?:appropriate|relevant|applicable)\s+\w+(?:\s+\w+){1,8}',
        r'Precedents?\s+for\s+(?:the\s+)?\w+(?:\s+\w+){1,6}',
        r'Whether\s+\w+(?:\s+\w+){1,8}',
        r'Our\s+(?:decision|view|analysis|conclusion)\s+on\s+\w+(?:\s+\w+){0,6}',
        r'The\s+(?:law|facts?|issues?|background)\s+(?:on|relating to|concerning)\s+\w+(?:\s+\w+){0,6}',
        r'Division\s+of\s+(?:the\s+)?matrimonial\s+assets?',
        r'Maintenance\s+of\s+the\s+\w+(?:\s+and\s+\w+)*',
        r'Principles?\s+governing\s+\w+(?:\s+\w+){0,6}',
    ]

    for heading_pattern in heading_patterns:
        # Look for heading after sentence-ending punctuation
        regex = rf'([.!?])\s+({heading_pattern})\.?\s+(\d+\.\s+|\n|[A-Z][a-z])'
        matches = list(re.finditer(regex, content, re.IGNORECASE))
        for match in reversed(matches):
            punct = match.group(1)
            heading = match.group(2).strip()
            after = match.group(3)
            # Insert newlines to separate heading
            replacement = f'{punct}\n\n{heading}\n\n{after}'
            content = content[:match.start()] + replacement + content[match.end():]

    return content


def fix_sgca_paragraph_heading_merge(content: str) -> str:
    """
    Fix SGCA-specific issue where paragraph numbers follow section headings without line break.
    E.g., "Precedents for division. 16. In MZ v NA..." -> proper line breaks
    """
    # Pattern: heading followed immediately by paragraph number
    pattern = r'([A-Z][a-z]+(?:\s+[a-z]+){2,10})\.\s*(\d{1,3})\.\s+([A-Z])'

    matches = list(re.finditer(pattern, content))
    for match in reversed(matches):
        heading = match.group(1)
        para_num = match.group(2)
        first_char = match.group(3)

        # Check if this looks like a heading (not just end of sentence)
        # Headings typically have specific patterns
        heading_lower = heading.lower()
        is_heading = any(kw in heading_lower for kw in [
            'precedent', 'division', 'maintenance', 'principle', 'whether',
            'appropriate', 'relevant', 'applicable', 'our decision', 'our view',
            'the law', 'the facts', 'background', 'conclusion', 'analysis'
        ])

        if is_heading:
            replacement = f'{heading}\n\n{para_num}. {first_char}'
            content = content[:match.start()] + replacement + content[match.end():]

    return content


def fix_paragraph_numbering(content: str) -> str:
    """Fix paragraph numbers that got corrupted (1. instead of 10., etc.)."""
    lines = content.split('\n')
    result = []
    last_para_num = 0
    in_core_judgment = False

    for line in lines:
        if 'CORE JUDGMENT' in line:
            in_core_judgment = True

        if not in_core_judgment:
            result.append(line)
            continue

        stripped = line.strip()
        para_match = re.match(r'^(\d+)\.\s+([A-Z])', stripped)
        if para_match:
            para_num = int(para_match.group(1))
            rest = stripped[len(para_match.group(1)) + 1:].strip()

            if para_num < 10 and last_para_num >= 10:
                expected_tens = (last_para_num // 10) * 10
                if last_para_num % 10 >= para_num - 1:
                    new_num = expected_tens + para_num
                    if new_num <= last_para_num:
                        new_num = expected_tens + 10 + para_num
                    line = f"{new_num}. {rest}"
                    last_para_num = new_num
                else:
                    last_para_num = para_num
            elif para_num > last_para_num or para_num == 1:
                last_para_num = para_num

        result.append(line)
    return '\n'.join(result)


def fix_list_spacing(content: str) -> str:
    """Add proper spacing between top-level list items."""
    content = re.sub(r'([.;:])\s*\n(\([a-z]\))', r'\1\n\n\2', content)
    content = re.sub(r'([.;])\s+(\([ivxlc]+\))', r'\1\n\2', content, flags=re.IGNORECASE)
    content = re.sub(r'([.;])\s+(\(\d+\))', r'\1\n\2', content)
    return content


# ============================================================================
# HTML OUTPUT GENERATION
# ============================================================================

HEADING_LINE = re.compile(r"^\(\s*\d{1,3}\s*\)\s+.+$")


def esc(s: str) -> str:
    """HTML escape a string."""
    return s.replace("&", "&amp;").replace("<", "&lt;").replace(">", "&gt;")


def core_text_to_html(core_display: str) -> str:
    """Convert core text to styled HTML paragraphs."""
    blocks = re.split(r"\n{2,}", core_display.strip())
    out = []
    for b in blocks:
        line = b.strip()
        if not line:
            continue
        if HEADING_LINE.match(line):
            out.append(f"<p><strong>{esc(line)}</strong></p>")
        else:
            parts = [esc(x) for x in line.split("\n")]
            out.append("<p>" + "<br>".join(parts) + "</p>")
    return "\n".join(out)


def build_output_html(head_text: str, core_display: str, case_file: str = "") -> str:
    """Generate styled HTML output with headnotes and core sections."""
    style = """
    <style>
      body { font-family: Arial, sans-serif; line-height: 1.55; padding: 20px; }
      .header { font-weight: bold; font-size: 18px; margin-bottom: 20px; color: #333; }
      .section-title { font-weight: bold; font-size: 20px; margin: 16px 0 8px 0; }
      .headnotes { background:#fff7c2; padding:12px; border-left:5px solid #e0c200; margin-bottom:25px; white-space:pre-wrap; }
      .core { background:#e8f0ff; padding:12px; border-left:5px solid #3b6bd6; }
      .core p { margin: 0 0 12px 0; }
    </style>
    """
    head_html = esc(head_text) if head_text.strip() else "No headnotes detected"
    core_html = core_text_to_html(core_display) if core_display.strip() else "<p>No core detected</p>"
    header_section = ""
    if case_file:
        header_section = f'<div class="header">&lt;Start&gt; ##{esc(case_file)}</div>'

    return f"""<!doctype html>
<html>
<head><meta charset="utf-8"><title>Cleaned Judgment {config.VERSION}</title>{style}</head>
<body>
  {header_section}
  <div class="section-title">Headnotes</div>
  <div class="headnotes">{head_html}</div>

  <div class="section-title">Core Judgment</div>
  <div class="core">
    {core_html}
  </div>
  <div class="header">&lt;/Start&gt;</div>
</body>
</html>
"""


# ============================================================================
# MAIN ENTRY POINTS
# ============================================================================

def process_html_file(filepath: str, jurisdiction: str = "UK") -> dict:
    """
    Process a single HTML file through the cleaning pipeline.

    Args:
        filepath: Path to the HTML file
        jurisdiction: "UK" or "SG"

    Returns:
        Dictionary containing cleaned content and metadata
    """
    soup = parse_html(filepath)
    soup = strip_rubbish_tags(soup)
    date = extract_date_html(soup)

    raw = soup.get_text(separator="\n")
    raw = normalize_text(raw)
    raw = delete_end_of_document(raw)
    raw = fix_split_company_names(raw)
    raw = reflow_lonely_numbered_paras(raw)
    raw = reflow_lonely_bracket_markers(raw)
    raw = repair_digit_stacks(raw)

    head_text, core_flat = segment_head_core(raw)

    if jurisdiction == "UK":
        core_display = rewrite_core_uk(core_flat)
    else:
        core_display = core_flat  # Singapore uses different structure

    case_file = os.path.basename(filepath).replace(".html", "")
    output_html = build_output_html(head_text, core_display, case_file)

    return {
        "headnotes": head_text,
        "core": core_flat,
        "core_display": core_display,
        "date": date,
        "output_html": output_html,
    }


def process_txt_file(filepath: str, jurisdiction: str = "SG") -> Tuple[str, Dict[str, int]]:
    """
    Process a single TXT file (from PDF extraction) through the cleaning pipeline.

    Args:
        filepath: Path to the TXT file
        jurisdiction: "SG" (Singapore) or other

    Returns:
        Tuple of (cleaned_content, stats_dict)
    """
    stats = {}

    try:
        with open(long_path(filepath), 'r', encoding='utf-8') as f:
            content = f.read()
    except Exception as e:
        raise IOError(f"Error reading {filepath}: {e}")

    original = content

    # Common fixes
    before = content
    content = delete_note_references(content)
    if content != before:
        stats['note_references'] = 1

    before = content
    content = remove_version_markers(content)
    if content != before:
        stats['version_markers'] = 1

    before = content
    content = remove_page_numbers(content)
    if content != before:
        stats['page_numbers'] = 1

    before = content
    content = fix_date_periods(content)
    if content != before:
        stats['date_periods'] = 1

    before = content
    content, had_toc = remove_table_of_contents(content)
    if had_toc:
        stats['table_of_contents'] = 1

    before = content
    content = fix_split_company_names(content)
    if content != before:
        stats['split_company_names'] = 1

    before = content
    content = fix_broken_citations(content)
    if content != before:
        stats['broken_citations'] = 1

    # Jurisdiction-specific fixes
    if jurisdiction == "SG":
        before = content
        content = remove_header_footer_citations(content, r'\[\d{4}\]\s*SG(?:CA|HC)\s*\d+')
        if content != before:
            stats['header_footer_citations'] = 1

        before = content
        content = remove_case_citation_from_core(content)
        if content != before:
            stats['citation_headers_removed'] = 1

        before = content
        content = fix_sg_judge_name_splits(content)
        if content != before:
            stats['judge_name_splits'] = 1

        before = content
        content = fix_truncated_judge_names(content)
        if content != before:
            stats['truncated_judge_names'] = 1

        before = content
        content = ensure_judge_attribution(content)
        if content != before:
            stats['judge_attribution'] = 1

    # Common structure fixes
    before = content
    content = fix_merged_headers(content)
    if content != before:
        stats['merged_headers'] = 1

    before = content
    content = fix_paragraph_numbering(content)
    if content != before:
        stats['paragraph_numbering'] = 1

    before = content
    content = fix_list_spacing(content)
    if content != before:
        stats['list_spacing'] = 1

    before = content
    content = clean_multiple_blanks(content)
    if content != before:
        stats['multiple_blanks'] = 1

    return content, stats


def detect_format(filepath: str) -> str:
    """Auto-detect file format from extension."""
    ext = os.path.splitext(filepath)[1].lower()
    if ext == '.html':
        return 'html'
    elif ext == '.txt':
        return 'txt'
    else:
        return 'unknown'


def detect_jurisdiction(filepath: str, content: str = None) -> str:
    """Auto-detect jurisdiction from filepath or content."""
    filepath_lower = filepath.lower()

    # Check filepath for court indicators
    if 'uksc' in filepath_lower or 'ukhl' in filepath_lower:
        return 'UK'
    if 'sgca' in filepath_lower or 'sghc' in filepath_lower:
        return 'SG'

    # Check content if available
    if content:
        if re.search(r'\[\d{4}\]\s+UK(?:SC|HL)', content):
            return 'UK'
        if re.search(r'\[\d{4}\]\s+SG(?:CA|HC)', content):
            return 'SG'

    # Default to UK (original behavior)
    return 'UK'


def process_file(filepath: str, format: str = "auto", jurisdiction: str = "auto") -> dict:
    """
    Main entry point: process a file with auto-detection of format and jurisdiction.

    Args:
        filepath: Path to the file
        format: "html", "txt", or "auto" to detect from extension
        jurisdiction: "UK", "SG", or "auto" to detect from path/content

    Returns:
        Dictionary with cleaned content and metadata
    """
    if format == "auto":
        format = detect_format(filepath)

    if jurisdiction == "auto":
        jurisdiction = detect_jurisdiction(filepath)

    if format == "html":
        return process_html_file(filepath, jurisdiction)
    elif format == "txt":
        content, stats = process_txt_file(filepath, jurisdiction)
        return {
            "content": content,
            "stats": stats,
            "format": "txt",
            "jurisdiction": jurisdiction,
        }
    else:
        raise ValueError(f"Unknown format: {format}")


# ============================================================================
# ADDITIONAL SGCA FUNCTIONS (from fix_all_2_0.py)
# ============================================================================

def fix_page_break_word_splits(content: str) -> str:
    """Fix words split across PDF pages with headers inserted mid-word."""
    lines = content.split('\n')
    result = []
    i = 0

    while i < len(lines):
        line = lines[i]

        # Check if line ends with a partial word (lowercase letters, no punctuation)
        if i + 1 < len(lines) and re.search(r'[a-z]$', line.rstrip()):
            next_line = lines[i + 1].strip()

            # Check if next line looks like a header/citation that got inserted
            if re.match(r'^\[\d{4}\]\s*SG(?:CA|HC)', next_line):
                # Skip the header line and check the line after
                if i + 2 < len(lines):
                    after_header = lines[i + 2].strip()
                    # If it starts with lowercase, it's likely continuation
                    if re.match(r'^[a-z]', after_header):
                        # Join the split word
                        result.append(line.rstrip() + after_header)
                        i += 3
                        continue

            # Check if next line starts with lowercase (word continuation)
            elif re.match(r'^[a-z]', next_line) and len(next_line) < 20:
                # Likely a split word
                result.append(line.rstrip() + next_line)
                i += 2
                continue

        result.append(line)
        i += 1

    return '\n'.join(result)


def fix_standalone_paragraph_numbers(content: str) -> str:
    """Fix standalone numbers like '14\\n\\n14.' becoming duplicates."""
    lines = content.split('\n')
    result = []
    i = 0

    while i < len(lines):
        line = lines[i].strip()

        # Check for standalone number
        if re.match(r'^\d{1,3}$', line):
            # Look ahead for the actual paragraph
            j = i + 1
            while j < len(lines) and not lines[j].strip():
                j += 1

            if j < len(lines):
                next_line = lines[j].strip()
                # Check if next line starts with same number + period
                match = re.match(r'^(\d{1,3})\.\s+', next_line)
                if match and match.group(1) == line:
                    # Skip the standalone number, keep the paragraph
                    i = j
                    continue

        result.append(lines[i])
        i += 1

    return '\n'.join(result)


def fix_duplicate_paragraph_numbers(content: str) -> str:
    """Fix consecutive duplicate paragraph numbers by renumbering."""
    lines = content.split('\n')
    result = []
    last_para_num = 0

    for line in lines:
        stripped = line.strip()
        match = re.match(r'^(\d{1,3})\.\s+(.+)$', stripped)

        if match:
            para_num = int(match.group(1))
            rest = match.group(2)

            # If this number is same as or less than last, increment
            if para_num <= last_para_num:
                para_num = last_para_num + 1
                line = f"{para_num}. {rest}"

            last_para_num = para_num

        result.append(line)

    return '\n'.join(result)


def fix_header_format(content: str) -> str:
    """Ensure CASE name is properly formatted inside === lines."""
    # Check if header already exists
    if re.search(r'^={10,}', content, re.MULTILINE):
        return content

    # Try to find case name from content
    case_match = re.search(r'CASE:\s*([^\n]+)', content)
    if case_match:
        case_name = case_match.group(1).strip()
        # Ensure proper header format
        header = "=" * 70 + f"\nCASE: {case_name}\n" + "=" * 70
        # Replace any malformed header
        content = re.sub(r'^[=\s]*CASE:[^\n]+[=\s]*', header + '\n', content, count=1)

    return content


def fix_duplicate_content(content: str) -> str:
    """Remove duplicate content that appears due to PDF page breaks."""
    lines = content.split('\n')
    result = []
    seen_paragraphs = set()

    i = 0
    while i < len(lines):
        line = lines[i]
        stripped = line.strip()

        # Check for paragraph with number
        match = re.match(r'^(\d{1,3})\.\s+(.{50,})', stripped)
        if match:
            para_key = match.group(1) + "|" + match.group(2)[:100]

            if para_key in seen_paragraphs:
                # Skip duplicate
                i += 1
                continue

            seen_paragraphs.add(para_key)

        result.append(line)
        i += 1

    return '\n'.join(result)


def fix_split_content_at_core_boundary(content: str) -> str:
    """Fix content incorrectly split at CORE JUDGMENT boundary."""
    # Find CORE JUDGMENT marker
    core_match = re.search(r'(-{10,}\s*\n\s*CORE JUDGMENT\s*\n\s*-{10,})', content)
    if not core_match:
        return content

    core_pos = core_match.start()
    before_core = content[:core_pos]
    after_core = content[core_pos:]

    # Check if text before CORE JUDGMENT ends mid-sentence
    before_stripped = before_core.rstrip()
    if before_stripped and not re.search(r'[.!?:]\s*$', before_stripped):
        # Find where the sentence continues after CORE JUDGMENT header
        header_end = core_match.end()
        remaining = content[header_end:].lstrip('\n')

        # Check if it starts with lowercase or continuation
        if remaining and (remaining[0].islower() or remaining.startswith(',')):
            # Find the end of the continued sentence
            sent_end = re.search(r'[.!?]\s', remaining)
            if sent_end:
                continuation = remaining[:sent_end.end()]
                rest = remaining[sent_end.end():]

                # Move continuation before CORE JUDGMENT
                content = before_stripped + continuation + '\n\n' + core_match.group(1) + '\n\n' + rest

    return content


def remove_duplicate_sections(content: str) -> str:
    """Remove duplicate HEADNOTES or CORE JUDGMENT sections."""
    # Count occurrences
    headnotes_count = len(re.findall(r'-{10,}\s*\n\s*HEADNOTES\s*\n\s*-{10,}', content))
    core_count = len(re.findall(r'-{10,}\s*\n\s*CORE JUDGMENT\s*\n\s*-{10,}', content))

    if headnotes_count > 1:
        # Keep only first HEADNOTES section
        parts = re.split(r'(-{10,}\s*\n\s*HEADNOTES\s*\n\s*-{10,})', content)
        if len(parts) >= 3:
            # Keep first header and content until next section
            result = parts[0] + parts[1]
            # Skip duplicate headers
            for i in range(2, len(parts)):
                if not re.match(r'-{10,}\s*\n\s*HEADNOTES\s*\n\s*-{10,}', parts[i]):
                    result += parts[i]
            content = result

    if core_count > 1:
        # Keep only first CORE JUDGMENT section
        parts = re.split(r'(-{10,}\s*\n\s*CORE JUDGMENT\s*\n\s*-{10,})', content)
        if len(parts) >= 3:
            result = parts[0] + parts[1]
            for i in range(2, len(parts)):
                if not re.match(r'-{10,}\s*\n\s*CORE JUDGMENT\s*\n\s*-{10,}', parts[i]):
                    result += parts[i]
            content = result

    return content


def fix_block_quotes(content: str) -> str:
    """Fix block quote formatting with proper indentation."""
    lines = content.split('\n')
    result = []
    in_quote = False

    for line in lines:
        stripped = line.strip()

        # Detect start of block quote (indented text after paragraph)
        if stripped.startswith('"') and len(stripped) > 50:
            in_quote = True
            result.append('')  # Add blank line before quote
            result.append('    ' + stripped)
            continue

        # Continue block quote
        if in_quote:
            if stripped.endswith('"') or stripped.endswith('."'):
                result.append('    ' + stripped)
                result.append('')  # Add blank line after quote
                in_quote = False
            elif stripped and not re.match(r'^\d+\.', stripped):
                result.append('    ' + stripped)
            else:
                in_quote = False
                result.append(line)
        else:
            result.append(line)

    return '\n'.join(result)


def add_missing_paragraph_numbers(content: str) -> str:
    """Add paragraph numbers to unnumbered paragraphs after section headers."""
    lines = content.split('\n')
    result = []
    last_para_num = 0
    in_core = False

    for i, line in enumerate(lines):
        if 'CORE JUDGMENT' in line:
            in_core = True

        if not in_core:
            result.append(line)
            continue

        stripped = line.strip()

        # Track paragraph numbers
        match = re.match(r'^(\d{1,3})\.\s+', stripped)
        if match:
            last_para_num = int(match.group(1))
            result.append(line)
            continue

        # Check if this looks like an unnumbered paragraph that should be numbered
        if (stripped and
            len(stripped) > 100 and
            stripped[0].isupper() and
            not stripped.startswith(('(', '[', '"'))):

            # Check if previous non-blank line was a header
            prev_idx = i - 1
            while prev_idx >= 0 and not lines[prev_idx].strip():
                prev_idx -= 1

            if prev_idx >= 0:
                prev_line = lines[prev_idx].strip()
                # If previous was a header (short, ends without period)
                if len(prev_line) < 50 and not prev_line.endswith('.'):
                    last_para_num += 1
                    line = f"{last_para_num}. {stripped}"

        result.append(line)

    return '\n'.join(result)


def fix_odd_line_breaks(content: str) -> str:
    """Fix sentences incorrectly split mid-line."""
    lines = content.split('\n')
    result = []
    i = 0

    while i < len(lines):
        line = lines[i]

        # Check if line ends mid-sentence (no terminal punctuation, not a header)
        if (line.strip() and
            not re.search(r'[.!?:;,]\s*$', line) and
            not re.match(r'^-{5,}', line) and
            not re.match(r'^={5,}', line) and
            len(line.strip()) > 20):

            # Check next line
            if i + 1 < len(lines):
                next_line = lines[i + 1].strip()

                # If next line starts with lowercase, join them
                if next_line and next_line[0].islower():
                    result.append(line.rstrip() + ' ' + next_line)
                    i += 2
                    continue

        result.append(line)
        i += 1

    return '\n'.join(result)


def fix_paragraph_periods(content: str) -> str:
    """Fix missing periods in paragraph numbers: '1 The' -> '1. The'.

    Excludes dates like '26 April 2005' from being treated as paragraph numbers.
    """
    # Negative lookahead to exclude month names (dates)
    months_pattern = r'(?!January|February|March|April|May|June|July|August|September|October|November|December)'
    content = re.sub(rf'^(\d{{1,3}})\s+{months_pattern}([A-Z])', r'\1. \2', content, flags=re.MULTILINE)
    return content


def clean_headnotes(content: str) -> str:
    """Clean up headnotes section formatting."""
    # Find HEADNOTES section
    match = re.search(r'(-{10,}\s*\nHEADNOTES\s*\n-{10,}\s*\n)(.*?)(-{10,}\s*\nCORE JUDGMENT)',
                      content, re.DOTALL)
    if not match:
        return content

    before = content[:match.start()]
    header = match.group(1)
    headnotes = match.group(2)
    after = match.group(3) + content[match.end():]

    # Clean up headnotes
    headnotes = re.sub(r'\n{4,}', '\n\n\n', headnotes)
    headnotes = headnotes.strip()

    return before + header + headnotes + '\n\n' + after


# ============================================================================
# UK HOUSE OF LORDS FUNCTIONS (from HTML_clean_HL_v1.7.py and v2.0.py)
# ============================================================================

def fix_encoding(text: str) -> str:
    """Fix Unicode and HTML entity encoding issues."""
    replacements = {
        "\u2019": "'", "\u2018": "'", "\u201c": '"', "\u201d": '"',
        "\u2014": "-", "\u2013": "-", "\u00a0": " ", "\u00ad": "",
        "&nbsp;": " ", "&amp;": "&", "&lt;": "<", "&gt;": ">",
        "&ndash;": "-", "&mdash;": "-", "&lsquo;": "'", "&rsquo;": "'",
        "&ldquo;": '"', "&rdquo;": '"',
    }
    for old, new in replacements.items():
        text = text.replace(old, new)
    return text


def fix_split_lord_names(text: str) -> str:
    """Fix Lord names that are split across lines in HTML source."""
    # Fix "LORD BROWNE\n-WILKINSON"
    text = re.sub(r'(LORD\s+[A-Z]+)\s*\n+\s*(-[A-Z]+)', r'\1\2', text)

    # Fix "LORD IRVINE OF LAIRG L\n.C." -> "LORD IRVINE OF LAIRG L.C."
    text = re.sub(r'(LORD\s+[A-Z]+\s+OF\s+[A-Z]+)\s+L\n+\.C\.', r'\1 L.C.', text)
    text = re.sub(r'(LORD\s+[A-Z]+)\s+L\n+\.C\.', r'\1 L.C.', text)

    # Fix LADY and BARONESS similarly
    text = re.sub(r'(LADY\s+[A-Z]+)\s*\n+\s*(-[A-Z]+)', r'\1\2', text)
    text = re.sub(r'(BARONESS\s+[A-Z]+)\s*\n+\s*(-[A-Z]+)', r'\1\2', text)

    return text


def remove_uk_source_boilerplate(text: str) -> str:
    """Remove UK source database boilerplate and navigation."""
    text = re.sub(r"(?:You are here:.*?(?=\n\n|\Z))", "", text, flags=re.DOTALL | re.IGNORECASE)
    text = re.sub(r"(?:Cite as:.*?(?=\n\n|\Z))", "", text, flags=re.DOTALL | re.IGNORECASE)
    text = re.sub(r"(?:URL:.*?(?=\n\n|\Z))", "", text, flags=re.DOTALL | re.IGNORECASE)
    text = re.sub(r"Judgments\s*-\s*", "", text)
    text = re.sub(r"^House of Lords Decisions\s*\n", "", text, flags=re.MULTILINE | re.IGNORECASE)
    return text


def remove_uk_source_footer(text: str) -> str:
    """Remove UK source database footer and copyright notices."""
    text = re.sub(r"\s*Copyright Policy\s*\|.*$", "", text, flags=re.DOTALL | re.IGNORECASE)
    text = re.sub(r"\s*&copy;?\s*\d{4}\s*Crown Copyright\.?\s*$", "", text, flags=re.IGNORECASE)
    text = re.sub(r"\s*©\s*\d{4}\s*Crown Copyright\.?\s*$", "", text, flags=re.IGNORECASE)
    return text.strip()


def format_lord_headers(text: str) -> str:
    """Format LORD/LADY/BARONESS headers with proper line breaks."""
    pattern = r'\b((?:LORD|LADY|BARONESS)\s+[A-Z][A-Z]+(?:-[A-Z]+)?(?:\s+OF\s+[A-Z][A-Z]+(?:\s+[A-Z]+)?)?(?:\s+L\.?C\.?)?)\b'

    def replace_header(match):
        name = match.group(1)
        name_parts = [p for p in name.split() if p not in ['LORD', 'LADY', 'BARONESS', 'OF', 'L.C.', 'LC']]
        if all(p.isupper() or p == 'L.C.' for p in name_parts):
            return f"\n\n{name}\n\n"
        return match.group(0)

    text = re.sub(pattern, replace_header, text)
    return text


def clean_headnotes_garbage_uk(headnotes: str, year: int) -> str:
    """Remove garbage from UK headnotes based on year."""
    if year >= 2003:
        match = re.search(r'OPINIONS OF THE LORDS OF APPEAL FOR JUDGMENT IN THE CAUSE', headnotes, re.IGNORECASE)
        if match:
            headnotes = headnotes[match.start():]
    else:
        match = re.search(r'HOUSE OF LORDS', headnotes, re.IGNORECASE)
        if match:
            headnotes = headnotes[match.start():]
    return headnotes.strip()


def find_core_judgment_start_uk(text: str) -> int:
    """Find where UK core judgment begins (after headnotes)."""
    # Pattern 1: LORD/LADY in ALL CAPS followed by My Lords
    match = re.search(
        r'\n((?:LORD|LADY|BARONESS)\s+[A-Z][A-Z]+(?:-[A-Z]+)?(?:\s+OF\s+[A-Z][A-Z\s]+)?(?:\s+L\.?C\.?)?)\s*\n+\s*My Lords',
        text, re.IGNORECASE
    )
    if match:
        return match.start()

    # Pattern 2: Standalone LORD header in ALL CAPS
    match = re.search(
        r'\n\s*((?:LORD|LADY|BARONESS)\s+[A-Z][A-Z]+(?:-[A-Z]+)?(?:\s+OF\s+[A-Z][A-Z\s]+)?(?:\s+L\.?C\.?)?)\s*\n',
        text
    )
    if match:
        return match.start()

    # Pattern 3: "My Lords," at start of speech
    match = re.search(r'\bMy Lords,', text)
    if match:
        return match.start()

    return 0


def has_para_anchors(soup: BeautifulSoup) -> bool:
    """Check if HTML has paragraph anchor tags like <a name='para1'>."""
    return bool(soup.find('a', attrs={'name': re.compile(r'^para\d+$', re.IGNORECASE)}))


def extract_para_numbers_from_anchors(soup: BeautifulSoup) -> set:
    """Extract paragraph numbers from HTML anchor tags."""
    para_nums = set()
    for anchor in soup.find_all('a', attrs={'name': re.compile(r'^para\d+$', re.IGNORECASE)}):
        name = anchor.get('name', '')
        match = re.search(r'para(\d+)', name, re.IGNORECASE)
        if match:
            para_nums.add(int(match.group(1)))
    return para_nums


def extract_judges_from_headnotes_uk(headnotes: str) -> list:
    """Extract UK judge names from headnotes."""
    judges = []

    # Appellate Committee pattern
    match = re.search(
        r'(?:Appellate Committee|Appeal Committee)\s+comprised:\s*(.*?)(?=HOUSE OF LORDS|OPINIONS|$)',
        headnotes, re.IGNORECASE | re.DOTALL
    )
    if match:
        names_block = match.group(1)
        for name_match in re.finditer(
            r'((?:Lord|Lady|Baroness)\s+[A-Za-z]+(?:-[A-Za-z]+)?(?:\s+of\s+[A-Za-z\-]+)?)',
            names_block, re.IGNORECASE
        ):
            name = name_match.group(1).strip()
            if name and name not in judges:
                judges.append(name)
        if judges:
            return judges

    # Individual name lines
    name_lines = re.findall(
        r'^((?:Lord|Lady|Baroness)\s+[A-Za-z]+(?:-[A-Za-z]+)?(?:\s+of\s+[A-Za-z\-]+)?)\s*$',
        headnotes, re.MULTILINE | re.IGNORECASE
    )
    judges = [name.strip() for name in name_lines if name.strip()]

    return judges


def extract_judges_from_core_uk(core_text: str) -> list:
    """Extract UK judge names from core judgment text."""
    judges = []
    pattern = r'^\s*((?:LORD|LADY|BARONESS)\s+[A-Z][A-Z]+(?:-[A-Z]+)?(?:\s+OF\s+[A-Z][A-Z]+(?:\s+[A-Z]+)?)?(?:\s+L\.?C\.?)?)\s*$'
    matches = re.findall(pattern, core_text, re.MULTILINE)

    for match in matches:
        name = match.strip()
        name = re.sub(r'\s+L\.?C\.?\s*$', '', name)
        words = name.split()
        title_case_words = []
        for word in words:
            if word.upper() == 'OF':
                title_case_words.append('of')
            elif '-' in word:
                parts = word.split('-')
                title_case_words.append('-'.join(p.capitalize() for p in parts))
            else:
                title_case_words.append(word.capitalize())
        name = ' '.join(title_case_words)
        if name not in judges:
            judges.append(name)

    return judges


# ============================================================================
# CITATION COUNTING BY JURISDICTION
# ============================================================================

# Jurisdiction abbreviation dictionaries
UK_REPORTERS = {
    'UKHL', 'UKSC', 'UKPC', 'EWCA', 'EWHC', 'EWCOP', 'EWFC',
    'CSIH', 'CSOH', 'NICA', 'NIQB', 'NIFAM', 'NICH',
    'UKUT', 'UKFTT', 'UKEAT', 'UKIAT', 'UKAIT', 'UKSIAC',
    'AC', 'WLR', 'QB', 'KB', 'Ch', 'Fam', 'P',
    'All ER', 'ALLER', 'Lloyd\'s Rep', 'Lloyds Rep',
    'BMLR', 'LGR', 'HLR', 'ICR', 'IRLR', 'Cr App R',
    'CMLR', 'EG', 'EGLR', 'P & CR', 'COD', 'Admin LR', 'Med LR',
    'RPC', 'FSR', 'EMLR', 'Env LR', 'JPL', 'PLR', 'RTR', 'STC',
    'TC', 'BTC', 'WTLR', 'PIQR', 'FLR', 'FCR', 'BCLC', 'BCC',
    'CLC', 'BHRC', 'LT', 'SLT', 'NI', 'NIJB', 'UKHRR',
    'IR', 'INLR', 'Imm AR', 'TLR', 'Build LR', 'BLR', 'PNLR',
    'App Cas', 'HL Cas', 'ER', 'Eng Rep', 'Cl & F',
}

AU_REPORTERS = {
    'HCA', 'FCAFC', 'FCA', 'NSWCA', 'NSWSC', 'NSWCCA',
    'VSCA', 'VSC', 'QCA', 'QSC', 'WASC', 'WASCA',
    'SASC', 'SASCFC', 'TASSC', 'TASFC', 'ACTSC', 'ACTCA',
    'NTSC', 'NTCA', 'AATA', 'FamCA', 'FamCAFC',
    'CLR', 'ALR', 'ALJR', 'NSWLR', 'VR', 'Qd R', 'SASR',
    'WAR', 'WALR', 'Tas R', 'ACTR', 'NTR',
}

USA_REPORTERS = {
    'US', 'USSC', 'SCOTUS', 'F 2d', 'F 3d', 'F 4th', 'Fed',
    'F Supp', 'S Ct', 'L Ed',
}

CAN_REPORTERS = {
    'SCC', 'SCR', 'FC', 'FCA', 'BCCA', 'BCSC', 'ONCA', 'ONSC',
    'ABCA', 'ABQB', 'SKCA', 'SKQB', 'MBCA', 'MBQB', 'NSCA', 'NSSC',
    'NBCA', 'NBQB', 'DLR', 'OR', 'AR', 'WWR', 'CR', 'CCC',
}

IND_REPORTERS = {'AIR', 'All India Rep'}

NZ_REPORTERS = {
    'NZSC', 'NZCA', 'NZHC', 'NZFC', 'NZDC', 'NZLR', 'NZFLR', 'NZAR',
}

SG_REPORTERS = {'SGCA', 'SGHC', 'SGDC', 'SLR', 'MLJ'}

EU_REPORTERS = {'ECHR', 'EHRR', 'ECJ', 'CJEU', 'ECR', 'EUECJ', 'BHRC'}

OTHER_REPORTERS = {'ICJ', 'PCIJ', 'ILR', 'ILM', 'ICSID', 'HKCA', 'HKCFI', 'HKLRD'}

ACADEMIC_JOURNALS = {
    'LQR', 'MLR', 'CLJ', 'OJLS', 'LS', 'JLS', 'Yale LJ', 'Harv L Rev',
    'Stan L Rev', 'Colum L Rev', 'Mich L Rev', 'Cal L Rev', 'NYU L Rev',
    'Cornell L Rev', 'Duke LJ', 'Va L Rev', 'Tex L Rev', 'U Chi L Rev',
    'Geo LJ', 'Vand L Rev', 'ICLQ', 'BYIL', 'AJIL', 'EJIL', 'Crim LR',
    'PL', 'Conv', 'JR', 'Jur Rev', 'Edin LR', 'Mod L Rev',
}

CASE_CITATION_PATTERN = re.compile(r'[\[\(](\d{4})[\]\)]\s*(\d+\s+)?([A-Z][A-Za-z\s&\'\.]+?)\s+(\d+)')


def classify_reporter(reporter: str) -> Optional[str]:
    """Classify a reporter abbreviation by jurisdiction."""
    reporter_clean = reporter.strip().upper().replace('.', '').replace(' ', '')

    for reporters, jurisdiction in [
        (UK_REPORTERS, 'UK'), (AU_REPORTERS, 'AU'), (USA_REPORTERS, 'USA'),
        (CAN_REPORTERS, 'CAN'), (IND_REPORTERS, 'IND'), (NZ_REPORTERS, 'NZ'),
        (SG_REPORTERS, 'SG'), (EU_REPORTERS, 'EU'), (OTHER_REPORTERS, 'OTHER'),
        (ACADEMIC_JOURNALS, 'ACADEMIC')
    ]:
        for r in reporters:
            r_clean = r.upper().replace('.', '').replace(' ', '')
            if reporter_clean == r_clean or reporter_clean.startswith(r_clean):
                return jurisdiction
    return None


def count_citations_by_jurisdiction(text: str) -> Dict[str, int]:
    """Count case citations by jurisdiction."""
    counts = {
        'UK': 0, 'AU': 0, 'USA': 0, 'CAN': 0, 'IND': 0,
        'NZ': 0, 'SG': 0, 'EU': 0, 'OTHER': 0, 'ACADEMIC': 0, 'total': 0
    }

    for match in CASE_CITATION_PATTERN.findall(text):
        reporter = match[2].strip()
        if len(reporter) >= 2 and not reporter.isdigit():
            jurisdiction = classify_reporter(reporter)
            if jurisdiction:
                counts[jurisdiction] += 1
                counts['total'] += 1

    return counts


# ============================================================================
# HTML FORMAT DETECTION (UK HL v2.0)
# ============================================================================

def detect_html_format_type(soup: BeautifulSoup) -> str:
    """Detect HTML format type: old (1709-~1900), mid (~1900-1990s), or modern (1990s-2009)."""
    # Check for old semantic tags
    if soup.find('facts') or soup.find('headnote') or soup.find('judgment'):
        return 'old'

    # Check for modern format
    html_str = str(soup).lower()
    if 'opinions of the lords' in html_str:
        if soup.find('li', {'value': True}):
            return 'modern'

    return 'mid'


def is_page_marker(element) -> bool:
    """Check if element is a page number marker (e.g., Page: 5)."""
    from bs4 import Tag
    if isinstance(element, Tag):
        text = element.get_text().strip()
        if re.match(r'^Page:\s*\d+', text):
            return True
        style = element.get('style', '')
        if 'color:#006600' in style and 'Page:' in text:
            return True
    return False


def is_source_boilerplate_element(element) -> bool:
    """Check if element is source database navigation/boilerplate."""
    from bs4 import Tag
    if isinstance(element, Tag):
        text = element.get_text().strip().lower()
        boilerplate_patterns = [
            'home', 'databases',
            'printable', 'help', 'feedback',
            'you are here', 'cite as:', 'url:',
        ]
        for pattern in boilerplate_patterns:
            if text.startswith(pattern):
                return True
    return False
