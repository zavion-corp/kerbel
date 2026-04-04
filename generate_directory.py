#!/usr/bin/env python3
"""
Kerbel Life Group Directory Generator
Reads data/data.tsv, downloads photos from Google Drive,
and produces index.html.
"""

import sys
import csv
import re
import base64
import io
import requests
from pathlib import Path
from PIL import Image, ImageOps
import pillow_heif

sys.stdout.reconfigure(encoding="utf-8")

# ─── Paths ────────────────────────────────────────────────────────────────────
BASE_DIR   = Path(r"c:\_data\source\zavion-corp\kerbel-dir")
DATA_FILE  = BASE_DIR / "data" / "data.tsv"
CACHE_DIR  = BASE_DIR / "images"
OUTPUT_WEB = BASE_DIR / "index.html"

CACHE_DIR.mkdir(exist_ok=True)

# ─── Column indices (0-based, no leading row-number column) ──────────────────
# 0=Timestamp, 1=Email, 2=Names, 3=Address, 4=HomePhone,
# 5=CellHis, 6=CellHer, 7=EmailHis, 8=EmailHer,
# 9=BdayHis, 10=BdayHer, 11=Children, 12=MarriedYears,
# 13=Anniversary, 14-20=survey questions, 21=Photo, 22=extra
C_NAMES      = 2
C_ADDRESS    = 3
C_HOME       = 4
C_CELL_HIS   = 5
C_CELL_HER   = 6
C_EMAIL_HIS  = 7
C_EMAIL_HER  = 8
C_BDAY_HIS   = 9
C_BDAY_HER   = 10
C_CHILDREN   = 11
C_MARRIED    = 12
C_ANNIV      = 13
C_PHOTO      = 21


# ─── Helpers ──────────────────────────────────────────────────────────────────

def clean(val):
    return val.strip() if val else ""


def fmt_phone(raw):
    digits = re.sub(r"\D", "", raw)
    if len(digits) == 10:
        return f"({digits[:3]}) {digits[3:6]}-{digits[6:]}"
    if len(digits) == 11 and digits[0] == "1":
        return f"({digits[1:4]}) {digits[4:7]}-{digits[7:]}"
    return raw.strip()


def drive_id(url):
    m = re.search(r"[?&]id=([a-zA-Z0-9_-]+)", url)
    return m.group(1) if m else None


def detect_mime(data: bytes) -> str:
    if data[:3] == b"\xff\xd8\xff":
        return "image/jpeg"
    if data[:8] == b"\x89PNG\r\n\x1a\n":
        return "image/png"
    if data[:4] == b"RIFF" and data[8:12] == b"WEBP":
        return "image/webp"
    if data[:6] in (b"GIF87a", b"GIF89a"):
        return "image/gif"
    return "image/jpeg"


def image_file_to_datauri(path: Path, max_px: int | None = None) -> str:
    """Load any image file (including HEIC) and return a JPEG base64 data-URI.
    If max_px is given, the image is scaled down so its longest side <= max_px.
    """
    if path.suffix.lower() == ".heic":
        pillow_heif.register_heif_opener()
    img = ImageOps.exif_transpose(Image.open(path))
    if max_px:
        img.thumbnail((max_px, max_px), Image.LANCZOS)
    buf = io.BytesIO()
    img.convert("RGB").save(buf, format="JPEG", quality=85)
    return "data:image/jpeg;base64," + base64.b64encode(buf.getvalue()).decode()


def build_manual_lookup() -> dict[str, Path]:
    """
    Scan CACHE_DIR for files that have a real extension (i.e. manually placed,
    not the bare Drive-ID cache files).  Key = lowercase filename stem words.
    Returns a list of (word_set, path) pairs for fuzzy matching.
    """
    IMAGE_EXTS = {".jpg", ".jpeg", ".png", ".gif", ".webp", ".heic"}
    entries = []
    for p in CACHE_DIR.iterdir():
        if p.suffix.lower() in IMAGE_EXTS:
            words = set(re.findall(r"[a-z]+", p.stem.lower()))
            entries.append((words, p))
    return entries


MANUAL_IMAGES = build_manual_lookup()


def find_manual_photo(member_names: str, max_px: int | None = None) -> str | None:
    """Return data-URI for the best manually-placed image match, or None."""
    name_words = set(re.findall(r"[a-z]+", member_names.lower()))
    name_words -= {"and", "the", "mr", "mrs", "dr", "sr", "jr"}

    best_path = None
    best_overlap = 0
    for (file_words, path) in MANUAL_IMAGES:
        overlap = len(name_words & file_words)
        if overlap > best_overlap:
            best_overlap = overlap
            best_path = path

    if best_overlap == 0:
        return None

    print(f"  [manual] {member_names} -> {best_path.name}")
    try:
        return image_file_to_datauri(best_path, max_px=max_px)
    except Exception as e:
        print(f"  [error]  converting {best_path.name}: {e}")
        return None


def download_photo(url: str, label: str) -> str | None:
    """Return a base64 data-URI, or None on failure."""
    fid = drive_id(url)
    if not fid:
        print(f"  [skip] no Drive ID in URL for {label}")
        return None

    cache_path = CACHE_DIR / fid
    if cache_path.exists():
        print(f"  [cache] {label}")
        raw = cache_path.read_bytes()
        return f"data:{detect_mime(raw)};base64,{base64.b64encode(raw).decode()}"

    dl_url = f"https://drive.google.com/uc?export=download&id={fid}"
    headers = {"User-Agent": "Mozilla/5.0"}

    try:
        s = requests.Session()
        r = s.get(dl_url, headers=headers, stream=True, timeout=30)

        # Google sometimes serves an HTML warning page for large files
        ct = r.headers.get("Content-Type", "")
        if "text/html" in ct:
            tok = re.search(r"confirm=([0-9A-Za-z_-]+)", r.text)
            if tok:
                r = s.get(f"{dl_url}&confirm={tok.group(1)}",
                          headers=headers, stream=True, timeout=30)

        if r.status_code != 200:
            print(f"  [fail] HTTP {r.status_code} for {label}")
            return None

        raw = r.content
        ct2 = r.headers.get("Content-Type", "")
        if "text/html" in ct2 or len(raw) < 1000:
            print(f"  [fail] unexpected response for {label} (may need sign-in)")
            return None

        cache_path.write_bytes(raw)
        print(f"  [ok]    {label} — {len(raw)//1024} KB")
        return f"data:{detect_mime(raw)};base64,{base64.b64encode(raw).decode()}"

    except Exception as e:
        print(f"  [error] {label}: {e}")
        return None


# ─── Read data ────────────────────────────────────────────────────────────────

members = []

with open(DATA_FILE, newline="", encoding="utf-8") as f:
    rows = list(csv.reader(f, delimiter="\t"))

for row in rows[1:]:          # skip header
    while len(row) < 24:
        row.append("")

    names = clean(row[C_NAMES])
    photo_url = clean(row[C_PHOTO])

    print(f"\nProcessing: {names or '(no name)'}")
    photo = find_manual_photo(names, max_px=1000) if names else None
    if not photo:
        photo = download_photo(photo_url, names) if photo_url else None

    # Collect phones, deduplicate
    phones = {}
    if clean(row[C_HOME]):
        phones["Home"] = fmt_phone(clean(row[C_HOME]))
    if clean(row[C_CELL_HIS]):
        phones["His cell"] = fmt_phone(clean(row[C_CELL_HIS]))
    if clean(row[C_CELL_HER]):
        phones["Her cell"] = fmt_phone(clean(row[C_CELL_HER]))

    # Collect emails, deduplicate
    emails = {}
    if clean(row[C_EMAIL_HIS]):
        emails["His email"] = clean(row[C_EMAIL_HIS])
    if clean(row[C_EMAIL_HER]):
        emails["Her email"] = clean(row[C_EMAIL_HER])

    members.append({
        "names":    names,
        "address":  clean(row[C_ADDRESS]),
        "phones":   phones,
        "emails":   emails,
        "bday_his": clean(row[C_BDAY_HIS]),
        "bday_her": clean(row[C_BDAY_HER]),
        "children": clean(row[C_CHILDREN]),
        "married":  clean(row[C_MARRIED]),
        "anniv":    clean(row[C_ANNIV]),
        "photo":    photo,
    })

# Sort by last name of whoever is listed first
def sort_key(m):
    parts = m["names"].split()
    return parts[-1].lower() if parts else "zzz"

members.sort(key=sort_key)


# ─── Build HTML ───────────────────────────────────────────────────────────────

PLACEHOLDER_SVG = (
    "data:image/svg+xml;base64,"
    + base64.b64encode(b"""<svg xmlns='http://www.w3.org/2000/svg' width='300' height='380'
        viewBox='0 0 300 380'>
      <rect width='300' height='380' fill='#e8eef5'/>
      <rect x='135' y='60' width='30' height='200' rx='6' fill='#1a3a5c'/>
      <rect x='75' y='110' width='150' height='30' rx='6' fill='#1a3a5c'/>
    </svg>""").decode()
)


def slug(name: str) -> str:
    return re.sub(r"[^a-z0-9]+", "-", name.lower()).strip("-")


def web_card(m: dict) -> str:
    photo_src = m["photo"] or PLACEHOLDER_SVG
    name = m["names"] or "Name not provided"

    def phone_item(label, val):
        if not val:
            return ""
        href = "tel:+" + re.sub(r"\D", "", val)
        return f'<li><span class="lbl">{label}:</span> <a href="{href}">{val}</a></li>'

    def email_item(label, val):
        if not val:
            return ""
        return f'<li><span class="lbl">{label}:</span> <a href="mailto:{val}">{val}</a></li>'

    def info_item(label, val):
        if not val:
            return ""
        return f'<li><span class="lbl">{label}:</span> {val}</li>'

    def address_item(val):
        if not val:
            return ""
        from urllib.parse import quote
        href = "https://maps.google.com/?q=" + quote(val)
        return f'<li><span class="lbl">Address:</span> <a href="{href}" target="_blank">{val}</a></li>'

    details = "".join([
        address_item(m["address"]),
        *[phone_item(k, v) for k, v in m["phones"].items()],
        *[email_item(k, v) for k, v in m["emails"].items()],
        info_item("His Birthday",   m["bday_his"]),
        info_item("Her Birthday",   m["bday_her"]),
        info_item("Anniversary",    m["anniv"]),
        info_item("Married",        m["married"]),
        info_item("Children",       m["children"]),
    ])

    return f"""
    <article class="card" id="{slug(name)}">
      <div class="card-photo">
        <img src="{photo_src}" alt="Photo of {name}" class="thumb" title="Click to enlarge">
      </div>
      <div class="card-info">
        <h2>{name}</h2>
        <ul>{details}</ul>
      </div>
    </article>"""


# Build alphabet index (letters that have at least one member)
used_letters = sorted({sort_key(m)[0].upper() for m in members if sort_key(m)})
alpha_links = " ".join(
    f'<a href="#{l.lower()}">{l}</a>' for l in used_letters
)

# Group members under letter headings
sections_html = []
current_letter = None
for m in members:
    first_letter = sort_key(m)[0].upper() if sort_key(m) else "#"
    if first_letter != current_letter:
        current_letter = first_letter
        sections_html.append(
            f'<h3 class="alpha-heading" id="{first_letter.lower()}">{first_letter}</h3>'
        )
    sections_html.append(web_card(m))

WEB_CSS = """
  *, *::before, *::after { box-sizing: border-box; margin: 0; padding: 0; }

  :root {
    --navy: #1a3a5c;
    --navy-light: #e8eef5;
    --text: #222;
    --muted: #555;
    --border: #ddd;
  }

  body {
    font-family: -apple-system, BlinkMacSystemFont, 'Segoe UI', sans-serif;
    background: #f4f6f9;
    color: var(--text);
    line-height: 1.5;
  }

  /* ── Header ── */
  header {
    background: var(--navy);
    color: #fff;
    padding: 1.5rem 2rem 1rem;
    position: sticky;
    top: 0;
    z-index: 100;
    box-shadow: 0 2px 8px rgba(0,0,0,0.3);
  }
  header h1 { font-size: 1.4rem; letter-spacing: 2px; text-transform: uppercase; }
  header p  { font-size: 0.85rem; opacity: 0.75; margin-top: 2px; }

  /* ── Search ── */
  .search-bar {
    margin-top: 0.75rem;
    display: flex;
    gap: 0.5rem;
  }
  .search-bar input {
    flex: 1;
    max-width: 340px;
    padding: 0.4rem 0.75rem;
    border: none;
    border-radius: 4px;
    font-size: 0.95rem;
  }

  /* ── Alpha index ── */
  .alpha-index {
    background: var(--navy-light);
    padding: 0.6rem 2rem;
    display: flex;
    flex-wrap: wrap;
    gap: 0.4rem;
    border-bottom: 1px solid var(--border);
  }
  .alpha-index a {
    color: var(--navy);
    font-weight: bold;
    font-size: 0.95rem;
    text-decoration: none;
    padding: 2px 6px;
    border-radius: 3px;
  }
  .alpha-index a:hover { background: var(--navy); color: #fff; }

  /* ── Main layout ── */
  main {
    max-width: 860px;
    margin: 2rem auto;
    padding: 0 1rem;
  }

  .alpha-heading {
    font-size: 1.6rem;
    color: var(--navy);
    border-bottom: 2px solid var(--navy);
    margin: 2rem 0 1rem;
    padding-bottom: 0.25rem;
  }

  /* ── Member card ── */
  .card {
    background: #fff;
    border: 1px solid var(--border);
    border-radius: 8px;
    margin-bottom: 1.25rem;
    display: flex;
    gap: 1.5rem;
    padding: 1.25rem;
    box-shadow: 0 1px 4px rgba(0,0,0,0.06);
  }
  .card:target { outline: 3px solid var(--navy); }

  .card-photo {
    flex: 0 0 160px;
    display: flex;
    align-items: flex-start;
  }
  .card-photo img {
    width: 160px;
    max-height: 200px;
    object-fit: contain;
    border-radius: 5px;
    border: 1px solid var(--border);
    display: block;
  }

  .card-info { flex: 1; }
  .card-info h2 {
    font-size: 1.2rem;
    color: var(--navy);
    margin-bottom: 0.6rem;
  }

  .card-info ul {
    list-style: none;
    font-size: 0.92rem;
    display: grid;
    grid-template-columns: 1fr 1fr;
    gap: 0.25rem 1rem;
  }
  .card-info li { color: var(--muted); }
  .lbl { font-weight: 600; color: var(--text); }
  .card-info a { color: var(--navy); text-decoration: none; }
  .card-info a:hover { text-decoration: underline; }

  /* ── Hidden by search ── */
  .card.hidden { display: none; }

  /* ── Lightbox ── */
  .thumb { cursor: zoom-in; }

  #lightbox {
    display: none;
    position: fixed;
    inset: 0;
    background: rgba(0,0,0,0.85);
    z-index: 1000;
    align-items: center;
    justify-content: center;
  }
  #lightbox.open { display: flex; }
  #lightbox img {
    max-width: 92vw;
    max-height: 92vh;
    object-fit: contain;
    border-radius: 4px;
    box-shadow: 0 8px 40px rgba(0,0,0,0.6);
  }
  #lightbox-close {
    position: fixed;
    top: 1rem;
    right: 1.25rem;
    font-size: 2rem;
    color: #fff;
    cursor: pointer;
    line-height: 1;
    user-select: none;
  }

  /* ── Responsive ── */
  @media (max-width: 560px) {
    .card { flex-direction: column; }
    .card-photo { flex: none; }
    .card-photo img { width: 100%; max-height: 260px; }
    .card-info ul { grid-template-columns: 1fr; }
  }
"""

web_html = f"""<!DOCTYPE html>
<html lang="en">
<head>
  <meta charset="UTF-8">
  <meta name="viewport" content="width=device-width, initial-scale=1.0">
  <title>Kerbel Life Group — Member Directory</title>
  <link rel="icon" type="image/svg+xml" href="data:image/svg+xml;base64,{base64.b64encode(b"<svg xmlns='http://www.w3.org/2000/svg' viewBox='0 0 32 32'><rect width='32' height='32' rx='4' fill='#1a3a5c'/><text x='16' y='23' font-family='Arial,sans-serif' font-size='13' font-weight='bold' fill='#fff' text-anchor='middle'>KLG</text></svg>").decode()}">
  <style>{WEB_CSS}</style>
</head>
<body>

<header>
  <h1>Kerbel Life Group</h1>
  <p>Member Directory &nbsp;&middot;&nbsp; 2026</p>
  <div class="search-bar">
    <input id="search" type="search" placeholder="Search members..." autocomplete="off">
  </div>
</header>

<nav class="alpha-index">{alpha_links}</nav>

<main>
  {"".join(sections_html)}
</main>

<div id="lightbox">
  <span id="lightbox-close" title="Close">&times;</span>
  <img id="lightbox-img" src="" alt="">
</div>

<script>
  // Search
  const input = document.getElementById('search');
  input.addEventListener('input', () => {{
    const q = input.value.toLowerCase();
    document.querySelectorAll('article.card').forEach(card => {{
      card.classList.toggle('hidden', q.length > 0 && !card.textContent.toLowerCase().includes(q));
    }});
  }});

  // Lightbox
  const lightbox    = document.getElementById('lightbox');
  const lightboxImg = document.getElementById('lightbox-img');

  document.querySelectorAll('img.thumb').forEach(img => {{
    img.addEventListener('click', () => {{
      lightboxImg.src = img.src;
      lightboxImg.alt = img.alt;
      lightbox.classList.add('open');
    }});
  }});

  function closeLightbox() {{
    lightbox.classList.remove('open');
    lightboxImg.src = '';
  }}

  document.getElementById('lightbox-close').addEventListener('click', closeLightbox);
  lightbox.addEventListener('click', e => {{ if (e.target === lightbox) closeLightbox(); }});
  document.addEventListener('keydown', e => {{ if (e.key === 'Escape') closeLightbox(); }});
</script>

</body>
</html>
"""

OUTPUT_WEB.write_text(web_html, encoding="utf-8")
print(f"\nDone! Directory written to:\n  {OUTPUT_WEB}")
