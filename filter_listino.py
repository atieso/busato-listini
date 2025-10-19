import os
import io
import csv
from ftplib import FTP, FTP_TLS, error_perm

# =========================
# ENV
# =========================
FTP_HOST = os.getenv("FTP_HOST")
FTP_USER = os.getenv("FTP_USER")
FTP_PASS = os.getenv("FTP_PASS")
FTP_PORT = int(os.getenv("FTP_PORT", "21"))
FTP_SECURE = os.getenv("FTP_SECURE", "true").lower() == "true"

FTP_INPUT_PATH = os.getenv("FTP_INPUT_PATH")  # es: /public_html/.../LISTINI.CSV
FILTER_MATCH = os.getenv("FILTER_MATCH", "LISTINO VENDITA 6")
FILTER_MODE = os.getenv("FILTER_MODE", "any")   # 'any' | 'column'
FILTER_COLUMN = os.getenv("FILTER_COLUMN", "")
OUTPUT_FILENAME = os.getenv("OUTPUT_FILENAME", "LISTINI_LISTINO_VENDITA_6.csv")
OUTPUT_DECIMAL = (os.getenv("OUTPUT_DECIMAL") or "dot").strip().lower()  # 'dot' | 'comma'

# Forza il delimitatore CSV (es. ';'). Se vuoto o 'auto', si tenta lo sniff.
CSV_DELIMITER = (os.getenv("CSV_DELIMITER") or "auto").strip().lower()

# =========================
# FTP
# =========================
def connect_ftp():
    if FTP_SECURE:
        try:
            ftps = FTP_TLS()
            ftps.connect(FTP_HOST, FTP_PORT, timeout=60)
            ftps.auth()
            ftps.prot_p()
            ftps.login(FTP_USER, FTP_PASS)
            ftps.set_pasv(True)
            print("[INFO] Connesso via FTPS.")
            return ftps
        except Exception as e:
            print(f"[WARN] FTPS fallito: {e}. Uso FTP normale...")
    ftp = FTP()
    ftp.connect(FTP_HOST, FTP_PORT, timeout=60)
    ftp.login(FTP_USER, FTP_PASS)
    ftp.set_pasv(True)
    print("[INFO] Connesso via FTP.")
    return ftp

def split_dir_and_file(path):
    path = path.replace("\\", "/")
    parts = path.rsplit("/", 1)
    if len(parts) == 1:
        return "/", parts[0]
    return parts[0] or "/", parts[1]

def cd(ftp, path):
    if not path or path == "/":
        ftp.cwd("/")
        return
    path = path.strip("/")
    ftp.cwd("/")
    for seg in path.split("/"):
        if not seg:
            continue
        try:
            ftp.cwd(seg)
        except error_perm:
            try:
                ftp.mkd(seg)
            except Exception:
                pass
            ftp.cwd(seg)

def download_file(ftp, remote_path):
    directory, filename = split_dir_and_file(remote_path)
    cd(ftp, directory)
    buf = io.BytesIO()
    ftp.retrbinary(f"RETR {filename}", buf.write)
    buf.seek(0)
    return buf.read(), directory

def upload_bytes(ftp, remote_dir, filename, data):
    cd(ftp, remote_dir)
    ftp.storbinary(f"STOR {filename}", io.BytesIO(data))

# =========================
# CSV
# =========================
def guess_csv(sample):
    """Ritorna (dialect, has_header). Se CSV_DELIMITER è impostato, lo usa."""
    text = sample.decode("utf-8", errors="replace")
    if CSV_DELIMITER and CSV_DELIMITER != "auto":
        # forza il delimitatore scelto
        class Forced(csv.Dialect):
            delimiter = CSV_DELIMITER
            quotechar = '"'
            escapechar = None
            doublequote = True
            skipinitialspace = False
            lineterminator = "\n"
            quoting = csv.QUOTE_MINIMAL
        # header heuristic
        has_header = csv.Sniffer().has_header(text[:4096])
        print(f"[INFO] Delimitatore forzato: '{CSV_DELIMITER}'")
        return Forced(), has_header

    # auto-sniff con preferenza europea (privilegia ';' se presente)
    try:
        first_line = text.splitlines()[0] if text else ""
        if ";" in first_line and "," in first_line:
            # se la prima riga contiene sia ';' che ',' (numeri), meglio ';'
            class Pref(csv.Dialect):
                delimiter = ";"
                quotechar = '"'
                escapechar = None
                doublequote = True
                skipinitialspace = False
                lineterminator = "\n"
                quoting = csv.QUOTE_MINIMAL
            has_header = csv.Sniffer().has_header(text[:4096])
            print("[INFO] Heuristica: preferito ';' come delimitatore.")
            return Pref(), has_header

        sniffer = csv.Sniffer()
        dialect = sniffer.sniff(text[:4096], delimiters=[",", ";", "\t", "|", ":"])
        has_header = sniffer.has_header(text[:4096])
        print(f"[INFO] Delimitatore auto-rilevato: '{dialect.delimiter}'")
        return dialect, has_header
    except Exception:
        class Simple(csv.Dialect):
            delimiter = ";"
            quotechar = '"'
            escapechar = None
            doublequote = True
            skipinitialspace = False
            lineterminator = "\n"
            quoting = csv.QUOTE_MINIMAL
        print("[WARN] Sniffer fallito. Uso fallback ';'.")
        return Simple(), True

def filter_rows(rows, headers):
    filtered = []
    if FILTER_MODE == "column" and FILTER_COLUMN in headers:
        idx = headers.index(FILTER_COLUMN)
        for r in rows:
            if len(r) > idx and (r[idx] or "").strip() == FILTER_MATCH:
                filtered.append(r)
    else:
        for r in rows:
            if any((c or "").strip() == FILTER_MATCH for c in r):
                filtered.append(r)
    return filtered

# =========================
# NUMERIC PARSER (robusto)
# =========================
def to_number(val):
    """
    Converte stringhe monetarie in float, evitando x100.
    Esempi:
      '2,53000' -> 2.53
      '2.530,00' -> 2530.00
      '2,530' -> 2530
      '2.530' -> 2530
    """
    if val is None:
        return None
    s = str(val).strip()
    if not s:
        return None

    neg = False
    if s.startswith("(") and s.endswith(")"):
        neg = True
        s = s[1:-1].strip()
    if s.startswith("-"):
        neg = True
        s = s[1:].strip()

    for ch in ["€", " ", "\u00A0"]:
        s = s.replace(ch, "")

    has_comma = "," in s
    has_dot = "." in s

    def is_thousands_tail(tail):
        return len(tail) in (3, 6, 9) and tail.isdigit()

    if has_comma and has_dot:
        # decimale = separatore più a destra
        if s.rfind(",") > s.rfind("."):
            s = s.replace(".", "").replace(",", ".")  # EU
        else:
            s = s.replace(",", "")                     # US
    elif has_comma:
        tail = s.split(",")[-1]
        if is_thousands_tail(tail) and s.count(",") >= 1:
            s = s.replace(",", "")
        else:
            s = s.replace(",", ".")
    elif has_dot:
        tail = s.split(".")[-1]
        if is_thousands_tail(tail) and s.count(".") >= 1:
            s = s.replace(".", "")
        # else: '.' già decimale

    try:
        num = float(s)
        if neg:
            num = -num
        return num
    except ValueError:
        return None

# =========================
# PREZZO_SCONTATO
# =========================
def add_prezzo_scontato(headers, rows):
    def find_col(name):
        name = name.lower()
        for i, h in enumerate(headers):
            if str(h).strip().lower() == name:
                return i
        return None

    idx_prezzo = find_col("liprezzo")
    idx_sconto = find_col("liscont1")

    if idx_prezzo is None or idx_sconto is None:
        print("[WARN] Colonne LIPREZZO/LISCONT1 non trovate. Nessun calcolo.")
        return

    if "PREZZO_SCONTATO" not in [str(h).strip() for h in headers]:
        headers.append("PREZZO_SCONTATO")

    count = 0
    for r in rows:
        if len(r) <= max(idx_prezzo, idx_sconto):
            r.append("")
            continue
        prezzo = to_number(r[idx_prezzo])
        sconto = to_number(r[idx_sconto])

        if prezzo is None:
            r.append("")
            continue

        # Normalizza lo sconto:
        # - vuoto/None → 0
        # - se negativo, usa il valore assoluto (sconto sempre in sottrazione)
        # - cap a 100 per sicurezza
        if sconto is None:
            sconto = 0.0
        else:
            sconto = abs(sconto)
            if sconto > 100:
                sconto = 100.0

        prezzo_scontato = prezzo * (1.0 - sconto / 100.0)
        r.append(fmt_decimal(prezzo_scontato))

        count += 1

    print(f"[INFO] PREZZO_SCONTATO calcolato su {count} righe.")


def fmt_decimal(num: float) -> str:
    s = f"{num:.2f}"
    return s if OUTPUT_DECIMAL == "dot" else s.replace(".", ",")
    
# =========================
# MAIN
# =========================
def main():
    if not (FTP_HOST and FTP_USER and FTP_PASS and FTP_INPUT_PATH):
        print("[ERRORE] Manca una variabile d'ambiente obbligatoria.")
        return

    ftp = connect_ftp()
    raw, input_dir = download_file(ftp, FTP_INPUT_PATH)
    dialect, has_header = guess_csv(raw)

    text = raw.decode("utf-8", errors="replace")
    reader = csv.reader(io.StringIO(text), dialect=dialect)
    rows = list(reader)
    if not rows:
        print("[ERRORE] File CSV vuoto.")
        ftp.quit()
        return

    headers = rows[0] if has_header else [f"col_{i+1}" for i in range(len(rows[0]))]
    body = rows[1:] if has_header else rows

    filtered = filter_rows(body, headers)
    add_prezzo_scontato(headers, filtered)

    out_io = io.StringIO()
    writer = csv.writer(out_io, dialect=dialect)
    writer.writerow(headers)
    writer.writerows(filtered)
    out_bytes = out_io.getvalue().encode("utf-8")
    

    upload_bytes(ftp, input_dir, OUTPUT_FILENAME, out_bytes)
    print(f"[OK] File creato: {input_dir}/{OUTPUT_FILENAME}")
    ftp.quit()

if __name__ == "__main__":
    main()
