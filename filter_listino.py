import os
import io
import csv
from ftplib import FTP, FTP_TLS, error_perm

# =========================
# Configurazione da variabili d'ambiente
# =========================
FTP_HOST = os.getenv("FTP_HOST")
FTP_USER = os.getenv("FTP_USER")
FTP_PASS = os.getenv("FTP_PASS")
FTP_PORT = int(os.getenv("FTP_PORT", "21"))
FTP_SECURE = os.getenv("FTP_SECURE", "true").lower() == "true"
FTP_INPUT_PATH = os.getenv("FTP_INPUT_PATH")
FILTER_MATCH = os.getenv("FILTER_MATCH", "LISTINO VENDITA 6")
FILTER_MODE = os.getenv("FILTER_MODE", "any")
FILTER_COLUMN = os.getenv("FILTER_COLUMN", "")
OUTPUT_FILENAME = os.getenv("OUTPUT_FILENAME", "LISTINI_LISTINO_VENDITA_6.csv")

# =========================
# Connessione FTP
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

# =========================
# Utility FTP
# =========================
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
# CSV helpers
# =========================
def guess_csv(sample):
    try:
        text = sample.decode("utf-8", errors="replace")
        sniffer = csv.Sniffer()
        dialect = sniffer.sniff(text[:4096], delimiters=[",", ";", "\t", "|", ":"])
        has_header = sniffer.has_header(text[:4096])
        return dialect, has_header
    except Exception:
        class Simple(csv.Dialect):
            delimiter = ";"
            quotechar = '"'
            doublequote = True
            skipinitialspace = False
            lineterminator = "\n"
            quoting = csv.QUOTE_MINIMAL
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
# Parser numerico robusto
# =========================
def to_number(val):
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
        if s.rfind(",") > s.rfind("."):
            s = s.replace(".", "").replace(",", ".")
        else:
            s = s.replace(",", "")
    elif has_comma:
        tail = s.split(",")[-1]
        if is_thousands_tail(tail):
            s = s.replace(",", "")
        else:
            s = s.replace(",", ".")
    elif has_dot:
        tail = s.split(".")[-1]
        if is_thousands_tail(tail):
            s = s.replace(".", "")
    try:
        num = float(s)
        if neg:
            num = -num
        return num
    except ValueError:
        return None

# =========================
# Calcolo PREZZO_SCONTATO
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
        print("[WARN] Colonne LIPREZZO/LISCONT1 non trovate.")
        return
    if "PREZZO_SCONTATO" not in [h.strip().upper() for h in headers]:
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
        if sconto is None or sconto == 0:
            prezzo_scontato = prezzo
        else:
            prezzo_scontato = prezzo * (1 - sconto / 100)
        r.append(f"{prezzo_scontato:.2f}".replace(".", ","))
        count += 1
    print(f"[INFO] PREZZO_SCONTATO calcolato su {count} righe.")

# =========================
# Main
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
    print(f"[OK] File caricato in: {input_dir}/{OUTPUT_FILENAME}")
    ftp.quit()

if __name__ == "__main__":
    main()
