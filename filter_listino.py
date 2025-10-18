import os
import io
import csv
import datetime
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
FTP_OUTPUT_DIR = os.getenv("FTP_OUTPUT_DIR", "/")

FILTER_MATCH = os.getenv("FILTER_MATCH", "LISTINO VENDITA 6")
FILTER_MODE = os.getenv("FILTER_MODE", "any")   # 'any' | 'column'
FILTER_COLUMN = os.getenv("FILTER_COLUMN", "")

# =========================
# Connessione FTP/FTPS
# =========================
def connect_ftp():
    """Prova FTPS, se fallisce usa FTP semplice."""
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
# Utility
# =========================
def split_dir_and_file(path: str):
    path = path.replace("\\", "/")
    parts = path.rsplit("/", 1)
    if len(parts) == 1:
        return "/", parts[0]
    return parts[0] or "/", parts[1]

def cd(ftp, path: str):
    if not path or path == "/":
        ftp.cwd("/")
        return
    path = path.strip("/")
    if not path:
        ftp.cwd("/")
        return
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

def download_file(ftp, remote_path: str) -> bytes:
    directory, filename = split_dir_and_file(remote_path)
    cd(ftp, directory)
    buf = io.BytesIO()
    ftp.retrbinary(f"RETR {filename}", buf.write)
    buf.seek(0)
    return buf.read()

def upload_bytes(ftp, remote_dir: str, filename: str, data: bytes):
    cd(ftp, remote_dir)
    ftp.storbinary(f"STOR {filename}", io.BytesIO(data))

# =========================
# CSV helpers
# =========================
def guess_csv(sample: bytes):
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
            escapechar = None
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
# Calcolo PREZZO_SCONTATO
# =========================
def to_number(val):
    if val is None:
        return None
    s = str(val).strip().replace("€", "").replace(" ", "").replace("\u00A0", "")
    if not s:
        return None
    if "," in s and "." in s:
        s = s.replace(".", "").replace(",", ".")
    else:
        s = s.replace(",", ".")
    try:
        return float(s)
    except ValueError:
        return None

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
    print(f"[INFO] Aggiunta colonna PREZZO_SCONTATO ({count} righe elaborate).")

# =========================
# Main
# =========================
def main():
    if not (FTP_HOST and FTP_USER and FTP_PASS and FTP_INPUT_PATH):
        print("[ERRORE] Manca una variabile d'ambiente obbligatoria.")
        return

    ftp = connect_ftp()
    try:
        raw = download_file(ftp, FTP_INPUT_PATH)
        dialect, has_header = guess_csv(raw)

        text = raw.decode("utf-8", errors="replace")
        reader = csv.reader(io.StringIO(text), dialect=dialect)
        rows = list(reader)
        if not rows:
            print("[ERRORE] File CSV vuoto.")
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

        ts = datetime.datetime.now().strftime("%Y%m%d_%H%M%S")
        out_name = f"LISTINI_LISTINO_VENDITA_6_{ts}.csv"
        upload_bytes(ftp, FTP_OUTPUT_DIR, out_name, out_bytes)
        print(f"[OK] File creato e caricato: {FTP_OUTPUT_DIR}/{out_name}")
    finally:
        try:
            ftp.quit()
        except Exception:
            pass

if __name__ == "__main__":
    main()
