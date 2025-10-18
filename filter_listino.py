import os
import io
import csv
import datetime
from ftplib import FTP, FTP_TLS, error_perm

# =========================
# Config da variabili d'ambiente
# =========================
FTP_HOST = os.getenv("FTP_HOST")
FTP_USER = os.getenv("FTP_USER")
FTP_PASS = os.getenv("FTP_PASS")
FTP_PORT = int(os.getenv("FTP_PORT", "21"))
FTP_SECURE = os.getenv("FTP_SECURE", "true").strip().lower() == "true"

FTP_INPUT_PATH = os.getenv("FTP_INPUT_PATH")  # es: /public_html/IMPORT_DATI_FULL_20230919_0940/LISTINI.CSV
FTP_OUTPUT_DIR = os.getenv("FTP_OUTPUT_DIR", "/")

FILTER_MATCH = os.getenv("FILTER_MATCH", "LISTINO VENDITA 6")
FILTER_MODE = os.getenv("FILTER_MODE", "any")   # 'any' | 'column'
FILTER_COLUMN = os.getenv("FILTER_COLUMN", "")

# =========================
# Connessione FTP/FTPS
# =========================
def _connect():
    """Prova FTPS (explicit). Se fallisce, fallback a FTP semplice."""
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
            print(f"[WARN] FTPS fallito: {e}. Fallback a FTP semplice...")
    ftp = FTP()
    ftp.connect(FTP_HOST, FTP_PORT, timeout=60)
    ftp.login(FTP_USER, FTP_PASS)
    ftp.set_pasv(True)
    print("[INFO] Connesso via FTP.")
    return ftp

# =========================
# Utility path e I/O
# =========================
def _split_dir_and_file(path: str):
    path = (path or "").replace("\\", "/")
    parts = path.rsplit("/", 1)
    if len(parts) == 1:
        return "/", parts[0]
    return parts[0] if parts[0] else "/", parts[1]

def _cd(ftp, path: str):
    if not path or path == "/":
        ftp.cwd("/")
        return
    path = path.strip("/")
    if not path:
        ftp.cwd("/")
        return
    segments = path.split("/")
    ftp.cwd("/")
    for seg in segments:
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

def _download_file(ftp, remote_path: str) -> bytes:
    dirpath, filename = _split_dir_and_file(remote_path)
    _cd(ftp, dirpath)
    buf = io.BytesIO()
    ftp.retrbinary(f"RETR {filename}", buf.write)
    buf.seek(0)
    return buf.read()

def _upload_bytes(ftp, remote_dir: str, filename: str, data: bytes):
    _cd(ftp, remote_dir)
    bio = io.BytesIO(data)
    ftp.storbinary(f"STOR {filename}", bio)

# =========================
# CSV helpers
# =========================
def _guess_csv(sample: bytes):
    """
    Ritorna (dialect_or_none, fmtparams_dict, has_header_bool).
    Se lo sniff fallisce, usa fallback delimiter=';'.
    """
    try:
        sample_txt = sample.decode("utf-8", errors="replace")
        sniffer = csv.Sniffer()
        dialect = sniffer.sniff(sample_txt[:4096], delimiters=[",", ";", "\t", "|", ":"])
        has_header = sniffer.has_header(sample_txt[:4096])
        return dialect, {}, has_header
    except Exception:
        # fallback
        fmt = {
            "delimiter": ";",
            "quotechar": '"',
            "doublequote": True,
            "skipinitialspace": False,
            "lineterminator": "\n",
            "quoting": csv.QUOTE_MINIMAL,
        }
        return None, fmt, True

def _filter_rows(rows, headers):
    """Filtra le righe in base a FILTER_*."""
    filtered = []
    if FILTER_MODE == "column" and FILTER_COLUMN:
        if FILTER_COLUMN not in headers:
            print(f"[WARN] Colonna '{FILTER_COLUMN}' non trovata. Nessun filtro applicato.")
            return []
        idx = headers.index(FILTER_COLUMN)
        for r in rows:
            try:
                if (r[idx] or "").strip() == FILTER_MATCH:
                    filtered.append(r)
            except IndexError:
                continue
    else:
        for r in rows:
            if any((c or "").strip() == FILTER_MATCH for c in r):
                filtered.append(r)
    return filtered

# =========================
# Calcolo PREZZO_SCONTATO
# =========================
def _to_number(value):
    """Converte stringhe tipo '1.234,56 €' in float; None se non convertibile."""
    if value is None:
        return None
    s = str(value).strip()
    if not s:
        return None
    for ch in " €\u00A0":
        s = s.replace(ch, "")
    if "," in s and "." in s:
        # supponiamo formato EU: 1.234,56
        s = s.replace(".", "").replace(",", ".")
    else:
        s = s.replace(",", ".")
    try:
        return float(s)
    except ValueError:
        return None

def _add_prezzo_scontato(headers, rows):
    """
    Aggiunge la colonna PREZZO_SCONTATO calcolata da:
    LIPREZZO * (1 - LISCONT1/100). Se LISCONT1 è 0 o vuoto -> LIPREZZO.
    Opera su 'rows' (lista di liste), modifica in-place.
    """
    # trova colonne per nome, case-insensitive
    def find_col(name):
        name = name.strip().lower()
        for i, h in enumerate(headers):
            if str(h).strip().lower() == name:
                return i
        return None

    idx_prezzo = find_col("liprezzo")
    idx_sconto = find_col("liscont1")

    if idx_prezzo is None or idx_sconto is None:
        print("[WARN] Colonne 'LIPREZZO' o 'LISCONT1' non trovate negli header. Nessuna colonna PREZZO_SCONTATO aggiunta.")
        return

    # evita duplicati header se già esistente
    if "PREZZO_SCONTATO" not in [str(h).strip() for h in headers]:
        headers.append("PREZZO_SCONTATO")

    count = 0
    for r in rows:
        # protezione per righe troppo corte
        if len(r) <= max(idx_prezzo, idx_sconto):
            r.append("")
            continue

        prezzo = _to_number(r[idx_prezzo])
        sconto = _to_number(r[idx_sconto])

        if prezzo is None:
            r.append("")  # non calcolabile
            continue

        if sconto is None or sconto == 0:
            prezzo_scontato = prezzo
        else:
            prezzo_scontato = prezzo * (1.0 - (sconto / 100.0))

        # formato EU: virgola decimale
        r.append(f"{prezzo_scontato:.2f}".replace(".", ","))
        count += 1

    print(f"[INFO] Aggiunta colonna PREZZO_SCONTATO ({count} righe elaborate).")

# =========================
# Main
# =========================
def main():
    if not (FTP_HOST and FTP_USER and FTP_PASS and FTP_INPUT_PATH):
        raise SystemExit("[ERROR] Manca una o più variabili d'ambiente: FTP_HOST, FTP_USER, FTP_PASS, FTP_INPUT_PATH.")

    ftp = _connect()
    try:
        # Scarica file
        raw = _download_file(ftp, FTP_INPUT_PATH)

        # Sniff CSV
        dialect, fmtparams, has_header = _guess_csv(raw)

        # Decode robusto
        text = None
        for enc in ("utf-8-sig", "utf-8", "cp1252", "latin-1"):
            try:
                text = raw.decode(enc)
                break
            except UnicodeDecodeError:
                continue
        if text is None:
            text = raw.decode("latin-1", errors="replace")

        # Leggi CSV
        sio = io.StringIO(text)
        if dialect is not None:
            reader = csv.reader(sio, dialect=dialect)
        else:
            reader = csv.reader(sio, **fmtparams)

        rows_all = list(reader)
        if not rows_all:
            raise SystemExit("[ERROR] Il file CSV è vuoto.")

        # Header / corpo
        if has_header:
            headers = rows_all[0]
            body = rows_all[1:]
        else:
            width = max(len(r) for r in rows_all)
            headers = [f"col_{i+1}" for i in range(width)]
            body = rows_all

        # Filtra solo le righe del listino richiesto
        filtered = _filter_rows(body, headers)

        # Aggiungi colonna PREZZO_SCONTATO
        _add_prezzo_scontato(headers, filtered)

        # Scrivi output CSV
        out_io = io.StringIO()
        if dialect is not None:
            writer = csv.writer(out_io, dialect=dialect)
        else:
            writer = csv.writer(out_io, **fmtparams)
        writer.writerow(headers)
        writer.writerows(filtered)
        out_bytes = out_io.getvalue().encode("utf-8")

        # Upload
        ts = datetime.datetime.now().strftime("%Y%m%d_%H%M%S")
        out_name = f"LISTINI_LISTINO_VENDITA_6_{ts}.csv"
        remote_dir = FTP_OUTPUT_DIR if FTP_OUTPUT_DIR else "/"
        _upload_bytes(ftp, remote_dir, out_name, out_bytes)

        print(f"[OK] File caricato: {remote_dir}/{out_name} | Righe filtrate: {len(filtered)}")
    finally:
        try:
            ftp.quit()
        except Exception:
            pass

if __name__ == "__main__":
    main()
