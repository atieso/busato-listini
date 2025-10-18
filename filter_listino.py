import os
import io
import csv
import datetime
from ftplib import FTP, FTP_TLS, error_perm

# --- Config da ENV ---
FTP_HOST = os.getenv("FTP_HOST")
FTP_USER = os.getenv("FTP_USER")
FTP_PASS = os.getenv("FTP_PASS")
FTP_PORT = int(os.getenv("FTP_PORT", "21"))
FTP_SECURE = os.getenv("FTP_SECURE", "true").lower() == "true"

FTP_INPUT_PATH = os.getenv("FTP_INPUT_PATH")
FTP_OUTPUT_DIR = os.getenv("FTP_OUTPUT_DIR", "/")

FILTER_MATCH = os.getenv("FILTER_MATCH", "LISTINO VENDITA 6")
FILTER_MODE = os.getenv("FILTER_MODE", "any")   # 'any' | 'column'
FILTER_COLUMN = os.getenv("FILTER_COLUMN", "")  # usato solo se FILTER_MODE='column'

# --- Utility ---

def _connect():
    """
    Tenta FTPS (TLS). Se fallisce, fa fallback a FTP plain.
    """
    if FTP_SECURE:
        try:
            ftps = FTP_TLS()
            ftps.connect(FTP_HOST, FTP_PORT, timeout=60)
            ftps.auth()
            ftps.prot_p()   # protezione dati
            ftps.login(FTP_USER, FTP_PASS)
            ftps.set_pasv(True)
            print("[INFO] Connesso via FTPS.")
            return ftps
        except Exception as e:
            print(f"[WARN] FTPS fallito: {e}. Fallback a FTP semplice...")
    # FTP plain
    ftp = FTP()
    ftp.connect(FTP_HOST, FTP_PORT, timeout=60)
    ftp.login(FTP_USER, FTP_PASS)
    ftp.set_pasv(True)
    print("[INFO] Connesso via FTP.")
    return ftp

def _split_dir_and_file(path: str):
    """
    Restituisce (dirpath, filename) a partire da un path assoluto tipo /a/b/c.csv
    """
    path = path.replace("\\", "/")
    parts = path.rsplit("/", 1)
    if len(parts) == 1:
        return "/", parts[0]
    return parts[0] if parts[0] else "/", parts[1]

def _cd(ftp, path: str):
    """
    Cambia directory in modo robusto, creando le cartelle se necessario.
    """
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
            # prova a creare e poi cd
            try:
                ftp.mkd(seg)
            except Exception:
                pass
            ftp.cwd(seg)

def _download_file(ftp, remote_path: str) -> bytes:
    """
    Scarica un file binario e restituisce bytes.
    """
    dirpath, filename = _split_dir_and_file(remote_path)
    _cd(ftp, dirpath)
    buf = io.BytesIO()
    ftp.retrbinary(f"RETR {filename}", buf.write)
    buf.seek(0)
    return buf.read()

def _guess_dialect(sample: bytes):
    """
    Prova a sniffare il dialetto CSV (delimitatore ecc).
    Fallback: delimiter=',' con quote standard.
    """
    try:
        sample_txt = sample.decode("utf-8", errors="replace")
        dialect = csv.Sniffer().sniff(sample_txt[:4096], delimiters=[",", ";", "\t", "|", ":"])
        has_header = csv.Sniffer().has_header(sample_txt[:4096])
        return dialect, has_header
    except Exception:
        class _Fallback(csv.Dialect):
            delimiter = ";"
            quotechar = '"'
            escapechar = None
            doublequote = True
            skipinitialspace = False
            lineterminator = "\n"
            quoting = csv.QUOTE_MINIMAL
        return _Fallback(), True

def _filter_rows(rows, headers):
    """
    Filtra le righe:
    - se FILTER_MODE='any', include la riga se QUALSIASI cella == FILTER_MATCH (match esatto, case sensitive).
    - se FILTER_MODE='column', include la riga se la colonna con header=FILTER_COLUMN == FILTER_MATCH.
    """
    filtered = []
    if FILTER_MODE == "column":
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
        # any
        for r in rows:
            if any((c or "").strip() == FILTER_MATCH for c in r):
                filtered.append(r)
    return filtered

def _upload_bytes(ftp, remote_dir: str, filename: str, data: bytes):
    """
    Carica bytes come file nel path remoto specificato.
    """
    _cd(ftp, remote_dir)
    bio = io.BytesIO(data)
    ftp.storbinary(f"STOR {filename}", bio)

def main():
    if not (FTP_HOST and FTP_USER and FTP_PASS and FTP_INPUT_PATH):
        raise SystemExit("[ERROR] Manca una o più variabili d'ambiente obbligatorie: FTP_HOST, FTP_USER, FTP_PASS, FTP_INPUT_PATH.")

    ftp = _connect()

    try:
        raw = _download_file(ftp, FTP_INPUT_PATH)
        dialect, has_header = _guess_dialect(raw)

        # Prova decodifica robusta
        text = None
        for enc in ("utf-8-sig", "utf-8", "cp1252", "latin-1"):
            try:
                text = raw.decode(enc)
                break
            except UnicodeDecodeError:
                continue
        if text is None:
            text = raw.decode("latin-1", errors="replace")

        reader = csv.reader(io.StringIO(text), dialect=dialect)
        rows = list(reader)
        if not rows:
            raise SystemExit("[ERROR] Il file CSV è vuoto.")

        # Header?
        if has_header:
            headers = rows[0]
            body = rows[1:]
        else:
            # genera header fittizio: col_1, col_2, ...
            width = max(len(r) for r in rows)
            headers = [f"col_{i+1}" for i in range(width)]
            body = rows

        filtered = _filter_rows(body, headers)


# --- CALCOLO PREZZO SCONTATO (LIPREZZO - LISCONT1%) ---

def _to_number(value):
    """Converte stringhe tipo '1.234,56 €' in float"""
    if value is None:
        return None
    s = str(value).strip()
    if not s:
        return None
    for ch in " €\u00A0":
        s = s.replace(ch, "")
    if "," in s and "." in s:
        s = s.replace(".", "").replace(",", ".")
    else:
        s = s.replace(",", ".")
    try:
        return float(s)
    except ValueError:
        return None


try:
    # Trova le colonne dagli header, case-insensitive
    def find_col(name):
        name = name.strip().lower()
        for i, h in enumerate(headers):
            if str(h).strip().lower() == name:
                return i
        return None

    idx_prezzo = find_col("liprezzo")
    idx_sconto = find_col("liscont1")

    if idx_prezzo is None or idx_sconto is None:
        print("[WARN] Colonne 'LIPREZZO' o 'LISCONT1' non trovate negli header.")
    else:
        headers.append("PREZZO_SCONTATO")
        count = 0
        for r in filtered:
            if len(r) <= max(idx_prezzo, idx_sconto):
                # Se la riga è corta, aggiungiamo una cella vuota
                r.append("")
                continue
            prezzo = _to_number(r[idx_prezzo])
            sconto = _to_number(r[idx_sconto])
            if prezzo is None:
                r.append("")  # nessun prezzo → cella vuota
                continue
            if sconto is None or sconto == 0:
                # se sconto mancante o 0 → prezzo pieno
                prezzo_scontato = prezzo
            else:
                prezzo_scontato = prezzo * (1 - (sconto / 100.0))
            # Aggiunge valore formattato
            r.append(f"{prezzo_scontato:.2f}".replace(".", ","))
            count += 1
        print(f"[INFO] Aggiunta colonna PREZZO_SCONTATO ({count} righe elaborate).")
except Exception as e:
    print(f"[WARN] Errore nel calcolo PREZZO_SCONTATO: {e}")



        # Se non trovi nulla, produci comunque un file con header e nessuna riga
        out_io = io.StringIO()
        writer = csv.writer(out_io, dialect=dialect)
        writer.writerow(headers)
        writer.writerows(filtered)
        out_bytes = out_io.getvalue().encode("utf-8")

        # Nome output
        ts = datetime.datetime.now().strftime("%Y%m%d_%H%M%S")
        out_name = f"LISTINI_LISTINO_VENDITA_6_{ts}.csv"
        remote_dir = FTP_OUTPUT_DIR if FTP_OUTPUT_DIR else "/"

        _upload_bytes(ftp, remote_dir, out_name, out_bytes)

        print(f"[OK] Filtrate {len(filtered)} righe su {len(body)}. File caricato: {remote_dir}/{out_name}")

    finally:
        try:
            ftp.quit()
        except Exception:
            pass

if __name__ == "__main__":
    main()
