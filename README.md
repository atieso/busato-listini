# Filter LISTINI.CSV (LISTINO VENDITA 6)

Script che:
1. Scarica via (FTPS/FTP) `LISTINI.CSV`
2. Filtra le righe che corrispondono a `LISTINO VENDITA 6`
3. Carica un nuovo CSV filtrato nella stessa cartella.

## Config via env
Vedi `render.yaml` per l’elenco completo.

## Esecuzione locale
```bash
export FTP_HOST="ftp.andreat257.sg-host.com"
export FTP_USER="admin@andreat257.sg-host.com"
export FTP_PASS="********"
export FTP_INPUT_PATH="/public_html/IMPORT_DATI_FULL_20230919_0940/LISTINI.CSV"
export FTP_OUTPUT_DIR="/public_html/IMPORT_DATI_FULL_20230919_0940"
python -m venv .venv && source .venv/bin/activate
pip install -r requirements.txt
python filter_listino.py

