"""Chemins du projet et source des données (un seul endroit à modifier)."""
from pathlib import Path

RACINE = Path(__file__).resolve().parent
DATA = RACINE / "data"

# Fiche data.gov.ma (à citer dans le rapport) et lien direct du fichier Excel du HCP
FICHE_URL = "https://data.gov.ma/data/fr/dataset/data_i4166"
SOURCE_URL = (
    "https://data.gov.ma/data/dataset/7582f4fb-f157-414b-814a-4c1b474b8d87/resource/"
    "bd692621-d8ba-4463-8d43-e66c3313ae59/download/i_i4166_data.xlsx"
)

RAW = DATA / "raw"                # fichier Excel d'origine
LANDING = DATA / "landing"        # 1 CSV par mois, prêt à être "publié"
STREAM_IN = DATA / "stream_in"    # dossier surveillé par Spark Structured Streaming
BRONZE = DATA / "bronze"
SILVER = DATA / "silver"
GOLD = DATA / "gold"
CHECKPOINTS = DATA / "checkpoints"

FICHIER_XLSX = RAW / "i_i4166_data.xlsx"

# Libellés de mois du HCP (ex. "Juill-2026") -> numéro du mois
MOIS_FR = {
    "Janv": 1, "Févr": 2, "Mars": 3, "Avr": 4, "Mai": 5, "Juin": 6,
    "Juill": 7, "Août": 8, "Sept": 9, "Oct": 10, "Nov": 11, "Déc": 12,
}
