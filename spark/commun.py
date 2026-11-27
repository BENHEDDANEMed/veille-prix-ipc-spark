"""Outils partagés par les scripts Spark : session, schéma des CSV, enrichissement en Spark SQL."""
import os
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
from config import MOIS_FR

from pyspark.sql import SparkSession, functions as F

# Schéma explicite : obligatoire en streaming (Spark ne peut pas deviner le schéma de fichiers à venir)
SCHEMA_CSV = "ville STRING, produit STRING, mois STRING, indice DOUBLE"


def creer_spark(nom_appli):
    # Toujours le Spark 3.5 installé par pip et le Python courant, même si le PC a d'autres réglages
    # globaux (ex. SPARK_HOME vers un Spark 4, PYSPARK_PYTHON vers Anaconda) : sinon versions incompatibles.
    os.environ.pop("SPARK_HOME", None)
    os.environ["PYSPARK_PYTHON"] = sys.executable
    spark = (
        SparkSession.builder.master("local[*]").appName(nom_appli)
        .config("spark.ui.showConsoleProgress", "false")      # logs lisibles
        .config("spark.sql.shuffle.partitions", "4")           # petites données : 200 partitions = lenteur inutile
        .config("spark.sql.sources.partitionOverwriteMode", "dynamic")  # réécrire un mois sans toucher aux autres
        .config("spark.sql.session.timeZone", "UTC")
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("WARN")
    return spark


# 'Juill-2026' -> 2026-07-01, via une table de correspondance Spark SQL construite depuis config.MOIS_FR
_TABLE_MOIS = "map(" + ", ".join(f"'{nom}', {num}" for nom, num in MOIS_FR.items()) + ")"

# Colonnes ajoutées, sous forme d'expressions Spark SQL, évaluées dans cet ordre.
# Produit = "(code) LIBELLÉ" ; GR = indice général, 2 chiffres = division, 3 = groupe, 4 = sous-groupe.
COLONNES_ENRICHIES = {
    "date": f"make_date(int(split(mois, '-')[1]), {_TABLE_MOIS}[split(mois, '-')[0]], 1)",
    "code": "regexp_extract(produit, '^[(]([0-9A-Z]+)[)]', 1)",
    "libelle": "trim(regexp_replace(produit, '^[(][0-9A-Z]+[)]', ''))",
    "niveau": """CASE WHEN code = 'GR' THEN 'general' WHEN length(code) = 2 THEN 'division'
                      WHEN length(code) = 3 THEN 'groupe' WHEN length(code) = 4 THEN 'sous-groupe' END""",
    "division": "CASE WHEN code = 'GR' THEN 'GR' ELSE substr(code, 1, 2) END",
}


def enrichir(df):
    for nom, expression in COLONNES_ENRICHIES.items():
        df = df.withColumn(nom, F.expr(expression))
    return df
