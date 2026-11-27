"""API FastAPI : le backend garde une SparkSession et répond par des requêtes Spark SQL sur les tables Parquet.

Routes (JSON) :
- GET /flux?ville=National        suivi en direct : dernier mois traité, évolution, alertes (> 4 % sur un an),
                                  journal des micro-batches (santé du pipeline)
- GET /previsions?ville=&division=&limite=10&tri=prevision|surprise
                                  top des risques du mois prochain (plus fortes hausses, ou hausses inattendues
                                  = au-delà de celle du même mois l'an dernier) + fiabilité du modèle
- GET /groupes                    groupes de villes (KMeans) et leur profil
Documentation interactive : /docs

Lancement : uvicorn main:app --app-dir api --port 8000
"""
import sys
from contextlib import asynccontextmanager
from pathlib import Path
from typing import Literal

from fastapi import FastAPI, HTTPException, Query

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "spark"))
from commun import creer_spark
from config import GOLD, LANDING
from pyspark.errors import AnalysisException

TABLES = {
    "suivi": GOLD / "suivi_flux",
    "journal": GOLD / "journal_flux",
    "previsions": GOLD / "previsions",
    "metriques": GOLD / "metriques",
    "metriques_division": GOLD / "metriques_division",
    "groupes": GOLD / "groupes_villes",
    "silhouette": GOLD / "silhouette",
}
SEUIL_ALERTE = 4.0
# Table pas encore écrite : le flux démarre, ou 04/05 n'ont pas encore été lancés
MESSAGE_ABSENT = ("Données pas encore disponibles ({}). "
                  "Attendre le flux ou lancer spark/04_spark_sql.py puis spark/05_mllib.py.")
spark = None


@asynccontextmanager
async def demarrage(app):
    global spark
    spark = creer_spark("ipc-api")  # une seule SparkSession pour toute la vie de l'API (démarrage ~10 s)
    yield
    spark.stop()


app = FastAPI(title="IPC : où les prix vont-ils monter ?", lifespan=demarrage)


tables_en_memoire = {}  # nom -> (signature sur disque, DataFrame en cache)
resultats = {}          # (requête, paramètres) -> lignes ; vidé dès qu'une table change


def signature(chemin):
    """Change dès qu'un mois est ajouté ou réécrit (on ignore _temporary et .spark-staging, en cours d'écriture)."""
    return tuple(sorted((f.name, f.stat().st_mtime_ns) for f in chemin.iterdir()
                        if not f.name.startswith(("_", "."))))


def charger(*noms):
    """Appelée UNE fois au début de chaque route : toutes les requêtes d'une même réponse voient la même
    version des tables (sinon un mois publié entre deux requêtes mélange deux états du flux).
    Garde chaque table en mémoire Spark et ne la relit que si elle a changé sur le disque."""
    try:
        for nom in noms:
            _charger_table(nom)
    except AnalysisException:  # table en cours de première écriture
        raise HTTPException(503, MESSAGE_ABSENT.format(", ".join(noms)))


def _charger_table(nom):
    chemin = TABLES[nom]
    actuelle = signature(chemin) if chemin.exists() else ()
    if not actuelle:
        raise HTTPException(503, MESSAGE_ABSENT.format(nom))
    if nom not in tables_en_memoire or tables_en_memoire[nom][0] != actuelle:
        if nom in tables_en_memoire:
            tables_en_memoire[nom][1].unpersist()
        table = spark.read.parquet(chemin.as_posix()).cache()
        table.createOrReplaceTempView(nom)
        tables_en_memoire[nom] = (actuelle, table)
        resultats.clear()


def requete(sql, **parametres):
    """Exécute une requête Spark SQL (tables chargées par charger()).
    Les valeurs venant de l'utilisateur passent en paramètres nommés (:ville), jamais dans le texte SQL.
    Entre deux publications la réponse ne change pas : le résultat est gardé, pas de nouveau job Spark."""
    cle = (sql, tuple(sorted(parametres.items())))
    if cle not in resultats:
        resultats[cle] = [ligne.asDict() for ligne in spark.sql(sql, args=parametres).collect()]
    return resultats[cle]


@app.get("/flux")
def flux(ville: str = "National"):
    charger("suivi", "journal")
    etat = requete("""SELECT max(date) AS dernier_mois, count(DISTINCT date) AS nb_mois,
                             count(DISTINCT ville, code) AS nb_series FROM suivi""")[0]
    if etat["dernier_mois"] is None:
        raise HTTPException(503, "Aucun mois traité pour l'instant.")
    villes = [l["ville"] for l in requete("SELECT DISTINCT ville FROM suivi ORDER BY ville <> 'National', ville")]
    if ville not in villes:
        raise HTTPException(404, f"Ville inconnue : {ville}")

    evolution = requete("""
        SELECT date,
               round(max(CASE WHEN ville = :ville THEN glissement_annuel END), 2) AS glissement_ville,
               round(max(CASE WHEN ville = 'National' THEN glissement_annuel END), 2) AS glissement_national,
               round(max(CASE WHEN ville = :ville THEN variation_mensuelle END), 2) AS variation_ville
        FROM suivi WHERE code = 'GR' GROUP BY date ORDER BY date
    """, ville=ville)
    alertes_par_mois = requete("""SELECT date, sum(int(alerte)) AS nb_alertes FROM suivi
                                  GROUP BY date ORDER BY date""")
    alertes = requete("""
        SELECT ville, code, libelle, round(glissement_annuel, 2) AS glissement_annuel,
               round(variation_mensuelle, 2) AS variation_mensuelle
        FROM suivi WHERE date = (SELECT max(date) FROM suivi) AND alerte
        ORDER BY glissement_annuel DESC
    """)
    # Un lot rejoué après une panne a 2 lignes dans le journal : on garde la plus récente
    journal = requete("""
        SELECT numero_lot, mois, lignes, alertes, duree_s, fin FROM (
            SELECT *, row_number() OVER (PARTITION BY numero_lot ORDER BY fin DESC) AS rang FROM journal)
        WHERE rang = 1 ORDER BY numero_lot DESC
    """)
    mois_publiables = len(list(LANDING.glob("ipc_*.csv")))  # CSV préparés par la collecte (116)
    return {**etat, "seuil_alerte": SEUIL_ALERTE, "ville": ville, "villes": villes,
            "evolution": evolution, "alertes_par_mois": alertes_par_mois, "alertes": alertes,
            "journal": journal, "mois_publiables": mois_publiables}


# tri=prevision : plus fortes hausses prévues ; tri=surprise : hausses inattendues (prévue - même mois l'an dernier),
# en ne gardant que les vraies hausses prévues (une baisse moins forte que l'an dernier n'est pas une hausse)
COLONNE_TRI = {"prevision": "prediction", "surprise": "surprise"}
FILTRE_TRI = {"prevision": "", "surprise": "AND prediction > 0"}


@app.get("/previsions")
def previsions(ville: str | None = None, division: str | None = None,
               limite: int = Query(10, ge=1, le=216), tri: Literal["prevision", "surprise"] = "prevision"):
    charger("previsions", "metriques", "metriques_division")
    # Combinaisons ville × division seulement : National et l'indice général ne sont pas des lieux d'action.
    # La colonne de tri vient d'une liste fixe (jamais du texte de l'utilisateur dans le SQL).
    risques = requete(f"""
        SELECT ville, code, libelle, date AS dernier_mois_connu, date_cible, prediction,
               round(variation_meme_mois_an_dernier, 2) AS variation_meme_mois_an_dernier, surprise,
               round(glissement_annuel, 2) AS glissement_annuel,
               round(variation_mensuelle, 2) AS variation_mensuelle
        FROM previsions
        WHERE ville <> 'National' AND code <> 'GR'
          AND (:ville IS NULL OR ville = :ville) AND (:division IS NULL OR code = :division)
          {FILTRE_TRI[tri]}
        ORDER BY {COLONNE_TRI[tri]} DESC
        LIMIT {limite}
    """, ville=ville, division=division)

    modele = requete("""SELECT modele, parametres, round(mae, 3) AS mae, round(rmse, 3) AS rmse, round(r2, 3) AS r2,
                               round(part_top10, 3) AS part_top10, round(part_top10_surprise, 3) AS part_top10_surprise,
                               retenu
                        FROM metriques WHERE ensemble = 'test' ORDER BY mae""")
    fiabilite = requete("""SELECT code, libelle, round(mae_modele, 3) AS mae_modele,
                                  round(mae_naif_mois_courant, 3) AS mae_naif_mois_courant,
                                  round(mae_naif_an_dernier, 3) AS mae_naif_an_dernier
                           FROM metriques_division ORDER BY code""")
    villes = [l["ville"] for l in requete("""SELECT DISTINCT ville FROM previsions
                                             WHERE ville <> 'National' ORDER BY ville""")]
    return {"risques": risques, "modeles_test": modele, "fiabilite_par_division": fiabilite,
            "villes": villes, "divisions": [d for d in fiabilite if d["code"] != "GR"]}


@app.get("/groupes")
def groupes():
    charger("groupes", "suivi", "silhouette")
    villes = requete("SELECT * FROM groupes ORDER BY groupe, ville")
    divisions = [c for c in villes[0] if c.startswith("d") and c[1:].isdigit()] if villes else []
    moyennes = ", ".join(f"round(avg({d}), 2) AS {d}" for d in divisions)  # noms de colonnes fixes, pas d'entrée utilisateur
    profils = requete(f"SELECT groupe, count(*) AS nb_villes, {moyennes} FROM groupes GROUP BY groupe ORDER BY groupe")
    libelles = requete("""SELECT DISTINCT code, libelle FROM suivi WHERE code <> 'GR' ORDER BY code""")
    silhouette = requete("SELECT k, round(silhouette, 3) AS silhouette, retenu FROM silhouette ORDER BY k")
    return {"villes": villes, "profils": profils, "divisions": libelles, "silhouette": silhouette}
