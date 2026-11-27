# Databricks notebook source
# MAGIC %md
# MAGIC # IPC : où les prix vont-ils monter le mois prochain ? (version Databricks)
# MAGIC
# MAGIC Bonus du mini-projet : le même pipeline que la version locale, sur Databricks.
# MAGIC - **Volume Unity Catalog** : fichier brut, CSV mensuels, dossier surveillé par le flux, checkpoints.
# MAGIC - **Auto Loader** (`cloudFiles`) : ingestion incrémentale des publications mensuelles.
# MAGIC - **Tables Delta** dans Unity Catalog : bronze, suivi du flux, silver, gold, résultats du modèle.
# MAGIC
# MAGIC **Lancement** : Workspace > Import > ce fichier ; attacher le calcul *serverless* ; *Run all*.
# MAGIC
# MAGIC Si le téléchargement depuis data.gov.ma est bloqué (Free Edition : accès Internet limité),
# MAGIC téléverser `i_i4166_data.xlsx` dans le Volume, dossier `raw/` (Catalog > workspace > ipc > donnees > Upload), puis relancer.
# MAGIC
# MAGIC Différences avec la version locale, imposées par le calcul serverless :
# MAGIC - seul le déclencheur `availableNow` est autorisé : le flux traite les fichiers arrivés puis s'arrête.
# MAGIC   La démo publie donc en deux fois (historique, puis les derniers mois) pour montrer l'incrémental ;
# MAGIC - Auto Loader ne garantit pas l'ordre des fichiers : chaque lot recalcule aussi les mois qui dépendent
# MAGIC   des mois reçus (m+1 pour la variation mensuelle, m+12 pour le glissement annuel) ;
# MAGIC - pas de `cache()` ni de `sparkContext`.
# MAGIC
# MAGIC Ce fichier tourne aussi en local (`python databricks/ipc_databricks.py`) pour le tester avant Databricks.

# COMMAND ----------

# MAGIC %pip install openpyxl==3.1.5

# COMMAND ----------

import os
import shutil
import sys
import time
import urllib.request
from datetime import date
from pathlib import Path

import pandas as pd
from pyspark.ml import Pipeline
from pyspark.ml.clustering import KMeans
from pyspark.ml.evaluation import ClusteringEvaluator, RegressionEvaluator
from pyspark.ml.feature import OneHotEncoder, StandardScaler, StringIndexer, VectorAssembler
from pyspark.ml.regression import GBTRegressor, LinearRegression, RandomForestRegressor
from pyspark.sql import Window, functions as F

SUR_DATABRICKS = "DATABRICKS_RUNTIME_VERSION" in os.environ
SOURCE_URL = ("https://data.gov.ma/data/dataset/7582f4fb-f157-414b-814a-4c1b474b8d87/resource/"
              "bd692621-d8ba-4463-8d43-e66c3313ae59/download/i_i4166_data.xlsx")
CATALOGUE, SCHEMA, VOLUME = "workspace", "ipc", "donnees"

if SUR_DATABRICKS:
    spark.sql(f"CREATE SCHEMA IF NOT EXISTS {CATALOGUE}.{SCHEMA}")
    spark.sql(f"CREATE VOLUME IF NOT EXISTS {CATALOGUE}.{SCHEMA}.{VOLUME}")
    RACINE = Path(f"/Volumes/{CATALOGUE}/{SCHEMA}/{VOLUME}")
else:  # test local du même notebook : Spark de pip, tables en Parquet dans data/databricks_test
    from pyspark.sql import SparkSession
    os.environ.pop("SPARK_HOME", None)
    os.environ["PYSPARK_PYTHON"] = sys.executable
    spark = (SparkSession.builder.master("local[*]").appName("ipc-databricks-test")
             .config("spark.ui.showConsoleProgress", "false").config("spark.sql.shuffle.partitions", "4")
             .getOrCreate())
    spark.sparkContext.setLogLevel("WARN")
    RACINE = Path(__file__).resolve().parents[1] / "data" / "databricks_test"
    display = lambda df: df.show(30, truncate=False)  # noqa: E731  (display() n'existe que sur Databricks)

RAW, LANDING, STREAM_IN, CHECKPOINT = (RACINE / d for d in ("raw", "landing", "stream_in", "checkpoints/flux"))
for dossier in (RAW, LANDING, STREAM_IN):
    dossier.mkdir(parents=True, exist_ok=True)
FICHIER_XLSX = RAW / "i_i4166_data.xlsx"
MOIS_FR = {"Janv": 1, "Févr": 2, "Mars": 3, "Avr": 4, "Mai": 5, "Juin": 6,
           "Juill": 7, "Août": 8, "Sept": 9, "Oct": 10, "Nov": 11, "Déc": 12}


def nom_table(nom):
    return f"{CATALOGUE}.{SCHEMA}.{nom}"


def lire(session, nom):
    if SUR_DATABRICKS:
        return session.table(nom_table(nom))
    return session.read.parquet((RACINE / "tables" / nom).as_posix())


def ecrire(df, nom):
    if SUR_DATABRICKS:
        df.write.mode("overwrite").option("overwriteSchema", "true").saveAsTable(nom_table(nom))
    else:
        df.write.mode("overwrite").parquet((RACINE / "tables" / nom).as_posix())


def remplacer_mois(df, nom, dates):
    """Réécrit uniquement les mois donnés : rejouer un lot ne crée jamais de doublon."""
    session = df.sparkSession
    if SUR_DATABRICKS:
        table = nom_table(nom)
        if session.catalog.tableExists(table):
            condition = "date IN (" + ", ".join(f"DATE'{d}'" for d in dates) + ")"
            df.write.mode("overwrite").option("replaceWhere", condition).saveAsTable(table)
        else:
            df.write.saveAsTable(table)
    else:
        (df.write.mode("overwrite").partitionBy("date").option("partitionOverwriteMode", "dynamic")
         .parquet((RACINE / "tables" / nom).as_posix()))


def table_existe(nom):
    if SUR_DATABRICKS:
        return spark.catalog.tableExists(nom_table(nom))
    return (RACINE / "tables" / nom).exists()


print("Databricks" if SUR_DATABRICKS else "Test local", "| données dans", RACINE)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1. Collecte : le fichier du HCP découpé en 116 publications mensuelles
# MAGIC Fiche : https://data.gov.ma/data/fr/dataset/data_i4166

# COMMAND ----------

if not FICHIER_XLSX.exists():
    try:
        requete = urllib.request.Request(SOURCE_URL, headers={"User-Agent": "ipc-bigdata/1.0"})
        with urllib.request.urlopen(requete, timeout=60) as reponse:
            contenu = reponse.read()
        assert contenu.startswith(b"PK"), "réponse qui n'est pas un fichier Excel"
        FICHIER_XLSX.write_bytes(contenu)
    except Exception as erreur:
        raise RuntimeError(f"Téléchargement impossible ({erreur}). Téléverser i_i4166_data.xlsx dans {RAW} "
                           "puis relancer le notebook.") from erreur

large = pd.read_excel(FICHIER_XLSX, header=1)  # ligne 0 = titre, ligne 1 = en-têtes
colonne_ville, colonne_produit, *colonnes_mois = large.columns
long = (large.melt(id_vars=[colonne_ville, colonne_produit], var_name="mois", value_name="indice")
        .rename(columns={colonne_ville: "ville", colonne_produit: "produit"}))
long["indice"] = pd.to_numeric(long["indice"], errors="raise").astype(float)
for libelle, publication in long.groupby("mois", sort=False):
    nom, annee = libelle.split("-")
    publication.to_csv(LANDING / f"ipc_{annee}_{MOIS_FR[nom]:02d}.csv", index=False, encoding="utf-8")

publications = sorted(LANDING.glob("ipc_*.csv"))
print(f"{len(publications)} CSV mensuels, {len(long):,} observations, {long['indice'].isna().sum()} manquante")
assert len(publications) == 116 and len(long) == 318_304

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2. Flux : Auto Loader + foreachBatch
# MAGIC Chaque lot écrit la couche **bronze** (brut enrichi), puis le **suivi** : variation mensuelle,
# MAGIC glissement annuel et alerte (> 4 % sur un an) pour chaque ville, l'indice général et les 12 divisions.

# COMMAND ----------

SCHEMA_CSV = "ville STRING, produit STRING, mois STRING, indice DOUBLE"
SEUIL_ALERTE = 4.0
FICHIERS_PAR_LOT = 1  # 1 fichier = 1 mois par micro-batch, comme en local
TABLE_MOIS = "map(" + ", ".join(f"'{n}', {m}" for n, m in MOIS_FR.items()) + ")"
COLONNES_ENRICHIES = {  # expressions Spark SQL, identiques à spark/commun.py
    "date": f"make_date(int(split(mois, '-')[1]), {TABLE_MOIS}[split(mois, '-')[0]], 1)",
    "code": "regexp_extract(produit, '^[(]([0-9A-Z]+)[)]', 1)",
    "libelle": "trim(regexp_replace(produit, '^[(][0-9A-Z]+[)]', ''))",
    "niveau": """CASE WHEN code = 'GR' THEN 'general' WHEN length(code) = 2 THEN 'division'
                      WHEN length(code) = 3 THEN 'groupe' WHEN length(code) = 4 THEN 'sous-groupe' END""",
    "division": "CASE WHEN code = 'GR' THEN 'GR' ELSE substr(code, 1, 2) END",
}


def decaler(jour, nb_mois):
    total = jour.year * 12 + jour.month - 1 + nb_mois
    return date(total // 12, total % 12 + 1, 1)


def traiter_lot(lot, numero_lot):
    session = lot.sparkSession  # sur serverless, la fonction tourne à part : pas de variable globale « spark »
    mois_recus = sorted(l.date for l in lot.select("date").distinct().collect())
    if not mois_recus:
        return
    if lot.filter("date IS NULL OR niveau IS NULL").count():
        raise ValueError(f"Lot {numero_lot} : mois ou code produit illisible")
    remplacer_mois(lot, "bronze", mois_recus)

    # Les fichiers peuvent arriver dans le désordre : on recalcule m, m+1 et m+12 pour chaque mois reçu
    a_recalculer = {decaler(m, n) for m in mois_recus for n in (0, 1, 12)}
    utiles = a_recalculer | {decaler(m, -1) for m in a_recalculer} | {decaler(m, -12) for m in a_recalculer}
    historique = (lire(session, "bronze").filter(F.col("date").isin(sorted(utiles)))
                  .filter(F.col("niveau").isin("general", "division"))
                  .select("ville", "code", "libelle", "date", "indice"))
    presents = sorted(l.date for l in historique.select("date").distinct().collect()
                      if l.date in a_recalculer)
    precedent = historique.select("ville", "code", F.add_months("date", 1).alias("date"),
                                  F.col("indice").alias("indice_mois_precedent"))
    an_dernier = historique.select("ville", "code", F.add_months("date", 12).alias("date"),
                                   F.col("indice").alias("indice_an_dernier"))
    suivi = (historique.filter(F.col("date").isin(presents))
             .join(precedent, ["ville", "code", "date"], "left")
             .join(an_dernier, ["ville", "code", "date"], "left")
             .withColumn("variation_mensuelle", F.round((F.col("indice") / F.col("indice_mois_precedent") - 1) * 100, 2))
             .withColumn("glissement_annuel", F.round((F.col("indice") / F.col("indice_an_dernier") - 1) * 100, 2))
             .withColumn("alerte", F.coalesce(F.col("glissement_annuel") > SEUIL_ALERTE, F.lit(False)))
             .drop("indice_mois_precedent", "indice_an_dernier"))
    remplacer_mois(suivi, "suivi_flux", presents)
    print(f"[lot {numero_lot:3d}] reçu {', '.join(f'{m:%Y-%m}' for m in mois_recus)} ; "
          f"suivi recalculé pour {len(presents)} mois", flush=True)


def lancer_flux():
    """Traite tout ce qui est arrivé dans stream_in depuis le dernier passage (checkpoint), puis s'arrête."""
    if SUR_DATABRICKS:
        lecture = (spark.readStream.format("cloudFiles").option("cloudFiles.format", "csv")
                   .option("cloudFiles.maxFilesPerTrigger", FICHIERS_PAR_LOT))
    else:
        lecture = spark.readStream.format("csv").option("maxFilesPerTrigger", FICHIERS_PAR_LOT)
    flux = lecture.schema(SCHEMA_CSV).option("header", True).load(STREAM_IN.as_posix())
    for nom, expression in COLONNES_ENRICHIES.items():
        flux = flux.withColumn(nom, F.expr(expression))
    flux = (flux.withColumn("fichier", F.col("_metadata.file_name"))
            .withColumn("date_ingestion", F.current_timestamp()))
    (flux.writeStream.foreachBatch(traiter_lot)
     .option("checkpointLocation", CHECKPOINT.as_posix())
     .trigger(availableNow=True)  # seul déclencheur autorisé sur serverless
     .start().awaitTermination())


def publier(fichiers):
    """Le producteur : copie des publications mensuelles dans le dossier surveillé."""
    for source in fichiers:
        cible = STREAM_IN / source.name
        if SUR_DATABRICKS:
            shutil.copyfile(source, cible)  # stockage objet : le fichier n'apparaît qu'une fois complet
        else:
            temporaire = STREAM_IN / f".{source.name}.tmp"  # fichier caché + renommage atomique
            shutil.copyfile(source, temporaire)
            os.replace(temporaire, cible)


def compter_flux():
    suivi = lire(spark, "suivi_flux")
    return suivi.select("date").distinct().count(), suivi.count()

# COMMAND ----------

# MAGIC %md
# MAGIC ### Démo de l'incrémental, en deux temps
# MAGIC Remise à zéro, puis publication de l'historique jusqu'à décembre 2025 et premier passage du flux ;
# MAGIC ensuite publication des 8 mois de 2026 : le second passage ne traite **que** les nouveaux fichiers (checkpoint).

# COMMAND ----------

shutil.rmtree(STREAM_IN, ignore_errors=True)
shutil.rmtree(CHECKPOINT, ignore_errors=True)
STREAM_IN.mkdir(parents=True, exist_ok=True)
for nom in ("bronze", "suivi_flux"):
    if SUR_DATABRICKS:
        spark.sql(f"DROP TABLE IF EXISTS {nom_table(nom)}")
    else:
        shutil.rmtree(RACINE / "tables" / nom, ignore_errors=True)

historique, nouveaux = publications[:-8], publications[-8:]
debut = time.time()
publier(historique)
lancer_flux()
print(f"Premier passage : {compter_flux()[0]} mois dans le suivi, {time.time() - debut:.0f} s")

publier(nouveaux)
debut = time.time()
lancer_flux()
nb_mois, nb_lignes = compter_flux()
print(f"Second passage : {nb_mois} mois, {nb_lignes:,} lignes de suivi, {time.time() - debut:.0f} s")

# COMMAND ----------

suivi = lire(spark, "suivi_flux")
controle = {r.date.isoformat(): r.glissement_annuel for r in suivi.filter("ville = 'National' AND code = 'GR'")
            .filter(F.col("date").isin(date(2018, 1, 1), date(2023, 2, 1), date(2026, 8, 1))).collect()}
alertes_fevrier_2023 = suivi.filter("date = DATE'2023-02-01' AND alerte").count()
print("Inflation nationale sur 12 mois :", controle, "| alertes en février 2023 :", alertes_fevrier_2023)
assert controle == {"2018-01-01": 2.01, "2023-02-01": 10.08, "2026-08-01": -0.33} and alertes_fevrier_2023 == 144
assert lire(spark, "bronze").count() == 318_304 and nb_lignes == 28_652
display(suivi.filter("date = DATE'2026-08-01' AND alerte").orderBy(F.desc("glissement_annuel")).limit(10))

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3. Spark SQL : silver (nettoyé) et gold (agrégats, variables du modèle)
# MAGIC Mêmes requêtes que `spark/04_spark_sql.py`.

# COMMAND ----------

lire(spark, "bronze").createOrReplaceTempView("bronze")
ipc_propre = spark.sql("""
    SELECT trim(ville) AS ville, code, libelle, niveau, division, date, year(date) AS annee, month(date) AS mois, indice
    FROM (SELECT *, ROW_NUMBER() OVER (PARTITION BY ville, produit, date ORDER BY date_ingestion DESC) AS version
          FROM bronze WHERE indice IS NOT NULL AND indice > 0 AND date IS NOT NULL)
    WHERE version = 1
""")
ipc_propre.createOrReplaceTempView("ipc_propre")
trous = spark.sql("""SELECT count(*) FROM (SELECT ville, code, count(*) AS n, months_between(max(date), min(date)) + 1 AS attendu
                     FROM ipc_propre GROUP BY ville, code) WHERE n <> attendu""").first()[0]
assert trous == 0, f"{trous} séries ont des mois manquants"
ecrire(spark.sql("""
    SELECT *, 100 * (indice / LAG(indice, 1) OVER serie - 1) AS variation_mensuelle,
              100 * (indice / LAG(indice, 12) OVER serie - 1) AS glissement_annuel
    FROM ipc_propre WINDOW serie AS (PARTITION BY ville, code ORDER BY date)
"""), "silver_ipc")
lire(spark, "silver_ipc").createOrReplaceTempView("silver")

ecrire(spark.sql("""
    WITH moyennes AS (SELECT ville, code, libelle, annee, avg(indice) AS indice_moyen, count(*) AS nb_mois
                      FROM silver WHERE niveau IN ('general', 'division') GROUP BY ville, code, libelle, annee),
    inflation AS (SELECT a.ville, a.code, a.libelle, a.annee, round(100 * (a.indice_moyen / b.indice_moyen - 1), 2) AS inflation
                  FROM moyennes a JOIN moyennes b ON a.ville = b.ville AND a.code = b.code AND a.annee = b.annee + 1
                  WHERE a.nb_mois = 12)
    SELECT *, CASE WHEN ville <> 'National' THEN RANK() OVER (PARTITION BY code, annee, ville = 'National' ORDER BY inflation DESC) END AS rang_ville,
              CASE WHEN code <> 'GR' THEN RANK() OVER (PARTITION BY ville, annee, code = 'GR' ORDER BY inflation DESC) END AS rang_division
    FROM inflation
"""), "gold_inflation_annuelle")

divisions_pivot = ", ".join(f"'{n:02d}' AS d{n:02d}" for n in range(1, 13))
ecrire(spark.sql(f"""
    SELECT * FROM (SELECT ville, code, glissement_annuel FROM silver
                   WHERE niveau = 'division' AND ville <> 'National' AND glissement_annuel IS NOT NULL)
    PIVOT (round(avg(glissement_annuel), 3) FOR code IN ({divisions_pivot}))
"""), "gold_profil_villes")

spark.sql("""
    CREATE OR REPLACE TEMP VIEW series AS
    SELECT ville, code, libelle, date, variation_mensuelle, glissement_annuel,
           LAG(variation_mensuelle, 1) OVER serie AS variation_m1, LAG(variation_mensuelle, 2) OVER serie AS variation_m2,
           LAG(variation_mensuelle, 11) OVER serie AS variation_meme_mois_an_dernier,
           LEAD(variation_mensuelle, 1) OVER serie AS cible
    FROM silver WHERE niveau IN ('general', 'division')
    WINDOW serie AS (PARTITION BY ville, code ORDER BY date)
""")
ecrire(spark.sql("""
    SELECT s.ville, s.code, s.libelle, s.date, add_months(s.date, 1) AS date_cible, month(add_months(s.date, 1)) AS mois_cible,
           s.variation_mensuelle, s.variation_m1, s.variation_m2, s.glissement_annuel, s.variation_meme_mois_an_dernier,
           n.variation_mensuelle AS variation_nationale_division, s.cible,
           CASE WHEN s.date = (SELECT max(date) FROM silver) THEN 'a_predire' WHEN year(s.date) <= 2023 THEN 'entrainement'
                WHEN year(s.date) = 2024 THEN 'validation' ELSE 'test' END AS ensemble
    FROM series s JOIN series n ON n.ville = 'National' AND n.code = s.code AND n.date = s.date
    WHERE s.date >= '2018-01-01'
"""), "gold_variables_modele")

lire(spark, "gold_inflation_annuelle").createOrReplaceTempView("inflation_annuelle")
display(spark.sql("SELECT annee, inflation FROM inflation_annuelle WHERE ville = 'National' AND code = 'GR' ORDER BY annee"))
display(lire(spark, "gold_variables_modele").groupBy("ensemble").count().orderBy("ensemble"))

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4. MLlib : prévoir la variation du mois suivant, et regrouper les villes
# MAGIC Mêmes choix que `spark/05_mllib.py` : réglage sur 2024, note finale sur 2025-2026, comparaison à deux règles naïves.
# MAGIC Plus les **hausses inattendues** : surprise = hausse prévue - hausse du même mois l'an dernier
# MAGIC (`surprise`, `rang_surprise` dans `gold_previsions`, score `part_top10_surprise` dans `gold_metriques`).

# COMMAND ----------

CATEGORIES = ["ville", "code", "mois_cible"]
NUMERIQUES = ["variation_mensuelle", "variation_m1", "variation_m2", "glissement_annuel",
              "variation_meme_mois_an_dernier", "variation_nationale_division"]
REFERENCES = {"Naïf : mois suivant = mois courant": "variation_mensuelle",
              "Naïf : même mois l'an dernier": "variation_meme_mois_an_dernier"}
GRILLE = {
    "Régression linéaire": [(f"regParam={r}", lambda r=r: LinearRegression(labelCol="cible", regParam=r)) for r in (0.0, 0.01, 0.1)],
    "Random Forest": [(f"100 arbres, profondeur {p}", lambda p=p: RandomForestRegressor(labelCol="cible", numTrees=100, maxDepth=p, seed=42))
                      for p in (5, 8)],
    "GBT": [(f"100 itérations, profondeur {p}", lambda p=p: GBTRegressor(labelCol="cible", maxIter=100, maxDepth=p, stepSize=0.05, seed=42))
            for p in (3, 5)],
}


def pipeline(estimateur):
    index, vecteurs = [f"{c}_index" for c in CATEGORIES], [f"{c}_vecteur" for c in CATEGORIES]
    return Pipeline(stages=[StringIndexer(inputCols=CATEGORIES, outputCols=index, handleInvalid="keep"),
                            OneHotEncoder(inputCols=index, outputCols=vecteurs),
                            VectorAssembler(inputCols=NUMERIQUES + vecteurs, outputCol="features"), estimateur])


def part_top10(predictions, colonne, reel="cible"):
    par_mois = Window.partitionBy("date")
    return (predictions.filter("ville <> 'National' AND code <> 'GR'")
            .withColumn("rang_predit", F.row_number().over(par_mois.orderBy(F.desc(colonne))))
            .withColumn("rang_reel", F.row_number().over(par_mois.orderBy(F.desc(reel))))
            .groupBy("date").agg((F.sum(((F.col("rang_predit") <= 10) & (F.col("rang_reel") <= 10)).cast("int")) / 10).alias("part"))
            .agg(F.avg("part")).first()[0])


def part_top10_surprise(predictions, colonne):
    # Hausses inattendues : surprise = hausse au-delà de celle du même mois l'an dernier (prévue et réelle)
    an_dernier = F.col("variation_meme_mois_an_dernier")
    return part_top10(predictions.withColumn("surprise_prevue", F.col(colonne) - an_dernier)
                      .withColumn("surprise_reelle", F.col("cible") - an_dernier), "surprise_prevue", reel="surprise_reelle")


def mesurer(predictions, colonne="prediction"):
    mesures = {m: RegressionEvaluator(labelCol="cible", predictionCol=colonne, metricName=m).evaluate(predictions)
               for m in ("mae", "rmse", "r2")}
    # La règle « même mois l'an dernier » prévoit une surprise nulle partout : pas de score pour elle
    surprise = None if colonne == "variation_meme_mois_an_dernier" else part_top10_surprise(predictions, colonne)
    return {**mesures, "part_top10": part_top10(predictions, colonne), "part_top10_surprise": surprise}


donnees = lire(spark, "gold_variables_modele")
entrainement, validation, test = (donnees.filter(F.col("ensemble") == e) for e in ("entrainement", "validation", "test"))
entrainement_validation = donnees.filter("ensemble IN ('entrainement', 'validation')")

metriques = [(nom, "-", e, mesurer(d, colonne)) for nom, colonne in REFERENCES.items()
             for e, d in (("validation", validation), ("test", test))]
retenus = {}
for famille, grille in GRILLE.items():
    essais = []
    for parametres, fabrique in grille:
        mae = RegressionEvaluator(labelCol="cible", metricName="mae").evaluate(
            pipeline(fabrique()).fit(entrainement).transform(validation))
        print(f"  {famille:20s} {parametres:28s} MAE validation = {mae:.4f}")
        essais.append((mae, parametres, fabrique))
    mae, parametres, fabrique = min(essais, key=lambda essai: essai[0])
    metriques.append((famille, parametres, "validation", mesurer(pipeline(fabrique()).fit(entrainement).transform(validation))))
    metriques.append((famille, parametres, "test", mesurer(pipeline(fabrique()).fit(entrainement_validation).transform(test))))
    retenus[famille] = fabrique

mae_validation = {m: mesures["mae"] for m, _, e, mesures in metriques if e == "validation"}
famille_retenue = min(retenus, key=lambda famille: mae_validation[famille])
ecrire(spark.createDataFrame(
    [(m, p, e, x["mae"], x["rmse"], x["r2"], x["part_top10"], x["part_top10_surprise"], m == famille_retenue)
     for m, p, e, x in metriques],
    "modele STRING, parametres STRING, ensemble STRING, mae DOUBLE, rmse DOUBLE, r2 DOUBLE, part_top10 DOUBLE, "
    "part_top10_surprise DOUBLE, retenu BOOLEAN"),
    "gold_metriques")
print("Modèle retenu (meilleure MAE en validation) :", famille_retenue)
display(lire(spark, "gold_metriques").filter("ensemble = 'test'").orderBy("mae"))

# COMMAND ----------

# Prévision réelle du mois prochain : modèle retenu, réentraîné sur toutes les données connues
modele_final = pipeline(retenus[famille_retenue]()).fit(donnees.filter("ensemble <> 'a_predire'"))
groupes_rang = Window.partitionBy(F.col("ville") == "National", F.col("code") == "GR")
combinaison = (F.col("ville") != "National") & (F.col("code") != "GR")  # rangs seulement pour ville × division
ecrire(modele_final.transform(donnees.filter("ensemble = 'a_predire'"))
       # Hausse inattendue : prévue au-delà de la hausse du même mois l'an dernier
       .withColumn("surprise", F.col("prediction") - F.col("variation_meme_mois_an_dernier"))
       .withColumn("rang", F.when(combinaison, F.row_number().over(groupes_rang.orderBy(F.desc("prediction")))))
       .withColumn("rang_surprise", F.when(combinaison, F.row_number().over(groupes_rang.orderBy(F.desc("surprise")))))
       .select("ville", "code", "libelle", "date", "date_cible", "variation_mensuelle", "glissement_annuel",
               "variation_meme_mois_an_dernier", F.round("prediction", 3).alias("prediction"),
               F.round("surprise", 3).alias("surprise"), "rang", "rang_surprise", F.lit(famille_retenue).alias("modele")),
       "gold_previsions")
display(lire(spark, "gold_previsions").filter("rang <= 10").orderBy("rang")
        .select("rang", "ville", "libelle", "prediction", "glissement_annuel"))
# Hausses inattendues, comme dans l'application : seulement les hausses prévues (prévue > 0)
display(lire(spark, "gold_previsions").filter("rang_surprise IS NOT NULL AND prediction > 0").orderBy(F.desc("surprise"))
        .select("ville", "libelle", "prediction", F.round("variation_meme_mois_an_dernier", 2).alias("an_dernier"), "surprise")
        .limit(10))

# COMMAND ----------

# KMeans : 18 villes, glissement annuel moyen par division, standardisé ; k de 2 à 6 choisi par la silhouette
# Avec 18 villes, KMeans dépend de son point de départ : 10 départs par k, on garde le coût minimal (comme n_init)
profil = lire(spark, "gold_profil_villes").orderBy("ville").coalesce(1)  # même ordre partout : résultat reproductible
divisions = [c for c in profil.columns if c != "ville"]
villes = Pipeline(stages=[VectorAssembler(inputCols=divisions, outputCol="brut"),
                          StandardScaler(inputCol="brut", outputCol="features", withMean=True, withStd=True)]).fit(profil).transform(profil)
meilleurs = {k: min((KMeans(k=k, seed=g).fit(villes) for g in range(10)), key=lambda m: m.summary.trainingCost)
             for k in range(2, 7)}
scores = {k: ClusteringEvaluator().evaluate(m.transform(villes)) for k, m in meilleurs.items()}
k_retenu = max(scores, key=scores.get)
print({k: round(s, 3) for k, s in scores.items()}, "-> k retenu :", k_retenu)
ecrire(meilleurs[k_retenu].transform(villes).select("ville", F.col("prediction").alias("groupe"), *divisions),
       "gold_groupes_villes")
display(lire(spark, "gold_groupes_villes").groupBy("groupe").agg(F.sort_array(F.collect_list("ville")).alias("villes")))
