"""Étape 03 : ingestion en temps réel avec Spark Structured Streaming.

Surveille data/stream_in/ (alimenté par 02_producteur_flux.py), un fichier = un mois par micro-batch.
Pour chaque micro-batch (foreachBatch) :
1. couche bronze : les données brutes enrichies (date, code, niveau, division, fichier source) ;
2. suivi : pour chaque ville, l'indice général et les 12 divisions, calcul de
   - la variation mensuelle   = indice(m) / indice(m-1)  - 1, en %
   - le glissement annuel     = indice(m) / indice(m-12) - 1, en %
   - l'alerte                 = glissement annuel > 4 % (False, pas null, les 12 premiers mois).

Lancement : python spark/03_streaming.py [--reset] [--une-fois]
  --reset    : repartir de zéro (efface checkpoint, bronze, suivi et journal)
  --une-fois : traite les fichiers présents puis s'arrête et affiche les contrôles
               (sinon le flux tourne en continu et attend les publications)
"""
import argparse
import shutil
import time

from commun import SCHEMA_CSV, creer_spark, enrichir
from config import BRONZE, CHECKPOINTS, GOLD, STREAM_IN
from pyspark.sql import functions as F

SUIVI = GOLD / "suivi_flux"
JOURNAL = GOLD / "journal_flux"   # une ligne par micro-batch : affichée par l'API (santé du pipeline)
CHECKPOINT = CHECKPOINTS / "streaming"
SEUIL_ALERTE = 4.0  # % de hausse sur un an


def traiter_lot(lot, numero_lot):
    debut = time.perf_counter()
    spark = lot.sparkSession
    lot.persist()
    mois_du_lot = [ligne.date for ligne in lot.select("date").distinct().collect()]
    if not mois_du_lot:
        return
    # Contrôle qualité : un libellé de mois ou un code produit inconnu doit arrêter le flux, pas passer en silence
    invalides = lot.filter(F.col("date").isNull() | F.col("niveau").isNull()).count()
    if invalides:
        raise ValueError(f"{invalides} lignes avec un mois ou un code produit illisible (lot {numero_lot})")

    # 1. Bronze, partitionné par mois (écrasement dynamique : rejouer un lot ne crée pas de doublons)
    lot.write.mode("overwrite").partitionBy("date").parquet(BRONZE.as_posix())

    # 2. Suivi : il faut le mois courant, le mois précédent et le même mois l'an dernier
    dates_utiles = [ligne.d for ligne in lot.select(F.explode(F.array(
        "date", F.add_months("date", -1), F.add_months("date", -12))).alias("d")).distinct().collect()]
    historique = (
        spark.read.parquet(BRONZE.as_posix())
        .filter(F.col("date").isin(dates_utiles))                   # ne lit que les partitions utiles
        .filter(F.col("niveau").isin("general", "division"))        # indice général + 12 divisions
        .select("ville", "code", "libelle", "date", "indice")
    )
    precedent = historique.select("ville", "code", F.add_months("date", 1).alias("date"),
                                  F.col("indice").alias("indice_mois_precedent"))
    an_dernier = historique.select("ville", "code", F.add_months("date", 12).alias("date"),
                                   F.col("indice").alias("indice_an_dernier"))
    suivi = (
        historique.filter(F.col("date").isin(mois_du_lot))
        .join(precedent, ["ville", "code", "date"], "left")
        .join(an_dernier, ["ville", "code", "date"], "left")
        .withColumn("variation_mensuelle",
                    F.round((F.col("indice") / F.col("indice_mois_precedent") - 1) * 100, 2))
        .withColumn("glissement_annuel",
                    F.round((F.col("indice") / F.col("indice_an_dernier") - 1) * 100, 2))
        .withColumn("alerte", F.coalesce(F.col("glissement_annuel") > SEUIL_ALERTE, F.lit(False)))
        .drop("indice_mois_precedent", "indice_an_dernier")
        .cache()
    )
    suivi.write.mode("overwrite").partitionBy("date").parquet(SUIVI.as_posix())

    # Journal du micro-batch
    national = suivi.filter("ville = 'National' AND code = 'GR'").first()
    nb_alertes = suivi.filter("alerte").count()
    nb_lignes, nb_series = lot.count(), suivi.count()
    glissement = "  n/d " if national.glissement_annuel is None else f"{national.glissement_annuel:6.2f}"
    print(f"[lot {numero_lot:3d}] {national.date:%Y-%m} : {nb_lignes:,} lignes | "
          f"inflation nationale {glissement} % | {nb_alertes:3d} alertes / {nb_series} séries", flush=True)
    # Journal : la fin est en secondes depuis 1970 (pas de fuseau horaire à gérer entre conteneurs et navigateur).
    # Un lot rejoué après une panne ajoute une 2e ligne au même numéro : l'API garde la plus récente.
    duree = round(time.perf_counter() - debut, 2)
    spark.createDataFrame(
        [(numero_lot, national.date, nb_lignes, nb_alertes, nb_series, duree, time.time())],
        "numero_lot LONG, mois DATE, lignes LONG, alertes LONG, series LONG, duree_s DOUBLE, fin DOUBLE",
    ).coalesce(1).write.mode("append").parquet(JOURNAL.as_posix())
    suivi.unpersist()
    lot.unpersist()


def controler(spark):
    """Contrôles sur les tables écrites, après un passage complet (--une-fois)."""
    bronze = spark.read.parquet(BRONZE.as_posix())
    suivi = spark.read.parquet(SUIVI.as_posix())
    print(f"\nBronze : {bronze.count():,} lignes, {bronze.select('date').distinct().count()} mois")
    print("Postes par niveau :", {r.niveau: r.n for r in bronze.groupBy("niveau")
                                  .agg(F.countDistinct("produit").alias("n")).collect()})
    print(f"Suivi  : {suivi.count():,} lignes ({suivi.select('ville', 'code').distinct().count()} séries), "
          f"alertes nulles : {suivi.filter(F.col('alerte').isNull()).count()}")
    alertes_fevrier_2023 = suivi.filter("date = '2023-02-01' AND alerte").count()
    print(f"Alertes en février 2023 : {alertes_fevrier_2023}")

    attendus = {"2018-01-01": 2.01, "2023-02-01": 10.08, "2026-08-01": -0.33}
    for date, attendu in attendus.items():
        ligne = suivi.filter(f"ville = 'National' AND code = 'GR' AND date = '{date}'").first()
        if ligne is not None:
            print(f"  inflation nationale {date[:7]} : {ligne.glissement_annuel:6.2f} %  (attendu {attendu})")
            assert ligne.glissement_annuel == attendu


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--reset", action="store_true", help="effacer checkpoint, bronze, suivi et journal")
    parser.add_argument("--une-fois", action="store_true", help="traiter les fichiers présents puis s'arrêter")
    args = parser.parse_args()

    if args.reset:
        for dossier in (CHECKPOINT, BRONZE, SUIVI, JOURNAL):
            shutil.rmtree(dossier, ignore_errors=True)
    STREAM_IN.mkdir(parents=True, exist_ok=True)

    spark = creer_spark("ipc-streaming")
    flux = (
        spark.readStream.schema(SCHEMA_CSV)
        .option("header", True)
        .option("maxFilesPerTrigger", 1)       # 1 fichier = 1 mois par micro-batch
        .csv(STREAM_IN.as_posix())
    )
    flux = (
        enrichir(flux)
        .withColumn("fichier", F.element_at(F.split(F.input_file_name(), "/"), -1))
        .withColumn("date_ingestion", F.current_timestamp())
    )

    ecriture = flux.writeStream.foreachBatch(traiter_lot).option("checkpointLocation", CHECKPOINT.as_posix())
    if args.une_fois:
        ecriture.trigger(availableNow=True).start().awaitTermination()  # respecte maxFilesPerTrigger
        controler(spark)
    else:
        print(f"En attente de fichiers dans {STREAM_IN} ...", flush=True)
        ecriture.trigger(processingTime="2 seconds").start().awaitTermination()
