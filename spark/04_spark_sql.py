"""Étape 04 : prétraitement avec Spark SQL (couches silver et gold).

Entrée : la couche bronze écrite par 03_streaming.py.
Sorties :
- silver/ipc               : données nettoyées (dédoublonnées, contrôlées) + variation mensuelle et
                             glissement annuel de toutes les séries (y compris groupes et sous-groupes)
- gold/inflation_annuelle  : inflation annuelle par ville et par division, avec classements
- gold/profil_villes       : 1 ligne par ville, glissement annuel moyen par division (entrée du KMeans)
- gold/variables_modele    : table du modèle : variables + cible (variation du mois suivant) + découpage

Tout le traitement est écrit en SQL, sur des vues temporaires.
Lancement : python spark/04_spark_sql.py
"""
from commun import creer_spark
from config import BRONZE, GOLD, SILVER

SILVER_IPC = SILVER / "ipc"
INFLATION_ANNUELLE = GOLD / "inflation_annuelle"
PROFIL_VILLES = GOLD / "profil_villes"
VARIABLES_MODELE = GOLD / "variables_modele"
DIVISIONS = [f"{numero:02d}" for numero in range(1, 13)]


def sql(requete):
    return spark.sql(requete)


def valeur(requete):
    return sql(requete).first()[0]


def construire_silver():
    spark.read.parquet(BRONZE.as_posix()).createOrReplaceTempView("bronze")

    # Nettoyage : un mois republié par le HCP (ou rejoué par le flux) ne garde que sa dernière version ;
    # on écarte les indices vides ou négatifs (aucun dans ce fichier, mais le flux pourrait en recevoir).
    sql("""
        CREATE OR REPLACE TEMP VIEW ipc_propre AS
        SELECT trim(ville) AS ville, code, libelle, niveau, division,
               date, year(date) AS annee, month(date) AS mois, indice
        FROM (
            SELECT *, ROW_NUMBER() OVER (PARTITION BY ville, produit, date
                                         ORDER BY date_ingestion DESC) AS version
            FROM bronze
            WHERE indice IS NOT NULL AND indice > 0 AND date IS NOT NULL
        )
        WHERE version = 1
    """)

    # LAG(…, 12) = « 12 lignes avant » : ce n'est le même mois de l'an dernier que si aucun mois ne manque.
    trous = valeur("""
        SELECT count(*) FROM (
            SELECT ville, code, count(*) AS nb_mois,
                   months_between(max(date), min(date)) + 1 AS nb_attendu
            FROM ipc_propre GROUP BY ville, code
        ) WHERE nb_mois <> nb_attendu
    """)
    assert trous == 0, f"{trous} séries ont des mois manquants"

    sql("""
        SELECT *,
               100 * (indice / LAG(indice, 1)  OVER serie - 1) AS variation_mensuelle,
               100 * (indice / LAG(indice, 12) OVER serie - 1) AS glissement_annuel
        FROM ipc_propre
        WINDOW serie AS (PARTITION BY ville, code ORDER BY date)
    """).write.mode("overwrite").parquet(SILVER_IPC.as_posix())

    # La suite lit la couche silver écrite (et non le calcul en mémoire)
    spark.read.parquet(SILVER_IPC.as_posix()).createOrReplaceTempView("silver")


def construire_inflation_annuelle():
    # Inflation annuelle (définition du HCP) = moyenne des indices de l'année / moyenne de l'année d'avant - 1.
    # Seulement les années complètes (12 mois). Classements : villes entre elles, divisions entre elles.
    sql("""
        WITH moyennes AS (
            SELECT ville, code, libelle, annee, avg(indice) AS indice_moyen, count(*) AS nb_mois
            FROM silver WHERE niveau IN ('general', 'division')
            GROUP BY ville, code, libelle, annee
        ),
        inflation AS (
            SELECT a.ville, a.code, a.libelle, a.annee,
                   round(100 * (a.indice_moyen / b.indice_moyen - 1), 2) AS inflation
            FROM moyennes a
            JOIN moyennes b ON a.ville = b.ville AND a.code = b.code AND a.annee = b.annee + 1
            WHERE a.nb_mois = 12
        )
        SELECT *,
               CASE WHEN ville <> 'National' THEN
                   RANK() OVER (PARTITION BY code, annee, ville = 'National' ORDER BY inflation DESC)
               END AS rang_ville,
               CASE WHEN code <> 'GR' THEN
                   RANK() OVER (PARTITION BY ville, annee, code = 'GR' ORDER BY inflation DESC)
               END AS rang_division
        FROM inflation
    """).write.mode("overwrite").parquet(INFLATION_ANNUELLE.as_posix())
    spark.read.parquet(INFLATION_ANNUELLE.as_posix()).createOrReplaceTempView("inflation_annuelle")


def construire_profil_villes():
    # Une ligne par ville (sans National), une colonne par division : glissement annuel moyen sur la période.
    colonnes = ", ".join(f"'{code}' AS d{code}" for code in DIVISIONS)
    sql(f"""
        SELECT * FROM (
            SELECT ville, code, glissement_annuel FROM silver
            WHERE niveau = 'division' AND ville <> 'National' AND glissement_annuel IS NOT NULL
        )
        PIVOT (round(avg(glissement_annuel), 3) FOR code IN ({colonnes}))
        ORDER BY ville
    """).write.mode("overwrite").parquet(PROFIL_VILLES.as_posix())


def construire_variables_modele():
    # Pour chaque série (19 zones × indice général + 12 divisions) et chaque mois m :
    # on connaît m et le passé, on veut prédire la variation de m+1 (la cible).
    sql("""
        CREATE OR REPLACE TEMP VIEW series AS
        SELECT ville, code, libelle, date, variation_mensuelle, glissement_annuel,
               LAG(variation_mensuelle, 1)  OVER serie AS variation_m1,
               LAG(variation_mensuelle, 2)  OVER serie AS variation_m2,
               -- m+1 l'an dernier = m-11 : « même mois l'an dernier », aussi la 2e référence naïve
               LAG(variation_mensuelle, 11) OVER serie AS variation_meme_mois_an_dernier,
               LEAD(variation_mensuelle, 1) OVER serie AS cible
        FROM silver
        WHERE niveau IN ('general', 'division')
        WINDOW serie AS (PARTITION BY ville, code ORDER BY date)
    """)
    # Découpage dans le temps, selon le mois m où la prévision est faite (jamais aléatoire).
    # Le dernier mois connu n'a pas encore de cible : c'est la prévision réelle du mois prochain.
    sql("""
        SELECT s.ville, s.code, s.libelle, s.date, add_months(s.date, 1) AS date_cible,
               month(add_months(s.date, 1)) AS mois_cible,
               s.variation_mensuelle, s.variation_m1, s.variation_m2, s.glissement_annuel,
               s.variation_meme_mois_an_dernier,
               n.variation_mensuelle AS variation_nationale_division,
               s.cible,
               CASE WHEN s.date = (SELECT max(date) FROM silver) THEN 'a_predire'
                    WHEN year(s.date) <= 2023 THEN 'entrainement'
                    WHEN year(s.date) = 2024 THEN 'validation'
                    ELSE 'test' END AS ensemble
        FROM series s
        JOIN series n ON n.ville = 'National' AND n.code = s.code AND n.date = s.date
        WHERE s.date >= '2018-01-01'
    """).write.mode("overwrite").parquet(VARIABLES_MODELE.as_posix())
    spark.read.parquet(VARIABLES_MODELE.as_posix()).createOrReplaceTempView("variables_modele")


def afficher_resultats():
    print(f"\nSilver : {valeur('SELECT count(*) FROM silver'):,} lignes")
    print("Inflation sur 12 mois, contrôle :")
    sql("""
        SELECT date_format(date, 'yyyy-MM') AS mois, round(glissement_annuel, 2) AS glissement
        FROM silver WHERE ville = 'National' AND code = 'GR'
          AND date IN ('2018-01-01', '2023-02-01', '2026-08-01') ORDER BY date
    """).show()

    print("Inflation annuelle nationale (indice général) :")
    sql("""SELECT annee, inflation FROM inflation_annuelle
           WHERE ville = 'National' AND code = 'GR' ORDER BY annee""").show()
    print("2023 : les 5 villes les plus touchées (indice général) :")
    sql("""SELECT rang_ville, ville, inflation FROM inflation_annuelle
           WHERE code = 'GR' AND annee = 2023 AND rang_ville <= 5 ORDER BY rang_ville""").show()
    print("2022 : les 3 divisions qui ont le plus augmenté au niveau national :")
    sql("""SELECT rang_division, code, libelle, inflation FROM inflation_annuelle
           WHERE ville = 'National' AND annee = 2022 AND rang_division <= 3
           ORDER BY rang_division""").show(truncate=55)

    profil = spark.read.parquet(PROFIL_VILLES.as_posix())
    print(f"Profil des villes : {profil.count()} villes x {len(profil.columns) - 1} divisions")
    profil.select("ville", "d01", "d04", "d07", "d11").show(3)

    print("Table du modèle, par ensemble :")
    sql("""SELECT ensemble, count(*) AS lignes, min(date) AS de, max(date) AS a,
                  round(avg(cible), 3) AS cible_moyenne, round(stddev(cible), 3) AS cible_ecart_type
           FROM variables_modele GROUP BY ensemble ORDER BY de""").show()
    vides = valeur("""
        SELECT count(*) FROM variables_modele WHERE ensemble <> 'a_predire' AND (
            cible IS NULL OR variation_mensuelle IS NULL OR variation_m1 IS NULL OR variation_m2 IS NULL
            OR glissement_annuel IS NULL OR variation_meme_mois_an_dernier IS NULL
            OR variation_nationale_division IS NULL)
    """)
    print(f"Lignes avec une variable ou une cible vide (hors a_predire) : {vides}")
    assert vides == 0


if __name__ == "__main__":
    spark = creer_spark("ipc-spark-sql")
    construire_silver()
    construire_inflation_annuelle()
    construire_profil_villes()
    construire_variables_modele()
    afficher_resultats()
