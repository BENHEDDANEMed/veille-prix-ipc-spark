"""Étape 05 : machine learning avec Spark MLlib.

A. Régression : prédire la variation mensuelle du mois suivant (%) pour 247 séries
   (19 zones × indice général + 12 divisions). Jamais l'indice lui-même.
   - 3 familles (régression linéaire, Random Forest, GBT) dans un Pipeline MLlib ;
   - hyperparamètres choisis sur la validation (2024), modèle retenu choisi sur la validation ;
   - évaluation finale sur le test (prévisions faites de janvier 2025 à juillet 2026), modèles réentraînés
     sur 2018-2024 ; comparaison à 2 références naïves : « mois suivant = mois courant » et
     « même mois l'an dernier » ;
   - MAE, RMSE, R², et part des 10 risques prédits parmi les 10 plus fortes hausses réelles
     (par mois, sur les 216 combinaisons ville × division, sans National ni indice général) ;
   - même mesure pour les « hausses inattendues » : surprise = hausse au-delà de celle du même mois
     l'an dernier, c'est-à-dire ce que la règle simple ne voit pas ;
   - prévision réelle du mois prochain avec le modèle retenu, réentraîné sur toutes les données connues.
B. KMeans : regrouper les 18 villes selon leur glissement annuel moyen par division (standardisé),
   k de 2 à 6 choisi par le score silhouette.

Sorties gold : metriques, metriques_division, predictions_test, previsions, groupes_villes, silhouette.
Lancement : python spark/05_mllib.py
"""
from commun import creer_spark
from config import GOLD
from pyspark.ml import Pipeline
from pyspark.ml.clustering import KMeans
from pyspark.ml.evaluation import ClusteringEvaluator, RegressionEvaluator
from pyspark.ml.feature import OneHotEncoder, StandardScaler, StringIndexer, VectorAssembler
from pyspark.ml.regression import GBTRegressor, LinearRegression, RandomForestRegressor
from pyspark.sql import Window, functions as F

CATEGORIES = ["ville", "code", "mois_cible"]
NUMERIQUES = ["variation_mensuelle", "variation_m1", "variation_m2", "glissement_annuel",
              "variation_meme_mois_an_dernier", "variation_nationale_division"]
INITIALISATIONS = 10  # départs du KMeans pour chaque k
REFERENCES = {"Naïf : mois suivant = mois courant": "variation_mensuelle",
              "Naïf : même mois l'an dernier": "variation_meme_mois_an_dernier"}

def candidats():
    """Petite grille par famille : (description des hyperparamètres, estimateur).
    Fonction et non constante : un estimateur MLlib ne peut être créé qu'une fois Spark démarré."""
    return {
        "Régression linéaire": [(f"regParam={r}", LinearRegression(labelCol="cible", regParam=r))
                                for r in (0.0, 0.01, 0.1)],
        "Random Forest": [(f"100 arbres, profondeur {p}",
                           RandomForestRegressor(labelCol="cible", numTrees=100, maxDepth=p, seed=42))
                          for p in (5, 8)],
        "GBT": [(f"100 itérations, profondeur {p}",
                 GBTRegressor(labelCol="cible", maxIter=100, maxDepth=p, stepSize=0.05, seed=42))
                for p in (3, 5)],
    }


def pipeline(estimateur):
    """Catégories -> index -> one-hot ; puis toutes les variables dans un seul vecteur ; puis le modèle."""
    index = [f"{c}_index" for c in CATEGORIES]
    vecteurs = [f"{c}_vecteur" for c in CATEGORIES]
    return Pipeline(stages=[
        StringIndexer(inputCols=CATEGORIES, outputCols=index, handleInvalid="keep"),
        OneHotEncoder(inputCols=index, outputCols=vecteurs),
        VectorAssembler(inputCols=NUMERIQUES + vecteurs, outputCol="features"),
        estimateur,
    ])


def part_top10(predictions, colonne, reel="cible"):
    """Chaque mois : combien des 10 plus fortes valeurs prédites sont dans les 10 plus fortes valeurs réelles."""
    combinaisons = predictions.filter("ville <> 'National' AND code <> 'GR'")
    par_mois = Window.partitionBy("date")
    return (
        combinaisons
        .withColumn("rang_predit", F.row_number().over(par_mois.orderBy(F.desc(colonne))))
        .withColumn("rang_reel", F.row_number().over(par_mois.orderBy(F.desc(reel))))
        .groupBy("date")
        .agg((F.sum(((F.col("rang_predit") <= 10) & (F.col("rang_reel") <= 10)).cast("int")) / 10)
             .alias("part"))
        .agg(F.avg("part")).first()[0]
    )


def part_top10_surprise(predictions, colonne):
    """Même mesure sur les « hausses inattendues » : surprise = hausse au-delà de celle du même mois l'an dernier
    (ce que la règle simple ne voit pas). Prévue : prévision - an dernier ; réelle : cible - an dernier."""
    an_dernier = F.col("variation_meme_mois_an_dernier")
    surprises = (predictions.withColumn("surprise_prevue", F.col(colonne) - an_dernier)
                 .withColumn("surprise_reelle", F.col("cible") - an_dernier))
    return part_top10(surprises, "surprise_prevue", reel="surprise_reelle")


def mesurer(predictions, colonne="prediction"):
    resultat = {m: RegressionEvaluator(labelCol="cible", predictionCol=colonne, metricName=m)
                .evaluate(predictions) for m in ("mae", "rmse", "r2")}
    resultat["part_top10"] = part_top10(predictions, colonne)
    # La règle « même mois l'an dernier » prévoit une surprise nulle partout : son classement n'a pas de sens
    resultat["part_top10_surprise"] = (None if colonne == "variation_meme_mois_an_dernier"
                                       else part_top10_surprise(predictions, colonne))
    return resultat


def regression(donnees):
    entrainement = donnees.filter("ensemble = 'entrainement'")
    validation = donnees.filter("ensemble = 'validation'")
    test = donnees.filter("ensemble = 'test'")
    entrainement_validation = donnees.filter("ensemble IN ('entrainement', 'validation')")
    connues = donnees.filter("ensemble <> 'a_predire'")
    a_predire = donnees.filter("ensemble = 'a_predire'")

    metriques = []  # (modèle, paramètres, ensemble, mesures)
    for nom, colonne in REFERENCES.items():
        metriques.append((nom, "-", "validation", mesurer(validation, colonne)))
        metriques.append((nom, "-", "test", mesurer(test, colonne)))

    # 1. Hyperparamètres sur la validation, puis réentraînement sur 2018-2024 et mesure sur le test
    retenus = {}
    for famille, grille in candidats().items():
        essais = []
        for parametres, estimateur in grille:
            mae = RegressionEvaluator(labelCol="cible", metricName="mae").evaluate(
                pipeline(estimateur).fit(entrainement).transform(validation))
            print(f"  {famille:20s} {parametres:28s} MAE validation = {mae:.4f}", flush=True)
            essais.append((mae, parametres, estimateur))
        _, parametres, estimateur = min(essais, key=lambda essai: essai[0])
        metriques.append((famille, parametres, "validation",
                          mesurer(pipeline(estimateur).fit(entrainement).transform(validation))))
        modele = pipeline(estimateur).fit(entrainement_validation)
        predictions_test = modele.transform(test).cache()
        metriques.append((famille, parametres, "test", mesurer(predictions_test)))
        retenus[famille] = (parametres, estimateur, modele, predictions_test)

    # 2. Modèle retenu = meilleure MAE sur la VALIDATION (le test ne sert qu'à la note finale)
    mae_validation = {m: mesures["mae"] for m, _, ensemble, mesures in metriques if ensemble == "validation"}
    famille_retenue = min(retenus, key=lambda famille: mae_validation[famille])
    parametres, estimateur, modele, predictions_test = retenus[famille_retenue]

    table_metriques = spark.createDataFrame(
        [(m, p, e, mes["mae"], mes["rmse"], mes["r2"], mes["part_top10"], mes["part_top10_surprise"],
          m == famille_retenue) for m, p, e, mes in metriques],
        "modele STRING, parametres STRING, ensemble STRING, mae DOUBLE, rmse DOUBLE, r2 DOUBLE, "
        "part_top10 DOUBLE, part_top10_surprise DOUBLE, retenu BOOLEAN")
    table_metriques.write.mode("overwrite").parquet((GOLD / "metriques").as_posix())

    # 3. Erreur par division sur le test : où le modèle aide-t-il, où la règle naïve suffit-elle ?
    erreurs = predictions_test.withColumn("erreur_modele", F.abs(F.col("prediction") - F.col("cible")))
    for nom, colonne in REFERENCES.items():
        erreurs = erreurs.withColumn(f"erreur_{colonne}", F.abs(F.col(colonne) - F.col("cible")))
    (erreurs.groupBy("code", "libelle")
     .agg(F.avg("erreur_modele").alias("mae_modele"),
          F.avg("erreur_variation_mensuelle").alias("mae_naif_mois_courant"),
          F.avg("erreur_variation_meme_mois_an_dernier").alias("mae_naif_an_dernier"))
     .orderBy("code")
     .write.mode("overwrite").parquet((GOLD / "metriques_division").as_posix()))

    (predictions_test.select("ville", "code", "libelle", "date", "date_cible", "cible", "prediction",
                             F.col("variation_mensuelle").alias("naif_mois_courant"),
                             F.col("variation_meme_mois_an_dernier").alias("naif_an_dernier"))
     .write.mode("overwrite").parquet((GOLD / "predictions_test").as_posix()))

    # 4. Prévision réelle du mois prochain : modèle retenu réentraîné sur toutes les données connues
    modele_final = pipeline(estimateur).fit(connues)
    # Rangs seulement pour les combinaisons ville × division (National et indice général ne sont pas des lieux d'action)
    groupes_rang = Window.partitionBy(F.col("ville") == "National", F.col("code") == "GR")
    combinaison = (F.col("ville") != "National") & (F.col("code") != "GR")
    previsions = (
        modele_final.transform(a_predire)
        # Hausse inattendue : prévue au-delà de la hausse habituelle de ce mois-là (même mois l'an dernier)
        .withColumn("surprise", F.col("prediction") - F.col("variation_meme_mois_an_dernier"))
        .withColumn("rang", F.when(combinaison, F.row_number().over(groupes_rang.orderBy(F.desc("prediction")))))
        .withColumn("rang_surprise", F.when(combinaison, F.row_number().over(groupes_rang.orderBy(F.desc("surprise")))))
        .select("ville", "code", "libelle", "date", "date_cible", "variation_mensuelle",
                "glissement_annuel", "variation_meme_mois_an_dernier",
                F.round("prediction", 3).alias("prediction"), F.round("surprise", 3).alias("surprise"),
                "rang", "rang_surprise", F.lit(famille_retenue).alias("modele"))
    )
    previsions.write.mode("overwrite").parquet((GOLD / "previsions").as_posix())

    afficher_importances(modele_final, connues, famille_retenue)
    return famille_retenue


def afficher_importances(modele, donnees, famille):
    """Pour un modèle à arbres : quelles variables pèsent le plus (one-hot regroupés par variable)."""
    dernier = modele.stages[-1]
    if not hasattr(dernier, "featureImportances"):
        return
    attributs = modele.transform(donnees.limit(1)).schema["features"].metadata["ml_attr"]["attrs"]
    noms = {a["idx"]: a["name"] for groupe in attributs.values() for a in groupe}
    importances = {}
    for indice, poids in enumerate(dernier.featureImportances.toArray()):
        variable = noms[indice].split("_vecteur")[0]  # "ville_vecteur_Agadir" -> "ville"
        importances[variable] = importances.get(variable, 0) + poids
    print(f"\nImportance des variables ({famille}) :")
    for variable, poids in sorted(importances.items(), key=lambda v: -v[1]):
        print(f"  {variable:32s} {poids:.3f}")


def kmeans():
    profil = spark.read.parquet((GOLD / "profil_villes").as_posix())
    divisions = [c for c in profil.columns if c != "ville"]
    # Standardiser : sinon les divisions qui varient beaucoup (transports, alimentation) dominent la distance
    preparation = Pipeline(stages=[
        VectorAssembler(inputCols=divisions, outputCol="brut"),
        StandardScaler(inputCol="brut", outputCol="features", withMean=True, withStd=True),
    ]).fit(profil)
    # Même ordre et une seule partition partout (local, Docker, Databricks) : résultat reproductible
    villes = preparation.transform(profil.orderBy("ville").coalesce(1)).cache()

    evaluateur = ClusteringEvaluator()  # score silhouette, de -1 à 1 (plus haut = groupes mieux séparés)
    essais = {}
    for k in range(2, 7):
        # Avec 18 villes, le résultat dépend du point de départ (le k retenu variait de 2 à 5 selon la graine).
        # Comme n_init dans scikit-learn : 10 départs, on garde les groupes les plus compacts (coût minimal).
        departs = [KMeans(k=k, seed=graine).fit(villes) for graine in range(INITIALISATIONS)]
        modele = min(departs, key=lambda m: m.summary.trainingCost)
        essais[k] = (evaluateur.evaluate(modele.transform(villes)), modele)
    k_retenu = max(essais, key=lambda k: essais[k][0])

    spark.createDataFrame([(k, s, k == k_retenu) for k, (s, _) in essais.items()],
                          "k INT, silhouette DOUBLE, retenu BOOLEAN") \
        .write.mode("overwrite").parquet((GOLD / "silhouette").as_posix())
    groupes = essais[k_retenu][1].transform(villes).select("ville", F.col("prediction").alias("groupe"),
                                                           *divisions)
    groupes.write.mode("overwrite").parquet((GOLD / "groupes_villes").as_posix())
    return essais, k_retenu


def afficher_resultats(famille_retenue, essais_kmeans, k_retenu):
    print("\nMétriques (MAE et RMSE en points de %, part_top10 = 10 risques prédits parmi les 10 réels,\n"
          "part_top10_surprise = même mesure pour les hausses inattendues) :")
    metriques = spark.read.parquet((GOLD / "metriques").as_posix())
    for ensemble, titre in (("validation", "VALIDATION 2024 (entraînement 2018-2023)"),
                            ("test", "TEST 2025-2026 (entraînement 2018-2024)")):
        print(titre)
        (metriques.filter(F.col("ensemble") == ensemble)
         .select("modele", "parametres", *[F.round(c, 4).alias(c)
                                           for c in ("mae", "rmse", "r2", "part_top10", "part_top10_surprise")])
         .orderBy("mae").show(truncate=False))
    print(f"Modèle retenu (meilleure MAE en validation) : {famille_retenue}")
    print("Hasard pour part_top10 : 10 / 216 = 0.046")

    print("\nMAE par division sur le test :")
    spark.read.parquet((GOLD / "metriques_division").as_posix()) \
        .select("code", F.substring("libelle", 1, 30).alias("libelle"),
                *[F.round(c, 3).alias(c) for c in ("mae_modele", "mae_naif_mois_courant", "mae_naif_an_dernier")]) \
        .show(truncate=False)

    previsions = spark.read.parquet((GOLD / "previsions").as_posix())
    mois = previsions.first().date_cible
    print(f"Top 10 des risques pour {mois:%m/%Y} (variation mensuelle prévue, en %) :")
    previsions.filter("rang <= 10").orderBy("rang") \
        .select("rang", "ville", "code", F.substring("libelle", 1, 35).alias("libelle"), "prediction",
                F.round("glissement_annuel", 2).alias("glissement_actuel")).show(truncate=False)
    print(f"Top 10 des hausses inattendues pour {mois:%m/%Y} (prévue - même mois l'an dernier, en points) :")
    previsions.filter("rang_surprise <= 10").orderBy("rang_surprise") \
        .select("rang_surprise", "ville", "code", F.substring("libelle", 1, 35).alias("libelle"), "prediction",
                F.round("variation_meme_mois_an_dernier", 2).alias("an_dernier"), "surprise").show(truncate=False)

    print("KMeans, score silhouette par k :")
    for k, (score, _) in essais_kmeans.items():
        print(f"  k = {k} : {score:.3f}{'   <- retenu' if k == k_retenu else ''}")
    groupes = spark.read.parquet((GOLD / "groupes_villes").as_posix())
    groupes.groupBy("groupe").agg(F.concat_ws(", ", F.sort_array(F.collect_list("ville"))).alias("villes")) \
        .orderBy("groupe").show(truncate=False)
    print("Profil moyen des groupes (glissement annuel moyen par division, en %) :")
    divisions = [c for c in groupes.columns if c.startswith("d")]
    groupes.groupBy("groupe").agg(*[F.round(F.avg(d), 2).alias(d) for d in divisions]).orderBy("groupe").show()


if __name__ == "__main__":
    spark = creer_spark("ipc-mllib")
    donnees = spark.read.parquet((GOLD / "variables_modele").as_posix()).cache()
    print("Régression : choix des hyperparamètres sur la validation 2024")
    famille_retenue = regression(donnees)
    essais_kmeans, k_retenu = kmeans()
    afficher_resultats(famille_retenue, essais_kmeans, k_retenu)
