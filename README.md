# IPC Big Data : où les prix vont-ils monter le mois prochain ?

Mini-projet « Processus décisionnel Big Data » (5e année IA et Data).
Données : HCP, Indice des prix à la consommation (Mensuel) (Base 100 2017),
fiche data.gov.ma : https://data.gov.ma/data/fr/dataset/data_i4166

## Pipeline

| Étape | Script | Rôle |
|---|---|---|
| 01 | `scripts/01_collecte.py` | Télécharge le xlsx du HCP, le découpe en 116 CSV mensuels (`data/landing/ipc_AAAA_MM.csv`) et contrôle les chiffres clés |
| 02 | `scripts/02_producteur_flux.py` | Publie un mois toutes les N secondes dans `data/stream_in/` (flux simulé) |
| 03 | `spark/03_streaming.py` | Spark Structured Streaming : couche bronze + suivi (variation mensuelle, glissement annuel, alertes > 4 %) |
| 04 | `spark/04_spark_sql.py` | Spark SQL : silver (nettoyé), gold (inflation annuelle + classements, profil des villes, table des variables du modèle) |
| 05 | `spark/05_mllib.py` | MLlib : régression (linéaire, Random Forest, GBT) contre 2 références naïves, prévision du mois prochain, KMeans des villes |
| 06 | `api/main.py` | FastAPI : une SparkSession, requêtes Spark SQL sur les tables Parquet (`/flux`, `/previsions`, `/groupes`) |
| 06 | `frontend/` | React + Vite + Recharts au style « Prism » (sombre/clair), 3 pages : flux en direct, prévisions, groupes de villes |

## Lancement avec Docker (référence)

Prérequis : Docker Desktop démarré.

```bash
docker compose up --build              # collecte, flux (1 mois toutes les 5 s), API et interface
# Interface : http://localhost:3000   API : http://localhost:8000/docs
INTERVALLE=2 docker compose up         # publication plus rapide
docker compose logs -f streaming       # suivre les micro-batches
docker compose run --rm streaming python spark/04_spark_sql.py   # silver + gold (quand le flux a tout traité)
docker compose run --rm streaming python spark/05_mllib.py      # modèles + prévisions (environ 4 min)
docker compose down -v                 # repartir de zéro (efface le volume de données)
```

Les données vivent dans le volume Docker `ipc-data` (environ 4 fois plus rapide qu'un dossier
Windows monté). Sans `down -v`, le flux reprend là où il s'était arrêté grâce au checkpoint Spark.

La page « Flux en direct » fonctionne dès le premier mois traité. Les pages « Prévisions » et
« Groupes de villes » ont besoin des étapes 04 et 05 (une fois le flux terminé, environ 10 min à 5 s par mois).

## Lancement sans Docker (Windows)

Prérequis : Python 3.11 (pas 3.13+), Java 17, et sous Windows `HADOOP_HOME` avec
`bin/winutils.exe` et `bin/hadoop.dll` (nécessaires pour écrire du Parquet).

```bash
uv venv --python 3.11 .venv && uv pip install --python .venv -r requirements.txt
# PowerShell : Spark 3.5 a besoin de Java 17
$env:JAVA_HOME = "C:\Program Files\Eclipse Adoptium\jdk-17.0.18.8-hotspot"
# (SPARK_HOME et PYSPARK_PYTHON globaux sont ignorés par spark/commun.py : il utilise le Spark de pip)

.venv\Scripts\python scripts/01_collecte.py
# Rejouer tout l'historique d'un coup (environ 8 min sous Windows, 4 min sous Docker) :
.venv\Scripts\python scripts/02_producteur_flux.py --intervalle 0 --reset
.venv\Scripts\python spark/03_streaming.py --reset --une-fois
.venv\Scripts\python spark/04_spark_sql.py
.venv\Scripts\python spark/05_mllib.py
# Ou en direct, dans deux terminaux :
.venv\Scripts\python spark/03_streaming.py --reset
.venv\Scripts\python scripts/02_producteur_flux.py --intervalle 5 --reset
# Application, dans deux autres terminaux :
.venv\Scripts\python -m uvicorn main:app --app-dir api --port 8000
cd frontend; npm install; npm run dev        # http://localhost:3000
```

## Notebook commenté

`notebooks/ipc_bigdata.ipynb` déroule tout le pipeline (collecte, streaming, Spark SQL, MLlib), explique chaque choix,
affiche les résultats avec des graphiques, puis l'utilité pour la décision et les limites. Il est enregistré **déjà
exécuté** (sorties visibles sans rien lancer). Il importe les scripts du projet et affiche leur code avant de les
exécuter : une seule version du pipeline.

Lancer Jupyter **par le Python du venv** (`python -m`) : `jupyter lab` seul peut prendre un autre Python du PC.

```bash
uv pip install --python .venv -r requirements.txt -r notebooks/requirements.txt
# PowerShell : Java 17 obligatoire pour Spark 3.5
$env:JAVA_HOME = "C:\Program Files\Eclipse Adoptium\jdk-17.0.18.8-hotspot"
.venv\Scripts\python -m jupyterlab notebooks/ipc_bigdata.ipynb   # puis Run All (environ 15 min)
# ou sans interface :
.venv\Scripts\python -m nbconvert --to notebook --execute --inplace notebooks/ipc_bigdata.ipynb --ExecutePreprocessor.timeout=2400
```

## Bonus : Databricks

`databricks/ipc_databricks.py` est un notebook (format source Databricks) qui refait tout le pipeline
avec un **Volume Unity Catalog**, **Auto Loader** (`cloudFiles`) et des **tables Delta** (`workspace.ipc.*`).

1. Databricks (Free Edition suffit) : Workspace > Import > choisir `databricks/ipc_databricks.py`.
2. Attacher le calcul serverless, puis *Run all* (le notebook crée le schéma `ipc` et le Volume `donnees`).
3. Si le téléchargement depuis data.gov.ma est bloqué : téléverser `data/raw/i_i4166_data.xlsx` dans le Volume,
   dossier `raw/`, puis relancer.

Sur serverless, seul le déclencheur `availableNow` est autorisé : le notebook publie l'historique, lance le flux,
puis publie les 8 mois de 2026 et le relance (seuls les nouveaux fichiers sont traités, grâce au checkpoint).
Le même fichier se teste en local : `python databricks/ipc_databricks.py`. La démo locale reste la référence le jour J.

## Chiffres de contrôle (vérifiés automatiquement par les scripts)

- 116 mois (janvier 2017 à août 2026), 19 zones, 148 postes, 2 744 séries, 318 304 observations, 0 manquante.
- Inflation nationale sur 12 mois : 2,01 % (janvier 2018), 10,08 % (février 2023), -0,33 % (août 2026).
- Février 2023 : 144 séries (ville × indice général ou division) au-dessus de 4 % sur un an.
- Inflation annuelle nationale (moyenne annuelle des indices) : 6,66 % en 2022, 6,09 % en 2023.
