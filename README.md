# USA_Population_Pipeline
#PLAN
- [1. Introduction](https://github.com/FenosoaSolofoniaina/USA_Population_Pipeline#1-introduction)
- [2. Architecture](https://github.com/FenosoaSolofoniaina/USA_Population_Pipeline#2-architecture)
- [3. Fonctionnalités](https://github.com/FenosoaSolofoniaina/USA_Population_Pipeline#3-fonctionnalit%C3%A9s)
- [4. Structures](https://github.com/FenosoaSolofoniaina/USA_Population_Pipeline#4-structures)
- [5. Stacks](https://github.com/FenosoaSolofoniaina/USA_Population_Pipeline#5-stacks)
- [6. Configuration](https://github.com/FenosoaSolofoniaina/USA_Population_Pipeline#6-configuration)
- [7. Execution](https://github.com/FenosoaSolofoniaina/USA_Population_Pipeline#7-execution)

## 1. Introduction
Le projet **USA_Population_Pipeline** est un projet de pipeline de données sur [Databricks](https://www.databricks.com/). Les données sont extraites depuis l'api [Data USA](https://datausa.io/about/api) avec le language [Python](https://www.python.org/), nettoyées et transformées en utilisant [PySpark](https://spark.apache.org/docs/latest/api/python/index.html) et SQL, pour être ensuite stockées dans des `Delta table` sur Databricks. Ces données seront ensuite téléversé dans *Google BigQuery* de [Google CLoud](https://cloud.google.com/) pour l'analyse plus tard.

## 2.Architecture
Le projet se base sur l'architecture `Médallion` où les données passent par 3 couches qui sont la couche `bronze`, la couche `silver` et la couche `Gold`
               
               Data USA API
                    │
                    ▼
          Bronze Delta Table
                    │
                    ▼
          Silver Delta Table
                    │
                    ▼
           Gold Delta Tables
                    │
                    ▼
        Google BigQuery (Data Warehouse)

## 3.Fonctionnalités
- Extraire les données depuis Data USA API.
- Stocker les données brutes dans la couche Bronze.
- Valider, nettoyer, transformer et encoder les données depuis la couche Bronze vers la couche Silver.
- Garder une historique des données dans la couche Silver pour pouvoir faire des comparaison ensuite entre les données déjà existantes et les données fraichement extraites.
- Créer des tables de KPI dans la couche Gold utiles pour les analyses plus tard.
- Envoyer les données sur le cloud.

## 4.Structures
    |-- USA_Population_Pipeline/
        |--data/   # Si besoin de stocker les données en CSV ou JSON (en cours... )
        |-- logs/  # Log pour suivre le déroulement du pipeline (en cours... )
        |-- queries/ # Contenant les fichiers SQL de test avant d'envoyer le code dans `gold_layer.py`
        |-- scripts/
            |-- extract.py    # Fonctions d'extraction de données brutes
            |-- transform.py  # Fonctions de nettoyage et d'encodeage des données
            |-- validate.py   # Valider les données avant les traitements
            |-- load.py       # Charger les données dans les "delta table"
            |-- utils.py      # Autres fonctions qui n'entrent pas dans le domaine des autres fichiers
        |-- README.md
        |-- bronze_layer.py   # Script responsable de l'extraction et stockage des données brutes
        |-- silver_layer.py   # Script responsable des transformations des données brutes en données propres
        |-- gold_layer.py     # Script responsable de la création des tables dans la couche Gold
        |-- .gitignore
        |-- requirements.txt

## 5. Stacks
|Techno | Utilisation |
|-------|-------------|
|Python	|Data ingestion |
|Apache Spark | Data cleaning, Data transformation |
|Databricks	| Data platform, Delta Lake	Storage |
|SQL | Analytics |
|GitHub | Version control |

## 6. Configuration
Il faut créer un fichier de configuration `.databricks.env` qui contient les variables d'environnements avec les variables suivants :
```txt
USER_AGENT=your_browser_user_agent
```
*your_browser_user_agent* sera l'user-agent de votre navigateur afin d'éviter que le site ne confonde notre requête avec un bot malveillant et donc d'éviter d'être bloqué.

```txt
PROJECT_DIR=your_project_directory
```
*your_project_directory* est le chemin vers le projet sur Databricks. Généralement, elle aura la forme `/Workspace/Users/<yourmail@gmail.com>/<project>`

```txt
BRONZE_TABLE_NAME=your_bronze_table_name
```
*your_bronze_table_name* est le chemin vers la table dans le catalogue qui contiendra les données brutes. De la forme `workspace.<schema>.<bronze_table_name>`,

```txt
SILVER_HISTORICAL_TABLE_NAME=your_silver_table_name_for_historical
```
*your_silver_table_name_for_historical* est le chemin vers la table dans le catalogue et qui va contenir les tous les données néttoyés.

```txt
SILVER_TABLE_NAME=your_silver_table_name
```
*your_silver_table_name*, idem que précédement, mais cette fois-ci, la table ne contient que les données les plus récents.

```txt
GOLD_SCHEMA_NAME=your_gold_schema
```
*your_gold_schema*, Le chemin pour stocker les données depuis la couche Silver vers la couche Gold. De la forme `workspace.<gold_schema>.`
NB: Seul le chemin vers le schema est nécessaire car des tables y seront automatiques créés à l'execution du pipeline.

Finalement, le fichier sera :
`.databricks.env`
```txt
USER_AGENT=your_browser_user_agent
PROJECT_DIR=your_project_directory_on_databricks
BRONZE_TABLE_NAME=your_bronze_table_name
SILVER_HISTORICAL_TABLE_NAME=your_silver_table_name_for_historical
SILVER_TABLE_NAME=your_silver_table_name
GOLD_SCHEMA_NAME=your_gold_schema
```

## 7. Execution
Il existe 3 différentes façons de lancer le Pipeline :
- Lancement manuellement des 3 couches :
    Cela consiste à lancer dans l'ordre les fichier `bronze_layer.py`, `silver_layer.py` et `gold_layer.py`. C'est idéal pour tester si la configuration de chaque fichier a bien fonctionné et cette méthode permet de détecter d'éventuelles erreurs qui pourront apparaître lors du l'execution du pipeline.

- Création de fichier `main.py` :
    Comme précédement, cela consiste à lancer les 3 fichiers mais de manière automatique depuis un fichier `main.py`. C'est idéal pour voir l'interaction et la dépendance entre les 3 couches.

- Lancement automatique du fichier :
    Une fois que les tests sur les 3 fichiers ont été bien vérifiés. Il est maintenant temps de lancer le pipeline de manière automatique depuis l'onglet `Jobs & Pipelines` > `Create` > `Job`. Le gros avantage est qu'on peut planifier l'execution automatique du pipeline via l'option `schedule`.


**Fenosoa SOLOFONIAINA**
&copy; 2026