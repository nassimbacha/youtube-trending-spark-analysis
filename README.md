# Big Data Analysis with Spark - YouTube Trending Videos (113 Countries)

*[Version française plus bas](#analyse-de-données-massives-avec-spark---youtube-trending-videos-113-pays)*

## Context

Academic project completed as part of the **Massive Databases** course
(Diploma in Applied Data Science, Collège La Cité, Winter 2025). Completed
as a team project.

## Objective

Perform a big data analysis using Apache Spark, applying concepts of Spark
architecture, RDDs, DataFrames, transformations, and actions on a
large-scale dataset.

## Dataset

**[Trending YouTube Video Statistics (113 Countries)](https://www.kaggle.com/datasets/asaniczka/trending-youtube-videos-113-countries)**
(Kaggle, by asaniczka) - daily list of trending YouTube videos across 113
countries.

- 2,139,661 observations (after fixing the CSV read), 18 variables
- The original dataset was larger; it was reduced to a 113-country subset
  to remain manageable within the Hadoop cluster environment used for this
  project.

## Technical environment

- Dockerized Hadoop cluster (`yacinoxe/hadoop-cluster` image), HDFS for
  dataset storage
- Apache Spark 3.5.0 (PySpark), run via `spark-submit` and the interactive
  shell

## Approach

**1. Ingestion and CSV read correction**
YouTube video descriptions often span multiple lines. An initial read
without the `multiLine`/`quote`/`escape` options produced an incorrect
total of roughly 34 million rows; correcting the read (`multiLine=true`)
revealed the true observation count: 2,139,661.

**2. Cleaning (`df_clean`)**
Filtering out incomplete rows (missing video ID, date, or country),
converting numeric columns (views, likes, comments) to `long` type to
enable aggregations.

**3. DataFrame transformations**
- `select`: narrowing down to relevant columns
- `filter`: isolating highly popular videos (>= 1 million views)
- `withColumn`: creating an engagement metric (`like_rate_pct`)
- `groupBy` + `agg` + `orderBy`: aggregating popular video counts by
  country
- `orderBy`: global TOP 20 ranking by view count

**4. Spark actions**
- `countDistinct`: number of distinct countries (113)
- `agg` + `sum`: total view count across the dataset
- `write.parquet`: writing the TOP 20 result to HDFS in Parquet format

**5. DataFrame vs RDD comparison**
The count of videos per country was computed using both APIs: the
DataFrame approach (`groupBy`/`agg`) is more concise and benefits from
Catalyst/Tungsten engine optimizations, while the RDD approach
(`map`/`reduceByKey`) is lower-level but remains useful for transformations
not directly expressible in SQL.

**6. Business questions**
- Which countries generate the most trending videos?
- Which countries have the highest average engagement rate (likes /
  views)? This rate is computed as the mean of each video's individual
  like/view ratio (not a ratio recomputed from total sums), giving equal
  weight to every video regardless of its view count.
- How many videos exceed 1 million views, per day?

## Tools

Python, PySpark (DataFrames and RDDs), Hadoop (HDFS), Docker

## Result

A reproducible Spark pipeline processing over 2.1 million observations,
including the diagnosis and correction of a real large-data ingestion
issue (multi-line descriptions), a reasoned comparison between the
DataFrame and RDD APIs, and three concrete business analyses.

---

# Analyse de Données Massives avec Spark - YouTube Trending Videos (113 pays)

## Contexte

Projet académique réalisé dans le cadre du cours **Base de données massive**
(Diplôme d'études collégiales en sciences des données appliquées, Collège La
Cité, hiver 2025). Projet réalisé en équipe.

## Objectif

Réaliser une analyse de données massives avec Apache Spark, en appliquant
les notions d'architecture Spark, de RDD, de DataFrames, de transformations
et d'actions sur un jeu de données volumineux.

## Dataset

**[Trending YouTube Video Statistics (113 Countries)](https://www.kaggle.com/datasets/asaniczka/trending-youtube-videos-113-countries)**
(Kaggle, par asaniczka) - liste quotidienne des vidéos tendances sur YouTube
dans 113 pays.

- 2 139 661 observations (après correction de la lecture du CSV), 18 variables
- Le dataset original contenait davantage de données ; il a été réduit à un
  sous-ensemble de 113 pays pour rester exploitable dans l'environnement du
  cluster Hadoop utilisé pour ce projet.

## Environnement technique

- Cluster Hadoop dockerisé (image `yacinoxe/hadoop-cluster`), HDFS pour le
  stockage du dataset
- Apache Spark 3.5.0 (PySpark), exécution via `spark-submit` et shell
  interactif

## Démarche

**1. Ingestion et correction de la lecture CSV**
Les descriptions vidéo YouTube s'étendent souvent sur plusieurs lignes. Une
première lecture sans les options `multiLine`/`quote`/`escape` faisait
apparaître un total erroné d'environ 34 millions de lignes ; la correction
de la lecture (`multiLine=true`) a permis d'obtenir le nombre réel
d'observations : 2 139 661.

**2. Nettoyage (`df_clean`)**
Filtrage des lignes incomplètes (identifiant vidéo, date ou pays manquants),
conversion des colonnes numériques (vues, likes, commentaires) en type
`long` pour permettre les agrégations.

**3. Transformations DataFrame**
- `select` : réduction aux colonnes utiles
- `filter` : isolement des vidéos très populaires (≥ 1 million de vues)
- `withColumn` : création d'un indicateur d'engagement (`like_rate_pct`)
- `groupBy` + `agg` + `orderBy` : agrégation du nombre de vidéos populaires
  par pays
- `orderBy` : classement TOP 20 mondial par nombre de vues

**4. Actions Spark**
- `countDistinct` : nombre de pays distincts (113)
- `agg` + `sum` : somme totale des vues du dataset
- `write.parquet` : écriture du résultat TOP 20 dans HDFS au format Parquet

**5. Comparaison DataFrame vs RDD**
Le comptage du nombre de vidéos par pays a été réalisé avec les deux API :
l'approche DataFrame (`groupBy`/`agg`) est plus concise et bénéficie des
optimisations du moteur Catalyst/Tungsten, tandis que l'approche RDD
(`map`/`reduceByKey`) est plus bas niveau mais reste utile pour des
transformations non exprimables directement en SQL.

**6. Questions métier**
- Quels pays génèrent le plus de vidéos tendances ?
- Quels pays ont le meilleur taux d'engagement moyen (likes / vues) ? Ce
  taux est calculé comme la moyenne des ratios individuels likes/vues de
  chaque vidéo (et non un ratio recalculé sur les sommes totales), donnant
  un poids égal à chaque vidéo indépendamment de son volume de vues.
- Quel est le nombre de vidéos dépassant 1 million de vues, par jour ?

## Outils

Python, PySpark (DataFrames et RDD), Hadoop (HDFS), Docker

## Résultat

Un pipeline Spark reproductible traitant plus de 2,1 millions
d'observations, avec un diagnostic et une correction d'un problème réel de
lecture de données volumineuses (descriptions multi-lignes), une
comparaison argumentée entre les API DataFrame et RDD, et trois analyses
métier concrètes.
