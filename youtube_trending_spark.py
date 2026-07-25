"""
Analyse de Données Massives avec Spark - YouTube Trending Videos (113 pays)
Projet académique - UA3 - Analyse de données massives avec Spark

Equipe : Yazid Aloui, Mahdi Sahli, Youva Hamani, Nassim Tazir

Ce script reproduit l'ensemble du pipeline exécuté initialement en mode
interactif (PySpark shell) : lecture et nettoyage du dataset, transformations
DataFrame, actions Spark, comparaison avec l'API RDD, et réponses aux
questions métier.
"""

from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col, count, countDistinct, sum as spark_sum, avg,
    round as spark_round
)

if __name__ == "__main__":

    # 1. Creation de la SparkSession
    spark = SparkSession.builder \
        .appName("YouTubeTrending_113Countries") \
        .master("local[*]") \
        .getOrCreate()

    spark.sparkContext.setLogLevel("ERROR")

    # ------------------------------------------------------------------
    # 2. Lecture du CSV avec gestion des descriptions multi-lignes
    # ------------------------------------------------------------------
    # Sans les options multiLine/quote/escape, Spark comptait les lignes de
    # description YouTube (souvent multi-lignes) comme des lignes CSV
    # distinctes, ce qui gonflait artificiellement le total a ~34 millions
    # de lignes "fantomes". Avec ces options, chaque video est correctement
    # regroupee sur une seule ligne.
    df = spark.read \
        .option("header", "true") \
        .option("multiLine", "true") \
        .option("quote", '"') \
        .option("escape", '"') \
        .csv("/user/root/youtube/trending_yt_videos_113_countries.csv")

    print("===== APERCU DES DONNEES =====")
    df.show(5, truncate=False)

    print("===== SCHEMA DU DATAFRAME =====")
    df.printSchema()

    print("===== NOMBRE TOTAL DE LIGNES (brut) =====")
    print(df.count())  # 2 139 661 lignes reelles apres correction multiLine

    # ------------------------------------------------------------------
    # 3. Nettoyage : creation de df_clean
    # ------------------------------------------------------------------
    df_clean = df.filter(
        col("video_id").isNotNull() &
        col("snapshot_date").isNotNull() &
        col("country").isNotNull()
    )

    # Conversion des colonnes numeriques (string -> long) pour permettre
    # les agregations (SUM, AVG, MAX...)
    df_clean = df_clean \
        .withColumn("view_count", col("view_count").cast("long")) \
        .withColumn("like_count", col("like_count").cast("long")) \
        .withColumn("comment_count", col("comment_count").cast("long"))

    # ------------------------------------------------------------------
    # 4. Transformations DataFrame
    # ------------------------------------------------------------------

    # Transformation 1 - select : colonnes utiles pour l'analyse
    df_simple = df_clean.select(
        "snapshot_date", "country", "title",
        "view_count", "like_count", "comment_count"
    )

    # Transformation 2 - filter : videos tres populaires (>= 1M vues)
    df_popular = df_clean.filter(col("view_count") >= 1_000_000)
    df_popular_simple = df_popular.select(
        "snapshot_date", "country", "title",
        "view_count", "like_count", "comment_count"
    )

    # Transformation 3 - withColumn : taux de like (engagement)
    df_popular_enriched = df_popular_simple.withColumn(
        "like_rate_pct",
        spark_round(col("like_count") * 100.0 / col("view_count"), 2)
    )

    # Transformation 4 - groupBy + agg + orderBy : videos populaires par pays
    df_country_stats = df_popular.groupBy("country").agg(
        count("*").alias("nb_popular_videos")
    )
    df_country_stats_sorted = df_country_stats.orderBy(
        col("nb_popular_videos").desc()
    )

    # Transformation 5 - orderBy : TOP 20 mondial par nombre de vues
    df_top20_global = df_popular_simple.orderBy(col("view_count").desc())

    print("===== TOP 20 VIDEOS LES PLUS VUES (MONDIAL) =====")
    df_top20_global.show(20, truncate=50)

    # ------------------------------------------------------------------
    # 5. Actions Spark
    # ------------------------------------------------------------------

    # Action 1 - countDistinct : nombre de pays distincts
    print("===== NOMBRE DE PAYS DISTINCTS =====")
    df_clean.select(
        countDistinct("country").alias("nb_countries")
    ).show()

    # Action 2 - agg + sum : somme totale des vues
    print("===== SOMME TOTALE DES VUES =====")
    df_clean.agg(
        spark_sum("view_count").alias("total_views")
    ).show()

    # Action 3 - write : ecriture du TOP 20 au format Parquet dans HDFS
    df_top20_global.write.mode("overwrite").parquet(
        "/user/root/youtube/output/top20_global"
    )
    print("===== ECRITURE PARQUET TERMINEE (top20_global) =====")

    # ------------------------------------------------------------------
    # 6. Comparaison partielle avec l'API RDD
    # ------------------------------------------------------------------

    # Nombre de videos par pays - version RDD (map / reduceByKey)
    rdd = df_clean.rdd
    videos_par_pays_rdd = (
        rdd
        .map(lambda row: (row.country, 1))
        .reduceByKey(lambda a, b: a + b)
    )
    top_pays_rdd = videos_par_pays_rdd.sortBy(lambda x: x[1], ascending=False)

    print("===== TOP 10 PAYS PAR NOMBRE DE VIDEOS (RDD) =====")
    print(top_pays_rdd.take(10))

    # Nombre de videos par pays - version DataFrame (groupBy / agg)
    df_videos_par_pays = (
        df_clean.groupBy("country")
        .agg(count("*").alias("nb_videos"))
        .orderBy(col("nb_videos").desc())
    )

    print("===== TOP 10 PAYS PAR NOMBRE DE VIDEOS (DataFrame) =====")
    df_videos_par_pays.show(10)

    # Conclusion (voir rapport) : l'API DataFrame est plus concise et
    # beneficie des optimisations du moteur Catalyst/Tungsten, tandis que
    # l'API RDD, plus bas niveau, reste utile pour des transformations
    # complexes non exprimables directement en SQL.

    # ------------------------------------------------------------------
    # 7. Questions metier
    # ------------------------------------------------------------------

    # Question 1 - Quels sont les pays qui generent le plus de videos
    # tendances ?
    print("===== QUESTION 1 : PAYS AVEC LE PLUS DE VIDEOS TENDANCES =====")
    df_clean.groupBy("country").agg(
        count("*").alias("nb_videos")
    ).orderBy(col("nb_videos").desc()).show(20)

    # Question 2 - Quels sont les pays avec le meilleur taux d'engagement
    # moyen (likes / vues) ?
    print("===== QUESTION 2 : TAUX D'ENGAGEMENT MOYEN PAR PAYS =====")
    df_engagement_country = df_popular_enriched.groupBy("country").agg(
        avg("like_rate_pct").alias("engagement_avg_pct")
    )
    df_engagement_country.orderBy(
        col("engagement_avg_pct").desc()
    ).show(20)

    # Question 3 - Nombre de videos depassant 1 million de vues, par jour
    print("===== QUESTION 3 : VIDEOS 1M+ VUES PAR JOUR =====")
    df_million_views_by_day = df_clean.withColumn(
        "is_million_plus",
        (col("view_count") >= 1_000_000).cast("int")
    ).groupBy("snapshot_date").agg(
        spark_sum("is_million_plus").alias("nb_videos_1M_plus")
    )
    df_million_views_by_day.orderBy(
        col("nb_videos_1M_plus").desc()
    ).show(20)

    # ------------------------------------------------------------------
    # 8. Fermeture propre de Spark
    # ------------------------------------------------------------------
    spark.stop()
