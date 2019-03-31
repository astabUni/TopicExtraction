package org.ptt.topicExtraction


import java.io.File

import org.apache.commons.io.FileUtils
import org.apache.spark.sql.types.{StringType, StructField, StructType}
import org.apache.spark.sql.{DataFrame, SparkSession, functions}

import scala.collection.mutable


object Main {

  val InputLocation = "src/main/resources/in/"
  val OutputLocation = "src/main/resources/out/"
  val ArticleLocation = InputLocation + "AA/wiki_00"
  val ArticleIDLocation = InputLocation + "category_to_articleids.json"
  val CatResult: mutable.HashMap[String, DataFrame] = mutable.HashMap.empty[String, DataFrame]
  val TopicDistributionLDA: mutable.HashMap[String, DataFrame] = mutable.HashMap.empty[String, DataFrame]


  def writeToFile(path: String): Unit = {

    FileUtils.deleteDirectory(new File(OutputLocation + path))

    if (path.matches("nouns/")) {
      for ((cat, df) <- CatResult) {
        df.write
          .option("header", "true")
          .option("inferSchema", "true")
          .csv(OutputLocation + path + cat)
      }
    }


    if (path.matches("lda/")) {
    for ((cat, df) <- CatResult) {
      df.coalesce(1).write.mode("append")
        .option("header", "true")
        .option("inferSchema", "true")
        .json(OutputLocation + path + cat)
    }

      for ((cat, df) <- TopicDistributionLDA) {
        df.coalesce(1).write.mode("append")
          .option("header", "true")
          .option("inferSchema", "true")
          .json(OutputLocation + path + cat + "/topicDistribution")
      }
    }

  }


  def countNouns(articles: DataFrame, catArticleIDs: DataFrame, sparkSesh: SparkSession): Unit = {

    import sparkSesh.implicits._

    // preprocess articles
    val preprocessed = Preprocessing.preprocess(articles, "posTags").cache()
    preprocessed.foreachPartition(x => {})


    // count nouns for each category
    catArticleIDs.columns.foreach(cat => {
      val articleIDs = catArticleIDs.select("`" + cat + "`").first().getAs[mutable.WrappedArray[String]](0)
      val tempDF = preprocessed.filter($"id".isin(articleIDs: _*))

      if (!tempDF.head(1).isEmpty) {
        val nounsDF = tempDF.select('filtered, functions.explode('filtered).as('nouns))
          .groupBy("nouns").count()
          .sort(functions.desc_nulls_last("count"))
          .limit(20)
        CatResult += (cat -> nounsDF)
      }
    })
    preprocessed.unpersist()

    writeToFile("nouns/")

  }


  def runLDA(articles: DataFrame, catArticleIDs: DataFrame, sparkSesh: SparkSession): Unit = {

    import sparkSesh.implicits._

    // preprocess articles
    val preprocessed = Preprocessing.preprocess(articles, "lemma").cache()
    preprocessed.foreachPartition(x => {})


    // run countVectorizer & LDA for each category
    catArticleIDs.columns.foreach(cat => {
      val articleIDs = catArticleIDs.select("`" + cat + "`").first().getAs[mutable.WrappedArray[String]](0)
      val tempDF = preprocessed.filter($"id".isin(articleIDs: _*))

      if (!tempDF.head(1).isEmpty) {
        tempDF.select("filtered").show(100)
        val (lda, cVector) = LDAPipeline.applyLDA(tempDF)
        CatResult += (cat -> lda)
        TopicDistributionLDA += (cat -> cVector)
      }
    })
    preprocessed.unpersist()

    writeToFile("lda/")

  }


  def main(args: Array[String]): Unit = {

    //provide schema for articles Dataframe
    val articlesSchema = StructType(Seq(StructField("id", StringType, true),
      StructField("text", StringType, true),
      StructField("title", StringType, true),
      StructField("url", StringType, true)))


    // init spark
    val spark = SparkSession
      .builder
      .master("local[8]")
      .appName("Domain Exploration")
      //.config("spark.eventLog.enabled", "true")
      //.config("spark.eventLog.dir", "src/main/resources/out/log/")
      .getOrCreate()


    // ingest data
    val articlesDF = spark.read.schema(articlesSchema)
      .json(ArticleLocation)
      .select("id", "text", "title").cache()

    val catTest = articlesDF.filter(articlesDF("id").isin(60525, 369456, 397905, 440644, 764616, 776343, 776818, 907940, 1604940, 3212696, 3593094, 3898076, 3906549, 4589953, 4591058, 4596063, 4597051, 4601056, 4601803, 5051859, 5142649, 7343721, 7463271, 7898037, 12314938, 12740151, 13418501, 13764323, 13869651, 16316920, 16317014, 16341329, 16400024, 16430047, 16435372, 16729930, 17689921, 18823880, 21772137, 23028629, 24096813, 24164712, 25087061, 25839957, 27277284, 30442608, 31122048, 32011253, 32068321, 34841174, 39848741, 43922705, 44302090, 45292535, 49924727, 52137353, 54185422, 55569888, 57420523, 59237611))

    // Category: ArticleIDs
    val catArticleIDs = spark.read.json(ArticleIDLocation).cache()

    //countNouns(articlesDF, catArticleIDs, spark)
    runLDA(articlesDF, catArticleIDs, spark)


    spark.close()
  }

}
