package org.ptt.topicExtraction


import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.types.{StringType, StructField, StructType}

import scala.collection.mutable.{ArrayBuffer, WrappedArray}


object Main {

  val InputLocation = "src/main/resources/in/"
  val OutputLocation = "src/main/resources/out/"


  def main(args: Array[String]): Unit = {

    val articlesSchema = StructType(Seq(StructField("id", StringType, true),
      StructField("text", StringType, true),
      StructField("title", StringType, true),
      StructField("url", StringType, true)))


    // init spark
    val spark = SparkSession
      .builder
      .master("local[8]")
      .appName( "Domain Exploration" )
      .config("spark.eventLog.enabled", "true")
      .config("spark.eventLog.dir", "src/main/resources/out/log/")
      .getOrCreate()


    import spark.implicits._
    // ingest data
    val jsonDF = spark.read.schema(articlesSchema)
      .json(InputLocation+"articles-d6.json")
      //.repartition(16).toDF()
      .select("id","text", "title").cache()

    val catLDA: ArrayBuffer[DataFrame] = new ArrayBuffer[DataFrame]()
    //val catTest = jsonDF.filter(jsonDF("id").isin(60525,369456,397905,440644,764616,776343,776818,907940,1604940,3212696,3593094,3898076,3906549,4589953,4591058,4596063,4597051,4601056,4601803,5051859,5142649,7343721,7463271,7898037,12314938,12740151,13418501,13764323,13869651,16316920,16317014,16341329,16400024,16430047,16435372,16729930,17689921,18823880,21772137,23028629,24096813,24164712,25087061,25839957,27277284,30442608,31122048,32011253,32068321,34841174,39848741,43922705,44302090,45292535,49924727,52137353,54185422,55569888,57420523,59237611))


    // Category: ArticleIDs
    val catArticleIDs = spark.read.json(InputLocation+"category_to_articleids-d6.json").cache()


    catArticleIDs.columns.foreach(cat => {

      val articleIDs = catArticleIDs.select("`"+cat+"`").first().getAs[WrappedArray[String]](0)

      val tempDF = jsonDF.filter($"id".isin(articleIDs:_*))//.select("id","filtered").show(60)

      val (feature, model) = Preprocessing.preprocess(tempDF)
      feature.cache()
      catLDA.append( ApplyLDA.applyLDA(feature))
      feature.unpersist()

    })


    for (elem <- catLDA) {elem.show(false)}


    spark.close()
  }

}
