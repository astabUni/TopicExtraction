package org.ptt.topicExtraction


import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import com.databricks.spark.xml._
import org.apache.spark.ml.clustering.{LDA, LDAModel}
import org.apache.spark.sql.functions.udf

object Main {

  def main(args: Array[String]): Unit = {


    // init spark
    val spark = SparkSession
      .builder
      .config( new SparkConf().setMaster( "local[8]" ) )
      .appName( "BasicLDAPipeline" )
      .getOrCreate()

    // ingest data
    val df = spark.read
      .option( "rowTag", "revision" )
      .option("excludeAttribute", true)
      .xml( "src/main/resources" )
      .select("id","parentid", "text")


    // preprocess
    val (feature, vocabulary) = new Preprocessing().preprocess(df)
    feature.cache()



    // run LDA and pretty print results
    val ldaModel = new LDA().setK( 10 ).setMaxIter( 10 ).fit(feature)//.setOptimizer("em")

    val vocList = for {v <- vocabulary} yield v

    val topics = ldaModel.describeTopics( 5 )
    val convertWordsUDF = udf( (array: collection.mutable.WrappedArray[Int]) => array.map( element => vocList( element ) ) )
    val results = topics.withColumn( "terms", convertWordsUDF( topics( "termIndices" ) ) )

    df.printSchema()
    results.show(false)

    // cosine similarity between categories




    spark.stop()
  }

}
