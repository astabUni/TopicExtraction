package org.ptt.topicExtraction

import com.databricks.spark.xml._
import org.apache.spark.ml.clustering.LDA
import org.apache.spark.ml.feature.CountVectorizerModel
import org.apache.spark.sql.functions.udf
import org.apache.spark.sql.{SparkSession, functions}

object Main {

  def main(args: Array[String]): Unit = {


    // init spark
    val spark = SparkSession
      .builder
      .master("local[8]")
      .appName( "BasicLDAPipeline" )
      .getOrCreate()


    // ingest data
    val df = spark.read
      .option( "rowTag", "page" )
      .option("excludeAttribute", true)
      .xml( "src/main/resources/articleTest.xml" )
      .select( "text")

    // pos-tagging & noun counting
    import spark.implicits._
    val nouns = df
      .select(functions.explode(CoreNLPImpl.ssplit('text)).as('sentence))
      .select(functions.explode(CoreNLPImpl.tokenize('sentence)).as('tokens))
      .withColumn("pos", CoreNLPImpl.pos('tokens))
      .select('tokens,functions.explode('pos).as('tags))
      .filter($"tags".like("NN"))
      .groupBy("tokens").count()
    nouns.show()



    // preprocess
    val startPreProcessTime = System.nanoTime()
    val (feature, model) = Preprocessing.preprocess(df)
    feature.cache()
    val elapsedPreProcessTime = (System.nanoTime() - startPreProcessTime) / 1e9
    println(s"\t PreProcessing time: $elapsedPreProcessTime sec")


    // run LDA and pretty print results
    val startLDATime = System.nanoTime()
    val ldaModel = new LDA()
      .setK( 10 )
      .setMaxIter( 10 )
      .setTopicConcentration(0.1)
      .setDocConcentration(0.1)
      .fit(feature)//.setOptimizer("em")
    val topics = ldaModel.describeTopics( 5 )
    val vocList = for {v <- model.stages(3).asInstanceOf[CountVectorizerModel].vocabulary} yield v
    val UDFconvertWords = udf( (array: collection.mutable.WrappedArray[Int]) => array.map( element => vocList( element ) ) )
    val results = topics.withColumn( "terms", UDFconvertWords( topics( "termIndices" ) ) )
    results.show(false)
    val elapsedLDATime = (System.nanoTime() - startPreProcessTime) / 1e9
    println(s"\t LDA time: $elapsedLDATime sec")



    // cosine similarity between categories



    spark.close()
  }

}
