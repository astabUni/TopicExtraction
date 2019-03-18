package org.ptt.topicExtraction

import org.apache.spark.ml.clustering.LDA
import org.apache.spark.ml.feature.{CountVectorizer, CountVectorizerModel}
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions.udf

object ApplyLDA {


  def applyLDA(preprocessedDoc: DataFrame): DataFrame ={

    val startCountTime = System.nanoTime()
    // prepare countvectorizer
    val cVectorizerModel = new CountVectorizer()
      .setInputCol( "filtered" )
      .setOutputCol( "features" )
      .setVocabSize( 100000 )
      .setMinDF( 1 )
      .fit(preprocessedDoc)
    val cachedCVectors = cVectorizerModel.transform(preprocessedDoc).cache()
    val elapsedCountTime = (System.nanoTime() - startCountTime) / 1e9
    println(s"\t CountVector time: $elapsedCountTime sec")



    // run LDA and pretty print results
    val startLDATime = System.nanoTime()
    val ldaModel = new LDA()
      .setK( 10 )
      .setMaxIter( 100 )
      //.setTopicConcentration(1.1)
      //.setDocConcentration(6)
      .setOptimizer("online")
      .fit(cachedCVectors)
    val topics = ldaModel.describeTopics( 5 )
    val vocList = for {v <- cVectorizerModel.vocabulary} yield v
    val UDFconvertWords = udf( (array: collection.mutable.WrappedArray[Int]) => array.map( element => vocList( element ) ) )
    val resultLDA = topics.withColumn( "terms", UDFconvertWords( topics( "termIndices" ) ) ).select("topic", "termWeights", "terms")
    resultLDA.show(false)
    val elapsedLDATime = (System.nanoTime() - startLDATime) / 1e9
    println(s"\t LDA time: $elapsedLDATime sec")

    topics

  }

}
