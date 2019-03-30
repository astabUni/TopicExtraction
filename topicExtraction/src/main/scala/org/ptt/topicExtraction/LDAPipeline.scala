package org.ptt.topicExtraction

import org.apache.spark.ml.clustering.LDA
import org.apache.spark.ml.feature.{CountVectorizer, CountVectorizerModel}
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions.udf

import scala.collection.mutable
import scala.util.{Success, Try}

object LDAPipeline {


  def applyLDA(preprocessedDoc: DataFrame): (DataFrame, DataFrame) ={

    val docCount = if(preprocessedDoc.take(5).length < 5) 1 else 5
    val cVectorizerModel = new CountVectorizer()
      .setInputCol( "filtered" )
      .setOutputCol( "features" )
      .setVocabSize( 100000 )
      .setMinDF( docCount )
      .fit(preprocessedDoc)
    val cachedCVectors = cVectorizerModel.transform(preprocessedDoc).cache()


    val ldaModel = new LDA()
      .setK( 10 )
      .setMaxIter( 100 )
      .setOptimizer("online")
      .fit(cachedCVectors)
    val topics = ldaModel.describeTopics( 5 )
    val vocList = for {v <- cVectorizerModel.vocabulary} yield v
    val UDFconvertWords = udf( (array: mutable.WrappedArray[Int]) => array.map(element => vocList( element ) ) )
    val resultLDA = topics.withColumn( "terms", UDFconvertWords( topics( "termIndices" ) ) ).select("topic", "termWeights", "terms")


    cachedCVectors.unpersist()
    (resultLDA, ldaModel.transform(cachedCVectors))

  }

}
