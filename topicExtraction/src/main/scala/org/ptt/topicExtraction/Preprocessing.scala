package org.ptt.topicExtraction

import java.util.Locale

import org.apache.spark.ml.Pipeline
import org.apache.spark.ml.feature.{CountVectorizer, CountVectorizerModel, RegexTokenizer, StopWordsRemover}
import org.apache.spark.mllib.feature.Stemmer
import org.apache.spark.sql.{DataFrame, SparkSession}

class Preprocessing {

  def preprocess(document: DataFrame): (DataFrame, Array[String])= {

    // filter redirects, sources, references etc.




    // prepare tokenizer
    val regexTokenizer = new RegexTokenizer()
      .setInputCol( "text" )
      .setOutputCol( "tokenized" )
      .setPattern( "\\W" ) // alternatively .setPattern("\\w+").setGaps(false)

    // set locale to en, because stopwordsremover.setlocale doesnt seem to work
    Locale.setDefault( Locale.ENGLISH )
    // prepare stopwordremover
    val swRemover = new StopWordsRemover()
      .setInputCol( "tokenized" )
      .setOutputCol( "filtered" )
      .setCaseSensitive( false )

    // prepare stemmer
    val stemmed = new Stemmer()
      .setInputCol( regexTokenizer.getOutputCol )
      .setOutputCol( "stemmed" )
      .setLanguage( "English" )

    // lemmatization?


    // prepare countvectorizer
    val cVectorizer = new CountVectorizer()
      .setInputCol( "stemmed" )
      .setOutputCol( "features" )
      .setVocabSize( 50000 )
      .setMinDF( 2 )

    val pipeline = new Pipeline()
      .setStages( Array( regexTokenizer, swRemover, stemmed, cVectorizer ) )

    val model = pipeline.fit( document )
    val resultDoc = model.transform(document).select("features")


    (resultDoc, model.stages(3).asInstanceOf[CountVectorizerModel].vocabulary)

  }

}
