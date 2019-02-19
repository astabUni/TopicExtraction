package org.ptt.topicExtraction

import java.util.Locale
import org.apache.spark.ml.{Pipeline, PipelineModel}
import org.apache.spark.ml.feature.{CountVectorizer, CountVectorizerModel, RegexTokenizer, StopWordsRemover}
import org.apache.spark.mllib.feature.Stemmer
import org.apache.spark.sql.{DataFrame, SparkSession}

object Preprocessing {

  def preprocess(document: DataFrame): (DataFrame, PipelineModel)= {

    // filter redirects, sources, references etc.



    // prepare tokenizer
    val regexTokenizer = new RegexTokenizer()
      .setInputCol( "text" )
      .setOutputCol( "tokenized" )
      .setMinTokenLength(2)
      .setPattern( "\\W" ) // alternatively .setPattern("\\w+").setGaps(false)


    // set locale to en, because stopwordsremover.setlocale doesnt seem to work
    Locale.setDefault( Locale.ENGLISH )
    // prepare stopwordremover
    val swRemover = new StopWordsRemover()
      .setInputCol( regexTokenizer.getOutputCol )
      .setOutputCol( "filtered" )
      .setCaseSensitive( false )
    // customize list of stopwords
    swRemover.setStopWords(swRemover.getStopWords ++ Array("com", "ref"))


    // prepare stemmer
    val stemmed = new Stemmer()
      .setInputCol( swRemover.getOutputCol )
      .setOutputCol( "stemmed" )
      .setLanguage( "English" )

    // lemmatization?



    // prepare countvectorizer
    val cVectorizer = new CountVectorizer()
      .setInputCol( stemmed.getOutputCol )
      .setOutputCol( "features" )
      .setVocabSize( 50000 )
      .setMinDF( 2 )

    val pipeline = new Pipeline()
      .setStages( Array( regexTokenizer, swRemover, stemmed, cVectorizer ) )

    val model = pipeline.fit( document )
    val resultDoc = model.transform(document).select("features")


    (resultDoc, model)

  }

}
