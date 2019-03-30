package org.ptt.topicExtraction

import java.util.Locale

import org.apache.spark.ml.feature.{RegexTokenizer, StopWordsRemover}
import org.apache.spark.ml.{Pipeline, PipelineModel, PipelineStage}
import org.apache.spark.sql.DataFrame
import org.ptt.topicExtraction.transformers.Lemmatizer

object Preprocessing {


  def preprocess(document: DataFrame, swInputCol: String): (DataFrame)= {

    val regexTokenizer = new RegexTokenizer()
      .setInputCol( "text" )
      .setOutputCol( "tokens" )
      .setMinTokenLength(2)
      .setPattern( "\\W" )
      .setToLowercase(if(swInputCol.matches("posTags")) false else true)

    val posTags = new POSTagger()
      .setInputCol("tokens")
      .setOutputCol("posTags")

    val lemmas = new Lemmatizer()
      .setInputCol("tokens")
      .setOutputCol("lemma")

    // set locale to en, because stopwordsremover.setlocale doesnt seem to work
    Locale.setDefault( Locale.ENGLISH )
    val swRemover = new StopWordsRemover()
      .setInputCol( swInputCol )
      .setOutputCol( "filtered" )
      .setCaseSensitive( false )
    // customize list of stopwords
    swRemover.setStopWords(swRemover.getStopWords ++ Array("use", "rrb", "lrb"))


    val nouns = Array(regexTokenizer, posTags, swRemover)
    val lda =  Array(regexTokenizer, posTags, lemmas, swRemover)


    val pipeline = new Pipeline()
      .setStages( if(swInputCol.matches("posTags")) nouns else lda)

    val model = pipeline.fit( document )
    val resultDoc = model.transform(document)
    resultDoc
  }
}
