package org.ptt.topicExtraction

import java.util.Locale

import org.apache.spark.ml.feature.{RegexTokenizer, StopWordsRemover}
import org.apache.spark.ml.{Pipeline, PipelineModel}
import org.apache.spark.sql.DataFrame

object Preprocessing {


  def preprocess(document: DataFrame): (DataFrame, PipelineModel)= {


    val regexTokenizer = new RegexTokenizer()
      .setInputCol( "text" )
      .setOutputCol( "tokens" )
      .setMinTokenLength(2)
      .setPattern( "\\W" )


    val lemmas = new UnaryLemmatizer()
      .setInputCol("tokens")
      .setOutputCol("lemma")


    // set locale to en, because stopwordsremover.setlocale doesnt seem to work
    Locale.setDefault( Locale.ENGLISH )

    val swRemover = new StopWordsRemover()
      .setInputCol( "lemma" )
      .setOutputCol( "filtered" )
      .setCaseSensitive( false )
    // customize list of stopwords
    swRemover.setStopWords(swRemover.getStopWords ++ Array("com", "ref", "use", "rrb", "lrb"))


    val pipeline = new Pipeline()
      .setStages( Array( regexTokenizer, lemmas, swRemover) )

    val model = pipeline.fit( document )
    val resultDoc = model.transform(document)


    (resultDoc, model)

  }

}
