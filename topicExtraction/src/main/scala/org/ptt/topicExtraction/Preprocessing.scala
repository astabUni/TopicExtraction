package org.ptt.topicExtraction

import java.util.Locale

import org.apache.spark.ml.Pipeline
import org.apache.spark.ml.feature.{RegexTokenizer, StopWordsRemover}
import org.apache.spark.sql.DataFrame
import org.ptt.topicExtraction.transformers.Lemmatizer

object Preprocessing {


  def preprocess(document: DataFrame, swInputCol: String): (DataFrame) = {

    val regexTokenizer = new RegexTokenizer()
      .setInputCol("text")
      .setOutputCol("tokens")
      .setMinTokenLength(3)
      .setPattern("\\W")
      .setToLowercase(if (swInputCol.matches("posTags")) false else true)


    val lemmas = new Lemmatizer()
      .setInputCol("tokens")
      .setOutputCol("lemma")


    val posTags = new POSTagger()
      .setInputCol("lemma")
      .setOutputCol("posTags")


    // set locale to en, because stopwordsremover.setlocale doesnt seem to work
    Locale.setDefault(Locale.ENGLISH)
    val swRemover = new StopWordsRemover()
      .setInputCol(swInputCol)
      .setOutputCol("filtered")
      .setCaseSensitive(false)
    // customize list of stopwords
    swRemover.setStopWords(swRemover.getStopWords ++ Array("use", "rrb", "lrb"))


    val nouns = Array(regexTokenizer, lemmas, posTags, swRemover)
    val lda = Array(regexTokenizer, lemmas, swRemover)


    val pipeline = new Pipeline()
      .setStages(if (swInputCol.matches("posTags")) nouns else lda)


    val model = pipeline.fit(document)
    val resultDoc = model.transform(document)
    resultDoc
  }
}
