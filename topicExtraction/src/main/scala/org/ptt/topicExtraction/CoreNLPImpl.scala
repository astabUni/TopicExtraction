package org.ptt.topicExtraction

import edu.stanford.nlp.process.Morphology
import edu.stanford.nlp.simple.{Document, Sentence}
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.udf

import scala.collection.JavaConverters._


object CoreNLPImpl {

  def tokenize = udf { sentence: String =>
    new Sentence(sentence).words().asScala
  }

  def ssplit = udf { document: String =>
    new Document(document).sentences().asScala.map(_.text())
  }

  def pos = udf { sentence: String =>
    new Sentence(sentence).posTags().asScala
  }

  def lemma = udf { sentence: String =>
    new Sentence(sentence).lemmas().asScala
  }

  def ner = udf { sentence: String =>
    new Sentence(sentence).nerTags().asScala
  }

}
