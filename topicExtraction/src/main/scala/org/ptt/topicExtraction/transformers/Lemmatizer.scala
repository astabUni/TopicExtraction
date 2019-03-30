package org.ptt.topicExtraction.transformers

import edu.stanford.nlp.simple.Sentence
import org.apache.spark.ml.UnaryTransformer
import org.apache.spark.ml.param.ParamMap
import org.apache.spark.ml.util.{DefaultParamsReadable, DefaultParamsWritable, Identifiable}
import org.apache.spark.sql.types.{ArrayType, DataType, StringType}

import scala.collection.JavaConverters._
import scala.collection.Seq




class Lemmatizer(override val uid: String) extends UnaryTransformer[Seq[String], Seq[String], Lemmatizer] with DefaultParamsWritable{

  def this() = this(Identifiable.randomUID("lemma"))


  override protected def createTransformFunc: Seq[String] => Seq[String] = { arrayStr =>
    arrayStr.flatMap(str => new Sentence(str).lemmas().asScala)
  }

  override protected def validateInputType(inputType: DataType): Unit = super.validateInputType(inputType)

  override protected def outputDataType: DataType = new ArrayType(StringType, false)

  override def copy(extra: ParamMap): Lemmatizer = defaultCopy(extra)

}

object Lemmatizer extends DefaultParamsReadable[Lemmatizer] {

  override def load(path: String): Lemmatizer = super.load(path)
}