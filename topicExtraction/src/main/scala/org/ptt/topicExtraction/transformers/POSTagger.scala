package org.ptt.topicExtraction

import edu.stanford.nlp.simple.Sentence
import org.apache.spark.ml.UnaryTransformer
import org.apache.spark.ml.param.ParamMap
import org.apache.spark.ml.util.{DefaultParamsReadable, DefaultParamsWritable, Identifiable}
import org.apache.spark.sql.types.{ArrayType, DataType, StringType}

import scala.collection.JavaConverters._
import scala.collection.{Seq, mutable}




class POSTagger(override val uid: String) extends UnaryTransformer[Seq[String], Seq[String], POSTagger] with DefaultParamsWritable{

  def this() = this(Identifiable.randomUID("posTag"))


  override protected def createTransformFunc: Seq[String] => Seq[String] = { arrayStr =>
    val posTags = arrayStr.flatMap(str => new Sentence(str).posTags().asScala)
    val filterTags = List("NN","NNS", "NNP", "NNPS")
    val nouns = (arrayStr zip posTags).filter(w => filterTags.toSet.contains(w._2)).map(_._1.toLowerCase).toList
    //val test = (arrayStr zip posTags).map(_._1).groupBy(identity).mapValues(_.size)
    nouns
  }

  override protected def validateInputType(inputType: DataType): Unit = super.validateInputType(inputType)

  override protected def outputDataType: DataType = new ArrayType(StringType, false)

  override def copy(extra: ParamMap): POSTagger = defaultCopy(extra)

}

object POSTagger extends DefaultParamsReadable[POSTagger] {

  override def load(path: String): POSTagger = super.load(path)
}