package study.catalyst.customCode

import org.apache.spark.annotation.Experimental
import org.apache.spark.ml.UnaryTransformer
import org.apache.spark.sql.types.DataType

/**
  * Simple transformer that takes a user function from T1 to T2.
  *
  * @param uid A string identifying the transformer.
  * @param transformFunction The actual transformation function.
  * @param nullFunction Null treatment; by default it uses the same transformation function,
  *                     but it is highly recommended to be defined externally.
  * @tparam T1 Source data type.
  * @tparam T2 Target data type.
  * @todo Add implicit null functions for various target types
  *
  * `Attention!` The functions are not by default serializable.
  * One can go around this by using a pattern like:
  * {{{
  *   object MyFunction extends Function1[String, Int] with Serializable {
  *     override def apply(str: String): Int = ???
  *   }
  * }}}
  * or one can define a function value, like this:
  * {{{
  *   val myFunction = (str: String) => ???
  * }}}
  *
  */
@Experimental
case class SimpleFeatureTransformer[T1, T2](override val uid: String, transformFunction: (T1) => T2,
                                            final override val outputDataType: DataType)(implicit nullFunction: (T1) => T2 = transformFunction)
  extends UnaryTransformer[T1, T2, SimpleFeatureTransformer[T1, T2]] {

  final override protected def createTransformFunc: (T1) => T2 = input =>
    Option(input) match {
      case Some(in) => transformFunction(in)
      case None => nullFunction(null.asInstanceOf[T1])
    }
}
