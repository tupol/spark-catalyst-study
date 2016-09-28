package study.catalyst.customCode

import scala.util.Try

/**
  * Clean the given URL from the noisy elements (like the protocol, query part, IDs...)
  * thus bringing it to a canonical form.
  */
object UrlCleaner extends Function1[String, String] with Serializable {

  import java.net.URLDecoder

  val urlDecoder: (String) => String =
    (input: String) => Try(URLDecoder.decode(input, "UTF-8")).toOption.getOrElse(input)

  val protocolRemover: (String) => String =
    (input: String) => input.replaceFirst(".*\\://", "")

  val urlQueryRemover: (String) => String =
    (input: String) => input.split("\\?")(0)

  // remover for what looks like an user ID
  val userIdRemover: (String) => String =
    (input: String) => input.
      replaceAll("""\/([\w\d_-]{43}\.)\/""", "/__ID__/").
      replaceAll("""\/([\w\d_-]{43}\.)\/""", "/__ID__/").
      replaceAll("""\/[\w\d_-]{43}\.$""", "/__ID__")

  // remover for what looks like an ID (just digits and maybe - or _)
  val genericIdRemover: (String) => String =
    (input: String) => input.
      replaceAll("""\/([\d])+([_-]\d+)*\/""", "/__ID__/").
      replaceAll("""\/([\d])+([_-]\d+)*$""", "/__ID__")

  // remover for orders pending delete
  val orderPendingDeleteIdRemover: (String) => String =
    (input: String) => input.
      replaceAll("""\/orders\/pending\/([\w\d_-])*\/delete$""", "/orders/pending/__ID__/delete")

  val urlCleaner: (String) => String =
    Function.chain(Seq(
      urlDecoder,
      protocolRemover,
      urlQueryRemover,
      userIdRemover,
      genericIdRemover,
      orderPendingDeleteIdRemover
    ))

  override def apply(input: String): String = urlCleaner(input)
}
