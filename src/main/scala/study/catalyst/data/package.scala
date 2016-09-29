package study.catalyst

/**
  *
  */
package object data {

  case class Click(timestamp: String, timeSpentSeconds: String, sourceURL: String, destinationURL: String, sourceIP: String, sourceCountryAndCity: String)


  val sampleClicks = Seq(
    Click("2016-06-07T07:00:15.101Z", "3", "http://google.com?search=something", "https://some-site.com", "123.123.123.1", "BE BRU"),
    Click("2016-06-07T07:01:16.202Z", "4", "http://google.com?search=something", "https://some-site.com", "123.123.123.2", "BE BRU"),
    Click("2016-06-07T07:00:15.303Z", "30", "http://google.com?search=something", "https://some-site.com", "123.223.123.1", "NL AMS"),
    Click("2016-06-07T07:03:15.404Z", "40", "http://google.com?search=something", "https://some-site.com", "123.223.123.2", "NL AMS"),
    Click("2016-06-07T07:01:15.505Z", "50", "http://google.com?search=something", "https://some-site.com", "123.223.123.3", "NL ROT"),
    Click("2016-06-07T07:05:15.606Z", "30", "http://google.com?search=something", "https://some-site.com", "123.223.123.4", "")
  )

}
