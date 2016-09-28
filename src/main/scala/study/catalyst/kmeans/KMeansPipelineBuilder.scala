package study.catalyst.kmeans

import org.apache.spark.ml.clustering.KMeans
import org.apache.spark.ml.feature.{MinMaxScaler, SQLTransformer, VectorAssembler}
import org.apache.spark.ml.{Pipeline, PipelineModel}
import org.apache.spark.sql._
import org.apache.spark.sql.types.{IntegerType, StringType}
import study.catalyst._
import study.catalyst.customCode.{SimpleFeatureTransformer, UrlCleaner}

import scala.util.Try

/**
  * A class that creates a fancy PipelineModel that does transformations and KMeans predictions.
  *
  * It contains various transformations of different complexities.
  *
  * This emulates a fairly complex ETL and prediction pipeline, that might be provided through Zeppelin
  * by a data scientist.
  */
object KMeansPipelineBuilder {

  def pipeline: PipelineModel = {

    val inputDS: DataFrame = spark.createDataFrame(sampleClicks)

    // Register a UDF that extracts the country
    spark.udf.register("extract_country", { input: String =>
        val in = input.split(" ")
        if (in.size < 1) "Unknown" else in(0)
      }
    )
    // Register a UDF that extracts the city
    spark.udf.register("extract_city", { input: String =>
        val in = input.split(" ")
        if (in.size < 2) "Unknown" else in(1)
      }
    )

    val sqlTransformer = new SQLTransformer().
      setStatement(
        """
        SELECT *,
               HOUR(`timestamp`) AS hour,
               MONTH(`timestamp`) AS month,
               CAST(DATE_FORMAT(`timestamp`, 'u') AS int) AS dayofweek,
               DAYOFMONTH(`timestamp`) AS dayofmonth,
               CASE WHEN sourceCountryAndCity IS NULL
                  THEN "Unknown"
                  ELSE extract_country(sourceCountryAndCity) END AS sourceCountry,
               CASE WHEN sourceCountryAndCity IS NULL
                  THEN "Unknown"
                  ELSE extract_city(sourceCountryAndCity) END AS sourceCity,
               CASE WHEN lower(trim(sourceURL)) = 'null' OR sourceURL is null OR trim(sourceURL) = ''
                  THEN "Unknown"
                  ELSE sourceURL
               END AS sourceURL_nn,
               CASE WHEN lower(trim(destinationURL)) = 'null' OR destinationURL is null OR trim(destinationURL) = ''
                  THEN "Unknown"
                  ELSE destinationURL
               END AS destinationURL_nn,
               CASE WHEN lower(trim(timeSpentSeconds)) = 'null' OR timeSpentSeconds is null OR trim(timeSpentSeconds) = '' OR lower(trim(timeSpentSeconds)) = 'unknown'
                  THEN -1
                  ELSE CAST(timeSpentSeconds AS int)
               END AS timeSpentSeconds_num
        FROM  __THIS__
        WHERE `timestamp` IS NOT NULL
        """
      )


    // Conver seonds into milliseconds
    def seconds2millis = new SimpleFeatureTransformer[Integer, Integer]("seconds2millis", { seconds: Integer => seconds * 1000 }, IntegerType).
      setInputCol("timeSpentSeconds_num").
      setOutputCol("timeSpentMillis")

    val cleanSourceURL = new SimpleFeatureTransformer[String, String](
      "canonicSourceURL", UrlCleaner, StringType). //((input: String) => "").
      setInputCol("sourceURL_nn").
      setOutputCol("sourceURL_canonical")

    val cleanDestinationURL = new SimpleFeatureTransformer[String, String](
      "canonicDestinationURL", UrlCleaner, StringType). //((input: String) => "").
      setInputCol("destinationURL_nn").
      setOutputCol("destinationURL_canonical")


    // Combine the selected feature columns into one vector column.
    val toFeaturesVector = new VectorAssembler().
      setInputCols(Array(
        "month", "dayofmonth", "dayofweek", "hour", "timeSpentSeconds_num",
        seconds2millis.getOutputCol
      )).setOutputCol("features_vector")

    // Scale the vectors so they will fit between 0 and 1
    val scaler = new MinMaxScaler().
      setInputCol(toFeaturesVector.getOutputCol).
      setOutputCol("scaled_features_vector")

    // Create a KMeans template that can be used in cross validation
    val kmeans = new KMeans().
      setFeaturesCol("scaled_features_vector").
      setK(3)


    // Build the pre-processing pipeline
    val pipelineTrainer = new Pipeline().
      setStages(
        Array(
          sqlTransformer,
          seconds2millis,
          cleanSourceURL,
          cleanDestinationURL,
          toFeaturesVector,
          scaler,
          kmeans
        )
      )

    // Don't scream! It's a POC not production code.
    Try(pipelineTrainer.fit(inputDS)).get
  }

}
