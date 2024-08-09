
import org.apache.spark.sql.SparkSession
import org.slf4j.{Logger, LoggerFactory}

object Utils {

  val _spark: SparkSession = SparkSession.builder.
    appName("Project Spark").
    config("spark.ui.port", "0").
    master("local[*]").
    getOrCreate()

  val _log: Logger = LoggerFactory.getLogger(Main.getClass)
}