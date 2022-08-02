
import org.apache.spark.sql.types.DateType
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.col

object AppEl extends App {

  val spark = SparkSession
    .builder()
    .enableHiveSupport()
    .master("yarn")
    .getOrCreate()

  val tab_s = spark.read
    .format("jdbc")
    .option("url", "№№№№№№№№")
    .option("dbtable", "bookings.seats")
    .option("user", args(0))
    .option("password", args(1))
    .option("driver", "org.postgresql.Driver")
    .load()

  val tab_f = spark.read
    .format("jdbc")
    .option("url", "№№№№№№№№")
    .option("dbtable", "bookings.flights_v")
    .option("user", args(0))
    .option("password", args(1))
    .option("driver", "org.postgresql.Driver")
    .load()

  tab_s
    .write.mode("overwrite")
    .saveAsTable("school_de.seats_№№№№№№№№")

  tab_f
    .withColumn("data_act_dep", col("actual_departure").cast(DateType))
    .write.mode("overwrite")
    .partitionBy("data_act_dep")
    .saveAsTable("school_de.flights_v_№№№№№№№№")

}
