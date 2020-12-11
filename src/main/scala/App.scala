import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions._

object App {
  def main(args: Array[String]) {
    val spark = SparkSession.builder().appName("job-1").master("local[*]").getOrCreate()

    val readData = new ReadData()
    val commune_df = readData.getCommunes()
    val departement_df = readData.getDepartementCommune()
    val coord_df = readData.getCoord()
    var stations_df = readData.getStations()
    stations_df = ConvertTemp(stations_df)

    val depart_with_commune = departement_df.join(
      commune_df,
      departement_df("CODE INSEE") === commune_df("DEPCOM"),
      "inner"
    )

    //depart_with_commune.show(4)

    val agg_by_depart = depart_with_commune.groupBy(depart_with_commune("Code Dept"))
    .agg(
      sum("PTOT")
    )

    //agg_by_depart.show()

    val df_coord_with_x_y = coord_df.withColumn("geom_x_y",concat(col("latitude"),lit(","),
      col("longitude")))

    df_coord_with_x_y.show()

    val depart_join_coord = departement_df.join(
      df_coord_with_x_y,
      departement_df("geom_x_y") === df_coord_with_x_y("geom_x_y"),
      "full_outer"
    )
    //stations_df.show()
    //depart_join_coord.show()

    val depart_with_coord_join_station = depart_join_coord.join(
      stations_df,
      depart_join_coord("ID") === stations_df("numer_sta"),
      "full_outer"
    )

    //depart_join_coord.show()

    val new_depart = depart_with_coord_join_station.select(col("Code Dept"),col("t"),col("date"))
    //new_depart.show()

    spark.stop
  }

  def ConvertTemp(stationsDf: DataFrame): DataFrame = {
    stationsDf.withColumn("t",col("t") - 273.15)
    return stationsDf
  }
}