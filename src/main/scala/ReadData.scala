import org.apache.spark.sql.{DataFrame, Row, SparkSession}

class ReadData {

  private val commune_csv_path = "/home/serigne/esgi_tp/Communes.csv";
  private val departement_csv_path = "/home/serigne/esgi_tp/code-insee-postaux-geoflar.csv";
  private val coord_csv_path = "/home/serigne/esgi_tp/postesSynop.txt";
  private val stations_csv_path = "/home/serigne/esgi_tp/synop.2020120512.txt";

  def getCommunes(): DataFrame = {
    val spark = SparkSession.builder().appName("job-1").master("local[*]").getOrCreate()

    val commune_df = spark.read.option("header","true").option("sep",";").csv(commune_csv_path)
    return commune_df

  }

  def getDepartementCommune(): DataFrame = {
    val spark = SparkSession.builder().appName("job-1").master("local[*]").getOrCreate()

    val departementCommune_df = spark.read.option("header","true").option("sep",";").csv(departement_csv_path)
    return departementCommune_df
  }

  def getCoord(): DataFrame = {
    val spark = SparkSession.builder().appName("job-1").master("local[*]").getOrCreate()

    val coordonnees_df = spark.read.option("header","true").option("sep",";").csv(coord_csv_path)
    return coordonnees_df
  }

  def getStations(): DataFrame = {
    val spark = SparkSession.builder().appName("job-1").master("local[*]").getOrCreate()

    val stations_df = spark.read.option("header","true").option("sep",";").csv(stations_csv_path)
    return stations_df
  }
}
