package questions

import data.{FlightData, PassengersData}
import org.apache.spark.sql.{Dataset, SaveMode, SparkSession}
import org.apache.spark.sql.functions._

import java.util.Date
import scala.collection.mutable
object Task extends App {

  val spark = SparkSession.builder
    .appName("Read File")
    .master("local[*]")
    .getOrCreate()

  spark.conf.set("mapreduce.fileoutputcommitter.marksuccessfuljobs", "false")
//Q1
  import spark.implicits._

  val flatFileLoader = new FlatFileLoader
  val flightPath = "data/flightData.csv"
  val rawFlightData = flatFileLoader.loadCsvByPath(spark, flightPath)
  val flightDS: Dataset[FlightData] =
    rawFlightData.select("*").as[FlightData].as('flight)
  val passengersPath = "data/passengers.csv"
  val rawPassengersData = flatFileLoader.loadCsvByPath(spark, passengersPath)
  val passengersDS: Dataset[PassengersData] =
    rawPassengersData.select("*").as[PassengersData].as('passenger)

  //Q1
  /** get the together passengers.
    *
    * @param flightDataset Dataset of flight data
    * @return nothing to return
    */
  def getFlightNumberEachMonth(flightDataset: Dataset[FlightData]): Unit = {
    val adjustedDS = flightDataset
      .groupByKey(r => r.getMonth()) //get the month number
      .agg(countDistinct("flightId").as[Long].name("Number of Flights"))
      .orderBy($"key")
    val resDS = adjustedDS.withColumnRenamed("key", "Month")
    resDS.show
    flatFileLoader.saveCsvByPath(resDS, SaveMode.Overwrite, "./myQ1Output")
  }

  //Q2:
  /** get the together passengers.
    *
    * @param passengerDataset Dataset of passengers data
    * @param flightDataset Dataset of flight data
    * @return nothing to return
    */
  def getFrequentFlyer(
      passengerDataset: Dataset[PassengersData],
      flightDataset: Dataset[FlightData]
  ): Unit = {
    val joined = passengerDataset
      .join(flightDataset, $"flight.passengerId" === $"passenger.passengerId")
      .groupBy($"passenger.passengerId")
      .agg(count($"flightId").alias("Number of Flights"))
      .as("joined")
    val finalDS = joined
      .join(
        passengerDataset,
        $"passenger.passengerId" === $"joined.passengerId"
      )
      .select(
        $"joined.passengerId",
        $"Number of Flights",
        $"passenger.firstName",
        $"passenger.lastName"
      )
      .orderBy($"Number of Flights".desc)
      .limit(100)
      .withColumnRenamed("passengerId", "Passenger ID")
      .withColumnRenamed("firstName", "First name")
      .withColumnRenamed("lastName", "Last name")
    finalDS.show
    flatFileLoader.saveCsvByPath(finalDS, SaveMode.Overwrite, "./myQ2Output")
  }

  //Q3
  /** get the together passengers.
    *
    * @param flightDataset Dataset of flight data
    * @return nothing to return
    */
  def getLongestRunBetweenUK(flightDataset: Dataset[FlightData]): Unit = {
    val groupDS = flightDataset
      .orderBy("date")
      .groupBy("passengerId")
      .agg(
        collect_list("from") as "from",
        collect_list("to") as "to"
      )

    val rawResult = groupDS.flatMap { item =>
      val p = item.getAs[Int]("passengerId")
      val fromList = item.getAs[Seq[String]]("from")
      val toList = item.getAs[Seq[String]]("to")

      // if final destination is UK, append last index to `ukIndex`
      val toListZip = toList.zipWithIndex
      val lastUkIndex = if (toListZip.last._1 == "uk") toListZip.last._2 else -1

      // get all from UK index
      val ukIndex = fromList.zipWithIndex.filter(x => x._1 == "uk").map(_._2)

      // judge final destination
      val fixedUkIndex =
        if (lastUkIndex == -1) ukIndex else ukIndex :+ (lastUkIndex + 1)

      // 5 9 11 14
      // 9 - 5, 11 - 9, 14 - 11
      // 4 ,      2,     3
      // https://stackoverflow.com/questions/31848509/subtract-adjacent-list-elements
      val diffSeq = (fixedUkIndex zip fixedUkIndex.drop(1)).map({ case (a, b) =>
        b - a
      })

      if (diffSeq.nonEmpty && diffSeq.max != 0) {
        //result(p) = diffSeq.max - 1
        Some(p -> (diffSeq.max - 1))
      } else None
    }

    val res = rawResult.toDF("Passenger ID", "Longest Run")
    val ukLongestRun = res.orderBy(desc("Longest Run"))
    ukLongestRun.show

    flatFileLoader.saveCsvByPath(
      ukLongestRun,
      SaveMode.Overwrite,
      s"./myQ3Output"
    )
  }

  //Q4
  /** get the together passengers.
    *
    * @param flightDataset Dataset of flight data
    * @return nothing to return
    */
  def getTogetherPassengers(flightDataset: Dataset[FlightData]): Unit = {
    val togetherFlight = flightDataset
      .as("df1")
      .join(
        flightDataset.as("df2"),
        $"df1.passengerId" < $"df2.passengerId" &&
          $"df1.flightId" === $"df2.flightId" &&
          $"df1.date" === $"df2.date",
        "inner"
      )
      .groupBy($"df1.passengerId", $"df2.passengerId")
      .agg(count("*").as("Number of flights together"))
      .where($"Number of flights together" >= 3)
      .select(
        $"df1.passengerId".as("Passenger 1 ID"),
        $"df2.passengerId".as("Passenger 2 ID"),
        $"Number of flights together"
      )
      .orderBy(desc("Number of flights together"))
    togetherFlight.show
    flatFileLoader.saveCsvByPath(
      togetherFlight,
      SaveMode.Overwrite,
      "./myQ4Output"
    )
  }

  //Q1
  getFlightNumberEachMonth(flightDS)
  //Q2
  getFrequentFlyer(passengersDS, flightDS)
  //Q3
  getLongestRunBetweenUK(flightDS)
  //Q4
  getTogetherPassengers(flightDS)
}
