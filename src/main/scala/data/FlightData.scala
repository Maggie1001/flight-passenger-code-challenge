package data

import questions.FlatFileLoader
import questions.Task.rawFlightData

import java.text.SimpleDateFormat
import java.util.Date

case class FlightData(
    passengerId: Int,
    flightId: Int,
    from: String,
    to: String,
    date: String
) {
  def getMonth(): Int = {
    val fmt = new SimpleDateFormat("yyyy-MM-dd")
    val month = fmt.parse(date).getMonth
    month + 1
  }
}

case class PassengersData(
    passengerId: Int,
    firstName: String,
    lastName: String
)
