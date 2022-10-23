<!-- GETTING STARTED -->
## Getting Started

This is an instruction on how to run the flight and passenger application for 4 questions and get the result csv files.

### Prerequisites
```
JDK version 1.8
scalaVersion := "2.12.10"
```

### Guidance

1. There is an assets folder under root. It contains the flightData.csv and passengers.csv
2. Under `src/main/scala/data`, the file FlightData is the case class for FlightData and PassengersData
3. Under `src/main/scala/questions`, FlatFileLoader is the class that contains the methods to load from csv and save to csv
4. ```src/main/scala/questions/Task.scala``` is the object that can be run directly to get the output csv files of 4 questions
5. The 4 output csv files will be under root:
   ```sh
   myQ1Output
   myQ2Output
   myQ3Output
   myQ4Output
   ```

<p align="right">(<a href="#readme-top">back to top</a>)</p>
