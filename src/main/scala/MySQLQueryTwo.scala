import java.sql.Connection
import javax.sql.DataSource
import com.mysql.cj.jdbc.MysqlDataSource
import scala.concurrent.duration._
import scala.math._

object MySQLQueryTwo {
  def main(args: Array[String]): Unit = {
    // Initialize the DataSource
    val dataSource: DataSource = createDataSource()

    // Measure the execution time for the query 20 times
    val executionTimes = measureTime(executeQuery(dataSource), 5)

    // Drop the first 5 measurements to account for warm-up
    val executionTimeMillis = executionTimes.drop(2).map(_.toMillis)

    // Calculate average execution time
    val average = executionTimeMillis.sum.toDouble / executionTimeMillis.size

    // Calculate variance
    val variance = executionTimeMillis.map(time => pow(time - average, 2)).sum / executionTimeMillis.size

    // Calculate standard deviation
    val stdDev = sqrt(variance)

    // Print the results
    println(f"Average execution time: $average%.2f ms")
    println(f"Standard deviation: $stdDev%.2f ms")
  }

  // Function to execute the query and print the results
  def executeQuery(dataSource: DataSource): Unit = {
    val connection: Connection = dataSource.getConnection()

    // Define the SQL query
    val query =
      """
     SELECT e.description AS Utdanningsnivå,
             AVG(CAST(p.poverty_income_ratio AS UNSIGNED)) AS GjennomsnittligFattigdomsProsent
      FROM person p
      JOIN education_level e ON p.education_level_id = e.id
      GROUP BY e.description
      ORDER BY GjennomsnittligFattigdomsProsent DESC;
      """

    // Execute the query
    val statement = connection.createStatement()
    statement.executeQuery(query)

    // Close the connection
    connection.close()
  }

  // Function to create and configure the DataSource
  def createDataSource(): DataSource = {
    val ds = new MysqlDataSource()
    ds.setURL("jdbc:mysql://localhost:3306/pums2")
    ds.setUser("root")
    ds.setPassword("")
    ds
  }

  // Function to measure execution time
  def measureTime[T](block: => T, repetitions: Int): List[Duration] = {
    (1 to repetitions).map { iteration =>
      val start = System.nanoTime()
      block // Execute the block of code
      val end = System.nanoTime()
      println(s"Query $iteration finished")
      Duration.fromNanos(end - start)
    }.toList
  }
}
