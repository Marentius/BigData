package MySQLDB

import com.mysql.cj.jdbc.MysqlDataSource

import java.sql.Connection
import javax.sql.DataSource
import scala.concurrent.duration._
import scala.math._

object MySQLQueryOne {
  def main(args: Array[String]): Unit = {



    val dataSource: DataSource = createDataSource()


    val executionTimes = measureTime(executeQuery(dataSource), 10)


    val executionTimeMillis = executionTimes.drop(4).map(_.toMillis)


    val average = executionTimeMillis.sum.toDouble / executionTimeMillis.size


    val variance = executionTimeMillis.map(time => pow(time - average, 2)).sum / executionTimeMillis.size


    val stdDev = sqrt(variance)


    println(f"Average execution time: $average%.2f ms")
    println(f"Standard deviation: $stdDev%.2f ms")
  }

  val password = "Tvilling123456"
  def executeQuery(dataSource: DataSource): Unit = {
    val connection: Connection = dataSource.getConnection()


    val query =
      """
      SELECT ce.description AS Utdanningsnivå,
      AVG(p.total_persons_earnings) AS GjennomsnittligInntekt
      FROM person p
      join current_education ce ON p.current_education_id = ce.id
      join when_last_worked wlw ON p.when_last_worked_id = wlw.id
      where p.age > 18 and wlw.description = "Within the past 12 months"
      group by ce.description
      order by GjennomsnittligInntekt DESC;
      """


    val statement = connection.createStatement()
    statement.executeQuery(query)


    connection.close()
  }


  def createDataSource(): DataSource = {
    val ds = new MysqlDataSource()
    ds.setURL("jdbc:mysql://localhost:3306/pums2")
    ds.setUser("root")
    ds.setPassword(password)
    ds
  }


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
