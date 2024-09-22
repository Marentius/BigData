package DBOne

import org.neo4j.driver.{AuthTokens, Driver, GraphDatabase, Session}

import scala.concurrent.duration._
import scala.math._

object QueryTwo {
  def main(args: Array[String]): Unit = {
    val uri = "bolt://localhost:7687"
    val user = "neo4j"
    val password = "pumspums"

    val driver: Driver = GraphDatabase.driver(uri, AuthTokens.basic(user, password))
    val session: Session = driver.session

    // Function to execute the query
    def testQuery(): Unit = {
      val query =
        """
        MATCH (p:Person)-[:HAR_UTDANNING]->(u:Utdanning),
              (p)-[:HAR_ØKONOMISK_STATUS]->(o:Økonomi)
        WITH u.education_level_id AS Utdanningsnivå,
             AVG(TOINTEGER(o.poverty_income_ratio)) AS GjennomsnittligFattigdomsProsent
        RETURN Utdanningsnivå, GjennomsnittligFattigdomsProsent
        ORDER BY GjennomsnittligFattigdomsProsent DESC
        """

      session.run(query)

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


    val executionTimes = measureTime(testQuery(), 10)


    val executionTimeMillis = executionTimes.drop(4).map(_.toMillis)


    val average = executionTimeMillis.sum.toDouble / executionTimeMillis.size


    val variance = executionTimeMillis.map(time => pow(time - average, 2)).sum / executionTimeMillis.size


    val stdDev = sqrt(variance)


    println(f"Average execution time: $average%.2f ms")
    println(f"Standard deviation: $stdDev%.2f ms")

    session.close()
    driver.close()
  }
}
