package DBOne

import org.neo4j.driver.{AuthTokens, Driver, GraphDatabase, Session}

import scala.concurrent.duration._
import scala.math._

object QueryOne {
  def main(args: Array[String]): Unit = {
    val uri = "bolt://localhost:7687"
    val user = "neo4j"
    val password = "pumspums"

    val driver: Driver = GraphDatabase.driver(uri, AuthTokens.basic(user, password))
    val session: Session = driver.session


    def testQuery(): Unit = {
      val query =
        """
           MATCH (p:Person)-[:HAS_EDUCATION]->(ed:Education),
              (p)-[:LAST_WORKED]->(w:WhenLastWorked),
              (p)-[:HAS_ECONOMIC_STATUS]->(ec:Economy)
        WHERE p.age > 18
          AND w.when_last_worked_id = "Within the past 12 months"
        WITH ed.education_level_id AS Utdanningsnivå, AVG(ec.total_persons_earnings) AS GjennomsnittligInntekt
        RETURN Utdanningsnivå, GjennomsnittligInntekt
        ORDER BY GjennomsnittligInntekt DESC;
        """

      session.run(query)
    }


    def measureTime[T](block: => T, repetitions: Int): List[Duration] = {
      (1 to repetitions).map { iteration =>
        val start = System.nanoTime()
        block
        val end = System.nanoTime()
        println(s"Query $iteration finished")
        Duration.fromNanos(end - start)
      }.toList
    }


    val executionTimes = measureTime(testQuery(), 100)


    val executionTimeMillis = executionTimes.drop(10).map(_.toMillis)


    val average = executionTimeMillis.sum.toDouble / executionTimeMillis.size


    val variance = executionTimeMillis.map(time => pow(time - average, 2)).sum / executionTimeMillis.size


    val stdDev = sqrt(variance)


    println(f"Average execution time: $average%.2f ms")
    println(f"Standard deviation: $stdDev%.2f ms")

    session.close()
    driver.close()
  }
}
