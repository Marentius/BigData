import org.neo4j.driver.{AuthTokens, Driver, GraphDatabase, Session, Record}
import scala.concurrent.duration._
import scala.collection.JavaConverters._
import scala.math._

object TaskOne {
  def main(args: Array[String]): Unit = {
    val uri = "bolt://localhost:7687"
    val user = "neo4j"
    val password = "pumspums"

    val driver: Driver = GraphDatabase.driver(uri, AuthTokens.basic(user, password))
    val session: Session = driver.session

    // Function to test the query
    def testQuery(): Unit = {
      val query =
        """
        MATCH (p:Person)-[:HAR_UTDANNING]->(u:Utdanning),
              (p)-[:JOBBET_SIST]->(w:NårSistJobbet),
              (p)-[:HAR_ØKONOMISK_STATUS]->(o:Økonomi)
        RETURN p.age AS Alder,
               p.sex AS Kjønn,
               u.education_level_id AS Utdanningsnivå,
               u.field_of_degree1_id AS Hovedfagfelt,
               u.field_of_degree2_id AS SekundærtFagfelt,
               w.when_last_worked_id AS NårSistJobbet,
               o.poverty_income_ratio AS FattigdomsInntektsForhold,
               o.total_persons_earnings AS TotalePersoninntekter
        ORDER BY o.total_persons_earnings DESC, p.age DESC
        LIMIT 50;
        """

      // Run the query and get results
      val result = session.run(query)

      // Convert result to list once to avoid issue with hasNext
      val records = result.list().asScala
      
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

    // Measure the execution time for the query 20 times
    val executionTimes = measureTime(testQuery(), 20)

    // Drop the first 10 measurements to account for warm-up
    val executionTimeMillis = executionTimes.drop(10).map(_.toMillis)

    // Calculate average execution time
    val average = executionTimeMillis.sum.toDouble / executionTimeMillis.size

    // Calculate variance
    val variance = executionTimeMillis.map(time => pow(time - average, 2)).sum / executionTimeMillis.size

    // Calculate standard deviation
    val stdDev = sqrt(variance)

    // Print the results
    println(f"Average execution time: $average%.2f ms")
    println(f"Standard deviation: $stdDev%.2f ms")

    session.close()
    driver.close()
  }
}
