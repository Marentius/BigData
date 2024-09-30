package MongoDBOne

import org.mongodb.scala._
import org.mongodb.scala.model.Filters.equal

import scala.concurrent.Await
import scala.concurrent.duration._
import scala.math._

object QueryTwo {

  def main(args: Array[String]): Unit = {
    // Kobling til MongoDB
    val mongoClient: MongoClient = MongoClient("mongodb://localhost:27017")
    val database: MongoDatabase = mongoClient.getDatabase("pums1") // Bytt med riktig database
    val collection: MongoCollection[Document] = database.getCollection("person_combined")

    // Funksjon for å kjøre spørringen
    def testQuery(): Unit = {
      // Finn alle menn
      val maleDocuments = collection.find(equal("sex.description", "Male")).toFuture()
      val maleResults = Await.result(maleDocuments, 10.seconds)

      // Finn alle kvinner
      val femaleDocuments = collection.find(equal("sex.description", "Female")).toFuture()
      val femaleResults = Await.result(femaleDocuments, 10.seconds)

      // Beregn gjennomsnittslønn for menn
      val maleSalaries = maleResults.map(doc => doc.get("total_persons_earnings").get.asInstanceOf[Double])
      val maleAvgSalary = maleSalaries.sum / maleSalaries.size

      // Beregn gjennomsnittslønn for kvinner
      val femaleSalaries = femaleResults.map(doc => doc.get("total_persons_earnings").get.asInstanceOf[Double])
      val femaleAvgSalary = femaleSalaries.sum / femaleSalaries.size

      // Skriv ut resultatene
      println(f"Average salary for men: $maleAvgSalary%.2f")
      println(f"Average salary for women: $femaleAvgSalary%.2f")
    }


    // Funksjon for å måle tiden
    def measureTime[T](block: => T, repetitions: Int): List[Duration] = {
      (1 to repetitions).map { iteration =>
        val start = System.nanoTime()
        block
        val end = System.nanoTime()
        println(s"Query $iteration finished")
        Duration.fromNanos(end - start)
      }.toList
    }

    // Mål utførelsestidene
    val executionTimes = measureTime(testQuery(), 100)

    // Utregning av gjennomsnitt og standardavvik
    val executionTimeMillis = executionTimes.drop(10).map(_.toMillis)
    val average = executionTimeMillis.sum.toDouble / executionTimeMillis.size
    val variance = executionTimeMillis.map(time => pow(time - average, 2)).sum / executionTimeMillis.size
    val stdDev = sqrt(variance)

    // Skriv ut resultater
    println(f"Average execution time: $average%.2f ms")
    println(f"Standard deviation: $stdDev%.2f ms")

    // Lukk MongoDB-klienten
    mongoClient.close()
  }
}
