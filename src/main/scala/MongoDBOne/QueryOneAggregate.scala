package MongoDBOne

import org.mongodb.scala._
import org.mongodb.scala.model.{Accumulators, Aggregates}

import scala.concurrent.Await
import scala.concurrent.duration._
import scala.math._

object QueryOne {

  def main(args: Array[String]): Unit = {
    // Kobling til MongoDB
    val mongoClient: MongoClient = MongoClient("mongodb://localhost:27017")
    val database: MongoDatabase = mongoClient.getDatabase("pums1") // Bytt med riktig database
    val collection: MongoCollection[Document] = database.getCollection("person_combined")

    // Funksjon for å kjøre spørringen
    def testQuery(): Unit = {
      val pipeline = Seq(
        Aggregates.group("$sex.description", Accumulators.avg("avg_salary", "$total_persons_earnings"))
      )

      val observable: Observable[Document] = collection.aggregate(pipeline)
      val results = Await.result(observable.toFuture(), 10.seconds)

      results.foreach(doc => println(doc.toJson()))
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
