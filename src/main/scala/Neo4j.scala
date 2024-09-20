import org.neo4j.driver.{AuthTokens, Driver, GraphDatabase, Session}

object Neo4j {
  def main(args: Array[String]): Unit = {
    val uri = "bolt://localhost:7687"
    val user = "neo4j"
    val password = "pumspums"

    val driver: Driver = GraphDatabase.driver(uri, AuthTokens.basic(user, password))


    val session: Session = driver.session

    val query =
      """
     MATCH (n) RETURN n LIMIT 50;
   """


    val result = session.run(query)


    while (result.hasNext) {
      val record = result.next()
      val node = record.get("n").asNode()


      println(s"Node ID: ${node.id()}")
      println(s"Egenskaper: ${node.asMap()}")
    }


    session.close()
    driver.close()
  }
}
