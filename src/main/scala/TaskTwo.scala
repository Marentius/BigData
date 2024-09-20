import org.neo4j.driver.{AuthTokens, Driver, GraphDatabase, Session, Record}

object TaskTwo {
  def main(args: Array[String]): Unit = {
    val uri = "bolt://localhost:7687"
    val user = "neo4j"
    val password = "pumspums"

    val driver: Driver = GraphDatabase.driver(uri, AuthTokens.basic(user, password))
    val session: Session = driver.session

    val query =
      """
      MATCH (p:Person)-[:HAR_UTDANNING]->(u:Utdanning),
            (p)-[:HAR_ØKONOMISK_STATUS]->(o:Økonomi)
      WITH u.education_level_id AS Utdanningsnivå,
           AVG(TOINTEGER(o.poverty_income_ratio)) AS GjennomsnittligFattigdomsProsent
      RETURN Utdanningsnivå, GjennomsnittligFattigdomsProsent
      ORDER BY GjennomsnittligFattigdomsProsent DESC
      """

    val result = session.run(query)

    if (!result.hasNext) {
      println("Ingen resultater ble funnet.")
    } else {
      // Print table header once, before the loop
      println(f"${"Utdanningsnivå"}%-35s ${"GjennomsnittligFattigdomsProsent"}%-30s")
      println("=" * 65)

      // Iterate over results and print formatted table rows
      while (result.hasNext) {
        val record: Record = result.next()

        val utdanningsnivå = record.get("Utdanningsnivå").asString()
        val gjennomsnittligFattigdomsProsent = record.get("GjennomsnittligFattigdomsProsent").asDouble()

        // Print each row with formatted columns
        printf(f"${utdanningsnivå}%-35s ${gjennomsnittligFattigdomsProsent}%-30.2f\n")
      }
    }

    session.close()
    driver.close()
  }
}
