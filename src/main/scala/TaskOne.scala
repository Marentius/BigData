import org.neo4j.driver.{AuthTokens, Driver, GraphDatabase, Session, Record}

object Neo4j {
  def main(args: Array[String]): Unit = {
    val uri = "bolt://localhost:7687"
    val user = "neo4j"
    val password = "pumspums"

    val driver: Driver = GraphDatabase.driver(uri, AuthTokens.basic(user, password))
    val session: Session = driver.session

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
      LIMIT 50000;
      """

    val result = session.run(query)

    while (result.hasNext) {
      val record: Record = result.next()

      // Safe conversion from String to Int, or default to 0 if it fails
      val alderStr = record.get("Alder").asString()
      val alder = try {
        alderStr.toInt
      } catch {
        case _: NumberFormatException => 0
      }

      val kjønn = record.get("Kjønn").asString()
      val utdanningsnivå = record.get("Utdanningsnivå").asString()
      val hovedfagfelt = record.get("Hovedfagfelt").asString()
      val sekundærfagfelt = record.get("SekundærtFagfelt").asString()
      val nårSistJobbet = record.get("NårSistJobbet").asString()
      val fattigdomsInntektsForhold = record.get("FattigdomsInntektsForhold").asString()
      val totalePersoninntekter = record.get("TotalePersoninntekter").asString()

      println(s"Alder: $alder, Kjønn: $kjønn, Utdanningsnivå: $utdanningsnivå, Hovedfagfelt: $hovedfagfelt, Sekundærfagfelt: $sekundærfagfelt, Når sist jobbet: $nårSistJobbet, FattigdomsInntektsForhold: $fattigdomsInntektsForhold, TotalePersoninntekter: $totalePersoninntekter")
    }

    session.close()
    driver.close()
  }
}
