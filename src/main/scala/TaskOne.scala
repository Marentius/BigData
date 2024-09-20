import org.neo4j.driver.{AuthTokens, Driver, GraphDatabase, Session, Record}

object TaskOne {
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
      LIMIT 1000;
      """

    val result = session.run(query)

    if (!result.hasNext) {
      println("Ingen resultater ble funnet.")
    } else {
      // Print table header once with adjusted column widths
      println(f"${"Alder"}%-6s ${"Kjønn"}%-10s ${"Utdanningsnivå"}%-70s ${"Hovedfagfelt"}%-1000s ${"Sekundærfagfelt"}%-45s ${"NårSistJobbet"}%-30s ${"FattigdomsInntektsForhold"}%-30s ${"TotalePersoninntekter"}%-20s")
      println("=" * 260)

      // Iterate over results and print formatted table rows
      while (result.hasNext) {
        val record: Record = result.next()

        val alderStr = record.get("Alder").asString()
        val alder = try {
          alderStr.toInt
        } catch {
          case _: NumberFormatException => 0
        }

        val kjønn = record.get("Kjønn").asString()
        val utdanningsnivå = record.get("Utdanningsnivå").asString()
        val hovedfagfelt = record.get("Hovedfagfelt").asString()
        val sekundærfagfelt = record.get("Sekundærfagfelt").asString()
        val nårSistJobbet = record.get("NårSistJobbet").asString()
        val fattigdomsInntektsForhold = record.get("FattigdomsInntektsForhold").asString()
        val totalePersoninntekter = record.get("TotalePersoninntekter").asString()

        // Print each row with formatted columns
        printf(f"$alder%-6d $kjønn%-10s $utdanningsnivå%-45s $hovedfagfelt%-45s $sekundærfagfelt%-45s $nårSistJobbet%-30s $fattigdomsInntektsForhold%-30s $totalePersoninntekter%-20s\n")
      }
    }

    session.close()
    driver.close()
  }
}
