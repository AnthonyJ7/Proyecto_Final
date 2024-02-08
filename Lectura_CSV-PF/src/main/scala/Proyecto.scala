  import com.github.tototoshi.csv.*
  import org.nspl.*
  import org.nspl.awtrenderer.*
  import org.nspl.data.HistogramData
  import org.saddle.{Index, Series, Vec}

  import java.io.File

//  implicit object CustomFormat extends DefaultCSVFormat {
//
//    override val delimiter: Char = ';'

  object Proyecto {

    @main
    def work() =

      val path2DataFile: String = "C:\\Users\\ASUS\\Desktop\\dsPartidosYGoles (4).csv"
      val reader = CSVReader.open(new File(path2DataFile))
      val contentFile: List[Map[String, String]] = reader.allWithHeaders()

      // Para cerrar el recurso una vez leido

      reader.close()

      //--------------------------------------------

      val path2DataFile1: String = "C:\\Users\\ASUS\\Desktop\\Home_Teams.csv"
      val reader1 = CSVReader.open(new File(path2DataFile1))
      val contentFile1: List[Map[String, String]] = reader1.allWithHeaders()

      // Para cerrar el recurso una vez leido

      reader1.close()

      //--------------------------------------------

      val path2DataFile2: String = "C:\\Users\\ASUS\\Desktop\\Away_Teams.csv"
      val reader2 = CSVReader.open(new File(path2DataFile2))
      val contentFile2: List[Map[String, String]] = reader2.allWithHeaders()

      // Para cerrar el recurso una vez leido

      reader2.close()

      //--------------------------------------------

      val path2DataFile3: String = "C:\\Users\\ASUS\\Desktop\\Squads.csv"
      val reader3 = CSVReader.open(new File(path2DataFile3))
      val contentFile3: List[Map[String, String]] = reader3.allWithHeaders()

      // Para cerrar el recurso una vez leido

      reader3.close()

      // ESTADISTICAS -----------------------------------------

      //-------------------------------------------------------

      // --Filtra y mapea la capacidad de cada estadio--
      val capacidadesEstadios: List[Int] = contentFile.map(row => row("stadiums_stadium_capacity").toInt)

      // Calcula la capacidad promedio
      val capacidadPromedio: Double = if (capacidadesEstadios.nonEmpty) capacidadesEstadios.sum.toDouble / capacidadesEstadios.length else 0.0
      println(s"Capacidad promedio de personas por estadio: $capacidadPromedio")

      // --Calcular numero Total Partidos:--
      val numeroTotalPartidos: Int = contentFile.length
      println(s"Número total de partidos: $numeroTotalPartidos")

      // --Calcular Promedio de goles por partido:--

      val sumaTotalGoles: Int = contentFile.map(x => x("matches_home_team_score").toInt + x("matches_away_team_score").toInt).sum
      val promedioGolesPorPartido: Double = if (numeroTotalPartidos > 0) sumaTotalGoles.toDouble / numeroTotalPartidos else 0.0
      println(s"Promedio de goles por partido: $promedioGolesPorPartido")

      //¿Cuál es el minuto más común en el que se han marcado un gol? Masculino

      val minComunMasculino = contentFile.filter(x => x("tournaments_tournament_name").contains("Men")) //Se filtra los que son masculinos
        .filter(_("goals_minute_regulation") != "NA") //Se filtran todos los minutos que sean diferentes de nulos
        .map(x => x("goals_minute_regulation")). //Se obtienen los minutos
        groupBy(identity) //Frecuencia de cada minuto
        .map(x => x._1 -> x._2.length).maxBy(_._2) //Aqui se obtiene el minuto que mas se repite
      println(minComunMasculino)

      //¿Cuál es el minuto más común en el que se han marcado un gol? Femenino

      val minComunFemenino = contentFile.filter(x => x("tournaments_tournament_name").contains("Women")) //Se filtra los que son masculinos
        .filter(_("goals_minute_regulation") != "NA") //Se filtran todos los minutos que sean diferentes de nulos
        .map(x => x("goals_minute_regulation")). //Se obtienen los minutos
        groupBy(identity) //Frecuencia de cada minuto
        .map(x => x._1 -> x._2.length).maxBy(_._2) //Aqui se obtiene el minuto que mas se repite
      println(minComunFemenino)

      // --Máximo y mínimo de goles anotados en un partido:--

      val maxGolesEnUnPartido: Int = contentFile.map(x => x("matches_home_team_score").toInt + x("matches_away_team_score").toInt).max
      val minGolesEnUnPartido: Int = contentFile.map(x => x("matches_home_team_score").toInt + x("matches_away_team_score").toInt).min
      println(s"Máximo de goles en un partido: $maxGolesEnUnPartido")
      println(s"Mínimo de goles en un partido: $minGolesEnUnPartido")

      // --Contar elementos nulos en el conjunto de datos--
      val cantidadNulos: Int = contentFile.flatMap(x => x.values).count(value => value == "NA")
      println(s"Cantidad de elementos nulos en el conjunto de datos: $cantidadNulos")

      // -- Periodo mas comun en los que se han marcado goles en todos los mundiales --
      val periodosGol: List[String] = contentFile.map(x => x("goals_match_period"))

      // Cuenta la frecuencia de cada periodo
      val frecuenciaPeriodos: Map[String, List[String]] = periodosGol.groupBy(identity)
      val conteoFrecueencia: Map[String, Int] = frecuenciaPeriodos.map((periodo, tamanio) => periodo -> tamanio.length)

      println(conteoFrecueencia)

      // CREACION GRAFICAS(HISTOGRAMAS)

      // Pregunta 1 ------------
      charting(contentFile)

      def charting(data: List[Map[String, String]]): Unit =
        val listMinGoals: List[Double] = data
          .map(row => row("stadiums_stadium_capacity")).filter(e => e != "NA").map(_.toDouble)

        val histMinGoals = xyplot(HistogramData(listMinGoals, 40) -> bar())(
          par
            .xlab("Capacidad")
            .ylab("Cantidad Est.")
            .main("Capacidad Estadios Mund.")
        )
        pngToFile(new File("C:\\Users\\ASUS\\Desktop\\Graficos-PF\\fhsn1.png"),
          histMinGoals.build, width = 800)


      // Pregunta 2

      charting2(contentFile)

      def charting2(data: List[Map[String, String]]): Unit =
        val listMinGoals: List[Double] = data
          .map(row => row("matches_home_team_score")).filter(e => e != "NA").map(_.toDouble)

        val histMinGoals = xyplot(HistogramData(listMinGoals, 40) -> bar())(
          par
            .xlab("Resultados-Home")
            .ylab("Cantidad")
            .main("Resultados")
        )
        pngToFile(new File("C:\\Users\\ASUS\\Desktop\\Graficos-PF\\fhsn2.png"),
          histMinGoals.build, width = 800)

      // Pregunta 3 ------------

      charting3(contentFile)

      def charting3(data: List[Map[String, String]]): Unit =
        val listMinGoals: List[Double] = data
          .map(row => row("tournaments_year")).filter(e => e != "NA").map(_.toDouble)

        val histMinGoals = xyplot(HistogramData(listMinGoals, 40) -> bar())(
          par
            .xlab("Anios")
            .ylab("Cantidad Est.")
            .main("Anios Torneo")
        )
        pngToFile(new File("C:\\Users\\ASUS\\Desktop\\Graficos-PF\\fhsn3.png"),
          histMinGoals.build, width = 800)

      // Pregunta 4 ------------

      charting4(contentFile1)

      def charting4(data: List[Map[String, String]]): Unit =
        val listMinGoals: List[Double] = data
          .map(row => row("home_womens_team")).filter(e => e != "NA").map(_.toDouble)

        val histMinGoals = xyplot(HistogramData(listMinGoals, 40) -> bar())(
          par
            .xlab("Anios")
            .ylab("Cantidad Est.")
            .main("Anios Torneo")
        )
        pngToFile(new File("C:\\Users\\ASUS\\Desktop\\Graficos-PF\\fhsn4.png"),
          histMinGoals.build, width = 800)

      // Pregunta 5 ------------

      charting5(contentFile2)

      def charting5(data: List[Map[String, String]]): Unit =
        val listMinGoals: List[Double] = data
          .map(row => row("away_womens_team")).filter(e => e != "NA").map(_.toDouble)

        val histMinGoals = xyplot(HistogramData(listMinGoals, 40) -> bar())(
          par
            .xlab("Anios")
            .ylab("Cantidad Est.")
            .main("Anios Torneo")
        )
        pngToFile(new File("C:\\Users\\ASUS\\Desktop\\Graficos-PF\\fhsn5.png"),
          histMinGoals.build, width = 800)

      // Pregunta 6 ------------

      charting6(contentFile3)

      def charting6(data: List[Map[String, String]]): Unit =
        val listMinGoals: List[Double] = data
          .map(row => row("squads_shirt_number")).filter(e => e != "NA").map(_.toDouble)

        val histMinGoals = xyplot(HistogramData(listMinGoals, 40) -> bar())(
          par
            .xlab("Anios")
            .ylab("Cantidad Est.")
            .main("Anios Torneo")
        )
        pngToFile(new File("C:\\Users\\ASUS\\Desktop\\Graficos-PF\\fhsn6.png"),
          histMinGoals.build, width = 800)

  }

