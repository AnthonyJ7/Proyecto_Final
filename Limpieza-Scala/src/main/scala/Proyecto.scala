import doobie.*
import doobie.implicits.*
import cats.*
import cats.effect.*
import cats.effect.unsafe.implicits.global
import com.github.tototoshi.csv.{CSVReader, DefaultCSVFormat}
import doobie.syntax.SqlInterpolator.SingleFragment.fromWrite
import com.github.tototoshi.csv.defaultCSVFormat

import java.io.File
import scala.language.postfixOps

//implicit object CustomFormat extends DefaultCSVFormat {
//  override val delimiter: Char = ';'
//}

case class Players(player_id: String,
                   players_family_name: String,
                   players_given_name: String,
                   players_birth_date: String,
                   players_gender: Boolean)

case class Home_Teams(matches_home_team_id: String,
                      home_team_name: String,
                      home_mens_team: String,
                      home_womens_team: String,
                      home_region_name: String)

case class Tournaments(matches_tournament_id: String,
                       tournaments_tournament_name: String,
                       tournaments_year: Int,
                       tournaments_host_country: String,
                       tournaments_winner: String,
                       tournaments_count_teams: Int)

case class Matches(matches_match_id: String,
                   matches_tournament_id: String,
                   matches_away_team_id: String,
                   matches_home_team_id: String,
                   matches_stadium_id: String,
                   matches_match_date: String,
                   matches_match_time: String,
                   matches_stage_name: String,
                   matches_home_team_score: Int,
                   matches_away_team_score: Int,
                   matches_extra_time: Boolean,
                   matches_penalty_shootout: Boolean,
                   matches_home_team_score_penalties: Int,
                   matches_away_team_score_penalties: Int,
                   matches_result: String)


object Proyecto {

  def cons1(): ConnectionIO[List[Tournaments]] =
    sql"""
        SELECT *
        FROM Tournaments
        WHERE tournaments_year = 1930;
    """
      .query[Tournaments]
      .to[List]

  def cons2(): ConnectionIO[List[Home_Teams]] =
    sql"""
        SELECT matches_home_team_id AS team_id,
           home_team_name AS team_name,
           home_mens_team AS mens_team,
           home_womens_team AS womens_team,
           home_region_name AS region_name
        FROM Home_Teams
        UNION
        SELECT matches_away_team_id AS team_id,
           away_team_name AS team_name,
           away_mens_team AS mens_team,
           away_womens_team AS womens_team,
           away_region_name AS region_name
        FROM Away_Teams;
    """
      .query[Home_Teams]
      .to[List]


  def cons3(): ConnectionIO[List[Tournaments]] =
    sql"""
        SELECT *
        FROM Tournaments;
    """
      .query[Tournaments]
      .to[List]

  @main
  def exportFunc() =
    val path2DataFile = "C:\\Users\\ASUS\\Desktop\\Matches.csv"
    val reader = CSVReader.open(new File(path2DataFile))
    val contentFile: List[Map[String, String]] =
      reader.allWithHeaders()

    reader.close()

    val xa = Transactor.fromDriverManager[IO](
      driver = "com.mysql.cj.jdbc.Driver",
      url = "jdbc:mysql://localhost:3306/Proyecto_Practicum",
      user = "root",
      password = "09/03/20Jhordy",
      logHandler = None
    )

    ////////////////////////////////////////////////////////

    // Cosultas SQL-SCALA

    val consulta1: List[Tournaments] = cons1()
      .transact(xa).unsafeRunSync()
    consulta1.foreach(println)

    println("-------")

    val consulta2: List[Home_Teams] = cons2()
      .transact(xa).unsafeRunSync()
    consulta2.foreach(println)

    println("-------")

    val consulta3: List[Tournaments] = cons3()
      .transact(xa).unsafeRunSync()
    consulta3.foreach(println)

    //////////////////////////////////////////////////////

    // Invocar 1ra forma de insercion de datos

    //    generarDataHomeTeams(contentFile)
    //    generarDataAwayTeams(contentFile)
    //    generarDataTournaments(contentFile)
    //    generarDataStadiums(contentFile)

    // Invocar 2da forma de insercion de datos

    //    generarDataPlayers(contentFile)
    //       .foreach(insert => insert.run.transact(xa).unsafeRunSync())

    //    generarDataSquadsDoobie(contentFile)
    //       .foreach(insert => insert.run.transact(xa).unsafeRunSync())

    //    generarDataMatchesDoobie(contentFile)
    //       .foreach(insert => insert.run.transact(xa).unsafeRunSync())

    //    generarDataGoalsDoobie(contentFile)
    //       .foreach(insert => insert.run.transact(xa).unsafeRunSync())


    //////////////////////////////////////////////////////


  // GENERAR LA INFORMACION PARA LA TABLA Teams -- (1RA FORMA)

  // Tabla Home Teams de la Base de Datos

  def generarDataHomeTeams(data: List[Map[String, String]]) = {
    val sqlInsert = s"INSERT INTO Home_Teams(mataches_home_tem_id, home_team_name, home_mens_team, home_womens_team, home_region_name) VALUES('%s', '%s', '%s', '%s', '%s');"
    val teamTuple = data
      .map(
        row => (
          row("matches_home_team_id").trim,
          row("home_team_name"),
          row("home_mens_team").trim,
          row("home_womens_team"),
          row("home_region_name")

        )
      ).distinct
      .map(t5 => sqlInsert.formatLocal(java.util.Locale.US, t5._1, t5._2, t5._3, t5._4, t5._5))

    teamTuple.foreach(println)
  }

  // Tabla Away Teams de la Base de Datos

  def generarDataAwayTeams(data: List[Map[String, String]]) = {
    val sqlInsert = s"INSERT INTO Away_Teams(matches_away_team_id, away_team_name, away_mens_team, away_womens_team, away_region_name) VALUES('%s', '%s', '%s', '%s', '%s');"
    val teamTuple = data
      .map(
        row => (
          row("matches_away_team_id").trim,
          row("away_team_name"),
          row("away_mens_team").trim,
          row("away_womens_team"),
          row("away_region_name")

        )
      ).distinct
      .map(t5 => sqlInsert.formatLocal(java.util.Locale.US, t5._1, t5._2, t5._3, t5._4, t5._5))

    teamTuple.foreach(println)
  }


  // Tabla Tournaments de la Base de Datos

  def generarDataTournaments(data: List[Map[String, String]]) = {
    val sqlInsert = s"INSERT INTO Tournaments(matches_tournament_id, tournaments_tournament_name, tournaments_year, tournaments_host_country, tournaments_winner, tournaments_count_teams) VALUES('%s', '%s','%s', '%s', '%s', '%s');"
    val tournamentTuple = data
      .map(
        row => (
          row("matches_tournament_id").trim,
          row("tournaments_tournament_name").replaceAll("'", "\\\\'"),
          row("tournaments_year").toInt,
          row("tournaments_host_country"),
          row("tournaments_winner"),
          row("tournaments_count_teams").toInt
        )
      ).distinct
      .map(t6 => sqlInsert.formatLocal(java.util.Locale.US, t6._1, t6._2, t6._3, t6._4, t6._5, t6._6))

    tournamentTuple.foreach(println)
  }

  // Tabla Stadiums de la Base de Datos

  def generarDataStadiums(data: List[Map[String, String]]) = {
    val sqlInsert = s"INSERT INTO Stadiums(stadium_id, stadiums_stadium_name, stadiums_city_name, stadiums_country_name, stadiums_stadium_capacity) VALUES('%s', '%s','%s', '%s', %d);"
    val stadiumsTuple = data
      .map(
        row => (
          row("stadium_id").trim,
          row("stadiums_stadium_name").replaceAll("'", "\\\\'"),
          row("stadiums_city_name"),
          row("stadiums_country_name"),
          row("stadiums_stadium_capacity").toInt
        )
      ).distinct
      .map(t5 => sqlInsert.formatLocal(java.util.Locale.US, t5._1, t5._2, t5._3, t5._4, t5._5))

    stadiumsTuple.foreach(println)
  }

  // GENERAR LA INFORMACION PARA LA TABLA PLAYERS -- (2DA FORMA)

  // Tabla Players de la Base de Datos

  def generarDataPlayers(data: List[Map[String, String]]) =

    val playersTuple = data
      .map(
        row => (
          row("player_id").trim,
          row.getOrElse("players_family_name", "").replaceAll("'", "\\\\'"),
          row.getOrElse("players_given_name", "").replaceAll("'", "\\\\'"),
          row("players_birth_date"),
          row("players_gender").toInt

        )
      )
      .distinct
      .map(t5 => sql"INSERT INTO Players(player_id, players_family_name, players_given_name, players_birth_date, players_gender) VALUES(${t5._1}, ${t5._2}, ${t5._3}, ${t5._4}, ${t5._5})".update)
    playersTuple

  // Tabla Squads de la Base de Datos

  def generarDataSquadsDoobie(data: List[Map[String, String]]) =

    val squadsTuple = data
      .map(
        row => (
          row("squads_team_id"),
          row("squads_player_id"),
          row("squads_tournament_id"),
          row("squads_shirt_number").toInt,
          row("squads_position_name"),
          row("players_goal_keeper").toInt,
          row("players_defender").toInt,
          row("players_midfielder").toInt,
          row("players_forward").toInt

        )
      )
      .distinct
      .map(t9 => sql"INSERT INTO Squads(squads_team_id, squads_player_id, squads_tournament_id, squads_shirt_number, squads_position_name, players_goal_keeper, players_defender, players_midfielder, players_forward) VALUES(${t9._1}, ${t9._2}, ${t9._3}, ${t9._4}, ${t9._5}, ${t9._6}, ${t9._7}, ${t9._8}, ${t9._9})".update)
    squadsTuple

  // Tabla Matches de la Base de Datos

  def generarDataMatchesDoobie(data: List[Map[String, String]]) =

    val matchesTuple = data
      .map(
        row => (
          row("matches_match_id"),
          row("matches_tournament_id"),
          row("matches_away_team_id"),
          row("matches_home_team_id"),
          row("matches_stadium_id"),
          row("matches_match_date"),
          row("matches_match_time"),
          row("matches_stage_name"),
          row("matches_home_team_score").toInt,
          row("matches_away_team_score").toInt,
          row("matches_extra_time").toInt,
          row("matches_penalty_shootout").toInt,
          row("matches_home_team_score_penalties").toInt,
          row("matches_away_team_score_penalties").toInt,
          row("matches_result"),

        )
      )
      .distinct
      .map(t15 =>
        sql"INSERT INTO Matches(matches_match_id, matches_tournament_id, matches_away_team_id, matches_home_team_id, matches_stadium_id, matches_match_date, matches_match_time, matches_stage_name, matches_home_team_score, matches_away_team_score, matches_extra_time, matches_penalty_shootout, matches_home_team_score_penalties, matches_away_team_score_penalties, matches_result) VALUES(${t15._1}, ${t15._2}, ${t15._3}, ${t15._4}, ${t15._5}, ${t15._6}, ${t15._7}, ${t15._8}, ${t15._9}, ${t15._10}, ${t15._11}, ${t15._12}, ${t15._13}, ${t15._14}, ${t15._15})".update)
    matchesTuple

  // Tabla Goals de la Base de Datos

  def generarDataGoalsDoobie(data: List[Map[String, String]]) = {
    val goalsTuple = data
      .filter(row => row.values.forall(_ != "NA")) // Filtra las filas que no contienen "NA"
      .map(
        row => (
          row("goals_goal_id"),
          row("goals_team_id"),
          row("goals_player_id"),
          row("goals_player_team_id"),
          row("goals_minute_label"),
          row("goals_minute_regulation").toInt,
          row("goals_minute_stoppage").toInt,
          row("goals_match_period"),
          row("goals_own_goal").toInt,
          row("goals_penalty").toInt,
        )
      )
      .distinct
      .map(t10 =>
        sql"INSERT INTO Goals(goals_goal_id, goals_team_id, goals_player_id, goals_player_team_id, goals_minute_label, goals_minute_regulation, goals_minute_stoppage, goals_match_period, goals_own_goal, goals_penalty) VALUES(${t10._1}, ${t10._2}, ${t10._3}, ${t10._4}, ${t10._5}, ${t10._6}, ${t10._7}, ${t10._8}, ${t10._9}, ${t10._10})".update)
    goalsTuple
  }
}