# Update the database with any new match data

library(dbplyr)
library(jsonlite)
library(RPostgreSQL)
library(dplyr)
library(progress)

source('~/DraftAnalyser/R/get_matches_from_leagues_from_opendota.R')
source('~/DraftAnalyser/R/get_match_details_from_opendota.R')

progre
drv = dbDriver("PostgreSQL")

con <- dbConnect(drv, dbname = 'postgres', host = "localhost", port = 5432, user = "justin", password = "diju1991")

kLeaguesIds <- as.integer(unlist(dbGetQuery(con, "SELECT leagueid FROM leagues")))

kDoneLeagues <- as.integer(unique(unlist(dbGetQuery(con, "SELECT leagueid FROM leaguematches"))))

diffset <- setdiff(kLeaguesIds, kDoneLeagues)

for(i in diffset) {
  
  leagueid <- i
    
  leagueMatches <- cbind.data.frame(GetMatchesOfLeagues(i), leagueid)
  
  databaseMatches <- as.data.frame(dbGetQuery(con, paste("SELECT * FROM leaguematches WHERE leagueid = ", leagueid)))$result.matches.match_id
  
  if (length(leagueMatches$result.matches.match_id) > length(databaseMatches)) {
    
    for (x in 1:nrow(leagueMatches)) {
      
      if (leagueMatches$result.matches.match_id[x] %in% databaseMatches) {
        next()
      } else {
        dbWriteTable(con, name = 'leaguematches', value = leagueMatches[x,-which(names(leagueMatches)=="result.matches.players")]
                       , row.names = FALSE, append = TRUE)
        print(paste("Added mathch", leagueMatches$result.matches.match_id[x], "for
              league", leagueid, sep=" ") )
      }
    }
  } else {
    print(paste("No need to update league", leagueid, sep=" "))
  }
}

professionalLeagues <- dbGetQuery(con, "SELECT leagueid FROM leagues WHERE tier = 'professional' ")

for(i in as.integer(professionalLeagues$leagueid)) {
  leaguesmatches <- dbGetQuery(con, paste("SELECT * FROM leaguematches WHERE leagueid = ", i, sep=""))
  alreadydone <- dbGetQuery(con, paste("SELECT match_id FROM meta_data WHERE leagueid = ",i,sep=""))
  matches <- setdiff(as.integer(leaguesmatches$result.matches.match_id), as.integer(alreadydone$match_id))
  print(paste("There are ", length(matches), "matches to download and store in the database", sep=" "))
  #pb <- progress_bar(total = length(matches))
  for(x in matches) {
    if(is.na(x)) next()
    openDotaResult <- MatchDetailsOpenDota(x)
    Sys.sleep(1)
    # need error handle here

    # need to correct column names and types here
    match_id <- if (is.null(openDotaResult$match_id)) NA else openDotaResult$match_id
    duration <- if (is.null(openDotaResult$duration)) NA else openDotaResult$duration
    engine <- if (is.null(openDotaResult$engine)) NA else openDotaResult$engine
    game_mode <- if (is.null(openDotaResult$game_mode)) NA else openDotaResult$game_mode
    leagueid <- if (is.null(openDotaResult$leagueid)) NA else openDotaResult$leagueid
    negative_votes <- if (is.null(openDotaResult$negative_votes)) NA else openDotaResult$negative_votes
    positive_votes <- if (is.null(openDotaResult$positive_votes)) NA else openDotaResult$positive_votes
    radiant_win <- if (is.null(openDotaResult$radiant_win)) NA else openDotaResult$radiant_win
    start_time <- if (is.null(openDotaResult$start_time)) NA else openDotaResult$start_time
    parser_version <-if (is.null(openDotaResult$version)) NA else openDotaResult$version
    series_id <- if (is.null(openDotaResult$series_id)) NA else openDotaResult$series_id
    series_type <- if (is.null(openDotaResult$series_type)) NA else openDotaResult$series_type
    radiant_team <- if (is.null(openDotaResult$radiant_team$team_id)) NA else openDotaResult$radiant_team$team_id
    dire_team <- if (is.null(openDotaResult$dire_team$team_id)) NA else openDotaResult$dire_team$team_id
    
    dbWriteTable(con, name = 'meta_data', 
                 value = cbind.data.frame(match_id, duration, engine, game_mode,leagueid,negative_votes,
                                          positive_votes, radiant_win, start_time, parser_version,
                                          series_id, series_type, radiant_team, dire_team), 
                 row.names = FALSE, append = TRUE)
    players <- openDotaResult$players
    match_id <- if (is.null(players$match_id)) NA else players$match_id
    player_slot <- if (is.null(players$player_slot)) NA else players$player_slot
    account_id <- if (is.null(players$account_id)) NA else players$account_id
    dbWriteTable(con, name = 'players', value = cbind.data.frame(match_id, player_slot, account_id), row.names = FALSE, append = TRUE)
    # Update picks_bans table
    if (is.null(openDotaResult$picks_bans)) {
      picks_bans$match_id <- as.integer(unique(unlist(match_id)))
      picks_bans <- as.data.frame(picks_bans)
    } else {
      picks_bans <- openDotaResult$picks_bans
    }
    dbWriteTable(con, name = 'picks_bans', value = picks_bans, row.names = FALSE, append = TRUE)
    # Update draft_timings table
    draft_timings <- openDotaResult$draft_timings
    draft_timings$match_id <- as.integer(unique(unlist(match_id)))
    if(length(draft_timings)==1) {
      draft_timings <- as.data.frame(draft_timings)
      dbWriteTable(con, name = 'draft_timings', value = draft_timings, row.names = FALSE, append = TRUE)
    } else {
      dbWriteTable(con, name = 'draft_timings', value = draft_timings, row.names = FALSE, append = TRUE)
    }
    #pb$tick()
    print(x)
  }
  print(paste("Complete getting opendota information for league", i, sep=""))
}
