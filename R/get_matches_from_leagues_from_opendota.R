# From the leagues from open dota get the matches from the leagues from Opendota

source('~/DraftAnalyser/R/valve_api_key.R')

GetMatchesOfLeagues <- function(kLeagueId) {
  print(paste("Getting the matches for league ", kLeagueId, sep=""))
  valveResult <- fromJSON(paste("http://api.steampowered.com/IDOTA2Match_570/GetMatchHistory/v1/?league_id=", 
                                kLeagueId, "&key=", key, sep=""))
  if (valveResult$result$total_results==0) {
    valveResult$result$matches = 0
    return(as.data.frame(valveResult))
  }
  leagueMatches <- as.data.frame(valveResult)
  Sys.sleep(1)
  if (unique(leagueMatches$result.results_remaining)==0) {
    return(leagueMatches)
  } else if (unique(leagueMatches$result.results_remaining)>0) {
    while(!(0 %in% leagueMatches$result.results_remaining)) {
      valveResult <- fromJSON(paste("http://api.steampowered.com/IDOTA2Match_570/GetMatchHistory/v1/?league_id=", 
                                    kLeagueId,"&start_at_match_id=",min(leagueMatches$result.matches.match_id),"&key=", key, sep=""))
      if (length(valveResult$result$matches)==0) {
        valveResult$result$matches = 0
        return(leagueMatches)
      }
      leagueMatches <- bind_rows(leagueMatches, as.data.frame(valveResult))
      print(paste("Results remaing", c(unique(leagueMatches$result.results_remaining))))
      Sys.sleep(1)
    }
    return(leagueMatches)
  }
}
