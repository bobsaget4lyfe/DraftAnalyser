# Get the match details from OpenDota

MatchDetailsOpenDota <- function(kMatchID) {
  matchDetails <- fromJSON(paste("https://api.opendota.com/api/matches/", kMatchID, sep=""))
  return(matchDetails)
}