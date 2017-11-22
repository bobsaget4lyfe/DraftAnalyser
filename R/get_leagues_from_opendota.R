# Get leagues from OpenDota

# get league information from OpenDota

GetLeagues <- function() {
  leagues <- fromJSON('https://api.opendota.com/api/leagues')
  return(leagues)
}

dbWriteTable(con, name = "leagues", value = leagues, row.names = FALSE, overwrite = TRUE)


