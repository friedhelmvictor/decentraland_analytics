library(data.table)
library(tidyjson)
library(ggplot2)

realms <- fread("/media/nvme/decentralanddata/decentraland-servers-2022-08-09.csv")
realms[, requestTime := as.POSIXct(requestTime, origin="1970-01-01")]

servers <- unique(realms[, list(server=baseUrl, serverName)])
servers <- servers[, list(serverName = paste(serverName, collapse = " | ")), by=server]

positions <- fread("/home/everything/Documents/Eigene Paper/work in progress/wip2022measuremeta/data/decentraland-positions-2022-10-03.csv")
positions[, requestTime := as.POSIXct(requestTime, origin="1970-01-01")]
uniqueDailyUsers <- positions[, list(uniqueUsers = length(unique(address))), by=list(date=as.POSIXct(cut(requestTime, "1 day")), server)]
uniqueDailyUsers <- merge(uniqueDailyUsers, servers, by = "server")
ggplot(uniqueDailyUsers) + geom_bar(aes(x=date, y=uniqueUsers, fill=server), color="black", stat="identity")
