library(data.table)
library(tidyjson)
library(ggplot2)
library(latex2exp)

positions <- fread("/media/nvme/decentralanddata/decentraland-positions-2022-08-09.csv")
positions <- fread("/home/everything/Documents/Eigene Paper/work in progress/wip2022measuremeta/data/decentraland-positions-2022-10-03.csv")
positions[, c("parcel_x", "parcel_y") := tstrsplit(gsub("\\{|\\}", "", parcel), ",")]
positions$parcel_x <- as.numeric(positions$parcel_x)
positions$parcel_y <- as.numeric(positions$parcel_y)
positions[, requestTime := as.POSIXct(requestTime, origin="1970-01-01")]

tiles <- fromJSON(txt="/media/nvme/decentralanddata/tiles-2022-08-09-23:31.json")
tiles <- tidyjson::read_json(path="/media/nvme/decentralanddata/tiles-2022-08-09-23:31.json")
#tiles <- tidyjson::read_json(path="/media/nvme/decentralanddata/small_tiles.json")
tiles_dt <- tiles %>% enter_object("data") %>% gather_object() %>% spread_all() %>%
  dplyr::select(x, y, top, estate_id, type, owner, name.2) %>% as.data.table()
tiles_dt[, ..JSON := NULL]
setnames(tiles_dt, "name.2", "name")


# filter to only those that have valid lastPing. This also removes invalid (empty) addresses
filters <- data.table()
addToFilter <- function(description, share, remaining) {
  filters <<- rbind(filters, data.table("Filter step"=description, "Percent removed"=share, remaining=remaining))
}
originalPositionCount <- nrow(positions)
positions <- positions[!is.na(lastPing)]
addToFilter("Remove users without lastPing", (1-(nrow(positions)/originalPositionCount))*100, nrow(positions))

positionsWithTiles <- merge(positions, tiles_dt, by.x = c("parcel_x", "parcel_y"), by.y = c("x", "y"))
addToFilter("Remove positions outside of tile map", (1-(nrow(positionsWithTiles)/originalPositionCount))*100, nrow(positionsWithTiles))

# 1. Test lastPing to requestTime delays
# Hypothesis: lastPing appears multiple times after logout
#positions[, list(count = .N), by=list(address, lastPing)][count > 1]

totalUniqueParcelVisitors <- positionsWithTiles[, list(uniqueUsers = uniqueN(address)), by=list(parcel_x, parcel_y)]
totalUniqueParcelVisitorsPlot <- ggplot(totalUniqueParcelVisitors) + geom_tile(aes(x=parcel_x, y=parcel_y, fill=uniqueUsers)) +
  scale_fill_gradient2(name = "Unique users\nseen in\nentire period", trans = "log", low = "grey", mid = "brown", high = "red", midpoint = 5,
                      breaks = 10^c(0:5), labels = scales::comma) +
  scale_x_continuous(expand=c(0,0)) + 
  scale_y_continuous(expand=c(0,0)) +
  labs(x="Parcel coordinate X", y="Parcel coordinate Y") +
  theme_bw() + theme(
    legend.position = c(.999, .999),
    legend.justification = c("right", "top"),
    legend.box.just = "right",
    legend.margin = margin(6, 6, 6, 6),
    legend.box.background = element_rect(color="black", size=0.5)
  )

ggsave("/tmp/totalUniqueParcelVisitorsPlot.pdf", totalUniqueParcelVisitorsPlot, width = 5.2, height=5)

tiles_dt$type <- as.factor(tiles_dt$type)
levels(tiles_dt$type) <- c("District", "Road", "Plaza", "Land", "On Sale")
tileMapPlot <- ggplot(tiles_dt) + geom_tile(aes(x=x, y=y, fill=type)) +
  scale_fill_manual(values = c("District"="#5054d4", "Road" = "#908b9a", "Plaza" = "#70ac76", "Land" = "#3d3a46", "On Sale" = "#00d3ff"),
                    name="Parcels by type:") +
  scale_x_continuous(expand=c(0,0)) + 
  scale_y_continuous(expand=c(0,0)) +
  labs(x="Parcel coordinate X", y="Parcel coordinate Y") +
  theme_bw() + theme(
    legend.position = c(.999, .999),
    legend.justification = c("right", "top"),
    legend.box.just = "right",
    legend.margin = margin(6, 6, 6, 6),
    legend.box.background = element_rect(color="black", size=0.5)
  )
tileMapPlot

ggsave("/tmp/tileMapPlot.pdf", tileMapPlot, width = 5.2, height=5)


# Users by number of visited parcels
usersByVisitedParcels <- positionsWithTiles[, list(parcelsVisited = uniqueN(.SD[, list(parcel_x, parcel_y)])), by=address]
highlightPoints <- c(10^c(0:3), 5000, 0)
highlightPoints <- data.table(x = highlightPoints,
                              y = sapply(X = highlightPoints, function(x) {nrow(usersByVisitedParcels[parcelsVisited > x])}))
ggplot(usersByVisitedParcels) + geom_step(aes(x=parcelsVisited, y=1-..y..), stat="ecdf") +
  geom_point(data = highlightPoints, aes(x=x, y=y/nrow(usersByVisitedParcels))) +
  scale_x_log10(labels = scales::comma, breaks = c(10^c(0:3), 5000)) +
  scale_y_log10(labels = scales::percent_format(accuracy=0.01), breaks = 10^c(0:-4),
                sec.axis = sec_axis( trans=~.*nrow(usersByVisitedParcels),
                                     labels=scales::comma, breaks = nrow(usersByVisitedParcels)/10^c(0:4), name="User count")) +
  annotation_logticks(sides = "lb") +
  ylab(TeX("$P(X > x)$")) + xlab("Number of distinct parcels visited") +
  theme_bw()

p  <- ggplot(usersByVisitedParcels, aes(parcelsVisited)) + stat_ecdf()
pg <- ggplot_build(p)$data[[1]]
ggplot(pg, aes(x = x, y = 1-y )) + geom_step() + scale_x_log10()


#############################################################
# extract sessions
#############################################################
sortedhead <- head(positions[order(address, lastPing)], 100000)
sortedhead[, lastPingTime := as.POSIXct(lastPing/1000, origin="1970-01-01")]
sortedhead[, tDiff := (lastPing - shift(lastPing))/1000, by=address]
sortedhead[, speed := sqrt((x-shift(x))^2+(y-shift(y))^2)/tDiff, by=address]
#speedPlot <- ggplot(sortedhead) + geom_line(aes(x=speed), stat="ecdf") + scale_x_log10()
sortedhead[, tDiff := ifelse(is.na(tDiff), 0, tDiff)]
sortedhead[, session := as.integer(factor(cumsum(tDiff >= 60*5))), by=address]
sortedhead[, idle := ifelse((shift(x) == x) & (shift(y) == y), 1, 0), by=address]
setnafill(x=sortedhead, type = "nocb", cols = c("idle"))
sortedhead[, teleport := (speed >= 10)]
sortedhead[speed >= 10]

sessions <- sortedhead[, list(sessionStart = min(lastPingTime), sessionEnd = max(lastPingTime)), by=list(address, session)]
sessions[, durationMins := as.numeric(sessionEnd - sessionStart)/60]
