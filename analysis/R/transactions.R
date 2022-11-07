library(data.table)

DCLaddresses <- unique(positions$address)

poly_tx = fread("../poly.csv")
ether_tx = fread("../ether.csv")
