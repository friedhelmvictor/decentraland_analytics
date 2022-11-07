import requests
import time
import pandas as pd
import numpy as np
#from pandas.io.json import json_normalize
from sqlalchemy import create_engine
import psycopg2

class DLCollector:
    def __init__(self):
        self.getServers()
        self.getRealms()
        self.dbEngine =  create_engine('postgresql+psycopg2://postgres:password@localhost:5432/decentraland')
        self.onlineUsers = pd.DataFrame()
        self.getAllServerUserPositions()


    def getServers(self): # TODO: maybe run once a day?
        # Get base_url, owner and id of each server (a.k.a. realm)
        # THESE CAN YIELD INACTIVE SERVERS LIKE MELONWAVE...
        URL = "https://peer.decentraland.org/lambdas/contracts/servers"
        reqTime = int(time.time())
        resp = requests.get(URL)
        result = resp.json()
        resultDF = pd.DataFrame(result)
        resultDF["requestTime"] = reqTime
        self.servers = resultDF
        return(resultDF)

    def getRealms(self): # TODO: maybe run once a day?
        # Get base_url, owner and id of each server (a.k.a. realm)
        URL = "https://peer.decentraland.org/lambdas/explore/realms"
        reqTime = int(time.time())
        resp = requests.get(URL)
        result = resp.json()
        resultDF = pd.DataFrame(result)
        resultDF["requestTime"] = reqTime
        self.realms = resultDF
        return(resultDF)

    def getUserPositionsOfServer(self, serverURL):
        URL = "{}/comms/islands".format(serverURL)
        reqTime = int(time.time())
        resp = requests.get(URL)
        result = resp.json()
        df_list = []
        for island in result["islands"]:
            peers_df = pd.DataFrame(island["peers"])
            peers_df["islandId"] = island["id"]
            peers_df["server"] = serverURL
            df_list.append(peers_df)

        if(len(df_list) == 0):
            return(pd.DataFrame())
        userPositions = pd.concat(df_list, sort=False).reset_index(drop=True)
        userPositions["requestTime"] = reqTime
        # # 1: EITHER FILTER TO THOSE USERS THAT HAVE A POSITION:
        # userPositions = userPositions[userPositions['position'].notna()].copy()
        # userPositions = userPositions[userPositions['id'].notna()].copy()
        # print(userPositions)
        #
        # # 2: OR SOMEHOW FIX
        # #userPositions.loc[userPositions['parcel'].isnull(), 'parcel'] = (None, None)
        #
        #
        # # TODO: BUG; CONCAT IS FUCKED
        # parcel = pd.DataFrame(userPositions['parcel'].tolist(), columns=['parcel_1', 'parcel_2'])
        # del userPositions["parcel"]
        # position = pd.DataFrame(userPositions['position'].tolist(), columns=['position_1', 'position_2', 'position_3'])
        # del userPositions["position"]
        # userPositions = pd.concat([userPositions, parcel, position], axis=1)
        # userPositions.reset_index(drop=True, inplace=True)
        return(userPositions)

    def getAllServerUserPositions(self):
        df_list = []
        for serverURL in self.realms["url"].values:
            print("Checking", serverURL)
            df_list.append(self.getUserPositionsOfServer(serverURL))

        allServerUserPositions = pd.concat(df_list, sort=False)
        allServerUserPositions.reset_index(drop=True, inplace=True)
        self.onlineUsers = allServerUserPositions[["server","address"]]
        return(allServerUserPositions)

    def getEvents(self):
        URL = "https://events.decentraland.org/api/events/"
        reqTime = int(time.time())
        resp = requests.get(URL)
        result = resp.json()
        resultDF = pd.DataFrame(result["data"])
        resultDF["requestTime"] = reqTime
        return(resultDF)

    def getProfiles(self, chunk_size=50):
        profiles_list = []
        totalAddressesChecked = 0
        users = self.onlineUsers
        grouped = users[users.address.notna()].groupby("server")
        for serverBaseURL, addressGroup in grouped:
            addresses = addressGroup["address"].values
            for i in range(0, len(addresses), chunk_size):
                addresses_chunk = addresses[i:i + chunk_size]
                reqTime = int(time.time())
                reqURL = serverBaseURL+"/lambdas/profiles?id="+("&id=".join(addresses_chunk))
                totalAddressesChecked += len(addresses_chunk)
                resp = requests.get(reqURL)
                result = resp.json()
                profiles_chunk = pd.json_normalize(result, 'avatars')
                profiles_chunk["requestTime"] = reqTime
                profiles_list.append(profiles_chunk)

        profiles = pd.concat(profiles_list, sort=False)
        profiles = profiles[['hasClaimedName', 'name', 'description', 'tutorialStep', 'ethAddress', 'userId', 'version', 'hasConnectedWeb3', 'email', 'avatar.bodyShape', 'avatar.wearables', 'avatar.snapshots.body', 'avatar.snapshots.face256', 'avatar.eyes.color.r', 'avatar.eyes.color.g', 'avatar.eyes.color.b', 'avatar.eyes.color.a', 'avatar.hair.color.r', 'avatar.hair.color.g', 'avatar.hair.color.b', 'avatar.hair.color.a', 'avatar.skin.color.r', 'avatar.skin.color.g', 'avatar.skin.color.b', 'avatar.skin.color.a', 'blocked', 'muted', 'interests', 'unclaimedName', 'avatar.snapshots.face', 'avatar.snapshots.face128', 'requestTime']]
        profiles = profiles[~profiles.astype(str).duplicated()] # sometimes the same userId is online on two realms... So we remove duplicates
        profiles.reset_index(drop=True, inplace=True)
        return(profiles)


#col = DLCollector()
#print(col.getEvents())
# userPos = col.getAllServerUserPositions()
# userPos.to_sql('positions', col.dbEngine, if_exists='append',index=False)
#print(col.getUserPositionsOfServer("https://peer-ec2.decentraland.org"))
