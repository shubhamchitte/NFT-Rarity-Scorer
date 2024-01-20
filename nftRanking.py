import math
from moralis import evm_api
import pandas as pd
from pandas import json_normalize
import json
import time
import warnings

warnings.simplefilter(action='ignore', category=FutureWarning)
pd.options.mode.chained_assignment = None

api_key = "your_api_key"

nftContract = ""
totalNFTs = 0
allTraits = None
allTraitsWithCountsDf = None

numTraitsList = []
traitsDf = pd.DataFrame() # with this we can calculate the occurance of each type of attributes

df = pd.DataFrame() # this is our data frame to collect all our nft


def read_csv_file() :
    # read csv file into pandas DataFrame
    csvDf = pd.read_csv("your\csv\path.csv",header=0)
    nftCollectionArr = csvDf.to_numpy()
    return nftCollectionArr


def get_total_nft_colletion_count(nftContract) :
    total_nft_result = evm_api.nft.get_nft_collection_stats(
        api_key=api_key,
        params={
            "chain": "eth",
            "address": nftContract
        },
    )
    return total_nft_result


def find_unique_traits() :
    global allTraits
    global allTraitsWithCountsDf

    # lets loop thru all the rows in our df
    for z in range(totalNFTs):
        traitsForSingleNFT = json_normalize(json.loads(df.iloc[z]["metadata"])["attributes"])
        numOfTraitsSingleNFT = len(traitsForSingleNFT.index)

        # append this to our array
        numTraitsList.append(numOfTraitsSingleNFT)

        global traitsDf
        if traitsDf.empty:
            traitsDf = traitsForSingleNFT
        else:
            traitsDf = pd.concat([traitsDf, traitsForSingleNFT])

        print(z + 1, "/", totalNFTs)

    # Lets calculate the rarity for each of this trait types
    allTraits = traitsDf["trait_type"].unique() # the total max of 6 nft trait types in our nft collection
    allTraitsWithCountsDf = pd.DataFrame(columns=['trait_type', 'counts']) # this df will ve the 6 rows for each trait types and another column which will ve count
    allTraitsWithCountsDf["trait_type"] = allTraits # for the trait type column we populate with the alltraits list we created b4

    # lets loop thru the len of all our traits, that will populate all our traits with the counts for each traits
    for traitNum in range(len(allTraits)):
        tempTraitDf = traitsDf[traitsDf['trait_type'] == allTraits[traitNum]]
        traitCounts = tempTraitDf["value"].value_counts()
        traitNonExsist = pd.Series([totalNFTs - len(tempTraitDf.index)], index=["null"]) # this will include the null val in the index
        traitCounts = pd.concat([traitCounts, traitNonExsist])
        traitCounts = 1 / (traitCounts / totalNFTs) # our trait count will be 1 % by the probability of having the trait count from the total nfts,
        allTraitsWithCountsDf.at[traitNum, "counts"] = traitCounts

    numOfTraitsSeries = pd.Series(numTraitsList).value_counts()
    numOfTraitsSeries = 1 / (numOfTraitsSeries / totalNFTs) # this will gives the rarity rate of 1, 3. 49, 86 for the traits 5, 6, 3, 4 correspondingly
    # lets create  the df with the rairity as one of the cell and then no. of trait as trait type cell and concat with all traits df
    numOfTraitsDf = pd.DataFrame(data={'trait_type': ["Number Of Traits"], 'counts': [numOfTraitsSeries]})
    allTraitsWithCountsDf = pd.concat([allTraitsWithCountsDf, numOfTraitsDf], ignore_index=True) # this df will ve the trait type and rarity count

    # create these 2 cols to our original df. and we can use this in our backend to rank this nft based on their total rarity score
    df['rarity_scores'] = pd.Series(dtype="object")
    df['total_rarity_score'] = pd.Series(dtype="int")


def calculate_rarity():
    global df
    # loop thru the total nft
    for j in range(totalNFTs):

        traitsForSingleNFT = json_normalize(json.loads(df.iloc[j]["metadata"])["attributes"])
        numOfTraitsSingleNFT = len(traitsForSingleNFT.index)

        numOfTraitsSingleNFTDf = pd.DataFrame(data={'trait_type': ["Number Of Traits"], 'value': [numOfTraitsSingleNFT]})
        traitsForSingleNFT = pd.concat([traitsForSingleNFT, numOfTraitsSingleNFTDf], ignore_index=True)
        # lets loop thru each trait in all the unique traits of the collection, bcoz if the trait isn't exist then we ve to add the null val (naN) or the rarity for the naN
        for trait in allTraits:
            if trait not in traitsForSingleNFT["trait_type"].values:
                missingTraitDf = pd.DataFrame(data={'trait_type': [trait], 'value': ['null']})
                traitsForSingleNFT = pd.concat([traitsForSingleNFT, missingTraitDf], ignore_index=True) # now we ve the piercing[6] -- null append to our traits for single nft
        # lets create the rarity scores column
        traitsForSingleNFT["rarity_score"] = pd.Series(dtype="int")

        # lets loop everpy single trait in the trait for sinlge nft, and fetch the rarity score associated with the trait type
        for row in range(len(traitsForSingleNFT)):
            indexOfTrait = allTraitsWithCountsDf.index[allTraitsWithCountsDf["trait_type"] == traitsForSingleNFT["trait_type"][row]].tolist()[0]
            rarityScore = allTraitsWithCountsDf["counts"][indexOfTrait][traitsForSingleNFT["value"][row]]
            traitsForSingleNFT["rarity_score"][row] = rarityScore # this will give us the rarity score for a single nft

            # lets push all the data(raritty scores) and the sum of these into our df, so for each nft we can get the obj of the rarity scores associated with the trait type and val
            if row == (len(traitsForSingleNFT) - 1):
                sumOfRaritys = traitsForSingleNFT["rarity_score"].sum()
                traitsInJson = traitsForSingleNFT.to_json(orient="records") # convert the df into json, easy to read on backend and frontend
                df["rarity_scores"][j] = traitsInJson
                df["total_rarity_score"][j] = sumOfRaritys

        print(j + 1, "/", totalNFTs)

    # lets sort our vals based on the total rarity scores , and then indexing them (from the range of 0 to 9999)
    df = df.sort_values(by="total_rarity_score", ascending=False)
    df.index = list(range(totalNFTs))


def reset_default():
    global totalNFTs
    global df
    global allTraits
    global allTraitsWithCountsDf
    totalNFTs=0
    df = pd.DataFrame()
    allTraits = None
    allTraitsWithCountsDf = None


def fetch_nfts() :
    for itemIndex in range(len(nftCollectionArr)) :
        global totalNFTs
        global df
        cursor = ""

        nftContract = nftCollectionArr[itemIndex][0]
        print("collection address : "+nftContract)
        total_nft_result = get_total_nft_colletion_count(nftContract)
        print("total_nft_result : "+total_nft_result["total_tokens"])

        # lets get the total amt of nfts
        totalNFTs = int(total_nft_result["total_tokens"])
        numOfReqs = math.ceil(totalNFTs/100)

        for x in range(numOfReqs):

            result = evm_api.nft.get_contract_nfts(
                api_key=api_key,
                params={
                    "address": nftContract,
                    "chain": "eth",
                    "cursor": cursor,
                    "limit": 100
                }
            )

            cursor = result["cursor"] # set the cursor = whatever the temporary result var has as a cursor key
            df2 = json_normalize(result["result"])

            # if the df is empty set it to the temp(df2) else concat df and the tmp df2
            if df.empty:
                df = df2
            else:
                df = pd.concat([df, df2])

            print(x + 1, "/", numOfReqs)
            time.sleep(0.21)

        # lets make all the indexes row in the df are from 1 to totalNfts
        df.index = list(range(totalNFTs))

        print("Finding Unique Traits")
        find_unique_traits()

        print("Calculating Rarity")
        calculate_rarity()

        # now that we ve our rarity scores(highes at the top and the lowest at the bottom) set for all the nft collections, convert em into the csv
        df.to_csv('./'+nftCollectionArr[itemIndex][1]+'.csv', header=True, index=True)

        reset_default()


#program starts from here
nftCollectionArr = read_csv_file()

print("fetching NFTs")
fetch_nfts()
