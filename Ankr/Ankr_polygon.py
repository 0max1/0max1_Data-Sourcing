import pandas as pd
import numpy as np
import requests
import json
import os
import time
from tqdm import tqdm
from web3 import Web3
from web3.exceptions import ABIFunctionNotFound
import re
from datetime import datetime as dt
from datetime import timedelta
from ratelimit import limits, sleep_and_retry

timestamp = dt.now().strftime("%Y%m%d%H%M%S")

# Specify the directory path
token_directory = "/home/user-1-dev/MAX_Data/Ankr/result/polygon/token"
pool_directory = "/home/user-1-dev/MAX_Data/Ankr/result/polygon/pool"

POLY_API_KEY = "G3BEGUG8FXF842BT18YVMY2ZI1IDGVJ3WF"
POLY_URL = "https://api.polygonscan.com/api"

RPC_URL = "https://rpc.ankr.com/polygon/b1bdd2819d3da7be4760d270af7ef985a74725cbe337c348bca633d096806c5a"
w3 = Web3(Web3.HTTPProvider(RPC_URL))

getAccountBalance_url = "https://rpc.ankr.com/multichain/79258ce7f7ee046decc3b5292a24eb4bf7c910d7e39b691384c7ce0cfb839a01/?ankr_getAccountBalanceHistorical="

# swap event topics
swap = ['0xd78ad95fa46c994b6551d0da85fc275fe613ce37657fb8d5e3d130840159d822',
        '0x2170c741c41531aec20e7c107c24eecfdd15e69c9bb0a8dd37b1840b9e0b207b',
        '0x606ecd02b3e3b4778f8e97b2e03351de14224efaa5fa64e62200afc9395c2499',
        '0xcd3829a3813dc3cdd188fd3d01dcf3268c16be2fdd2dd21d0665418816e46062',
        '0x34660fc8af304464529f48a778e03d03e4d34bcd5f9b6f0cfbf3cd238c642f7f',
        '0x0874b2d545cb271cdbda4e093020c452328b24af12382ed62c4d00f5c26709db',
        '0xe5aa1cf5889c85b4317294c95f7f50feb57435a6defc7fa30823011ec18212a1',
        '0xbfd50a04f1e6e4aee344f5d0e7f15d74d0dbb58cd1f711daa6463094ca9508cd',
        '0xc42079f94a6350d7e6235f29174924f928cc2ac818eb64fed8004e115fbcca67',
        '0x06dfeb25e76d44e08965b639a9d9307df8e1c3dbe2a6364194895e9c3992f033',
        '0x45f377f845e1cc76ae2c08f990e15d58bcb732db46f92a4852b956580c3a162f',
        '0xea368a40e9570069bb8e6511d668293ad2e1f03b0d982431fd223de9f3b70ca6',
        '0x19352a4fd24c5ab4622d014da898a203a0b0d96176ae805137ab1d55c8b6a759',
        '0xd6d34547c69c5ee3d2667625c188acf1006abb93e0ee7cf03925c67cf7760413',
        '0x03e874ff0400af332019e3388ba3ec252559e9e6b33b393b1335aaf4755384e3',
        '0x6ac6c02c73a1841cb185dff1fe5282ff4499ce709efd387f7fc6de10a5124320',
        '0xfa2dda1cc1b86e41239702756b13effbc1a092b5c57e3ad320fbe4f3b13fe235']

#latest_block = w3.eth.block_number
#start_block = latest_block - 200
#end_block = latest_block

#filter = w3.eth.get_logs({
#    "fromBlock": start_block,
#    "toBlock": end_block
#})

#df = pd.DataFrame(filter)

#first_elements = [lst[0] if len(lst) > 0 else None for lst in df['topics']]
#df['new_topics'] = first_elements
#poly = df[['address', 'new_topics']]
#poly = poly.dropna().reset_index(drop=True)

grouped_swap = [swap[i:i + 2] for i in range(0, len(swap), 2)]
last_group_length = len(grouped_swap[-1])
if last_group_length < 2:
    grouped_swap[-1] += [None] * (2 - last_group_length)
    
latest_block = w3.eth.block_number

def get_event_log(end_block, block_diff):
    rows = []
    for topic0, topic1 in grouped_swap:
        params = {
            "module": "logs",
            "action": "getLogs",
            "fromBlock": end_block - block_diff,
            "toBlock": end_block,
            "topic0": topic0,
            "topic1": topic1,
            "topic0_1_opr": "or",
            "apikey": POLY_API_KEY
        }
        response = requests.post(POLY_URL, params=params)
        time.sleep(0.5)
        data = json.loads(response.text)
        result = data['result']
        rows += result
    return rows

def get_contracts(num, block_diff):
    end_block = latest_block
    rows = []
    for count in tqdm(range(num)):
        rows += get_event_log(end_block, block_diff)
        end_block -= block_diff
    df = pd.DataFrame(rows)
    contracts = df[['address']].rename(columns={'address': 'contract'}) # Select only the contract addresses
    return contracts

contracts = get_contracts(4, 100)
poly_final = contracts.drop_duplicates(ignore_index = True).dropna().reset_index(drop=True)
    
#rows = []
#for i in range(0, len(poly)):
#  if poly.new_topics[i].hex() in swap:
#    contract = poly.address[i]
#    rows.append({'contract': contract})
#poly_contract = pd.DataFrame(rows)
#poly_final = poly_contract.drop_duplicates(ignore_index = True)

token_type = ['NATIVE']
rows = []

for i in tqdm(range(0, len(poly_final))):
  payload = {
    "jsonrpc": "2.0",
    "method": "ankr_getAccountBalanceHistorical",
    "params": {
        "blockchain": ["polygon"],
        "walletAddress": poly_final.contract[i],
        "nativeFirst": True,
        "onlyWhitelisted": True,
        "pageSize": 2
    },
    "id": 1
  }
  headers = {
    "accept": "application/json",
    "content-type": "application/json"
  }

  response = requests.post(getAccountBalance_url, json=payload, headers=headers)
  time.sleep(1)
  if response.status_code == 200:
    data = json.loads(response.text)
    for stuffs in data:
      if 'result' not in stuffs:
        continue
      tvl = data['result']['totalBalanceUsd']
      asset = data['result']['assets']
      for items in asset:
          if items['tokenType'] in token_type:
            token = items['holderAddress']
          else:
            token = items['contractAddress']
          contract = items['holderAddress']
          rows.append({'tvl': tvl, 'contract': contract, 'token': token})
poly_pool = pd.DataFrame(rows)

grouped = poly_pool.groupby(['tvl', 'contract'])['token'].apply(lambda x: ','.join(x)).reset_index()
split_symbols = grouped['token'].str.split(',', expand=True).apply(pd.Series)
merged = pd.concat([grouped.drop(columns=['token']), split_symbols], axis=1)
num_columns = len(merged.columns)-1
token_columns = ['token_{}'.format(i) for i in range(1, num_columns)]
column_names = ['tvl', 'contract'] + token_columns
merged.columns = column_names
cleaned = merged.dropna(axis=0)
cleaned = cleaned.reset_index(drop = True)

# Returns the Solidity source code
def get_source_code(contract_address):
    params = {
        "module": "contract",
        "action": "getsourcecode",
        "address": contract_address,
        "apikey": POLY_API_KEY
    }
    response = requests.post(POLY_URL, params=params)
    data = json.loads(response.text)
    result = data['result'][0]
    return result

def get_contract(contract_address, source):
    abi = source["ABI"]

    checksum_address = Web3.to_checksum_address(contract_address)
    contract = w3.eth.contract(address=checksum_address, abi=abi)
    return contract

def clean_name(name):
    name = name.lower()
    name = name.replace("lps", "")
    name = name.replace("lp", "")
    name = name.replace("pair", "")
    name = name.replace("pool", "")
    name = name.replace("-", "")
    name = name.replace("tokens", "")
    name = name.replace("token", "")
    name = name.replace(" ", "")
    name = name.strip()
    if name == "" or name.lower() == "swap" or name.lower() == "swaps":
        return None
    return name


# Get name from source code
def get_name_from_source(source, contract):
    try:
        name = source["ContractName"]
        return clean_name(name)
    except Exception as e:
        return None


# Get contract name from name function
def get_name_from_name(source, contract):
    try:
        name = clean_name(contract.functions.name().call())
        return clean_name(name)
    except Exception as e:
        return None


# Get name from owner contract
def get_name_from_owner(source, contract):
    try:
        owner_address = contract.functions.owner().call()
        owner_source = get_source_code(owner_address)
        owner_name = owner_source["ContractName"]
        return clean_name(owner_name)
    except Exception as e:
        return None


def get_name(source, contract):
    name_dict = {
        "uniswapv3" : "uniswap_v3",
        "uniswapv2" : "uniswap",
        "apeswapfinance" : "apeswap",
        "algebra" : "quickswap_v3",
    }

    name_fun = [
        get_name_from_source,
        get_name_from_name,
        get_name_from_owner
    ]
    for fun in name_fun:
        name = fun(source, contract)
        if name is not None:
            break

    return name_dict.get(name, name)

def get_factory_from_factory(contract):
    try:
        factory_address = contract.functions.factory().call()
        return factory_address.lower()
    except Exception as e:
        return None


def get_factory_from_getFactory(contract):
    try:
        factory_address = contract.functions.getFactory().call()
        return factory_address.lower()
    except Exception as e:
        return None


def get_factory(contract):
    factory_fun = [
        get_factory_from_factory,
        get_factory_from_getFactory
    ]

    for fun in factory_fun:
        factory = fun(contract)
        if factory:
            return factory
    return factory

def get_fee_from_common_pattern(contract, sourceCode, name):
    try:
        sourceCode = sourceCode.replace(" ", "").replace("\r\n", "").replace("\\n", "")
        pattern = r"balance0\.mul\((\w+)\)\.sub\(.*?amount0In\.mul\((\w+)\)"
        match_pattern = re.search(pattern, sourceCode, re.IGNORECASE)
        fee_denominator = float(match_pattern.group(1))
        fee_numerator = float(match_pattern.group(2))
        fee = fee_numerator / fee_denominator
        return fee
    except Exception as e:
        return None


def get_fee_from_fee(contract, sourceCode, name):
    deno_dict = {
        "uniswap_v3" : 10**6,

    }
    if name in deno_dict:
        fee = float(contract.functions.fee().call())
        fee = fee / deno_dict[name]
        return fee
    elif name == "fraxswap":
        fee = 10000 - float(contract.functions.fee().call())
        fee = fee / 10000
        return fee
    return None


def get_fee_from_swapFeeUnits(contract, sourceCode, name):
    if name == "kyberswapv2reinvestment":
        swapFeeUnits = float(contract.functions.swapFeeUnits().call())
        fee = swapFeeUnits / 100000
        return fee
    return None


def get_fee_from_swapFee(contract, sourceCode, name):
    if name == "meerkat":
        swapFee = float(contract.functions.swapFee().call())
        fee = swapFee / 10000
        return fee
    return None


def get_fee_from_globalState(contract, sourceCode, name):
    if name == "quickswap_v3":
        globalState = contract.functions.globalState().call()
        fee = globalState[2]
        fee = fee / 1000
        return fee
    return None



def get_fee(contract, source, name):
    sourceCode = source["SourceCode"]

    fix_fee_dict = {
        "uniswap" : 3/1000,
        "sushiswap" : 3/1000,
        "apeswap" : 2/1000,
        "jetswap" : 1/1000,
        "dyst" : 1/2000,
    }

    fee_fun = [
        get_fee_from_fee,
        get_fee_from_common_pattern,
        get_fee_from_swapFeeUnits,
        get_fee_from_swapFee,
        get_fee_from_globalState,
    ]

    if name in fix_fee_dict:
        return fix_fee_dict[name]

    for fun in fee_fun:
        fee = fun(contract, sourceCode, name)
        if fee:
            return fee
    return fee

rows = []

for i in tqdm(range(0, len(cleaned))):
    contract_address = cleaned.contract[i]
    try:
        source = get_source_code(contract_address)
        contract = get_contract(contract_address, source)

        name = get_name(source, contract)
        if not int(source['Proxy']) and 'proxy' not in name.lower():
            factory = get_factory(contract)
            fee = get_fee(contract, source, name)

            row = {
                "name": name,
                "factory": factory,
                "tvl": cleaned.tvl[i],
                "fee": fee,
                "contract": contract_address,
                "token0": cleaned.token_1[i],
                "token1": cleaned.token_2[i]
            }

            rows.append(row)
    except Exception as e:
        continue

poly_pool = pd.DataFrame(rows)
poly_pool = poly_pool.dropna(subset=poly_pool.columns.difference(['factory']))

poly_used = poly_pool.copy()
poly_used = poly_used.sort_values(by='tvl', ascending=False).reset_index(drop = True)

tokens = pd.concat([cleaned['token_1'],cleaned['token_2']],axis=0)
unique_tokens = tokens.unique()
token_info = pd.DataFrame(unique_tokens, columns=["all_token"])


url = "https://rpc.ankr.com/multichain/79258ce7f7ee046decc3b5292a24eb4bf7c910d7e39b691384c7ce0cfb839a01/?ankr_getTokenHolders="

rows = []

for i in tqdm(range(0, len(token_info))):
  payload = {
    "jsonrpc": "2.0",
    "method": "ankr_getTokenHolders",
    "params": {
        "blockchain": "polygon",
        "contractAddress": token_info.all_token[i],
        "pageSize": 1,
        "skipSyncCheck": True
      },
    "id": 1
  }
  headers = {
    "accept": "application/json",
    "content-type": "application/json"
  }

  response = requests.post(url, json=payload, headers=headers)
  time.sleep(1)
  if response.status_code == 200:
    data = json.loads(response.text)
    for items in data:
      if 'result' in items:
        holder = data['result']['holdersCount']
        decimal = data['result']['tokenDecimals']
        address = data['result']['contractAddress']
        rows.append({'address': address, 'decimal': decimal, 'holder': holder})
token = pd.DataFrame(rows)
token_final = token.sort_values(by='holder', ascending=False).reset_index(drop = True)

token_data = token_final.to_json(orient='records')
token_name = f"polygon_token_{timestamp}.json"
token_path = os.path.join(token_directory, token_name)
with open(token_path, "w") as outfile:
    outfile.write(token_data)
    
pool_data = poly_used.to_json(orient='records')
pool_name = f"polygon_pool_{timestamp}.json"
pool_path = os.path.join(pool_directory, pool_name)
with open(pool_path, "w") as outfile:
    outfile.write(pool_data)