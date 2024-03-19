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
from bs4 import BeautifulSoup
from datetime import datetime as dt
from datetime import timedelta
from ratelimit import limits, sleep_and_retry

timestamp = dt.now().strftime("%Y%m%d%H%M%S")

# Specify the directory path
token_directory = "/home/user-1-dev/MAX_Data/Ankr/result/eth/token"
pool_directory = "/home/user-1-dev/MAX_Data/Ankr/result/eth/pool"

ETH_API_KEY = "MNQGA1SMKKMM15T4BMI5E7YQV29SBJ6JHD"
ETH_URL = "https://api.etherscan.io/api"

RPC_URL = "https://rpc.ankr.com/eth/b1bdd2819d3da7be4760d270af7ef985a74725cbe337c348bca633d096806c5a"
w3 = Web3(Web3.HTTPProvider(RPC_URL))

getAccountBalance_url = "https://rpc.ankr.com/multichain/79258ce7f7ee046decc3b5292a24eb4bf7c910d7e39b691384c7ce0cfb839a01/?ankr_getAccountBalanceHistorical="

# swap event topics
swap = ['0x49926bbebe8474393f434dfa4f78694c0923efa07d19f2284518bfabd06eb737',
        '0x45e6b781bd057c809483af8c0f74bf1d164816674295f01beb2a50239a594a40',
        '0x06dfeb25e76d44e08965b639a9d9307df8e1c3dbe2a6364194895e9c3992f033',
        '0xa24f288a343811d26ac1ec29998e37b87ff6503cefe399a3c8fb747eb0464e58',
        '0xe1d4504fa5e661f80f16e8d613b5bc290ee6afe00a96b833a972d8e4490976e1',
        '0xfc431937278b84c6fa5b23bcc58f673c647fea974d3656e766b22d8c1412e544',
        '0x19b47279256b2a23a1665c810c8d55a1758940ee09377d4f8d26497a3577dc83',
        '0x7af2bc3f8ec800c569b6555feaf16589d96a9d04a49d1645fd456d75fa0b372b',
        '0x3b841dc9ab51e3104bda4f61b41e4271192d22cd19da5ee6e292dc8e2744f713',
        '0x77f92a1b6a1a11de8ca49515ad4c1fad45632dd3442167d74b90b304a3c7a758',
        '0xcd3829a3813dc3cdd188fd3d01dcf3268c16be2fdd2dd21d0665418816e46062',
        '0xea368a40e9570069bb8e6511d668293ad2e1f03b0d982431fd223de9f3b70ca6',
        '0xd6d34547c69c5ee3d2667625c188acf1006abb93e0ee7cf03925c67cf7760413',
        '0xfa2dda1cc1b86e41239702756b13effbc1a092b5c57e3ad320fbe4f3b13fe235',
        '0x9d329ac70bdb6c978dc1e9db93c81a0c263a2d5423a0e1899c81b73fccdb1720',
        '0xb3e2773606abfd36b5bd91394b3a54d1398336c65005baf7bf7a05efeffaf75b',
        '0x606ecd02b3e3b4778f8e97b2e03351de14224efaa5fa64e62200afc9395c2499',
        '0x053d794b2310b8d186a24ae24a65ee066983a52a6efa6bd3df09a7601a3cb4f3',
        '0xd78ad95fa46c994b6551d0da85fc275fe613ce37657fb8d5e3d130840159d822',
        '0x13dead11e061a2fd5b015354dd80fe34365086cab93338864695d4f4462ba5ce',
        '0xac50a83c5dcd42ce33ea749192a73b769e02b8b4fd4aecd74f4adb25515ac506',
        '0xd5fe17cd50e0d3d39b905ea598bbabccf2f8cda62a3b2fc64e09de00247a4724',
        '0x015fc8ee969fd902d9ebd12a31c54446400a2b512a405366fe14defd6081d220',
        '0x34660fc8af304464529f48a778e03d03e4d34bcd5f9b6f0cfbf3cd238c642f7f',
        '0x829000a5bc6a12d46e30cdcecd7c56b1efd88f6d7d059da6734a04f3764557c4']

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
            "apikey": ETH_API_KEY
        }
        response = requests.post(ETH_URL, params=params)
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
eth_final = contracts.drop_duplicates(ignore_index = True).dropna().reset_index(drop=True)

#start_block = latest_block - 250
#end_block = latest_block
#filter = w3.eth.get_logs({
#    "fromBlock": start_block,
#    "toBlock": end_block
#})

#df = pd.DataFrame(filter)
#first_elements = [lst[0] if len(lst) > 0 else None for lst in df['topics']]
#df['new_topics'] = first_elements
#eth = df[['address', 'new_topics']]
#eth = eth.dropna().reset_index(drop=True)

#rows = []
#for i in range(0, len(eth)):
#  if eth.new_topics[i].hex() in swap:
#    contract = eth.address[i].lower()
#    rows.append({'contract': contract})

#eth_contract = pd.DataFrame(rows)
#eth_final = eth_contract.drop_duplicates(ignore_index = True)

token_type = ['NATIVE']
rows = []

for i in tqdm(range(0, len(eth_final))):
  payload = {
    "jsonrpc": "2.0",
    "method": "ankr_getAccountBalanceHistorical",
    "params": {
        "blockchain": ["eth"],
        "walletAddress": eth_final.contract[i],
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
eth_pool = pd.DataFrame(rows)

grouped = eth_pool.groupby(['tvl', 'contract'])['token'].apply(lambda x: ','.join(x)).reset_index()
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
        "apikey": ETH_API_KEY
    }
    response = requests.post(ETH_URL, params=params)
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
        "composablestable": "balancerv2",
        "metastable": "balancerv2",
        "weighted" : "balancerv2",
        "weighted2" : "balancerv2",
        "stable" : "balancerv2",
        "gyroec" : "balancerv2",
        "convergentcurve" : "balancerv2",
        "noprotocolfeeliquiditybootstrapping" : "balancerv2",
        "liquiditybootstrapping" : "balancerv2",
        "erc4626linear" : "balancerv2",
        "aavelinear" : "balancerv2",
        "eulerlinear" : "balancerv2",
        "yearnlinear" : "balancerv2",
        "gearboxlinear" : "balancerv2",
        "bancorbnt": "bancorv3",
        "vyper_contract": "curve",
        "pancakev3": "pancakeswap",
        "swapflashloan": "saddle",
        "metaswap" : "saddle",
        "permissionlessmetaswap" : "saddle",
        "sakeswap": "sakeswap_pool",
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
        "pancakeswap": 10000*100,
        "curve": 10**8,
        "uniswapv3": 10000*100
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


# NomiswapStable
def get_fee_from_swapFee(contract, sourceCode, name):
    try:
        swapFee = float(contract.functions.swapFee().call())
        max_fee = float(contract.functions.MAX_FEE().call())
        fee = swapFee / max_fee
        return fee
    except Exception as e:
        return None


def get_fee_from_getSwapFeePercentage(contract, sourceCode, name):
    if name == "balancerv2":
        swapFeePercentage = float(contract.functions.getSwapFeePercentage().call())
        fee = swapFeePercentage / 10**18
        return fee
    return None


# saddle
def get_fee_from_swapStorage(contract, sourceCode, name):
    if name == "saddle":
        swapStorage = contract.functions.swapStorage().call()
        swapFee = float(swapStorage[4])
        fee = swapFee / 10**8
        return fee
    return None


def get_fee_from_tradingFeePercent(contract, sourceCode, name):
    if name == "linkswap":
        tradingFeePercent = float(contract.functions.tradingFeePercent().call())
        fee = tradingFeePercent / 10**6
        return fee
    return None


def get_fee_from_factory(contract, sourceCode, name):
    if name == "crodefiswap":
        factory_address = contract.functions.factory().call()
        factory_source = get_source_code(factory_address)
        factory_contract = get_contract(factory_address, factory_source)
        totalFeeBasisPoint = float(factory_contract.functions.totalFeeBasisPoint().call())
        fee = totalFeeBasisPoint / 10000
        return fee
    return None


def get_fee(contract, source, name):
    sourceCode = source["SourceCode"]

    fix_fee_dict = {
        "uniswapv2": 3/1000,
        "verseexchange": 3/1000,
        "saitaswap": 2/1000,
        "sakeswap_pool": 3/1000,
        "xchange": 200/100000,
        "kingswap": 25/10/1000,
    }

    fee_fun = [
        get_fee_from_fee,
        get_fee_from_common_pattern,
        get_fee_from_swapFee,
        get_fee_from_getSwapFeePercentage,
        get_fee_from_swapStorage,
        get_fee_from_tradingFeePercent,
        get_fee_from_factory
    ]

    if name in fix_fee_dict:
        return fix_fee_dict[name]

    for fun in fee_fun:
        fee = fun(contract, sourceCode, name)
        if fee:
            break
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

eth_pool = pd.DataFrame(rows)
eth_pool = eth_pool.dropna(subset=eth_pool.columns.difference(['factory']))

eth_used = eth_pool.copy()
eth_used = eth_used.sort_values(by='tvl', ascending=False).reset_index(drop = True)

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
        "blockchain": "eth",
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
token_name = f"token_{timestamp}.json"
token_path = os.path.join(token_directory, token_name)
with open(token_path, "w") as outfile:
    outfile.write(token_data)
    
pool_data = eth_used.to_json(orient='records')
pool_name = f"pool_{timestamp}.json"
pool_path = os.path.join(pool_directory, pool_name)
with open(pool_path, "w") as outfile:
    outfile.write(pool_data)