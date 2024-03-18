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
token_directory = "/home/user-1-dev/MAX_Data/Ankr/result/bsc/token"
pool_directory = "/home/user-1-dev/MAX_Data/Ankr/result/bsc/pool"

BSC_API_KEY = "UDI71M98AKCK7ECH57A7SUIN6H6N3IVNI3"
BSC_URL = 'https://api.bscscan.com/api'

RPC_URL = "https://rpc.ankr.com/bsc/b1bdd2819d3da7be4760d270af7ef985a74725cbe337c348bca633d096806c5a"
w3 = Web3(Web3.HTTPProvider(RPC_URL))

getAccountBalance_url = "https://rpc.ankr.com/multichain/79258ce7f7ee046decc3b5292a24eb4bf7c910d7e39b691384c7ce0cfb839a01/?ankr_getAccountBalanceHistorical="

# swap event topics
swap = ['0xd6d34547c69c5ee3d2667625c188acf1006abb93e0ee7cf03925c67cf7760413',
        '0x606ecd02b3e3b4778f8e97b2e03351de14224efaa5fa64e62200afc9395c2499',
        '0x06dfeb25e76d44e08965b639a9d9307df8e1c3dbe2a6364194895e9c3992f033',
        '0xc528cda9e500228b16ce84fadae290d9a49aecb17483110004c5af0a07f6fd73',
        '0xcd3829a3813dc3cdd188fd3d01dcf3268c16be2fdd2dd21d0665418816e46062',
        '0x2170c741c41531aec20e7c107c24eecfdd15e69c9bb0a8dd37b1840b9e0b207b',
        '0xfa2dda1cc1b86e41239702756b13effbc1a092b5c57e3ad320fbe4f3b13fe235',
        '0x13dead11e061a2fd5b015354dd80fe34365086cab93338864695d4f4462ba5ce',
        '0xad7d6f97abf51ce18e17a38f4d70e975be9c0708474987bb3e26ad21bd93ca70',
        '0x34660fc8af304464529f48a778e03d03e4d34bcd5f9b6f0cfbf3cd238c642f7f',
        '0x20efd6d5195b7b50273f01cd79a27989255356f9f13293edc53ee142accfdb75',
        '0xc42079f94a6350d7e6235f29174924f928cc2ac818eb64fed8004e115fbcca67',
        '0x54787c404bb33c88e86f4baf88183a3b0141d0a848e6a9f7a13b66ae3a9b73d1',
        '0xff3715fa8f2d4d791dd7a610a545050b8c6fe3a62b0f6c38f2f96a00598fe483',
        '0x6950339c7661cca450281e53722525cc136590e622b011d5be7e4c4993685a6c',
        '0x48b43f133ac721cb5f6e2a3d8ab7caee987bccfb4197537cf43c9f907f057f3a',
        '0xd78ad95fa46c994b6551d0da85fc275fe613ce37657fb8d5e3d130840159d822',
        '0xe7d6f812e1a54298ddef0b881cd08a4d452d9de35eb18b5145aa580fdda18b26',
        '0x45f377f845e1cc76ae2c08f990e15d58bcb732db46f92a4852b956580c3a162f',
        '0xe3a54b69726c85299f4e794bac96150af56af801be76cafd11947a1103b6308a',
        '0x41be64157af67faaa87968ac720941ca696c67a9d2daf8aaf7ed098637f19015',
        '0x19b47279256b2a23a1665c810c8d55a1758940ee09377d4f8d26497a3577dc83',
        '0x2ad8739d64c070ab4ae9d9c0743d56550b22c3c8c96e7a6045fac37b5b8e89e3',
        '0x0874b2d545cb271cdbda4e093020c452328b24af12382ed62c4d00f5c26709db',
        '0x77f92a1b6a1a11de8ca49515ad4c1fad45632dd3442167d74b90b304a3c7a758']

grouped_swap = [swap[i:i + 4] for i in range(0, len(swap), 4)]
last_group_length = len(grouped_swap[-1])
if last_group_length < 4:
    grouped_swap[-1] += [None] * (4 - last_group_length)

latest_block = w3.eth.block_number

def get_event_log(end_block, block_diff):
    rows = []
    for topic0, topic1, topic2, topic3 in grouped_swap:
        params = {
            "module": "logs",
            "action": "getLogs",
            "fromBlock": end_block - block_diff,
            "toBlock": end_block,
            "topic0": topic0,
            "topic1": topic1,
            "topic2": topic2,
            "topic3": topic3,
            "topic0_1_opr": "or",
            "topic0_2_opr": "or",
            "topic0_3_opr": "or",
            "topic1_2_opr": "or",
            "topic1_3_opr": "or",
            "topic2_3_opr": "or",
            "apikey": BSC_API_KEY
        }
        response = requests.post(BSC_URL, params=params)
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
bsc_final = contracts.drop_duplicates(ignore_index = True).dropna().reset_index(drop=True)

#start_block = latest_block - 300
#end_block = latest_block

#filter = w3.eth.get_logs({
#    "fromBlock": start_block,
#    "toBlock": end_block
#})

#df = pd.DataFrame(filter)
#first_elements = [lst[0] if len(lst) > 0 else None for lst in df['topics']]
#df['new_topics'] = first_elements
#bsc = df[['address', 'new_topics']]
#bsc = bsc.dropna().reset_index(drop=True)

#rows = []
#for i in range(0, len(bsc)):
#  if bsc.new_topics[i].hex() in swap:
#    contract = bsc.address[i]
#    rows.append({'contract': contract})
#bsc_contract = pd.DataFrame(rows)
#bsc_final = bsc_contract.drop_duplicates(ignore_index = True)

token_type = ['NATIVE']
rows = []

for i in tqdm(range(0, len(bsc_final))):
  payload = {
    "jsonrpc": "2.0",
    "method": "ankr_getAccountBalanceHistorical",
    "params": {
        "blockchain": ["bsc"],
        "walletAddress": bsc_final.contract[i],
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
bsc_pool = pd.DataFrame(rows)

grouped = bsc_pool.groupby(['tvl', 'contract'])['token'].apply(lambda x: ','.join(x)).reset_index()
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
        "apikey": BSC_API_KEY
    }
    response = requests.post(BSC_URL, params=params)
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
    name = name.strip()
    if name == "" or name.lower() == "swap":
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
        "pancakev3": "pancakeswap_v3",
        "algebra": "thena_fusion",
        "qiao": "qiaoswap",
        "apeswapfinance": "apeswap",
        "knight": "knightswap",
        "uniswapv3": "uniswap_v3",
        "uniswapv2": "uniswap_v2"
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

    if "volatilev1" in name or "stablev1" in name:
        return "thena_v1"

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
        "pancakeswap_v3": 10000*100,
        "uniswap_v3": 10000*100,
        "vyper_contract": 10**10,
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


# Try get fee from swapFee function in abi (Biswap, Nomiswap, Global LPs, Fins)
def get_fee_from_swapFee(contract, sourceCode, name):
    deno_dict = {
        "biswap": 1000,
        "nomiswap": 1000,
        "nomiswapStable": 100000,
        "global": 100000,
        "fins": 10000,
        "plearn": 1000
    }

    if name in deno_dict:
        swap_fee = float(contract.functions.swapFee().call())
        fee = swap_fee / deno_dict[name]
        return fee
    return None


# Try get fee from sum of devFee and userFee functions in abi (Honor)
def get_fee_from_DevUser(contract, sourceCode, name):
    try:
        devFee = float(contract.functions.devFee().call())
        userFee = float(contract.functions.userFee().call())
        fee = (devFee + userFee) / 10000
        return fee
    except Exception as e:
        return None


# Try get fee from getPairFee function in abi (Unifi)
def get_fee_from_getPairFee(contract, sourceCode, name):
    try:
        pair_fee = float(contract.functions.getPairFee().call())
        fee = pair_fee / 1000
        return fee
    except Exception as e:
        return None


# Try get fee from feeAmount function in abi (Orbital)
def get_fee_from_feeAmount(contract, sourceCode, name):
    try:
        fee_amount = float(contract.functions.feeAmount().call())
        fee = fee_amount / 10000
        return fee
    except Exception as e:
        return None


# Try get fee from getSwapFee function in abi (ValueLiquid)
def get_fee_from_getSwapFee(contract, sourceCode, name):
    try:
        swap_fee = float(contract.functions.getSwapFee().call())
        fee = swap_fee / 1000
        return fee
    except Exception as e:
        return None


# Try get fee from getTotalFee function in abi (DSSwap)
def get_fee_from_getTotalFee(contract, sourceCode, name):
    try:
        total_fee = float(contract.functions.getTotalFee().call())
        fee = total_fee / 10000
        return fee
    except Exception as e:
        return None


def get_fee_for_thena(contract, sourceCode, name):
    try:
        isStable = bool(contract.functions.stable().call())
        if isStable:
            fee = 4 / (100*100)
        else:
            fee = 18 / (100*100)
        return fee
    except Exception as e:
        return None


# Get fee from factory using factory function of the abi (Mdex, LP, Ninja)
def get_fee_from_factory(contract, sourceCode, name):
    # From feeRateNumerator (Mdex)
    def from_feeRateNumerator(factory_contract):
        try:
            fee_numerator = float(factory_contract.functions.feeRateNumerator().call())
            fee_denominator = float(factory_contract.functions.FEE_RATE_DENOMINATOR().call())
            fee = fee_numerator / fee_denominator
            return fee
        except Exception as e:
            return None


    # From swapFee (LP, Ninja)
    def from_swapFee(factory_contract):
        try:
            fee_numerator = float(factory_contract.functions.swapFee.call())
            fee_denominator = float(factory_contract.functions.FEE_DENOMINATOR().call())
            fee = fee_numerator / fee_denominator
            return fee
        except Exception:
            try:
                fee_numerator = float(factory_contract.functions.swapFee.call())
                fee = fee_numerator / 1000
                return fee
            except Exception as e:
                return None


    # From feeScale (MarsSwap)
    def from_feeScale(factory_contract):
        try:
            fee_scale = float(factory_contract.functions.feeScale.call())
            fee = (1000 - fee_scale) / 1000
            return fee
        except Exception as e:
            return None


    try:
        factory_address = contract.functions.factory().call()
        factory_source = get_source_code(factory_address)

        factory_name = clean_name(factory_source["ContractName"])
        factory_sourceCode = factory_source["SourceCode"]
        factory_abi = factory_source["ABI"]

        factory_checksum_address = Web3.to_checksum_address(factory_address)
        factory_contract = w3.eth.contract(address=factory_checksum_address, abi=factory_abi)

        factory_fee_fun = [
            from_feeRateNumerator,
            from_swapFee,
            from_feeScale
        ]

        for fun in factory_fee_fun:
            fee = fun(factory_contract)
            if fee:
                break
        return fee
    except Exception as e:
        return None


def get_fee(contract, source, name):
    sourceCode = source["SourceCode"]

    fix_fee_dict = {
        "apeswap" : 2/1000,
        "pancake": 25/10000,
        "uniswap_v2": 3/1000,
        "babydoge": 100/1000,
        "gibx": 3/1000,
        "p2e": 3/1000
    }

    fee_fun = [
        get_fee_from_fee,
        get_fee_from_swapFee,
        get_fee_from_common_pattern,
        get_fee_from_DevUser,
        get_fee_from_getPairFee,
        get_fee_from_feeAmount,
        get_fee_from_getSwapFee,
        get_fee_from_getTotalFee,
        get_fee_from_factory,
        get_fee_for_thena
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

bsc_pool = pd.DataFrame(rows)
bsc_pool = bsc_pool.dropna(subset=bsc_pool.columns.difference(['factory']))
bsc_used = bsc_pool.copy()
bsc_used = bsc_used.sort_values(by='tvl', ascending=False).reset_index(drop = True)

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
        "blockchain": "bsc",
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
token_name = f"bsc_token_{timestamp}.json"
token_path = os.path.join(token_directory, token_name)
with open(token_path, "w") as outfile:
    outfile.write(token_data)
    
pool_data = bsc_used.to_json(orient='records')
pool_name = f"bsc_pool_{timestamp}.json"
pool_path = os.path.join(pool_directory, pool_name)
with open(pool_path, "w") as outfile:
    outfile.write(pool_data)