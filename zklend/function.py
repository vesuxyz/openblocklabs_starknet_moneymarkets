import asyncio
from datetime import datetime
from decimal import Decimal
from typing import Dict, List, Union
from starknet_py.contract import Contract
from starknet_py.net.full_node_client import FullNodeClient
import pandas as pd
import json

Z_TOKEN_ABI: List = json.load(open('./zklend/ztoken.abi.json'))
MARKET_ABI: List = json.load(open('./zklend/market.abi.json'))

NODE_URL = "https://starknet-mainnet.public.blastapi.io"
MARKET = "0x04c0a5193d58f74fbace4b74dcf65481e734ed1714121bdc571da345540efa05"

LENDING_ACCUMULATOR_DECIMALS = 27

assets: List[Dict[str, Union[str, int]]] = [{
    "symbol": "STRK",
    "token_decimals": 18,
    "underlying": "0x04718f5a0fc34cc1af16a1cdee98ffb20c31f5cd61d6ab07201858f4287c938d",
    "z_token": "0x06d8fa671ef84f791b7f601fa79fea8f6ceb70b5fa84189e3159d532162efc21",
},
{
    "symbol": "ETH",
    "token_decimals": 18,
    "underlying": "0x049d36570d4e46f48e99674bd3fcc84644ddd6b96f7c741b1562b82f9e004dc7",
    "z_token": "0x01b5bd713e72fdc5d63ffd83762f81297f6175a5e0a4771cdadbc1dd5fe72cb1",
},
{
    "symbol": "USDC",
    "token_decimals": 6,
    "underlying": "0x053c91253bc9682c04929ca02ed00b3e423f6710d2ee7e0d5ebb06f3ecf368a8",
    "z_token": "0x047ad51726d891f972e74e4ad858a261b43869f7126ce7436ee0b2529a98f486",
},
{
    "symbol": "USDT",
    "token_decimals": 6,
    "underlying": "0x068f5c6a61780768455de69077e07e89787839bf8166decfbf92b645209c0fb8",
    "z_token": "0x00811d8da5dc8a2206ea7fd0b28627c2d77280a515126e62baa4d78e22714c4a",
}]

client = FullNodeClient(node_url=NODE_URL)

def get_today() -> str:
    formatted_date = datetime.now().strftime("%Y-%m-%d")
    return formatted_date

async def get_supply(z_token_address: int, block_number: int) -> int:
    contract = Contract(
        address=z_token_address,
        abi=Z_TOKEN_ABI,
        provider=client,
    )
    (uint_face_value,) = await contract.functions["totalSupply"].call(block_number=block_number)
    return uint_face_value

async def get_debt(underlying_address: int, block_number: int) -> int:
    contract = Contract(
        address=MARKET,
        abi=MARKET_ABI,
        provider=client,
    )
    (uint_face_value,) = await contract.functions["get_total_debt_for_token"].call(underlying_address, block_number=block_number)
    return uint_face_value

async def get_lending_accumulator(underlying_address: int, block_number: int) -> int:
    contract = Contract(
        address=MARKET,
        abi=MARKET_ABI,
        provider=client,
    )
    (uint_value,) = await contract.functions["get_lending_accumulator"].call(underlying_address, block_number=block_number)
    return uint_value

def scale_down(value: int, decimals: int) -> Decimal:
    return Decimal(value) / Decimal(f"1e{decimals}")

async def main():
    a = [get_data(asset) for asset in assets];
    results = await asyncio.gather(*a)
    df = pd.DataFrame(results)
    df.to_csv('output_zklend.csv', index=False)

async def get_data(asset):
    z_token_int = int(asset["z_token"], 16)
    underlying_int = int(asset["underlying"], 16)
    block_height = await client.get_block_number()
    supply = await get_supply(z_token_int, block_height)
    debt = await get_debt(underlying_int, block_height)
    lending_accumulator = await get_lending_accumulator(underlying_int, block_height)
    return {
        "protocol": "zkLend",
        "date": get_today(),
        "market": asset["underlying"],
        "tokenSymbol": asset["symbol"],
        "supply_token": scale_down(supply, asset["token_decimals"]),
        "borrow_token": scale_down(debt, asset["token_decimals"]),
        "net_supply_token": scale_down(supply - debt, asset["token_decimals"]),
        "non_recursive_supply_token": 0,
        "block_height": block_height,
        "lending_index_rate": scale_down(lending_accumulator, LENDING_ACCUMULATOR_DECIMALS),
    }

if __name__ == "__main__":
  asyncio.run(main())