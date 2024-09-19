import asyncio
import json
import pandas as pd

from datetime import datetime
from typing import Dict, List, Union
from starknet_py.contract import Contract
from starknet_py.net.full_node_client import FullNodeClient


NODE_URL = "https://starknet-mainnet.public.blastapi.io"
SHRINE = "0x0498edfaf50ca5855666a700c25dd629d577eb9afccdf3b5977aec79aee55ada"

GATE_ABI = json.load(
    open("./openblocklabs_starknet_moneymarkets/protocols/opus/gate.json")
)
SHRINE_ABI = json.load(
    open("./openblocklabs_starknet_moneymarkets/protocols/opus/shrine.json")
)


COLLATERAL: List[Dict[str, Union[str, int]]] = [
    {
        "asset": "0x49d36570d4e46f48e99674bd3fcc84644ddd6b96f7c741b1562b82f9e004dc7",
        "name": "Ether",
        "symbol": "ETH",
        "decimals": 18,
        "gate": "0x0315ce9c5d3e5772481181441369d8eea74303b9710a6c72e3fcbbdb83c0dab1",
    },
    {
        "asset": "0x04718f5a0fc34cc1af16a1cdee98ffb20c31f5cd61d6ab07201858f4287c938d",
        "name": "Starknet Token",
        "symbol": "STRK",
        "decimals": 18,
        "gate": "0x031a96fe18fe3fdab28822c82c81471f1802800723c8f3e209f1d9da53bc637d",
    },
    {
        "asset": "0x042b8f0484674ca266ac5d08e4ac6a3fe65bd3129795def2dca5c34ecc5f96d2",
        "name": "Starknet Wrapped Staked Ether",
        "symbol": "wstETH",
        "decimals": 18,
        "gate": "0x02d1e95661e7726022071c06a95cdae092595954096c373cde24a34bb3984cbf",
    },
    {
        "asset": "0x03fe2b97c1fd336e750087d68b9b867997fd64a2661ff3ca5a7c771641e8e7ac",
        "name": "Wrapped BTC",
        "symbol": "WBTC",
        "decimals": 18,
        "gate": "0x05bc1c8a78667fac3bf9617903dbf2c1bfe3937e1d37ada3d8b86bf70fb7926e",
    },
    {
        "asset": "0x053c91253bc9682c04929ca02ed00b3e423f6710d2ee7e0d5ebb06f3ecf368a8",
        "name": "USD Coin",
        "symbol": "USDC",
        "decimals": 6,
        "gate": "",
    },
    {
        "asset": "0x068f5c6a61780768455de69077e07e89787839bf8166decfbf92b645209c0fb8",
        "name": "Tether USD",
        "symbol": "USDT",
        "decimals": 6,
        "gate": "",
    },
]


async def get_collateral_info(
    provider: FullNodeClient,
    collateral_info: Dict[str, Union[str, int]],
    block: int,
    date: str,
) -> Dict[str, Union[str, int, float]]:
    gate = collateral_info["gate"]
    if gate:
        gate = Contract(provider=provider, abi=GATE_ABI, address=gate, cairo_version=1)
        scale = 10 ** collateral_info["decimals"]
        (deposited,) = await gate.functions["get_total_assets"].call()
        deposited /= scale
    else:
        deposited = 0

    return {
        "protocol": "Opus",
        "date": date,
        "market": collateral_info["asset"],
        "tokenSymbol": collateral_info["symbol"],
        "supply_token": deposited,
        "borrow_token": 0,
        "net_supply_token": deposited,
        "non_recursive_supply_token": deposited,
        "block_height": block,
        "lending_index_rate": 1,
    }


async def get_cash_info(
    provider: FullNodeClient,
    block: int,
    date: str,
) -> Dict[str, Union[str, int, float]]:
    shrine = Contract(
        provider=provider, abi=SHRINE_ABI, address=SHRINE, cairo_version=1
    )
    scale = 10**18
    (health,) = await shrine.functions["get_shrine_health"].call()
    debt = health["debt"]["val"] / scale

    return {
        "protocol": "Opus",
        "date": date,
        "market": "0x0stable",
        "tokenSymbol": "STB",
        "supply_token": 0,
        "borrow_token": debt,
        "net_supply_token": -debt,
        "non_recursive_supply_token": 0,
        "block_height": block,
        "lending_index_rate": 1,
    }


async def main():
    """
    Supply your calculation here according to the Guidelines.
    """
    provider = FullNodeClient(node_url=NODE_URL)
    block = await provider.get_block_number()
    today = datetime.now().strftime("%Y-%m-%d")

    res = [
        await get_collateral_info(provider, collateral_info, block, today)
        for collateral_info in COLLATERAL
    ]
    res.append(await get_cash_info(provider, block, today))
    df = pd.DataFrame(res)

    return df


if __name__ == "__main__":
    asyncio.run(main())
