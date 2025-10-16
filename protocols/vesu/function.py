import asyncio
from starknet_py.contract import Contract
from starknet_py.net.full_node_client import FullNodeClient
from datetime import datetime
import pandas as pd
from itertools import permutations

# Configs
# Make sure to add your Nethermind RPC key below or switch to a different provider
RPC_KEY = "exysiEzs64QtlHQzZeYNf"
NODE_URL = f"https://rpc.nethermind.io/mainnet-juno/?apikey={RPC_KEY}" #"https://starknet-mainnet.public.blastapi.io"
NODE_URL = f"https://starknet-mainnet.g.alchemy.com/starknet/version/rpc/v0_8/{RPC_KEY}"
SINGLETON=0x000d8d6dfec4d33bfb6895de9f3852143a17c6f92fd2a21da3d6924d34870160 #0x2545b2e5d519fc230e9cd781046d3a64e092114f07e44771e0d719d148725ef
ORACLE=0xfe4bfb1b353ba51eb34dff963017f94af5a5cf8bdf3dfc191c504657f3c05
ELIGIBLE = ["STRK", "ETH", "USDC", "USDT", "xSTRK", "wstETH", "WBTC"]
STABLES = ["USDC", "USDT"]
NYB = ["WBTC"]
SCALE = 10**18

# Note: 
# - The extension and vToken addresses are based on the Vesu V1.1 contract deployment
# - For Vesu V2 markets, we set the extension to 0x0
MARKETS = [
    { # Genesis pool
        "pool": "0x4dc4f0ca6ea4961e4c8373265bfd5317678f4fe374d76f3fd7135f57763bf28",
        "extension": "0x4e06e04b8d624d039aa1c3ca8e0aa9e21dc1ccba1d88d0d650837159e0ee054",
        "asset": "0x049d36570d4e46f48e99674bd3fcc84644ddd6b96f7c741b1562b82f9e004dc7",
        "name": "Ether",
        "symbol": "ETH",
        "decimals": 18,
        "vToken": "0x0391bd9b58695b952aa15cffce50ba4650c954105df405ca8fc976ad7a65d646",
        "version": 1
    }, 
    {
        "pool": "0x4dc4f0ca6ea4961e4c8373265bfd5317678f4fe374d76f3fd7135f57763bf28",
        "extension": "0x4e06e04b8d624d039aa1c3ca8e0aa9e21dc1ccba1d88d0d650837159e0ee054",
        "asset": "0x03fe2b97c1fd336e750087d68b9b867997fd64a2661ff3ca5a7c771641e8e7ac",
        "name": "Wrapped BTC",
        "symbol": "WBTC",
        "decimals": 8,
        "vToken": "0x00c452bacd439bab4e39aeea190b4ff81f44b019d4b3a25fa4da04a1cae7b6ff",
        "version": 1
    },
    {
        "pool": "0x4dc4f0ca6ea4961e4c8373265bfd5317678f4fe374d76f3fd7135f57763bf28",
        "extension": "0x4e06e04b8d624d039aa1c3ca8e0aa9e21dc1ccba1d88d0d650837159e0ee054",
        "asset": "0x053c91253bc9682c04929ca02ed00b3e423f6710d2ee7e0d5ebb06f3ecf368a8",
        "name": "USD Coin",
        "symbol": "USDC",
        "decimals": 6,
        "vToken": "0x0227942991ea19a1843ed6d28af9458cf2566a3c2d6fccb2fd28f0424fce44b4",
        "version": 1
    },
    {
        "pool": "0x4dc4f0ca6ea4961e4c8373265bfd5317678f4fe374d76f3fd7135f57763bf28",
        "extension": "0x4e06e04b8d624d039aa1c3ca8e0aa9e21dc1ccba1d88d0d650837159e0ee054",
        "asset": "0x068f5c6a61780768455de69077e07e89787839bf8166decfbf92b645209c0fb8",
        "name": "Tether USD",
        "symbol": "USDT",
        "decimals": 6,
        "vToken": "0x040e480d202b47eb9335c31fc328ecda216231425dae74f87d1a97e6e7901dce",
        "version": 1
    },
    {
        "pool": "0x4dc4f0ca6ea4961e4c8373265bfd5317678f4fe374d76f3fd7135f57763bf28",
        "extension": "0x4e06e04b8d624d039aa1c3ca8e0aa9e21dc1ccba1d88d0d650837159e0ee054",
        "asset": "0x57912720381af14b0e5c87aa4718ed5e527eab60b3801ebf702ab09139e38b",
        "name": "Wrapped Staked Ether",
        "symbol": "wstETH",
        "decimals": 18,
        "vToken": "0x07cb1a46709214b94f51655be696a4ff6f9bdbbb6edb19418b6a55d190536048",
        "version": 1
    },
    {
        "pool": "0x4dc4f0ca6ea4961e4c8373265bfd5317678f4fe374d76f3fd7135f57763bf28",
        "extension": "0x4e06e04b8d624d039aa1c3ca8e0aa9e21dc1ccba1d88d0d650837159e0ee054",
        "asset": "0x04718f5a0fc34cc1af16a1cdee98ffb20c31f5cd61d6ab07201858f4287c938d",
        "name": "Starknet Token",
        "symbol": "STRK",
        "decimals": 18,
        "vToken": "0x0147ae3337b168ac9abe80a7214f0cb9e874b25c3db530a8e04beb98a134e07a",
        "version": 1
    },
    { # Re7 xSTRK pool
        "pool": "0x052fb52363939c3aa848f8f4ac28f0a51379f8d1b971d8444de25fbd77d8f161",
        "extension": "0x4e06e04b8d624d039aa1c3ca8e0aa9e21dc1ccba1d88d0d650837159e0ee054",
        "asset": "0x028d709c875c0ceac3dce7065bec5328186dc89fe254527084d1689910954b0a",
        "name": "Endur xSTRK",
        "symbol": "xSTRK",
        "decimals": 18,
        "vToken": "0x040f67320745980459615f4f3e7dd71002dbe6c68c8249c847c82dbe327b23cb",
        "version": 1
    },
    {
        "pool": "0x052fb52363939c3aa848f8f4ac28f0a51379f8d1b971d8444de25fbd77d8f161",
        "extension": "0x4e06e04b8d624d039aa1c3ca8e0aa9e21dc1ccba1d88d0d650837159e0ee054",
        "asset": "0x04718f5a0Fc34cC1AF16A1cdee98fFB20C31f5cD61D6Ab07201858f4287c938D",
        "name": "Starknet Token",
        "symbol": "STRK",
        "decimals": 18,
        "vToken": "0x05f4c1bc95be3e8c234c633b239a8ec965b748230c9b04319688ca8012e034c3",
        "version": 1
    },
    { # Re7 USDC pool
        "pool": "0x07f135b4df21183991e9ff88380c2686dd8634fd4b09bb2b5b14415ac006fe1d",
        "extension": "0x4e06e04b8d624d039aa1c3ca8e0aa9e21dc1ccba1d88d0d650837159e0ee054",
        "asset": "0x049d36570d4e46f48e99674bd3fcc84644ddd6b96f7c741b1562b82f9e004dc7",
        "name": "Ether",
        "symbol": "ETH",
        "decimals": 18,
        "vToken": "0x02ad74b0a40b2ee2a68ad3bec91a99e9d6a8690a079901d998a5473763917f7f",
        "version": 1
    }, 
    {
        "pool": "0x07f135b4df21183991e9ff88380c2686dd8634fd4b09bb2b5b14415ac006fe1d",
        "extension": "0x4e06e04b8d624d039aa1c3ca8e0aa9e21dc1ccba1d88d0d650837159e0ee054",
        "asset": "0x03fe2b97c1fd336e750087d68b9b867997fd64a2661ff3ca5a7c771641e8e7ac",
        "name": "Wrapped BTC",
        "symbol": "WBTC",
        "decimals": 8,
        "vToken": "0x037f38969b64cfaae0c40cd1565dc7c61a0e6e7dd3da3709fcea2303755ae648",
        "version": 1
    },
    {
        "pool": "0x07f135b4df21183991e9ff88380c2686dd8634fd4b09bb2b5b14415ac006fe1d",
        "extension": "0x4e06e04b8d624d039aa1c3ca8e0aa9e21dc1ccba1d88d0d650837159e0ee054",
        "asset": "0x053c91253bc9682c04929ca02ed00b3e423f6710d2ee7e0d5ebb06f3ecf368a8",
        "name": "USD Coin",
        "symbol": "USDC",
        "decimals": 6,
        "vToken": "0x079824ac0f81aa0e4483628c3365c09fa74d86650fadccb2a733284d3a0a8b85",
        "version": 1
    },
    {
        "pool": "0x07f135b4df21183991e9ff88380c2686dd8634fd4b09bb2b5b14415ac006fe1d",
        "extension": "0x4e06e04b8d624d039aa1c3ca8e0aa9e21dc1ccba1d88d0d650837159e0ee054",
        "asset": "0x57912720381af14b0e5c87aa4718ed5e527eab60b3801ebf702ab09139e38b",
        "name": "Wrapped Staked Ether",
        "symbol": "wstETH",
        "decimals": 18,
        "vToken": "0x02d19cfe8e7a21306adc37d3f3be61699db07618b9175bc49cff1502d09ad253",
        "version": 1
    },
    {
        "pool": "0x07f135b4df21183991e9ff88380c2686dd8634fd4b09bb2b5b14415ac006fe1d",
        "extension": "0x4e06e04b8d624d039aa1c3ca8e0aa9e21dc1ccba1d88d0d650837159e0ee054",
        "asset": "0x04718f5a0fc34cc1af16a1cdee98ffb20c31f5cd61d6ab07201858f4287c938d",
        "name": "Starknet Token",
        "symbol": "STRK",
        "decimals": 18,
        "vToken": "0x00dddeb3a7dec8d69a447aff8bc2f126d1b02814c341427dc5658e852f1d3524",
        "version": 1
    },
    { # Re7 wstETH pool
        "pool": "0x59ae5a41c9ae05eae8d136ad3d7dc48e5a0947c10942b00091aeb7f42efabb7",
        "extension": "0x4e06e04b8d624d039aa1c3ca8e0aa9e21dc1ccba1d88d0d650837159e0ee054",
        "asset": "0x049d36570d4e46f48e99674bd3fcc84644ddd6b96f7c741b1562b82f9e004dc7",
        "name": "Ether",
        "symbol": "ETH",
        "decimals": 18,
        "vToken": "0x02ba0f8f2defa6986b50d861b720984185296e48faed2133ca14712ddc6aaaf1",
        "version": 1
    }, 
    {
        "pool": "0x59ae5a41c9ae05eae8d136ad3d7dc48e5a0947c10942b00091aeb7f42efabb7",
        "extension": "0x4e06e04b8d624d039aa1c3ca8e0aa9e21dc1ccba1d88d0d650837159e0ee054",
        "asset": "0x57912720381af14b0e5c87aa4718ed5e527eab60b3801ebf702ab09139e38b",
        "name": "Wrapped Staked Ether",
        "symbol": "wstETH",
        "decimals": 18,
        "vToken": "0x008ccba554dd24f8346f439a9198d018c09dfb86a1ed652f6656644e3a5a500c",
        "version": 1
    },
    { # Re7 Starknet Ecosystem
        "pool": "0x6febb313566c48e30614ddab092856a9ab35b80f359868ca69b2649ca5d148d",
        "extension": "0x4e06e04b8d624d039aa1c3ca8e0aa9e21dc1ccba1d88d0d650837159e0ee054",
        "asset": "0x053c91253bc9682c04929ca02ed00b3e423f6710d2ee7e0d5ebb06f3ecf368a8",
        "name": "USD Coin",
        "symbol": "USDC",
        "decimals": 6,
        "vToken": "0x048f4e75c12ca9d35d6172b1cb5f1f70b094888003f9c94fe19f12a67947fd6d",
        "version": 1
    },
    {
        "pool": "0x6febb313566c48e30614ddab092856a9ab35b80f359868ca69b2649ca5d148d",
        "extension": "0x4e06e04b8d624d039aa1c3ca8e0aa9e21dc1ccba1d88d0d650837159e0ee054",
        "asset": "0x04718f5a0fc34cc1af16a1cdee98ffb20c31f5cd61d6ab07201858f4287c938d",
        "name": "Starknet Token",
        "symbol": "STRK",
        "decimals": 18,
        "vToken": "0x04228f2b404fa26a518d3ae4e8bb717f3e7f6d21ee5160517813a0eaec76e711",
        "version": 1
    },
    { # Braavos Pool
        "pool": "0x43f475012ed51ff6967041fcb9bf28672c96541ab161253fc26105f4c3b2afe",
        "extension": "0x04e06e04b8d624d039aa1c3ca8e0aa9e21dc1ccba1d88d0d650837159e0ee054",
        "asset": "0x03fe2b97c1fd336e750087d68b9b867997fd64a2661ff3ca5a7c771641e8e7ac",
        "name": "Wrapped BTC",
        "symbol": "WBTC",
        "decimals": 8,
        "vToken": "0x6d507cf5c751a6569d3a10447aee58f9b1410bb6a7d9c52d22875cd5377b29",
        "version": 1
    },
    {
        "pool": "0x43f475012ed51ff6967041fcb9bf28672c96541ab161253fc26105f4c3b2afe",
        "extension": "0x04e06e04b8d624d039aa1c3ca8e0aa9e21dc1ccba1d88d0d650837159e0ee054",
        "asset": "0x053c91253bc9682c04929ca02ed00b3e423f6710d2ee7e0d5ebb06f3ecf368a8",
        "name": "USD Coin",
        "symbol": "USDC",
        "decimals": 6,
        "vToken": "0x411587c1e5e6b09c9c1416efbebfe5adb5009ac99171b219caef4e59123b3ed",
        "version": 1
    },
    { # Alterscope CASH pool
        "pool": "0x7bafdbd2939cc3f3526c587cb0092c0d9a93b07b9ced517873f7f6bf6c65563",
        "extension": "0x4e06e04b8d624d039aa1c3ca8e0aa9e21dc1ccba1d88d0d650837159e0ee054",
        "asset": "0x0498EDFaF50CA5855666a700C25Dd629D577EB9aFcCDf3B5977aEC79AEE55ADA",
        "name": "Cash",
        "symbol": "CASH",
        "decimals": 18,
        "vToken": "0x00597354f0c1f01fde571fee8bc32d6c5479171561eab28fdb30448b9ed9d3c9",
        "version": 1
    },
    {
        "pool": "0x7bafdbd2939cc3f3526c587cb0092c0d9a93b07b9ced517873f7f6bf6c65563",
        "extension": "0x4e06e04b8d624d039aa1c3ca8e0aa9e21dc1ccba1d88d0d650837159e0ee054",
        "asset": "0x049d36570d4e46f48e99674bd3fcc84644ddd6b96f7c741b1562b82f9e004dc7",
        "name": "Ether",
        "symbol": "ETH",
        "decimals": 18,
        "vToken": "0x0115bd2016ba0d9a1a0075e943f8cc3098ea969baf5e57cde870865896cc9ca3",
        "version": 1
    }, 
    {
        "pool": "0x7bafdbd2939cc3f3526c587cb0092c0d9a93b07b9ced517873f7f6bf6c65563",
        "extension": "0x4e06e04b8d624d039aa1c3ca8e0aa9e21dc1ccba1d88d0d650837159e0ee054",
        "asset": "0x03fe2b97c1fd336e750087d68b9b867997fd64a2661ff3ca5a7c771641e8e7ac",
        "name": "Wrapped BTC",
        "symbol": "WBTC",
        "decimals": 8,
        "vToken": "0x0628c6ee44401855310f6a6657cf151194ba61d1b66139c942860a2138fac521",
        "version": 1
    },
    {
        "pool": "0x7bafdbd2939cc3f3526c587cb0092c0d9a93b07b9ced517873f7f6bf6c65563",
        "extension": "0x4e06e04b8d624d039aa1c3ca8e0aa9e21dc1ccba1d88d0d650837159e0ee054",
        "asset": "0x053c91253bc9682c04929ca02ed00b3e423f6710d2ee7e0d5ebb06f3ecf368a8",
        "name": "USD Coin",
        "symbol": "USDC",
        "decimals": 6,
        "vToken": "0x030902db47321a71202d4473a59b54db2b1ad11897a0328ead363db7e9dce4c8",
        "version": 1
    },
    {
        "pool": "0x7bafdbd2939cc3f3526c587cb0092c0d9a93b07b9ced517873f7f6bf6c65563",
        "extension": "0x4e06e04b8d624d039aa1c3ca8e0aa9e21dc1ccba1d88d0d650837159e0ee054",
        "asset": "0x068f5c6a61780768455de69077e07e89787839bf8166decfbf92b645209c0fb8",
        "name": "Tether USD",
        "symbol": "USDT",
        "decimals": 6,
        "vToken": "0x04258ae715716d47880bcf05d20c1670474d9ea66c09b27883df20537e1eb91b",
        "version": 1
    },
    {
        "pool": "0x7bafdbd2939cc3f3526c587cb0092c0d9a93b07b9ced517873f7f6bf6c65563",
        "extension": "0x4e06e04b8d624d039aa1c3ca8e0aa9e21dc1ccba1d88d0d650837159e0ee054",
        "asset": "0x04718f5a0fc34cc1af16a1cdee98ffb20c31f5cd61d6ab07201858f4287c938d",
        "name": "Starknet Token",
        "symbol": "STRK",
        "decimals": 18,
        "vToken": "0x0351efe34e732a283a35fa91bf52c0a3a6e89b0cc88ae32d4fde84e541b4fec2",
        "version": 1
    },
    { # Alterscope xSTRK pool
        "pool": "0x27f2bb7fb0e232befc5aa865ee27ef82839d5fad3e6ec1de598d0fab438cb56",
        "extension": "0x4e06e04b8d624d039aa1c3ca8e0aa9e21dc1ccba1d88d0d650837159e0ee054",
        "asset": "0x028d709c875c0ceac3dce7065bec5328186dc89fe254527084d1689910954b0a",
        "name": "Endur xSTRK",
        "symbol": "xSTRK",
        "decimals": 18,
        "vToken": "0x020478f0a1b1ef010aa24104ba0e91bf60efcabed02026b75e1d68690809e453",
        "version": 1
    },
    {
        "pool": "0x27f2bb7fb0e232befc5aa865ee27ef82839d5fad3e6ec1de598d0fab438cb56",
        "extension": "0x4e06e04b8d624d039aa1c3ca8e0aa9e21dc1ccba1d88d0d650837159e0ee054",
        "asset": "0x049d36570d4e46f48e99674bd3fcc84644ddd6b96f7c741b1562b82f9e004dc7",
        "name": "Ether",
        "symbol": "ETH",
        "decimals": 18,
        "vToken": "0x0530d3af399e4345ac3093c59715da3aacb8d9535c7ec50653573b32fbfbb7ad",
        "version": 1
    }, 
    {
        "pool": "0x27f2bb7fb0e232befc5aa865ee27ef82839d5fad3e6ec1de598d0fab438cb56",
        "extension": "0x4e06e04b8d624d039aa1c3ca8e0aa9e21dc1ccba1d88d0d650837159e0ee054",
        "asset": "0x03fe2b97c1fd336e750087d68b9b867997fd64a2661ff3ca5a7c771641e8e7ac",
        "name": "Wrapped BTC",
        "symbol": "WBTC",
        "decimals": 8,
        "vToken": "0x000a570a7382c0eb80db2a2317d4c74bf307c866f673b0367e28a6dffcc288a9",
        "version": 1
    },
    {
        "pool": "0x27f2bb7fb0e232befc5aa865ee27ef82839d5fad3e6ec1de598d0fab438cb56",
        "extension": "0x4e06e04b8d624d039aa1c3ca8e0aa9e21dc1ccba1d88d0d650837159e0ee054",
        "asset": "0x053c91253bc9682c04929ca02ed00b3e423f6710d2ee7e0d5ebb06f3ecf368a8",
        "name": "USD Coin",
        "symbol": "USDC",
        "decimals": 6,
        "vToken": "0x072803e813eb69d9aaea1c458ed779569c81bde0a2fc03ea2869876d13fa08d4",
        "version": 1
    },
    {
        "pool": "0x27f2bb7fb0e232befc5aa865ee27ef82839d5fad3e6ec1de598d0fab438cb56",
        "extension": "0x4e06e04b8d624d039aa1c3ca8e0aa9e21dc1ccba1d88d0d650837159e0ee054",
        "asset": "0x068f5c6a61780768455de69077e07e89787839bf8166decfbf92b645209c0fb8",
        "name": "Tether USD",
        "symbol": "USDT",
        "decimals": 6,
        "vToken": "0x02d647a41926df2a438ade8daf9d3f0026d97290f82edbaab4012673c7f5d81b",
        "version": 1
    },
    {
        "pool": "0x27f2bb7fb0e232befc5aa865ee27ef82839d5fad3e6ec1de598d0fab438cb56",
        "extension": "0x4e06e04b8d624d039aa1c3ca8e0aa9e21dc1ccba1d88d0d650837159e0ee054",
        "asset": "0x04718f5a0fc34cc1af16a1cdee98ffb20c31f5cd61d6ab07201858f4287c938d",
        "name": "Starknet Token",
        "symbol": "STRK",
        "decimals": 18,
        "vToken": "0x00427c4afb5a3733af903ca3cd4661a5ab52183b8bcdfe17f3899fc72984b181",
        "version": 1
    },
    { # Prime pool
        "pool": "0x451fe483d5921a2919ddd81d0de6696669bccdacd859f72a4fba7656b97c3b5",
        "extension": "0x0",
        "asset": "0x049d36570d4e46f48e99674bd3fcc84644ddd6b96f7c741b1562b82f9e004dc7",
        "name": "Ether",
        "symbol": "ETH",
        "decimals": 18,
        "vToken": "0x6ac248c18c69e57573aa3eeccbb7f8cd29e3024561be252ee7b34b96c1043e",
        "version": 2
    }, 
    {
        "pool": "0x451fe483d5921a2919ddd81d0de6696669bccdacd859f72a4fba7656b97c3b5",
        "extension": "0x0",
        "asset": "0x03fe2b97c1fd336e750087d68b9b867997fd64a2661ff3ca5a7c771641e8e7ac",
        "name": "Wrapped BTC",
        "symbol": "WBTC",
        "decimals": 8,
        "vToken": "0x4ecb0667140b9f45b067d026953ed79f22723f1cfac05a7b26c3ac06c88f56c",
        "version": 2
    },
    {
        "pool": "0x451fe483d5921a2919ddd81d0de6696669bccdacd859f72a4fba7656b97c3b5",
        "extension": "0x0",
        "asset": "0x053c91253bc9682c04929ca02ed00b3e423f6710d2ee7e0d5ebb06f3ecf368a8",
        "name": "USD Coin",
        "symbol": "USDC",
        "decimals": 6,
        "vToken": "0x79c83c3eb20df05d9e3ebdd45990060101bd126666181de622e432948f3e9",
        "version": 2
    },
    {
        "pool": "0x451fe483d5921a2919ddd81d0de6696669bccdacd859f72a4fba7656b97c3b5",
        "extension": "0x0",
        "asset": "0x068f5c6a61780768455de69077e07e89787839bf8166decfbf92b645209c0fb8",
        "name": "Tether USD",
        "symbol": "USDT",
        "decimals": 6,
        "vToken": "0x6be9f8980779930045b93c295105c6810d38191ec522b5175ddf7dbf9b22f9d",
        "version": 2
    },
    {
        "pool": "0x451fe483d5921a2919ddd81d0de6696669bccdacd859f72a4fba7656b97c3b5",
        "extension": "0x0",
        "asset": "0x57912720381af14b0e5c87aa4718ed5e527eab60b3801ebf702ab09139e38b",
        "name": "Wrapped Staked Ether",
        "symbol": "wstETH",
        "decimals": 18,
        "vToken": "0x7d231447ac838f45740ed823c3ae0982d94377bc9f165f751a371a41e9c1740",
        "version": 2
    },
    {
        "pool": "0x451fe483d5921a2919ddd81d0de6696669bccdacd859f72a4fba7656b97c3b5",
        "extension": "0x0",
        "asset": "0x04718f5a0fc34cc1af16a1cdee98ffb20c31f5cd61d6ab07201858f4287c938d",
        "name": "Starknet Token",
        "symbol": "STRK",
        "decimals": 18,
        "vToken": "0x6d6d2bf905dd199c78f2e421521d8473042737be9f47904e7578536c10f279d",
        "version": 2
    },
    { # Re7 xBTC pool
        "pool": "0x03a8416bf20d036df5b1cf3447630a2e1cb04685f6b0c3a70ed7fb1473548ecf",
        "extension": "0x0",
        "asset": "0x03fe2b97c1fd336e750087d68b9b867997fd64a2661ff3ca5a7c771641e8e7ac",
        "name": "Wrapped BTC",
        "symbol": "WBTC",
        "decimals": 8,
        "vToken": "0x131cc09160f144ec5880a0bc1a0633999030fa6a546388b5d0667cb171a52a0",
        "version": 2
    },
    {
        "pool": "0x03a8416bf20d036df5b1cf3447630a2e1cb04685f6b0c3a70ed7fb1473548ecf",
        "extension": "0x0",
        "asset": "0x0593e034dda23eea82d2ba9a30960ed42cf4a01502cc2351dc9b9881f9931a68",
        "name": "Solv BTC",
        "symbol": "SolvBTC",
        "decimals": 18,
        "vToken": "0x590117befc944f23b39ca5b0401e6aaa7834e90f2eb284baa2bfc475bd66190",
        "version": 2
    },
    {
        "pool": "0x03a8416bf20d036df5b1cf3447630a2e1cb04685f6b0c3a70ed7fb1473548ecf",
        "extension": "0x0",
        "asset": "0x04daa17763b286d1e59b97c283c0b8c949994c361e426a28f743c67bdfe9a32f",
        "name": "Threshold BTC",
        "symbol": "tBTC",
        "decimals": 18,
        "vToken": "0x4cbe8b13ebadd744254b09a40f4395f580e8a4a30acb2653849f61d12bfa039",
        "version": 2
    },
    {
        "pool": "0x03a8416bf20d036df5b1cf3447630a2e1cb04685f6b0c3a70ed7fb1473548ecf",
        "extension": "0x0",
        "asset": "0x036834a40984312f7f7de8d31e3f6305b325389eaeea5b1c0664b2fb936461a4",
        "name": "Lombard BTC",
        "symbol": "LBTC",
        "decimals": 8,
        "vToken": "0x73476ed5b0d781182ede4c806241a93cb47cb00b6de354855a1fc6233a13b35",
        "version": 2
    },
    {
        "pool": "0x03a8416bf20d036df5b1cf3447630a2e1cb04685f6b0c3a70ed7fb1473548ecf",
        "extension": "0x0",
        "asset": "0x6a567e68c805323525fe1649adb80b03cddf92c23d2629a6779f54192dffc13",
        "name": "Endur WBTC",
        "symbol": "xWBTC",
        "decimals": 8,
        "vToken": "0x62a162d0827db6f43ebb850cbef3c99fc7969e3070b83a2236c9f3713c89fd8",
        "version": 2
    },
    {
        "pool": "0x03a8416bf20d036df5b1cf3447630a2e1cb04685f6b0c3a70ed7fb1473548ecf",
        "extension": "0x0",
        "asset": "0x580f3dc564a7b82f21d40d404b3842d490ae7205e6ac07b1b7af2b4a5183dc9",
        "name": "Endur SolvBTC",
        "symbol": "xsBTC",
        "decimals": 18,
        "vToken": "0x76ea5335932dafb727f31dec684e75169e7a582478d681fe3a73494669940fb",
        "version": 2
    },
    {
        "pool": "0x03a8416bf20d036df5b1cf3447630a2e1cb04685f6b0c3a70ed7fb1473548ecf",
        "extension": "0x0",
        "asset": "0x43a35c1425a0125ef8c171f1a75c6f31ef8648edcc8324b55ce1917db3f9b91",
        "name": "Endur tBTC",
        "symbol": "xtBTC",
        "decimals": 18,
        "vToken": "0x3d90538d9b66c7fa3e582e7af5e96018a4f8f1e43d5eace23ba820fbe06ff70",
        "version": 2
    },
    {
        "pool": "0x03a8416bf20d036df5b1cf3447630a2e1cb04685f6b0c3a70ed7fb1473548ecf",
        "extension": "0x0",
        "asset": "0x7dd3c80de9fcc5545f0cb83678826819c79619ed7992cc06ff81fc67cd2efe0",
        "name": "Endur LBTC",
        "symbol": "xLBTC",
        "decimals": 8,
        "vToken": "0x31e5609fee92e0bc200436449ee2cc07a141fe77859c474427fc9490f87e637",
        "version": 2
    },
    {
        "pool": "0x03a8416bf20d036df5b1cf3447630a2e1cb04685f6b0c3a70ed7fb1473548ecf",
        "extension": "0x0",
        "asset": "0x04e4fb1a9ca7e84bae609b9dc0078ad7719e49187ae7e425bb47d131710eddac",
        "name": "Midas Re7 BTC",
        "symbol": "mRe7BTC",
        "decimals": 18,
        "vToken": "0x13448c4404424a534d22a46330432bd2ef5d884740e8b9fba7f4c273f85ada3",
        "version": 2
    },
    { # Re7 USDC Core pool
        "pool": "0x3976cac265a12609934089004df458ea29c776d77da423c96dc761d09d24124",
        "extension": "0x0",
        "asset": "0x053c91253bc9682c04929ca02ed00b3e423f6710d2ee7e0d5ebb06f3ecf368a8",
        "name": "USD Coin",
        "symbol": "USDC",
        "decimals": 6,
        "vToken": "0x60e91c92fdad9e7245b9bb4e143b880e4e9354d0b95c5c2d33dc347dded3bf0",
        "version": 2
    },
    {
        "pool": "0x03976cac265a12609934089004df458ea29c776d77da423c96dc761d09d24124",
        "extension": "0x0",
        "asset": "0x023a312ece4a275e38c9fc169e3be7b5613a0cb55fe1bece4422b09a88434573",
        "name": "uniBTC",
        "symbol": "uniBTC",
        "decimals": 8,
        "vToken": "0x6d656d23f38ca239877ea261ce265129cc3fae66f8e9d8948cf19a146c736c5",
        "version": 2
    },
    {
        "pool": "0x03976cac265a12609934089004df458ea29c776d77da423c96dc761d09d24124",
        "extension": "0x0",
        "asset": "0x0593e034dda23eea82d2ba9a30960ed42cf4a01502cc2351dc9b9881f9931a68",
        "name": "Solv BTC",
        "symbol": "SolvBTC",
        "decimals": 18,
        "vToken": "0x7e3d504483981d498c124e5901355af870760f99b09c01082de242e3ca7b002",
        "version": 2
    },
    {
        "pool": "0x03976cac265a12609934089004df458ea29c776d77da423c96dc761d09d24124",
        "extension": "0x0",
        "asset": "0x04daa17763b286d1e59b97c283c0b8c949994c361e426a28f743c67bdfe9a32f",
        "name": "Threshold BTC",
        "symbol": "tBTC",
        "decimals": 18,
        "vToken": "0x31f1d4092f343a4c0a7672ff6215740c2ccdb88c9679f8a54b0a311da5e2779",
        "version": 2
    },
    {
        "pool": "0x03976cac265a12609934089004df458ea29c776d77da423c96dc761d09d24124",
        "extension": "0x0",
        "asset": "0x036834a40984312f7f7de8d31e3f6305b325389eaeea5b1c0664b2fb936461a4",
        "name": "Lombard BTC",
        "symbol": "LBTC",
        "decimals": 8,
        "vToken": "0x4e37a738586ef9113e12e72ccfb018e5d18c0c10704d8380fb99510d5586ae2",
        "version": 2
    },
    { # Re7 USDC Prime pool
        "pool": "0x2eef0c13b10b487ea5916b54c0a7f98ec43fb3048f60fdeedaf5b08f6f88aaf",
        "extension": "0x0",
        "asset": "0x053c91253bc9682c04929ca02ed00b3e423f6710d2ee7e0d5ebb06f3ecf368a8",
        "name": "USD Coin",
        "symbol": "USDC",
        "decimals": 6,
        "vToken": "0x5abd079783b72ef84935dbca1ea82d1687d76b5f0dd45a075cfcde47533f650",
        "version": 2
    },
    {
        "pool": "0x2eef0c13b10b487ea5916b54c0a7f98ec43fb3048f60fdeedaf5b08f6f88aaf",
        "extension": "0x0",
        "asset": "0x03fe2b97c1fd336e750087d68b9b867997fd64a2661ff3ca5a7c771641e8e7ac",
        "name": "Wrapped BTC",
        "symbol": "WBTC",
        "decimals": 8,
        "vToken": "0x1a71039b15e5f5413ea450216387877adf962d5908811780c8f3dda5386b166",
        "version": 2
    },
    { # Re7 USDC Stable Core pool
        "pool": "0x073702fce24aba36da1eac539bd4bae62d4d6a76747b7cdd3e016da754d7a135",
        "extension": "0x0",
        "asset": "0x053c91253bc9682c04929ca02ed00b3e423f6710d2ee7e0d5ebb06f3ecf368a8",
        "name": "USD Coin",
        "symbol": "USDC",
        "decimals": 6,
        "vToken": "0x415e4dd356cae963d24f7c8f31443ff77369d021880ab452c36e6ad7ade43e0",
        "version": 2
    },
    {
        "pool": "0x073702fce24aba36da1eac539bd4bae62d4d6a76747b7cdd3e016da754d7a135",
        "extension": "0x0",
        "asset": "0x04be8945e61dc3e19ebadd1579a6bd53b262f51ba89e6f8b0c4bc9a7e3c633fc",
        "name": "Midase Re7 Yield",
        "symbol": "mRe7YIELD",
        "decimals": 18,
        "vToken": "0x133269be4c0a147ebe2bb28b1b0dd203ea1cd43357caf5cf48bc424ccb5e7f9",
        "version": 2
    },
    { # Re7 USDC Frontier pool
        "pool": "0x05c03e7e0ccfe79c634782388eb1e6ed4e8e2a013ab0fcc055140805e46261bd",
        "extension": "0x0",
        "asset": "0x053c91253bc9682c04929ca02ed00b3e423f6710d2ee7e0d5ebb06f3ecf368a8",
        "name": "USD Coin",
        "symbol": "USDC",
        "decimals": 6,
        "vToken": "0x5abd079783b72ef84935dbca1ea82d1687d76b5f0dd45a075cfcde47533f650",
        "version": 2
    },
    {
        "pool": "0x05c03e7e0ccfe79c634782388eb1e6ed4e8e2a013ab0fcc055140805e46261bd",
        "extension": "0x0",
        "asset": "0x02cab84694e1be6af2ce65b1ae28a76009e8ec99ec4bc17047386abf20cbb688",
        "name": "Yield BTC.B",
        "symbol": "YBTC.B",
        "decimals": 8,
        "vToken": "0x1a71039b15e5f5413ea450216387877adf962d5908811780c8f3dda5386b166",
        "version": 2
    }
]

# Fetch data for a specific market from Vesu singleton directly
async def get_market_info(market_info, provider):
    block = await provider.get_block_number()
    now = datetime.now()
    formatted_date = now.strftime("%Y-%m-%d")
    # singleton.asset_config_unsafe returns per-current block market data
    # - reserve: the remaining liquidity in the market
    # - total_nominal_debt: the total outstanding nominal (excluding accrued interest) debt
    # - last_rate_accumulator: the index tracking accrued interest per current block
    #
    # Notes:
    # - the pool (identified by POOL) does not support "recursive" supply/borrowing
    #   so supply = non-recursive-supply by default
    # - rate_accumulator is an interest accrual index that converts to users' total
    #   borrowed by multiplication with total_nominal_debt
    # - we add the raw rate_accumulator to the return values so 'get_stables_info' 
    #   can reuse, then drop the column of the DataFrame at the end again

    # deal with different versions of Vesu
    if market_info['version'] == 1:
        target_contract = await Contract.from_address(provider=provider, address=SINGLETON)
        asset_config = (await target_contract.functions["asset_config_unsafe"].call(
            int(market_info['pool'], base=16), int(market_info['asset'], base=16)))[0][0]
    else:
        target_contract = await Contract.from_address(provider=provider, address=market_info['pool'])
        asset_config = (await target_contract.functions["asset_config"].call(
            int(market_info['asset'], base=16)))[0]

    asset_scale = asset_config['scale']
    reserve = asset_config['reserve'] / asset_scale
    rate_accumulator = asset_config['last_rate_accumulator'] / SCALE
    total_borrowed = rate_accumulator * asset_config['total_nominal_debt']  / SCALE
    total_supplied = reserve + total_borrowed
    # lending_index_rate is fetched from the pool's vTokens directly
    lending_index_rate = (await asyncio.gather(get_index(market_info, provider)))[0]
    return {
        "protocol": "Vesu",
        "date": formatted_date,
        "market": market_info['asset'],
        "pool_id": market_info['pool'],
        "tokenSymbol": market_info['symbol'],
        "supply_token": total_supplied,
        "borrow_token": total_borrowed,
        "net_supply_token": reserve,
        "non_recursive_supply_token": total_supplied,
        "block_height": block,
        "lending_index_rate": lending_index_rate / asset_scale,
        "rate_accumulator": rate_accumulator
    }

# Fetch data for "same-asset-pairs" combined: STB and NYB
# The extension contract tracks total debt and supplied liquidity per "lending pair".
# We thus clean total supply from recursive supply/borrowing by deducting the debt from
# the recursive lending pairs (e.g. USDC/USDT, USDT/USDC, etc.)
async def get_stables_info(markets, results_markets, same_assets, same_assets_name, same_assets_addy, provider):
    df_markets = pd.DataFrame(results_markets)
    coroutines = [get_pair_info(pair_info, provider)
                      for pair_info in permutations(markets, 2) 
                      if pair_info[0]['symbol'] in same_assets
                      and pair_info[1]['symbol'] in same_assets
                      and pair_info[0]['pool'] == pair_info[1]['pool']]
    results_pairs = await asyncio.gather(*coroutines)
    # Need to handle case where no recursive pairs exist (eg currently only wBTC asset)
    if len(results_pairs) > 0:
        df_pairs = pd.DataFrame(results_pairs).groupby('asset').sum()
    
    coroutines = [get_price(market_info, provider) 
                  for market_info in markets 
                  if market_info['symbol'] in same_assets]
    results_prices = await asyncio.gather(*coroutines)
    df_prices = pd.DataFrame(results_prices).groupby('asset').mean().reset_index()
    total_supply = 0
    total_borrow = 0
    total_non_recursive_supplied = 0
    for asset in df_prices.asset:
        supply = df_markets.query('market == @asset').supply_token.sum()
        borrow = df_markets.query('market == @asset').borrow_token.sum()
        # Need to handle case where no recursive pairs exist (eg currently only wBTC asset)
        if len(results_pairs) > 0:
            recursive_borrow = (df_markets.query('market == @asset').rate_accumulator.iloc[0] * 
                df_pairs.query('asset == @asset').recursive_nominal_debt.iloc[0])
        else:
            recursive_borrow = 0
        
        non_recursive_supplied = supply - recursive_borrow
        price = df_prices.query('asset == @asset').price.iloc[0]
        total_supply += price * supply
        total_borrow += price * borrow
        total_non_recursive_supplied += price * max(0, non_recursive_supplied)
    
    # Convert to BTC for non-recursive BTC supply (NYB)
    if same_assets_name == "NYB":
        conversion_price = df_prices.query('asset == "0x03fe2b97c1fd336e750087d68b9b867997fd64a2661ff3ca5a7c771641e8e7ac"').price.iloc[0]
        total_supply = total_supply / conversion_price
        total_borrow = total_borrow / conversion_price
        total_non_recursive_supplied = total_non_recursive_supplied / conversion_price
    
    return {
        "protocol": "Vesu",
        "date": df_markets.date[0],
        "market": same_assets_addy,
        "pool_id": df_markets.pool_id[0],
        "tokenSymbol": same_assets_name,
        "supply_token": total_supply,
        "borrow_token": total_borrow,
        "net_supply_token": total_supply - total_borrow,
        "non_recursive_supply_token": total_non_recursive_supplied,
        "block_height": df_markets.block_height[0],
        "lending_index_rate": 1.0
    }

# Fetch total supply and debt for a specific lending pair
async def get_pair_info(pair_info, provider):
    collateral_asset = pair_info[0]['asset']
    debt_asset = pair_info[1]['asset']

    # deal with different versions of Vesu
    if pair_info[0]['version'] == 1:
        target_contract =  await Contract.from_address(provider=provider, address=pair_info[0]['extension'])
        pair_info = (await target_contract.functions['pairs'].call(
            int(pair_info[0]['pool'], base=16), int(collateral_asset, base=16), int(debt_asset, base=16)))[0]
    else:
        target_contract =  await Contract.from_address(provider=provider, address=pair_info[0]['pool'])
        pair_info = (await target_contract.functions['pairs'].call(
            int(collateral_asset, base=16), int(debt_asset, base=16)))[0]
    
    return {
        "asset": debt_asset,
        "recursive_nominal_debt": pair_info['total_nominal_debt'] / SCALE
    }

# Fetch the oracle price for a specific asset (pulls from Pragma)
async def get_price(market_info, provider):
    asset = market_info['asset']
    oracle_contract = await Contract.from_address(provider=provider, address=ORACLE)
    asset_price = (await oracle_contract.functions['price'].call(int(asset, base=16)))[0]['value']

    #target_contract = await Contract.from_address(provider=provider, address=market_info['extension'])
    #asset_price = (await target_contract.functions['price'].call(
    #    int(market_info['pool'], base=16), int(asset, base=16)))[0]['value']
    
    return {
        "asset": asset,
        "price": asset_price / SCALE
    }

# Fetch the lending index rate (pulls from vToken)
async def get_index(market_info, provider):
    vToken_contract = await Contract.from_address(provider=provider, address=market_info['vToken'])
    index = (await vToken_contract.functions['convert_to_assets'].call(
        int(SCALE)))[0]
    return index

async def main():
    """
    Supply your calculation here according to the Guidelines.
    """
    provider = FullNodeClient(node_url=NODE_URL)
    # Fetch individual markets
    coroutines = [get_market_info(market_info, provider)
                    for market_info in MARKETS 
                    if market_info['symbol'] in ELIGIBLE]
    results_markets = await asyncio.gather(*coroutines)
    # Fetch aggregated, non-recursive stables
    results_stb = await get_stables_info(MARKETS, results_markets, STABLES, "STB", "0x0stable", provider)
    results_markets.append(results_stb)
    # Fetch aggregated, non-recursive bitcoin
    results_nyb = await get_stables_info(MARKETS, results_markets, NYB, "NYB", "0x0nybbtc", provider)
    results_markets.append(results_nyb)
    df = pd.DataFrame(results_markets)
    df.drop('rate_accumulator', axis=1, inplace=True)
    print(df.to_string())
    return df

if __name__ == "__main__":
    asyncio.run(main())
