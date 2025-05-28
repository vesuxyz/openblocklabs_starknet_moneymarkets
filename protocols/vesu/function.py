import asyncio
from starknet_py.contract import Contract
from starknet_py.net.full_node_client import FullNodeClient
from datetime import datetime
import pandas as pd
from itertools import permutations

# Configs
# Make sure to add your Nethermind RPC key below or switch to a different provider
RPC_KEY = ""
NODE_URL = f"https://rpc.nethermind.io/mainnet-juno/?apikey={RPC_KEY}" #"https://starknet-mainnet.public.blastapi.io"
SINGLETON=0x000d8d6dfec4d33bfb6895de9f3852143a17c6f92fd2a21da3d6924d34870160 #0x2545b2e5d519fc230e9cd781046d3a64e092114f07e44771e0d719d148725ef
ELIGIBLE = ["STRK", "ETH", "USDC", "USDT", "xSTRK", "wstETH", "WBTC"]
STABLES = ["USDC", "USDT"]
NYB = ["WBTC"]
SCALE = 10**18
# Note: The extension and vToken addresses are based on the Vesu V2 contract deployment
MARKETS = [
    { # Genesis pool
        "pool": "0x4dc4f0ca6ea4961e4c8373265bfd5317678f4fe374d76f3fd7135f57763bf28",
        "extension": "0x4e06e04b8d624d039aa1c3ca8e0aa9e21dc1ccba1d88d0d650837159e0ee054",
        "asset": "0x049d36570d4e46f48e99674bd3fcc84644ddd6b96f7c741b1562b82f9e004dc7",
        "name": "Ether",
        "symbol": "ETH",
        "decimals": 18,
        "vToken": "0x0391bd9b58695b952aa15cffce50ba4650c954105df405ca8fc976ad7a65d646"
    }, 
    {
        "pool": "0x4dc4f0ca6ea4961e4c8373265bfd5317678f4fe374d76f3fd7135f57763bf28",
        "extension": "0x4e06e04b8d624d039aa1c3ca8e0aa9e21dc1ccba1d88d0d650837159e0ee054",
        "asset": "0x03fe2b97c1fd336e750087d68b9b867997fd64a2661ff3ca5a7c771641e8e7ac",
        "name": "Wrapped BTC",
        "symbol": "WBTC",
        "decimals": 8,
        "vToken": "0x00c452bacd439bab4e39aeea190b4ff81f44b019d4b3a25fa4da04a1cae7b6ff"
    },
    {
        "pool": "0x4dc4f0ca6ea4961e4c8373265bfd5317678f4fe374d76f3fd7135f57763bf28",
        "extension": "0x4e06e04b8d624d039aa1c3ca8e0aa9e21dc1ccba1d88d0d650837159e0ee054",
        "asset": "0x053c91253bc9682c04929ca02ed00b3e423f6710d2ee7e0d5ebb06f3ecf368a8",
        "name": "USD Coin",
        "symbol": "USDC",
        "decimals": 6,
        "vToken": "0x0227942991ea19a1843ed6d28af9458cf2566a3c2d6fccb2fd28f0424fce44b4"
    },
    {
        "pool": "0x4dc4f0ca6ea4961e4c8373265bfd5317678f4fe374d76f3fd7135f57763bf28",
        "extension": "0x4e06e04b8d624d039aa1c3ca8e0aa9e21dc1ccba1d88d0d650837159e0ee054",
        "asset": "0x068f5c6a61780768455de69077e07e89787839bf8166decfbf92b645209c0fb8",
        "name": "Tether USD",
        "symbol": "USDT",
        "decimals": 6,
        "vToken": "0x040e480d202b47eb9335c31fc328ecda216231425dae74f87d1a97e6e7901dce"
    },
    {
        "pool": "0x4dc4f0ca6ea4961e4c8373265bfd5317678f4fe374d76f3fd7135f57763bf28",
        "extension": "0x4e06e04b8d624d039aa1c3ca8e0aa9e21dc1ccba1d88d0d650837159e0ee054",
        "asset": "0x57912720381af14b0e5c87aa4718ed5e527eab60b3801ebf702ab09139e38b",
        "name": "Wrapped Staked Ether",
        "symbol": "wstETH",
        "decimals": 18,
        "vToken": "0x07cb1a46709214b94f51655be696a4ff6f9bdbbb6edb19418b6a55d190536048"
    },
    {
        "pool": "0x4dc4f0ca6ea4961e4c8373265bfd5317678f4fe374d76f3fd7135f57763bf28",
        "extension": "0x4e06e04b8d624d039aa1c3ca8e0aa9e21dc1ccba1d88d0d650837159e0ee054",
        "asset": "0x04718f5a0fc34cc1af16a1cdee98ffb20c31f5cd61d6ab07201858f4287c938d",
        "name": "Starknet Token",
        "symbol": "STRK",
        "decimals": 18,
        "vToken": "0x0147ae3337b168ac9abe80a7214f0cb9e874b25c3db530a8e04beb98a134e07a"
    },
    { # Re7 xSTRK pool
        "pool": "0x052fb52363939c3aa848f8f4ac28f0a51379f8d1b971d8444de25fbd77d8f161",
        "extension": "0x4e06e04b8d624d039aa1c3ca8e0aa9e21dc1ccba1d88d0d650837159e0ee054",
        "asset": "0x028d709c875c0ceac3dce7065bec5328186dc89fe254527084d1689910954b0a",
        "name": "Endur xSTRK",
        "symbol": "xSTRK",
        "decimals": 18,
        "vToken": "0x040f67320745980459615f4f3e7dd71002dbe6c68c8249c847c82dbe327b23cb"
    },
    {
        "pool": "0x052fb52363939c3aa848f8f4ac28f0a51379f8d1b971d8444de25fbd77d8f161",
        "extension": "0x4e06e04b8d624d039aa1c3ca8e0aa9e21dc1ccba1d88d0d650837159e0ee054",
        "asset": "0x04718f5a0Fc34cC1AF16A1cdee98fFB20C31f5cD61D6Ab07201858f4287c938D",
        "name": "Starknet Token",
        "symbol": "STRK",
        "decimals": 18,
        "vToken": "0x05f4c1bc95be3e8c234c633b239a8ec965b748230c9b04319688ca8012e034c3"
    },
    { # Re7 sSTRK pool
        "pool": "0x02e06b705191dbe90a3fbaad18bb005587548048b725116bff3104ca501673c1",
        "extension": "0x4e06e04b8d624d039aa1c3ca8e0aa9e21dc1ccba1d88d0d650837159e0ee054",
        "asset": "0x0356f304b154d29d2a8fe22f1cb9107a9b564a733cf6b4cc47fd121ac1af90c9",
        "name": "Staked Starknet Token",
        "symbol": "sSTRK",
        "decimals": 18,
        "vToken": "0x0644eb0e0807b7a929ce82dd53a9538b18782945e33080df17a3535d18278931"
    },
    {
        "pool": "0x02e06b705191dbe90a3fbaad18bb005587548048b725116bff3104ca501673c1",
        "extension": "0x4e06e04b8d624d039aa1c3ca8e0aa9e21dc1ccba1d88d0d650837159e0ee054",
        "asset": "0x04718f5a0fc34cc1af16a1cdee98ffb20c31f5cd61d6ab07201858f4287c938d",
        "name": "Starknet Token",
        "symbol": "STRK",
        "decimals": 18,
        "vToken": "0x027fb7238bce02c6633d04840bcfea53a1ec3bb135dc547c43c6a9fdf88e0969"
    },
    { # Re7 USDC pool
        "pool": "0x07f135b4df21183991e9ff88380c2686dd8634fd4b09bb2b5b14415ac006fe1d",
        "extension": "0x4e06e04b8d624d039aa1c3ca8e0aa9e21dc1ccba1d88d0d650837159e0ee054",
        "asset": "0x049d36570d4e46f48e99674bd3fcc84644ddd6b96f7c741b1562b82f9e004dc7",
        "name": "Ether",
        "symbol": "ETH",
        "decimals": 18,
        "vToken": "0x02ad74b0a40b2ee2a68ad3bec91a99e9d6a8690a079901d998a5473763917f7f"
    }, 
    {
        "pool": "0x07f135b4df21183991e9ff88380c2686dd8634fd4b09bb2b5b14415ac006fe1d",
        "extension": "0x4e06e04b8d624d039aa1c3ca8e0aa9e21dc1ccba1d88d0d650837159e0ee054",
        "asset": "0x03fe2b97c1fd336e750087d68b9b867997fd64a2661ff3ca5a7c771641e8e7ac",
        "name": "Wrapped BTC",
        "symbol": "WBTC",
        "decimals": 8,
        "vToken": "0x037f38969b64cfaae0c40cd1565dc7c61a0e6e7dd3da3709fcea2303755ae648"
    },
    {
        "pool": "0x07f135b4df21183991e9ff88380c2686dd8634fd4b09bb2b5b14415ac006fe1d",
        "extension": "0x4e06e04b8d624d039aa1c3ca8e0aa9e21dc1ccba1d88d0d650837159e0ee054",
        "asset": "0x053c91253bc9682c04929ca02ed00b3e423f6710d2ee7e0d5ebb06f3ecf368a8",
        "name": "USD Coin",
        "symbol": "USDC",
        "decimals": 6,
        "vToken": "0x079824ac0f81aa0e4483628c3365c09fa74d86650fadccb2a733284d3a0a8b85"
    },
    {
        "pool": "0x07f135b4df21183991e9ff88380c2686dd8634fd4b09bb2b5b14415ac006fe1d",
        "extension": "0x4e06e04b8d624d039aa1c3ca8e0aa9e21dc1ccba1d88d0d650837159e0ee054",
        "asset": "0x57912720381af14b0e5c87aa4718ed5e527eab60b3801ebf702ab09139e38b",
        "name": "Wrapped Staked Ether",
        "symbol": "wstETH",
        "decimals": 18,
        "vToken": "0x02d19cfe8e7a21306adc37d3f3be61699db07618b9175bc49cff1502d09ad253"
    },
    {
        "pool": "0x07f135b4df21183991e9ff88380c2686dd8634fd4b09bb2b5b14415ac006fe1d",
        "extension": "0x4e06e04b8d624d039aa1c3ca8e0aa9e21dc1ccba1d88d0d650837159e0ee054",
        "asset": "0x04718f5a0fc34cc1af16a1cdee98ffb20c31f5cd61d6ab07201858f4287c938d",
        "name": "Starknet Token",
        "symbol": "STRK",
        "decimals": 18,
        "vToken": "0x00dddeb3a7dec8d69a447aff8bc2f126d1b02814c341427dc5658e852f1d3524"
    },
    { # Re7 wstETH pool
        "pool": "0x59ae5a41c9ae05eae8d136ad3d7dc48e5a0947c10942b00091aeb7f42efabb7",
        "extension": "0x4e06e04b8d624d039aa1c3ca8e0aa9e21dc1ccba1d88d0d650837159e0ee054",
        "asset": "0x049d36570d4e46f48e99674bd3fcc84644ddd6b96f7c741b1562b82f9e004dc7",
        "name": "Ether",
        "symbol": "ETH",
        "decimals": 18,
        "vToken": "0x02ba0f8f2defa6986b50d861b720984185296e48faed2133ca14712ddc6aaaf1"
    }, 
    {
        "pool": "0x59ae5a41c9ae05eae8d136ad3d7dc48e5a0947c10942b00091aeb7f42efabb7",
        "extension": "0x4e06e04b8d624d039aa1c3ca8e0aa9e21dc1ccba1d88d0d650837159e0ee054",
        "asset": "0x57912720381af14b0e5c87aa4718ed5e527eab60b3801ebf702ab09139e38b",
        "name": "Wrapped Staked Ether",
        "symbol": "wstETH",
        "decimals": 18,
        "vToken": "0x008ccba554dd24f8346f439a9198d018c09dfb86a1ed652f6656644e3a5a500c"
    },
    { # Re7 Starknet Ecosystem
        "pool": "0x6febb313566c48e30614ddab092856a9ab35b80f359868ca69b2649ca5d148d",
        "extension": "0x4e06e04b8d624d039aa1c3ca8e0aa9e21dc1ccba1d88d0d650837159e0ee054",
        "asset": "0x053c91253bc9682c04929ca02ed00b3e423f6710d2ee7e0d5ebb06f3ecf368a8",
        "name": "USD Coin",
        "symbol": "USDC",
        "decimals": 6,
        "vToken": "0x048f4e75c12ca9d35d6172b1cb5f1f70b094888003f9c94fe19f12a67947fd6d"
    },
    {
        "pool": "0x6febb313566c48e30614ddab092856a9ab35b80f359868ca69b2649ca5d148d",
        "extension": "0x4e06e04b8d624d039aa1c3ca8e0aa9e21dc1ccba1d88d0d650837159e0ee054",
        "asset": "0x04718f5a0fc34cc1af16a1cdee98ffb20c31f5cd61d6ab07201858f4287c938d",
        "name": "Starknet Token",
        "symbol": "STRK",
        "decimals": 18,
        "vToken": "0x04228f2b404fa26a518d3ae4e8bb717f3e7f6d21ee5160517813a0eaec76e711"
    },
    { # Alterscope wstETH pool
        "pool": "0x5c678347b60b99b72f245399ba27900b5fc126af11f6637c04a193d508dda26",
        "extension": "0x4e06e04b8d624d039aa1c3ca8e0aa9e21dc1ccba1d88d0d650837159e0ee054",
        "asset": "0x57912720381af14b0e5c87aa4718ed5e527eab60b3801ebf702ab09139e38b",
        "name": "Wrapped Staked Ether",
        "symbol": "wstETH",
        "decimals": 18,
        "vToken": "0x0753bb8a6b65cb799316618e566bcec80260c6c18b47781468b567e9786dad49"
    },
    {
        "pool": "0x5c678347b60b99b72f245399ba27900b5fc126af11f6637c04a193d508dda26",
        "extension": "0x4e06e04b8d624d039aa1c3ca8e0aa9e21dc1ccba1d88d0d650837159e0ee054",
        "asset": "0x049d36570d4e46f48e99674bd3fcc84644ddd6b96f7c741b1562b82f9e004dc7",
        "name": "Ether",
        "symbol": "ETH",
        "decimals": 18,
        "vToken": "0x0008dd328c41b57422a5b9d8823c08882072a64ebd6bfab91a20fe2d43170f0d"
    }, 
    {
        "pool": "0x5c678347b60b99b72f245399ba27900b5fc126af11f6637c04a193d508dda26",
        "extension": "0x4e06e04b8d624d039aa1c3ca8e0aa9e21dc1ccba1d88d0d650837159e0ee054",
        "asset": "0x03fe2b97c1fd336e750087d68b9b867997fd64a2661ff3ca5a7c771641e8e7ac",
        "name": "Wrapped BTC",
        "symbol": "WBTC",
        "decimals": 8,
        "vToken": "0x011ab5b9bf6d21943339e89f0cc4e15c04a036f4d02be4d68db476d9ef6920e1"
    },
    {
        "pool": "0x5c678347b60b99b72f245399ba27900b5fc126af11f6637c04a193d508dda26",
        "extension": "0x4e06e04b8d624d039aa1c3ca8e0aa9e21dc1ccba1d88d0d650837159e0ee054",
        "asset": "0x053c91253bc9682c04929ca02ed00b3e423f6710d2ee7e0d5ebb06f3ecf368a8",
        "name": "USD Coin",
        "symbol": "USDC",
        "decimals": 6,
        "vToken": "0x02814990be52a1f8532d100f22cb26ad6aeda2928abc18480e409ef75df8ce84"
    },
    {
        "pool": "0x5c678347b60b99b72f245399ba27900b5fc126af11f6637c04a193d508dda26",
        "extension": "0x4e06e04b8d624d039aa1c3ca8e0aa9e21dc1ccba1d88d0d650837159e0ee054",
        "asset": "0x068f5c6a61780768455de69077e07e89787839bf8166decfbf92b645209c0fb8",
        "name": "Tether USD",
        "symbol": "USDT",
        "decimals": 6,
        "vToken": "0x05bda0367f4fb74471d0fcef1a645b1cba4d0f6ba9ca757e0cf3cbf997ac3999"
    },
    {
        "pool": "0x5c678347b60b99b72f245399ba27900b5fc126af11f6637c04a193d508dda26",
        "extension": "0x4e06e04b8d624d039aa1c3ca8e0aa9e21dc1ccba1d88d0d650837159e0ee054",
        "asset": "0x04718f5a0fc34cc1af16a1cdee98ffb20c31f5cd61d6ab07201858f4287c938d",
        "name": "Starknet Token",
        "symbol": "STRK",
        "decimals": 18,
        "vToken": "0x05974b310a4da473d0249353c7b26e8db5eb31fda66b583a7297aca8c9820a13"
    },
    { # Alterscope CASH pool
        "pool": "0x7bafdbd2939cc3f3526c587cb0092c0d9a93b07b9ced517873f7f6bf6c65563",
        "extension": "0x4e06e04b8d624d039aa1c3ca8e0aa9e21dc1ccba1d88d0d650837159e0ee054",
        "asset": "0x0498EDFaF50CA5855666a700C25Dd629D577EB9aFcCDf3B5977aEC79AEE55ADA",
        "name": "Cash",
        "symbol": "CASH",
        "decimals": 18,
        "vToken": "0x00597354f0c1f01fde571fee8bc32d6c5479171561eab28fdb30448b9ed9d3c9"
    },
    {
        "pool": "0x7bafdbd2939cc3f3526c587cb0092c0d9a93b07b9ced517873f7f6bf6c65563",
        "extension": "0x4e06e04b8d624d039aa1c3ca8e0aa9e21dc1ccba1d88d0d650837159e0ee054",
        "asset": "0x049d36570d4e46f48e99674bd3fcc84644ddd6b96f7c741b1562b82f9e004dc7",
        "name": "Ether",
        "symbol": "ETH",
        "decimals": 18,
        "vToken": "0x0115bd2016ba0d9a1a0075e943f8cc3098ea969baf5e57cde870865896cc9ca3"
    }, 
    {
        "pool": "0x7bafdbd2939cc3f3526c587cb0092c0d9a93b07b9ced517873f7f6bf6c65563",
        "extension": "0x4e06e04b8d624d039aa1c3ca8e0aa9e21dc1ccba1d88d0d650837159e0ee054",
        "asset": "0x03fe2b97c1fd336e750087d68b9b867997fd64a2661ff3ca5a7c771641e8e7ac",
        "name": "Wrapped BTC",
        "symbol": "WBTC",
        "decimals": 8,
        "vToken": "0x0628c6ee44401855310f6a6657cf151194ba61d1b66139c942860a2138fac521"
    },
    {
        "pool": "0x7bafdbd2939cc3f3526c587cb0092c0d9a93b07b9ced517873f7f6bf6c65563",
        "extension": "0x4e06e04b8d624d039aa1c3ca8e0aa9e21dc1ccba1d88d0d650837159e0ee054",
        "asset": "0x053c91253bc9682c04929ca02ed00b3e423f6710d2ee7e0d5ebb06f3ecf368a8",
        "name": "USD Coin",
        "symbol": "USDC",
        "decimals": 6,
        "vToken": "0x030902db47321a71202d4473a59b54db2b1ad11897a0328ead363db7e9dce4c8"
    },
    {
        "pool": "0x7bafdbd2939cc3f3526c587cb0092c0d9a93b07b9ced517873f7f6bf6c65563",
        "extension": "0x4e06e04b8d624d039aa1c3ca8e0aa9e21dc1ccba1d88d0d650837159e0ee054",
        "asset": "0x068f5c6a61780768455de69077e07e89787839bf8166decfbf92b645209c0fb8",
        "name": "Tether USD",
        "symbol": "USDT",
        "decimals": 6,
        "vToken": "0x04258ae715716d47880bcf05d20c1670474d9ea66c09b27883df20537e1eb91b"
    },
    {
        "pool": "0x7bafdbd2939cc3f3526c587cb0092c0d9a93b07b9ced517873f7f6bf6c65563",
        "extension": "0x4e06e04b8d624d039aa1c3ca8e0aa9e21dc1ccba1d88d0d650837159e0ee054",
        "asset": "0x04718f5a0fc34cc1af16a1cdee98ffb20c31f5cd61d6ab07201858f4287c938d",
        "name": "Starknet Token",
        "symbol": "STRK",
        "decimals": 18,
        "vToken": "0x0351efe34e732a283a35fa91bf52c0a3a6e89b0cc88ae32d4fde84e541b4fec2"
    },
    { # Alterscope xSTRK pool
        "pool": "0x27f2bb7fb0e232befc5aa865ee27ef82839d5fad3e6ec1de598d0fab438cb56",
        "extension": "0x4e06e04b8d624d039aa1c3ca8e0aa9e21dc1ccba1d88d0d650837159e0ee054",
        "asset": "0x028d709c875c0ceac3dce7065bec5328186dc89fe254527084d1689910954b0a",
        "name": "Endur xSTRK",
        "symbol": "xSTRK",
        "decimals": 18,
        "vToken": "0x020478f0a1b1ef010aa24104ba0e91bf60efcabed02026b75e1d68690809e453"
    },
    {
        "pool": "0x27f2bb7fb0e232befc5aa865ee27ef82839d5fad3e6ec1de598d0fab438cb56",
        "extension": "0x4e06e04b8d624d039aa1c3ca8e0aa9e21dc1ccba1d88d0d650837159e0ee054",
        "asset": "0x049d36570d4e46f48e99674bd3fcc84644ddd6b96f7c741b1562b82f9e004dc7",
        "name": "Ether",
        "symbol": "ETH",
        "decimals": 18,
        "vToken": "0x0530d3af399e4345ac3093c59715da3aacb8d9535c7ec50653573b32fbfbb7ad"
    }, 
    {
        "pool": "0x27f2bb7fb0e232befc5aa865ee27ef82839d5fad3e6ec1de598d0fab438cb56",
        "extension": "0x4e06e04b8d624d039aa1c3ca8e0aa9e21dc1ccba1d88d0d650837159e0ee054",
        "asset": "0x03fe2b97c1fd336e750087d68b9b867997fd64a2661ff3ca5a7c771641e8e7ac",
        "name": "Wrapped BTC",
        "symbol": "WBTC",
        "decimals": 8,
        "vToken": "0x000a570a7382c0eb80db2a2317d4c74bf307c866f673b0367e28a6dffcc288a9"
    },
    {
        "pool": "0x27f2bb7fb0e232befc5aa865ee27ef82839d5fad3e6ec1de598d0fab438cb56",
        "extension": "0x4e06e04b8d624d039aa1c3ca8e0aa9e21dc1ccba1d88d0d650837159e0ee054",
        "asset": "0x053c91253bc9682c04929ca02ed00b3e423f6710d2ee7e0d5ebb06f3ecf368a8",
        "name": "USD Coin",
        "symbol": "USDC",
        "decimals": 6,
        "vToken": "0x072803e813eb69d9aaea1c458ed779569c81bde0a2fc03ea2869876d13fa08d4"
    },
    {
        "pool": "0x27f2bb7fb0e232befc5aa865ee27ef82839d5fad3e6ec1de598d0fab438cb56",
        "extension": "0x4e06e04b8d624d039aa1c3ca8e0aa9e21dc1ccba1d88d0d650837159e0ee054",
        "asset": "0x068f5c6a61780768455de69077e07e89787839bf8166decfbf92b645209c0fb8",
        "name": "Tether USD",
        "symbol": "USDT",
        "decimals": 6,
        "vToken": "0x02d647a41926df2a438ade8daf9d3f0026d97290f82edbaab4012673c7f5d81b"
    },
    {
        "pool": "0x27f2bb7fb0e232befc5aa865ee27ef82839d5fad3e6ec1de598d0fab438cb56",
        "extension": "0x4e06e04b8d624d039aa1c3ca8e0aa9e21dc1ccba1d88d0d650837159e0ee054",
        "asset": "0x04718f5a0fc34cc1af16a1cdee98ffb20c31f5cd61d6ab07201858f4287c938d",
        "name": "Starknet Token",
        "symbol": "STRK",
        "decimals": 18,
        "vToken": "0x00427c4afb5a3733af903ca3cd4661a5ab52183b8bcdfe17f3899fc72984b181"
    },
    { # Alterscope Cornerstone pool
        "pool": "0x2906e07881acceff9e4ae4d9dacbcd4239217e5114001844529176e1f0982ec",
        "extension": "0x4e06e04b8d624d039aa1c3ca8e0aa9e21dc1ccba1d88d0d650837159e0ee054",
        "asset": "0x049d36570d4e46f48e99674bd3fcc84644ddd6b96f7c741b1562b82f9e004dc7",
        "name": "Ether",
        "symbol": "ETH",
        "decimals": 18,
        "vToken": "0x07edbd742a8804b21b39c9bb66552462b2d75ba0dc8e3f9a3b90004d7bb9721a"
    }, 
    {
        "pool": "0x2906e07881acceff9e4ae4d9dacbcd4239217e5114001844529176e1f0982ec",
        "extension": "0x4e06e04b8d624d039aa1c3ca8e0aa9e21dc1ccba1d88d0d650837159e0ee054",
        "asset": "0x03fe2b97c1fd336e750087d68b9b867997fd64a2661ff3ca5a7c771641e8e7ac",
        "name": "Wrapped BTC",
        "symbol": "WBTC",
        "decimals": 8,
        "vToken": "0x0090b7dd422a77e4daec4bf5ecfa612fe973fa76acd1679e2927b811a8b67480"
    },
    {
        "pool": "0x2906e07881acceff9e4ae4d9dacbcd4239217e5114001844529176e1f0982ec",
        "extension": "0x4e06e04b8d624d039aa1c3ca8e0aa9e21dc1ccba1d88d0d650837159e0ee054",
        "asset": "0x053c91253bc9682c04929ca02ed00b3e423f6710d2ee7e0d5ebb06f3ecf368a8",
        "name": "USD Coin",
        "symbol": "USDC",
        "decimals": 6,
        "vToken": "0x07b207a4f928f1b729c9ad26da5e99d6a13b3a23216c597ad545390048cd051f"
    },
    {
        "pool": "0x2906e07881acceff9e4ae4d9dacbcd4239217e5114001844529176e1f0982ec",
        "extension": "0x4e06e04b8d624d039aa1c3ca8e0aa9e21dc1ccba1d88d0d650837159e0ee054",
        "asset": "0x068f5c6a61780768455de69077e07e89787839bf8166decfbf92b645209c0fb8",
        "name": "Tether USD",
        "symbol": "USDT",
        "decimals": 6,
        "vToken": "0x025ca5e450d801e56b36f03147cd2c4cbaf515480002959321ed4ee807b59c97"
    },
    {
        "pool": "0x2906e07881acceff9e4ae4d9dacbcd4239217e5114001844529176e1f0982ec",
        "extension": "0x4e06e04b8d624d039aa1c3ca8e0aa9e21dc1ccba1d88d0d650837159e0ee054",
        "asset": "0x04718f5a0fc34cc1af16a1cdee98ffb20c31f5cd61d6ab07201858f4287c938d",
        "name": "Starknet Token",
        "symbol": "STRK",
        "decimals": 18,
        "vToken": "0x06682141475134b4c4c249cba0a7171126f66efff49a4b57e71acd8d49f670f5"
    },
    { # Re7 rUSDC pool
        "pool": "0x3de03fafe6120a3d21dc77e101de62e165b2cdfe84d12540853bd962b970f99",
        "extension": "0x04e06e04b8d624d039aa1c3ca8e0aa9e21dc1ccba1d88d0d650837159e0ee054",
        "asset": "0x049d36570d4e46f48e99674bd3fcc84644ddd6b96f7c741b1562b82f9e004dc7",
        "name": "Ether",
        "symbol": "ETH",
        "decimals": 18,
        "vToken": "0x004aeb34b4c27a165e836dd4543e8cfcf3dcc3ab8d8c6c6d178f963de502e612"
    }, 
    {
        "pool": "0x3de03fafe6120a3d21dc77e101de62e165b2cdfe84d12540853bd962b970f99",
        "extension": "0x04e06e04b8d624d039aa1c3ca8e0aa9e21dc1ccba1d88d0d650837159e0ee054",
        "asset": "0x03fe2b97c1fd336e750087d68b9b867997fd64a2661ff3ca5a7c771641e8e7ac",
        "name": "Wrapped BTC",
        "symbol": "WBTC",
        "decimals": 8,
        "vToken": "0x07f1027e02f240d57d552e53efe5aaea65d80dfcb52d75cb843fead08f6ce948"
    },
    {
        "pool": "0x3de03fafe6120a3d21dc77e101de62e165b2cdfe84d12540853bd962b970f99",
        "extension": "0x04e06e04b8d624d039aa1c3ca8e0aa9e21dc1ccba1d88d0d650837159e0ee054",
        "asset": "0x053c91253bc9682c04929ca02ed00b3e423f6710d2ee7e0d5ebb06f3ecf368a8",
        "name": "USD Coin",
        "symbol": "USDC",
        "decimals": 6,
        "vToken": "0x0150a0af5a972d0d0b4e6a87c21afe68f12dd4abcd7bc6f67cb49dbbec518238"
    },
    {
        "pool": "0x3de03fafe6120a3d21dc77e101de62e165b2cdfe84d12540853bd962b970f99",
        "extension": "0x04e06e04b8d624d039aa1c3ca8e0aa9e21dc1ccba1d88d0d650837159e0ee054",
        "asset": "0x57912720381af14b0e5c87aa4718ed5e527eab60b3801ebf702ab09139e38b",
        "name": "Wrapped Staked Ether",
        "symbol": "wstETH",
        "decimals": 18,
        "vToken": "0x06ab84857f7988d033dfa20f81feb4ba1403094111c046b0fde9b643535ff551"
    },
    {
        "pool": "0x3de03fafe6120a3d21dc77e101de62e165b2cdfe84d12540853bd962b970f99",
        "extension": "0x04e06e04b8d624d039aa1c3ca8e0aa9e21dc1ccba1d88d0d650837159e0ee054",
        "asset": "0x04718f5a0fc34cc1af16a1cdee98ffb20c31f5cd61d6ab07201858f4287c938d",
        "name": "Starknet Token",
        "symbol": "STRK",
        "decimals": 18,
        "vToken": "0x02cc368fe8f4075cee60013fe3ace1bb01ef861ba697321aab928b19a265db67"
    },
    {
        "pool": "0x3de03fafe6120a3d21dc77e101de62e165b2cdfe84d12540853bd962b970f99",
        "extension": "0x04e06e04b8d624d039aa1c3ca8e0aa9e21dc1ccba1d88d0d650837159e0ee054",
        "asset": "0x028d709c875c0ceac3dce7065bec5328186dc89fe254527084d1689910954b0a",
        "name": "Endur xSTRK",
        "symbol": "xSTRK",
        "decimals": 18,
        "vToken": "0x318761ecb936a2905306c371c7935d2a6a0fa24493ac7c87be3859a36e2563a"
    },
    {
        "pool": "0x3de03fafe6120a3d21dc77e101de62e165b2cdfe84d12540853bd962b970f99",
        "extension": "0x04e06e04b8d624d039aa1c3ca8e0aa9e21dc1ccba1d88d0d650837159e0ee054",
        "asset": "0x02019e47a0bc54ea6b4853c6123ffc8158ea3ae2af4166928b0de6e89f06de6c",
        "name": "Relend USDC",
        "symbol": "rUSDC",
        "decimals": 18,
        "vToken": "0x87d42b833c5e9f59269e23723510456f1d140b7554a24ab46e6fc8c00e8f24"
    }, # Braavos Pool
    {
        "pool": "0x43f475012ed51ff6967041fcb9bf28672c96541ab161253fc26105f4c3b2afe",
        "extension": "0x04e06e04b8d624d039aa1c3ca8e0aa9e21dc1ccba1d88d0d650837159e0ee054",
        "asset": "0x03fe2b97c1fd336e750087d68b9b867997fd64a2661ff3ca5a7c771641e8e7ac",
        "name": "Wrapped BTC",
        "symbol": "WBTC",
        "decimals": 8,
        "vToken": "0x6d507cf5c751a6569d3a10447aee58f9b1410bb6a7d9c52d22875cd5377b29"
    },
    {
        "pool": "0x43f475012ed51ff6967041fcb9bf28672c96541ab161253fc26105f4c3b2afe",
        "extension": "0x04e06e04b8d624d039aa1c3ca8e0aa9e21dc1ccba1d88d0d650837159e0ee054",
        "asset": "0x053c91253bc9682c04929ca02ed00b3e423f6710d2ee7e0d5ebb06f3ecf368a8",
        "name": "USD Coin",
        "symbol": "USDC",
        "decimals": 6,
        "vToken": "0x411587c1e5e6b09c9c1416efbebfe5adb5009ac99171b219caef4e59123b3ed"
    }
]

# Fetch data for a specific market from Vesu singleton directly
async def get_market_info(market_info, singleton_contract, provider):
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
    asset_config = (await singleton_contract.functions["asset_config_unsafe"].call(
        int(market_info['pool'], base=16), int(market_info['asset'], base=16)))[0][0]
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
    coroutines = [get_pair_info(pair_info, await Contract.from_address(provider=provider, address=pair_info[0]['extension']), pair_info[0]['pool'])
                      for pair_info in permutations(markets, 2) 
                      if pair_info[0]['symbol'] in same_assets
                      and pair_info[1]['symbol'] in same_assets
                      and pair_info[0]['pool'] == pair_info[1]['pool']]
    results_pairs = await asyncio.gather(*coroutines)
    # Need to handle case where no recursive pairs exist (eg currently only wBTC asset)
    if len(results_pairs) > 0:
        df_pairs = pd.DataFrame(results_pairs).groupby('asset').sum()
    
    coroutines = [get_price(market_info, await Contract.from_address(provider=provider, address=market_info['extension']), market_info['pool']) 
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
async def get_pair_info(pair_info, extension_contract, pool):
    collateral_asset = pair_info[0]['asset']
    debt_asset = pair_info[1]['asset']
    pair_info = (await extension_contract.functions['pairs'].call(
        int(pool, base=16), int(collateral_asset, base=16), int(debt_asset, base=16)))[0]
    return {
        "asset": debt_asset,
        "recursive_nominal_debt": pair_info['total_nominal_debt'] / SCALE
    }

# Fetch the oracle price for a specific asset (pulls from Pragma)
async def get_price(market_info, extension_contract, pool):
    asset = market_info['asset']
    asset_price = (await extension_contract.functions['price'].call(
        int(pool, base=16), int(asset, base=16)))[0]['value']
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
    singleton_contract = await Contract.from_address(provider=provider, address=SINGLETON)
    # Fetch individual markets
    coroutines = [get_market_info(market_info, singleton_contract, provider)
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
