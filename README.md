1. The first update method, Subgraph
    - The starting point of the data processing flow.
    - Obtains foundational data from various DeFi protocols and blockchain networks about liquidity pools.
    - This data includes the addresses of the liquidity pools, their Total Value Locked (TVL), fees, and participating tokens.

2. The second update method, Factory: (Manually performed to increase coverage not reached)
    - After obtaining basic liquidity pool data, it interacts with specific protocol Factory contracts.
    - Factory contracts are used to create new liquidity pools or tokens, involving code for data handling or the creation of new pools.

3. The third update method, Coverage:
    - After obtaining the foundational data of liquidity pools and processing the logic related to Factory contracts, it's necessary to acquire insurance or coverage information about these pools.
    - The code involves risk assessment and the calculation of insurance coverage (covering the proportion of databases online, acting as a monitor).

4. The fourth update method, Ankr:
    - Finally, interacts with the Ankr API to obtain information about specific token holders.
    - This data is used to analyze token distribution, holder diversity, etc., which is very important for risk management and market analysis.


Each file includes Binance Smart Chain (BSC), Ethereum (ETH), and Polygon (Poly). These code modules are designed to handle data from different blockchain networks, each offering distinct advantages such as transaction speed, fees, security, or user base.

Each file contains logic for processing data from these three networks to:
- 1. Achieve multi-chain compatibility: allowing apps or services to operate on different blockchains to attract a broader user base.

- 2. Aggregate data: providing a comprehensive market view or conducting integrated analysis.

- 3. Diversify risk: operating across multiple networks to spread risk since it's less likely for all networks to face issues simultaneously.

Each file handling data from different networks aims to ensure the app or service can interact with these diverse blockchain networks, thereby fully leveraging each network's advantages.
