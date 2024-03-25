1. The first update method, Subgraph
    - The starting point of the data processing flow.
    - Obtains foundational data from various DeFi protocols and blockchain networks about liquidity pools.
    - This data includes the addresses of the liquidity pools, their Total Value Locked (TVL), fees, and participating tokens.

2. The second update method, Factory: (Manually performed to increase coverage not reached)
    - After obtaining basic liquidity pool data, it interacts with specific protocol Factory contracts.
    - Factory contracts are used to create new liquidity pools or tokens, involving code for data handling or the creation of new pools.

3. Third update method, Coverage:
    - 在获取流动性池的基础数据并处理完Factory合约相关的逻辑后，需要获取关于这些池子的保险或覆盖信息.
    - code涉及到风险评估和保险覆盖的计算. (涵盖线上数数据库的比例, 起到monitor的效果)

4. 第四种更新方法Ankr:
    - 最后，需要与Ankr API交互，获取特定代币的持有者信息.
    - 这部分数据用于分析代币的分布、持有者的多样性等，对于风险管理和市场分析非常重要.


每个文件包含了bsc(Binance Smart Chain), eth(Ethereum), 和poly(Polygon). 这些代码模块被设计来处理来自不同区块链网络的数据, 不同的区块链网络提供了不同的优势，比如交易速度、费用、安全性或者用户基础. 

每个文件都包含处理这三个网络数据的逻辑：
- 1. 多链兼容性：为了让应用或服务可以在不同的区块链上运行，吸引更广泛的用户基础.

- 2. 数据聚合：提供一个全面的市场视图或进行综合分析.

- 3. 风险分散：在多个网络上操作可以分散风险，因为不太可能所有网络同时出现问题.

每个文件处理不同网络的数据可能是为了确保应用或服务可以与这些不同的区块链网络互动，从而充分利用每个网络的优势.
