# cita-cloud-etl
ETL for CITA-Cloud

ETL，全称 Extract-Transform-Load，它是将大量的原始数据经过提取（extract）、转换（transform）、加载（load）到目标存储数据仓库的过程。

本工具的作用是将CITA-Cloud链上的区块，交易，回执等信息从链上提取出来，转换成表格，写入数据仓库或者CSV文件。

## 设计

### 提取

提取CITA-Cloud链上信息有几种来源：
1. 通过节点RPC，需要集成CITA-CLoud的SDK。
2. 通过cloud-cli，调用cloud-cli子命令获取json格式的结果。
3. 通过S3共享存储。CITA-Cloud支持使用S3共享存储，可以直接读取其中的内容。

### 转换

从链上提取出来的信息为结构化信息，需要转换成表格，即一条记录一行，每行的字段都一样，每个字段尽量简单。

```JSON
# overlord
cldi> get block 863105
{
  "height": 863105,
  "prev_hash": "0xed0e726a289e0314931d830779badc76c5b975fe6c33e7e3b393669974a0ca0b",
  "proof": "0xf88c830d2b8180a0567beaeb37e6de1683c9716b84f607f56895eb563110da7fcfe4fd18433b29d9f864b86083a641900baa0062abfaa90b9c5db8741c154c9f36e02b051bbd8b77304d2f7bb4fb056cb0214ccaa97a74dfba8c9f1b02dc85b199de93aeb977d1c76762d2f7fce996d15001700f3e8e2cbec81677c5b6f5c259b012bd7f4788a2f8fb6c962081d0",
  "proposer": "0x928404a9b72168d138cca6719a740026a4fcd617",
  "state_root": "0xcbc3a451e94d0b5c5b744d320e880044eb29d6ad94ed21e693e7f2796b047bdd",
  "time": "2023-12-03 19:44:09 +08:00",
  "timestamp": 1701603849137,
  "transaction_root": "0xd5ff61a158f4a5d7e2d7d0d21d38ba672c8d85919fa2e8b9be30042c8d8472db",
  "tx_count": 1,
  "tx_hashes": [
    "0x0877fee622bb27c5ce5920e2b2953e1b65cd3f77777ea70833dd3de70e01703c"
  ],
  "version": 0
}

# raft
cldi> get block 833015
{
  "height": 833015,
  "prev_hash": "0x3e5f519b5b4f31dccaa7487d8afd52f3830f1b3dd2a8aa7c8fd014a65d3b3c2e",
  "proof": "0x",
  "proposer": "0x8083d21410d4df1bbf9fc184da8cf369c3dcd2b0",
  "state_root": "0x1ffb37f417fb1cab5cddfaac4e6f1ec0a6d21a842a233c156e05a6b6b33164ef",
  "time": "2023-07-19 14:19:20 +08:00",
  "timestamp": 1689747560785,
  "transaction_root": "0x1ab21d8355cfa17f8e61194831e81a8f22bec8c728fefb747ed035eb5082aa2b",
  "tx_count": 0,
  "tx_hashes": [],
  "version": 0
}

cldi> get block-hash 863105
0xeef541e05f1a47bdc3b529b05b061e76bdcabb7b73b1485e9460fae202de40d2

cldi> rpc parse-proof --consensus OVERLORD 0xf88c830d2b8180a0567beaeb37e6de1683c9716b84f607f56895eb563110da7fcfe4fd18433b29d9f864b86083a641900baa0062abfaa90b9c5db8741c154c9f36e02b051bbd8b77304d2f7bb4fb056cb0214ccaa97a74dfba8c9f1b02dc85b199de93aeb977d1c76762d2f7fce996d15001700f3e8e2cbec81677c5b6f5c259b012bd7f4788a2f8fb6c962081d0
{
  "address_bitmap": "1101",
  "height": 863105,
  "proposal_hash": "0x567beaeb37e6de1683c9716b84f607f56895eb563110da7fcfe4fd18433b29d9",
  "round": 0,
  "signature": "0x83a641900baa0062abfaa90b9c5db8741c154c9f36e02b051bbd8b77304d2f7bb4fb056cb0214ccaa97a74dfba8c9f1b02dc85b199de93aeb977d1c76762d2f7fce996d15001700f3e8e2cbec81677c5b6f5c259b012bd7f4788a2f8fb6c9620",
  "validators": [
    "*0x865bb0941877571820ea99f0e72398d6173655b7684a94598518b71d17a5f35f170a10c6a76a8df5c9c6113f8db4a3d9",
    "*0x925a72bceb7f5ebaaf0f50ab525a1967fc48bc113795ddad2ae6d472d073234cd68d9837375d808a0bc6d1aa16d4d6e5",
    "0x810a1bbbb3d300498f777502c1d62d0da074b849a24f77ae247f7d73c0978916c9cfd58dde6b8fc09a3d04cbd8f1cfb6",
    "*0x928404a9b72168d138cca6719a740026a4fcd617eea69bf243fb3e0f5b6a5b2d9ab83ccb8fac7e14a069ed3f1ac5618b"
  ]
}

cldi> get tx 0x0877fee622bb27c5ce5920e2b2953e1b65cd3f77777ea70833dd3de70e01703c
{
  "height": 863105,
  "index": 0,
  "transaction": {
    "transaction": {
      "chain_id": "0xd00da2a44f0b3d12111a7bfd71db25fd77a765493d6a4c9e66717d743e2ebbbe",
      "data": "0x007341dabd8a9f2b99a2ed9e539814395000000000014e78dadae1cff0c36781ff1db6f9663cefd89e171fce089cbd7c6765c684869f92d26b3c4ecbe94c8b64dedaccab5dbc206993fa11c3dd4dd636dd73dbae3fe5cd9eb45cf7c9851aaee9f3e7463f372f5e9ada929a93f9e500c4c0f6cdd7558c2d32223352a7b89ed57abf445dafe02aab98db198b03b7369bd63d6e68e6d52e59b0b6e1cf5987d63be9cced0d95a766bbcc58515554a06ddd901338f974fba427d1b741067e871a287a6485a9d9ddf4a7d34a979fd2d8b364e643dd2753cffd925c3d714e88dd9345dfd29a79b54b1748281dac3d20b5daeb8ef3f66abe0fb3d6e75e98b4263bffaeb829d7ccf4cd5e7bb94006fe821a38f1c64bf6cfa78e9cd56465afdc60b865ceaea0adf9db9d641b354e9b9e9bd11620dcccab5db620f73c4bcacbec9647ae296b7bef453d4bfe3ba367fa62ab5cc9bbb7da3c9f1d2adc0832f0ef0140000000ffff3005a30801",
      "nonce": "6821847339739185719",
      "quota": 26620,
      "to": "0xff00000000000000000000000000000000042069",
      "valid_until_block": 863199,
      "value": "0x0000000000000000000000000000000000000000000000000000000000000000",
      "version": 0
    },
    "transaction_hash": "0x0877fee622bb27c5ce5920e2b2953e1b65cd3f77777ea70833dd3de70e01703c",
    "witness": {
      "sender": "0xa8a1624d6e29e4d4ef9a5c905e4c0c189c786629",
      "signature": "0x88dd3bcac58290fa18502f35872ca7724781b4fa11399658ca3133a985381321222791c925a6111a11a460de27cba2772101ea15d6fa790c8295fabb67112ec25d7d82dd967847773332db42b49018aa6384fda61c367a4031e917097c290cdce0d49cadea2ce4b46fc7dea561729ec5c8f90134d3c7b0588bb26799887fcc10"
    }
  },
  "type": "Normal"
}

# UTXO
{
	"height": 106,
	"index": 0,
	"transaction": {
		"transaction": {
			"lock_id": 1005,
			"output": "0x00",
			"pre_tx_hash": "0x000000000000000000000000000000000000000000000000000000000000000000",
			"version": 0
		},
		"transaction_hash": "0x5b5aaa42b4312fb204a156d279e0f98805e62a98cdf9293c4d5f361fcefc3d88",
		"witnesses": [{
			"sender": "0x9bab5858df4a9e84ff3958884a01a4fce5e07edb",
			"signature": "0x5704b155328220bd1734849b5d0c6f5380d9e8eef33e6f1728b174bbe4b7ae465b46fc350d21c4baaf151591174aea5210b1c61775eb4307fe64946425c77a49ee4a509a1b6e864df8d84ee8a880a0567908709bbbcf17727fd6051cc5a1abbc3dadcf754248d7cd33c7c5cf8c8410b915422cb0e00d684752756870d1c11b6b"
		}]
	},
	"type": "Utxo"
}

cldi> get receipt 0x0877fee622bb27c5ce5920e2b2953e1b65cd3f77777ea70833dd3de70e01703c
{
  "block_hash": "0xeef541e05f1a47bdc3b529b05b061e76bdcabb7b73b1485e9460fae202de40d2",
  "block_number": 863105,
  "contract_addr": "0x0000000000000000000000000000000000000000",
  "cumulative_quota_used": "0x0000000000000000000000000000000000000000000000000000000000005208",
  "error_msg": "",
  "logs": [],
  "logs_bloom": "0x00000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000",
  "quota_used": "0x0000000000000000000000000000000000000000000000000000000000005208",
  "state_root": "0x26de213a6fef9ae8db594109dd7f867e923f265e39679e0af078bc7b73c9fc77",
  "tx_hash": "0x0877fee622bb27c5ce5920e2b2953e1b65cd3f77777ea70833dd3de70e01703c",
  "tx_index": 0
}

"logs": [
      {
        "address": "0xdac17f958d2ee523a2206206994597c13d831ec7",
        "topics": [
          "0xddf252ad1be2c89b69c2b068fc378daa952ba7f163c4a11628f55a4df523b3ef",
          "0x000000000000000000000000b8262c6a2dcabd92a77df1d5bd074afd07fc5829",
          "0x000000000000000000000000dac17f958d2ee523a2206206994597c13d831ec7"
        ],
        "data": "0x000000000000000000000000000000000000000000000000000000016512c902",
        "blockNumber": "0xd19505",
        "transactionHash": "0xae2a33da8396a6bc40e874b0f32b9967113a3dbf071ab1290c44c62d86873d36",
        "transactionIndex": "0x71",
        "blockHash": "0xb0d0e3b6c5e59b7b3e7e16701f6d6cb0c3c93487415b03839e88b3f7a241c528",
        "logIndex": "0xa0",
        "removed": false
      }
    ],
message Log {
  bytes address = 1;
  repeated bytes topics = 2;
  bytes data = 3;
  bytes block_hash = 4;
  uint64 block_number = 5;
  bytes transaction_hash = 6;
  uint64 transaction_index = 7;
  uint64 log_index = 8;
  uint64 transaction_log_index = 9;
}

cldi> get sc
{
  "admin": "0x9bab5858df4a9e84ff3958884a01a4fce5e07edb",
  "admin_pre_hash": "0x000000000000000000000000000000000000000000000000000000000000000000",
  "block_interval": 3,
  "block_interval_pre_hash": "0x000000000000000000000000000000000000000000000000000000000000000000",
  "block_limit": 100,
  "block_limit_pre_hash": "0x000000000000000000000000000000000000000000000000000000000000000000",
  "chain_id": "0xd00da2a44f0b3d12111a7bfd71db25fd77a765493d6a4c9e66717d743e2ebbbe",
  "chain_id_pre_hash": "0x000000000000000000000000000000000000000000000000000000000000000000",
  "emergency_brake": false,
  "emergency_brake_pre_hash": "0x000000000000000000000000000000000000000000000000000000000000000000",
  "quota_limit": 1073741824,
  "quota_limit_pre_hash": "0x000000000000000000000000000000000000000000000000000000000000000000",
  "validators": [
    "0x865bb0941877571820ea99f0e72398d6173655b7684a94598518b71d17a5f35f170a10c6a76a8df5c9c6113f8db4a3d9",
    "0x925a72bceb7f5ebaaf0f50ab525a1967fc48bc113795ddad2ae6d472d073234cd68d9837375d808a0bc6d1aa16d4d6e5",
    "0x810a1bbbb3d300498f777502c1d62d0da074b849a24f77ae247f7d73c0978916c9cfd58dde6b8fc09a3d04cbd8f1cfb6",
    "0x928404a9b72168d138cca6719a740026a4fcd617eea69bf243fb3e0f5b6a5b2d9ab83ccb8fac7e14a069ed3f1ac5618b"
  ],
  "validators_pre_hash": "0x000000000000000000000000000000000000000000000000000000000000000000",
  "version": 0,
  "version_pre_hash": "0x000000000000000000000000000000000000000000000000000000000000000000"
}
```

因此需要将数据分成独立的几张表格，相互之间通过一些字段进行关联：
1. 区块
  ```SQL
  CREATE TABLE IF NOT EXISTS blocks
  (
    `height` BIGINT INDEX,
    `block_hash` VARCHAR INDEX,
    `prev_hash` VARCHAR,
    -- proof begin
    `proposal_hash` VARCHAR,
    `round` INT,
    `signature` VARCHAR,
    `validators` VARCHAR,
    -- proof end
    `proposer` VARCHAR,
    `state_root` VARCHAR,
    `timestamp` BIGINT,
    `transaction_root` VARCHAR,
    `tx_count` INT,
    `version` INT,
  )
  ```
2. 交易
  正常交易
  ```SQL
  CREATE TABLE IF NOT EXISTS txs
  (
    `height` BIGINT INDEX,
    `index` INT,
    `tx_hash` VARCHAR INDEX,
    `data` VARCHAR,
    `nonce` VARCHAR,
    `quota` BIGINT,
    `to` VARCHAR,
    `valid_until_block` BIGINT,
    `value` VARCHAR,
    `version` INT,
    -- witness begin
    `sender` VARCHAR,
    `signature` VARCHAR,
    -- witness end
  )
  ```
    UTXO交易
  ```SQL
  CREATE TABLE IF NOT EXISTS utxos
  (
    `height` BIGINT INDEX,
    `index` INT,
    `tx_hash` VARCHAR INDEX,
    `lock_id` INT,
    `output` VARCHAR,
    `pre_tx_hash` VARCHAR,
    `version` INT,
    -- witness begin
    `sender` VARCHAR,
    `signature` VARCHAR,
    -- witness end
  )
  ```  
3. 回执
  ```SQL
  CREATE TABLE IF NOT EXISTS receipts
  (
    `height` BIGINT INDEX,
    `index` INT,
    `contract_addr` VARCHAR,
    `cumulative_quota_used` VARCHAR,
    `quota_used` VARCHAR,
    `error_msg` VARCHAR,
    `logs_bloom` VARCHAR,
    `tx_hash` VARCHAR INDEX,
  )
  ```
4. 事件
  ```SQL
  CREATE TABLE IF NOT EXISTS logs
  (
    `address` VARCHAR INDEX,
    `topics` VARCHAR,
    `data` VARCHAR,
    `height` BIGINT INDEX,
    `log_index` INT,
    `tx_log_index` INT,
    `tx_hash` VARCHAR INDEX,
  )
  ```
5. 其他
  ```SQL
  CREATE TABLE IF NOT EXISTS system_config
  (
    `height` BIGINT INDEX,
    `admin` VARCHAR,
    `block_interval` INT,
    `block_limit` INT,
    `chain_id` VARCHAR,
    `emergency_brake` BOOLEAN,
    `quota_limit` BIGINT,
    `validators` VARCHAR,
    `version` INT,
  )
  ```

### 加载

将转换之后的表格加载到数据仓库，或者本地CSV文件中。

### 配置

ETL 是数据应用过程中的一个数据流（pipeline）的处理技术，从上游获取数据，进行处理，然后流向下游。

因此配置的时候可以分为上游配置和下游配置。

上游配置即提取数据的来源，下游配置即处理后的数据保存到什么地方。

这样可以方便用户灵活组合。


