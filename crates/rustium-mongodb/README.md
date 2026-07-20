# rustium-mongodb

MongoDB Change Streams source connector for Rustium.

## English

The connector uses the official Rust MongoDB driver and persists the opaque
Change Stream resume token in the Rustium checkpoint. Replica sets or sharded
clusters are required by MongoDB for Change Streams. updateLookup and
pre-images are opt-in through the source configuration.

## 中文

连接器使用官方 Rust MongoDB 驱动，并将 Change Stream 的不透明 resume token
持久化到 Rustium checkpoint。MongoDB 必须运行副本集或分片集群才能使用 Change
Streams；updateLookup 与 pre-image 通过 source 配置启用。
