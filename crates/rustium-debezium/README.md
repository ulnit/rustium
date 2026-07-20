# rustium-debezium

Durable HTTP and Kafka adapter for Debezium database source engines.

## English

This crate integrates Debezium connectors whose CDC protocols depend on
proprietary clients, node-local commit logs, or distributed stream APIs. The
HTTP bridge delays its success response until Rustium has durably delivered and
checkpointed the event. The Kafka bridge commits consumer offsets only after
the same acknowledgement. This preserves Rustium's checkpoint ordering while
using Debezium's proven database-specific engine.

## 中文

该 crate 用于集成依赖专有客户端、节点本地 commit log 或分布式 stream API 的
Debezium 数据库连接器。HTTP bridge 仅在 Rustium 完成持久投递与 checkpoint 后
返回成功；Kafka bridge 同样只在确认后提交 consumer offset，从而在使用 Debezium
成熟数据库引擎的同时保留 Rustium 的 checkpoint 顺序契约。
