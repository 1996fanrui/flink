# 死锁预防：并发系统中的内存/资源隔离策略

## 1. 参考来源

| 编号 | 来源 | URL |
|------|------|-----|
| R1 | Wikipedia: Deadlock Prevention Algorithms | https://en.wikipedia.org/wiki/Deadlock_prevention_algorithms |
| R2 | Wikipedia: Deadlock (Computer Science) | https://en.wikipedia.org/wiki/Deadlock_(computer_science) |
| R3 | PostgreSQL Docs: Lock Management | https://www.postgresql.org/docs/current/runtime-config-locks.html |
| R4 | PostgreSQL Docs: Resource Consumption | https://www.postgresql.org/docs/current/runtime-config-resource.html |
| R5 | MySQL Docs: InnoDB Deadlocks | https://dev.mysql.com/doc/refman/8.0/en/innodb-deadlocks.html |
| R6 | MySQL Docs: InnoDB Deadlock Detection | https://dev.mysql.com/doc/refman/8.0/en/innodb-deadlock-detection.html |
| R7 | Flink Docs: Memory Setup | https://nightlies.apache.org/flink/flink-docs-stable/docs/deployment/memory/mem_setup/ |
| R8 | Flink Docs: Network Memory Tuning | https://nightlies.apache.org/flink/flink-docs-release-1.20/docs/deployment/memory/network_mem_tuning/ |

## 2. Coffman 条件与预防策略

死锁发生需同时满足四个条件（Coffman, 1971）：互斥、持有并等待、不可抢占、循环等待。

行业首选策略：**资源分区 + 资源排序**。
- 资源分区：将不同用途的资源分到物理隔离的池中，消除资源竞争前提条件
- 资源排序：强制按固定顺序获取资源，消除循环等待

## 3. 行业实践

### PostgreSQL：多级内存池隔离
- `shared_buffers`（页面缓存）、`work_mem`（查询操作）、`maintenance_work_mem`（维护）等 12+ 个独立池
- 按用途隔离、按会话隔离、固定上限

### MySQL InnoDB：多实例 Buffer Pool + 锁排序
- `innodb_buffer_pool_instances` 拆分为多个独立实例
- 所有事务按一致顺序访问表和行
- Wait-For Graph 死锁检测作为兜底

### Flink：严格的内存区域隔离
- Framework Heap / Task Heap / Network Buffers / Managed Memory 四区隔离
- Network Buffer 独立池，有独立最小/最大值约束
- Overdraft Buffer 机制：每个 Gate 默认 5 个额外缓冲区

## 4. 行业共识

1. **能分区就分区**：不同用途、不同生命周期的资源划分到不同池，直接消除资源竞争
2. **能排序就排序**：共享同一池时，强制消费者按固定顺序获取资源
3. **限制并发持有量**：通过数量限制或顺序处理，减少同一时刻持有的资源数量
4. **兜底检测不可少**：即使做了预防，仍应部署检测机制作为安全网

## 5. 方案对标验证

本设计方案（Heap/Pool 内存隔离 + 顺序处理）与行业实践高度一致：

| 设计要素 | 对应行业实践 | 打破的 Coffman 条件 |
|---------|------------|-------------------|
| Source Buffer 用 Heap，Filtered Buffer 用 Pool | PostgreSQL 多内存池、Flink 四区隔离 | 资源竞争 |
| 每 Gate 最多 5 个 Heap Buffer | Flink Overdraft Buffer 上限 | 持有并等待 |
| Gate 内按 Virtual Channel 顺序处理 | 数据库一致锁排序 | 循环等待 |

内存开销：5 × 32KB = 160KB/Gate，在 GB 级 Heap 中微不足道。与 Flink Overdraft Buffer 机制设计思路一致。
