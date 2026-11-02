# 实测记录：2026-09-09

数据与完整账本保存在 Docker 卷 `wiki-corpus-pipeline_corpus-data`。本文件只记录聚合结果，不包含 Wikipedia 原文。

## 功能与实际调度

- Python 3.11、Spark 3.5.9、JDK 17、DuckDB 1.4.5、Airflow 3.3.1；正式镜像 `python -m pip check` 无依赖冲突。
- 聚焦合同测试 **10 passed in 0.38s**；最终完整测试 **24 passed in 22.25s**，使用真实 Spark，包括发布恢复、跨批冲突、精确去重、确定性抽样和合成 MinHash 测试。
- 固定来源 revision `5428a5d9ebb8d68732fc1227ebc5e8d5cec28592`，实际完成 10k、100k 和全六分片 Airflow 运行。
- 初次全量运行在官方下载阶段遇到 CDN 连接拒绝；保留完整下载文件，对剩余分片有限重试后恢复。没有切换来源、修改源文件或把失败当作成功。
- 成功全量运行 ID：`verify_wiki_full_retry_20260909`，五阶段均成功。六片清洗 134.18 秒、累计构建 65.89 秒、发布检查 12.42 秒、有限审计 74.15 秒。这些是本次 Airflow 任务时长，不是受控吞吐实验。

## 全量发布结果

版本：`wiki-full-20260909`；卷内路径：`releases/wiki-full-20260909`。已通过 `read_ready()` 验证可消费。

| 项目 | 实测记录数 |
|---|---:|
| 原始输入 | 1,384,748 |
| 隔离 | 0 |
| 有效／源唯一 | 1,384,748 |
| 源记录重复 | 0 |
| 正文精确重复 | 5,570 |
| 最终保留 | 1,379,178 |
| train | 1,310,799 |
| validation | 68,379 |

四级行数守恒及精确重复集合隔离均通过。短文本标记 226,801、长文本标记 14、重复行标记 1,445；这些是规则标记，不代表已证实低质量，也没有因此删除文章。

单分片固定 100k 版本 `wiki-100k-20260909`：输入 100,000、精确重复 2、保留 99,998；train 95,003、validation 4,995。该样本不是全文随机样本。

## 有限近重复审计

- 从全量 READY 版本确定性抽取 2,000 篇，得到 16 对 Jaccard ≥ 0.8 的候选。
- 16 对进入 `manual_review.csv`，人工标签尚未填写；不报告人工核验准确率。
- 40 对合成扰动测试：正例检出 20/20、负例误报 0/20。这只说明固定合成测试结果，不能外推到全语料语义召回率。
- 审计未修改任何已发布数据。

审计目录：`staging/audit-wiki-full-20260909-2323f7b86ad3421aa900ae47cb50ad45`，其中保存 `summary.json`、候选 Parquet 与待复核清单。

## 100k 并行度实验

固定 `train-00002-of-00006.parquet`（126,796,358 字节），每次处理同一确定性 100,000 条输入；六次均保留 99,998 条。独立容器限制 2 CPU、6 GiB，driver heap 3 GiB；Airflow 与其他 Spark 服务已停止。

| Spark master | 三次端到端耗时（秒） | 中位数（秒） | 三次观测的最大进程树 RSS 之和 |
|---|---|---:|---:|
| local[1] | 24.8680 / 24.4943 / 25.3077 | 24.8680 | 3,111,567,360 字节 |
| local[2] | 25.5350 / 24.1196 / 23.5242 | 24.1196 | 2,931,073,024 字节 |

测量包含每次新 JVM 启动、清洗、去重、质量检查与发布 IO。先测 local[1] 三次，再测 local[2] 三次；OS 页缓存未清空，首段同机还有镜像构建活动。因此只将结果视为本机运行观测：两组耗时接近，**不能据此宣称并行度产生显著性能提升**。

内存按 Linux `/proc` 每 0.2 秒采样 worker 进程树 RSS 求和；可能重复计入共享页、漏掉瞬时峰值，不是整机物理内存峰值。六次最大观测约 2.90 GiB。

每次日志、结果与汇总：卷内 `staging/benchmark-adff30e72bf9438599f039c2dfa5adde/benchmark.json`。

## 复现

先按 [README](README.md) 构建镜像；功能测试与性能实验分开运行：

```powershell
docker compose run --rm --no-deps --entrypoint python cli -m pytest -q tests/test_contracts.py
docker compose run --rm --no-deps --entrypoint python cli -m pytest -q
docker compose run --rm --no-deps cli inspect --release-id wiki-full-20260909
docker compose stop
docker compose run --rm --no-deps cli benchmark --input /opt/airflow/data/raw/5428a5d9ebb8d68732fc1227ebc5e8d5cec28592/20231101.zh/train-00002-of-00006.parquet
```

`stop` 保留数据卷。重新运行同一输入版本无需重复下载；规则或实现变更则按 README 使用新规则版本和发布 ID，不能覆盖旧版。
