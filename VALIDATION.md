# 实测记录

## 2026-11-02：recipe_v2 实测

本次固定来源 revision 为 `5428a5d9ebb8d68732fc1227ebc5e8d5cec28592`，完整处理 `20231101.zh` 六个分片。来源文件 SHA-256 与 v1 一致，原有 READY 版本在此次运行后保持不变；两项核验结果均记录在 [v2 发布与审计证据](evidence/2026-11-02/release-v2.json) 中。源码版本为 `41a698c`，发布 ID 为 `wiki-full-v2-20261102`。

环境：Python 3.11.15、PySpark 3.5.9、JDK 17、DuckDB 1.4.5、PyArrow 20.0.0、Airflow 3.3.1。Spark `local[2]`、driver heap 3 GiB，调度容器限制 2 CPU / 6 GiB；依赖版本和实现指纹保存在证据文件中。

### 功能验证

- 合同测试：[20 passed in 0.88s](evidence/2026-11-02/contracts.txt)。
- 新规则 Spark 聚焦测试：[1 passed in 8.88s](evidence/2026-11-02/v2-focused.txt)。
- 完整测试：[35 passed in 22.24s](evidence/2026-11-02/tests.txt)，使用真实 Spark/JVM。
- [独立临时目录 CLI 演示](evidence/2026-11-02/cli-demo.txt)：9 条输入，2 条隔离、1 条源重复、2 条正文精确重复，4 条保留，发布状态 READY。
- v2 镜像 `python -m pip check` 无依赖冲突。

### 全量调度与发布

[Airflow 运行证据](evidence/2026-11-02/airflow-success.json)：`verify_wiki_v2_full_20261102` 的 prepare、ingest、build、publish、audit 五阶段均首次执行成功，DAG 总时长 **524.17 秒**。清洗、构建、发布、审计任务分别为 **390.37 / 51.74 / 13.04 / 62.35 秒**。这是此次实际调度时长，包含该次运行条件，不是受控性能实验，也不用于与 v1 做性能提升比较。

发布目录为卷内 `releases/wiki-full-v2-20261102`，通过 READY 消费入口核验。

| 项目 | v2 实测记录数 |
|---|---:|
| 原始输入 | 1,384,748 |
| 隔离 | 0 |
| 有效／源唯一 | 1,384,748 |
| 源记录重复 | 0 |
| 正文精确重复 | 5,570 |
| 最终保留 | 1,379,178 |
| train | 1,309,840 |
| validation | 69,338 |

四级行数守恒、正文精确去重与内容哈希划分校验均通过。v2 的重复正文总数与 v1 相同，但有限宽度转换等规则改变了规范化正文及内容哈希；train/validation 数量已发生变化，不能把相同重复总数解释为规则没有生效。

### v2 质量标记与有限审计

下列计数均针对最终保留的代表文章，标记可以重叠，不统计全部输入，也不合并已移除文章的标记；这些规则没有触发文章删除。

| 标记 | 最终代表文章数 |
|---|---:|
| 短文本 | 226,801 |
| 长文本 | 14 |
| 重复行比例超过 50% | 1,447 |
| 实际清理过控制字符 | 50 |
| 汉字占 Unicode 字母字符比例低于 50% | 66,907 |
| 无字母字符，无法判断中文占比 | 1 |

从本次 READY 版本确定性抽取 **2,000 篇**，生成 **16 对** Jaccard ≥ 0.8 的待人工复核候选；人工标签尚未填写，不报告人工核验准确率。40 对合成扰动测试正例检出 **20/20**、负例误报 **0/20**，仅代表固定合成测试，不代表全语料语义召回率。审计没有修改已发布数据。

### v2 复现入口

按 [README](README.md) 构建镜像、运行聚焦和完整测试。检查本次现存发布：

```powershell
docker compose run --rm --no-deps cli inspect --release-id wiki-full-v2-20261102
```

在新的本地环境复现全量链路时，启动 Airflow 并触发 `wiki_corpus_release`，设置 `source_kind=wikimedia`、`scope=full`、`shards` 为六个固定文件名、`run_audit=true`，选择新的 release ID。源文件缺失时按固定 revision 下载，正文、候选文本对与运行数据留在本地数据卷。

## 2026-09-09：recipe_v1 历史实测

2026-11-02 已核对旧版 READY 清单与账本身份，并导出原审计和性能报告的聚合内容，见 [历史 v1 证据](evidence/2026-11-02/historical-v1.json)。下列性能、数量和候选结果来自旧规则；本轮没有重跑旧版性能实验。

数据与完整账本保存在 Docker 卷 `wiki-corpus-pipeline_corpus-data`。本文件只记录聚合结果，不包含 Wikipedia 原文。

### 功能与实际调度

- Python 3.11、Spark 3.5.9、JDK 17、DuckDB 1.4.5、Airflow 3.3.1；正式镜像 `python -m pip check` 无依赖冲突。
- 聚焦合同测试 **10 passed in 0.38s**；最终完整测试 **24 passed in 22.25s**，使用真实 Spark，包括发布恢复、跨批冲突、精确去重、确定性抽样和合成 MinHash 测试。
- 固定来源 revision `5428a5d9ebb8d68732fc1227ebc5e8d5cec28592`，实际完成 10k、100k 和全六分片 Airflow 运行。
- 初次全量运行在官方下载阶段遇到 CDN 连接拒绝；保留完整下载文件，对剩余分片有限重试后恢复。没有切换来源、修改源文件或把失败当作成功。
- 成功全量运行 ID：`verify_wiki_full_retry_20260909`，五阶段均成功。六片清洗 134.18 秒、累计构建 65.89 秒、发布检查 12.42 秒、有限审计 74.15 秒。这些是本次 Airflow 任务时长，不是受控吞吐实验。

### 全量发布结果

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

### 有限近重复审计

- 从全量 READY 版本确定性抽取 2,000 篇，得到 16 对 Jaccard ≥ 0.8 的候选。
- 16 对进入 `manual_review.csv`，人工标签尚未填写；不报告人工核验准确率。
- 40 对合成扰动测试：正例检出 20/20、负例误报 0/20。这只说明固定合成测试结果，不能外推到全语料语义召回率。
- 审计未修改任何已发布数据。

审计目录：`staging/audit-wiki-full-20260909-2323f7b86ad3421aa900ae47cb50ad45`，其中保存 `summary.json`、候选 Parquet 与待复核清单。

### 100k 并行度实验

固定 `train-00002-of-00006.parquet`（126,796,358 字节），每次处理同一确定性 100,000 条输入；六次均保留 99,998 条。独立容器限制 2 CPU、6 GiB，driver heap 3 GiB；Airflow 与其他 Spark 服务已停止。

| Spark master | 三次端到端耗时（秒） | 中位数（秒） | 三次观测的最大进程树 RSS 之和 |
|---|---|---:|---:|
| local[1] | 24.8680 / 24.4943 / 25.3077 | 24.8680 | 3,111,567,360 字节 |
| local[2] | 25.5350 / 24.1196 / 23.5242 | 24.1196 | 2,931,073,024 字节 |

测量包含每次新 JVM 启动、清洗、去重、质量检查与发布 IO。先测 local[1] 三次，再测 local[2] 三次；OS 页缓存未清空，首段同机还有镜像构建活动。因此只将结果视为本机运行观测：两组耗时接近，**不能据此宣称并行度产生显著性能提升**。

内存按 Linux `/proc` 每 0.2 秒采样 worker 进程树 RSS 求和；可能重复计入共享页、漏掉瞬时峰值，不是整机物理内存峰值。六次最大观测约 2.90 GiB。

每次日志、结果与汇总：卷内 `staging/benchmark-adff30e72bf9438599f039c2dfa5adde/benchmark.json`。

### 历史版本复现

仅查询旧 READY 版本可以使用当前代码；重新运行下列 v1 测试或性能实验，应在 `baseline-v1` 源码上构建镜像，并将功能测试与性能实验分开运行：

```powershell
docker compose run --rm --no-deps --entrypoint python cli -m pytest -q tests/test_contracts.py
docker compose run --rm --no-deps --entrypoint python cli -m pytest -q
docker compose run --rm --no-deps cli inspect --release-id wiki-full-20260909
docker compose stop
docker compose run --rm --no-deps cli benchmark --input /opt/airflow/data/raw/5428a5d9ebb8d68732fc1227ebc5e8d5cec28592/20231101.zh/train-00002-of-00006.parquet
```

`stop` 保留数据卷。重新运行同一输入版本无需重复下载；规则或实现变更则按 README 使用新规则版本和发布 ID，不能覆盖旧版。
