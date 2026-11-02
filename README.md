# 中文训练语料质量与版本发布流水线

一个独立、单机可复现的数据工程项目。借鉴 Data-Juicer 的处理与去重设计，自行实现文件批次、保守清洗、正文精确去重、质量账本、不可变版本发布和中断恢复。不运行完整 Data-Juicer，不依赖 Ray、GPU、模型 API、对象存储或爬虫。

**业务产物是可追溯的数据版本，不是聊天应用，也不等于已经证明提升了模型质量。** 本项目与其他项目使用独立目录、DuckDB、Docker 网络及数据卷。

真实六分片处理、测试与审计结果见 [实测记录](VALIDATION.md)。

## 运行环境

- Python 3.11、PySpark 3.5.9 / JDK 17、DuckDB 1.4.5、PyArrow 20.0.0。
- MinHash 审计使用 PySpark ML、NumPy 1.26.4、SciPy 1.13.1；没有模型训练。
- Airflow 3.3.1 LocalExecutor，PostgreSQL 仅保存 Airflow 元数据。业务清单和质量账本保存在 DuckDB。
- 16 GB 本机，单作业 `local[2]`、driver heap 从 3 GB 开始；Airflow 并发为 1，其他重型项目应停止后再运行。本项目数据预留 20 GB 磁盘。
- PowerShell 7 + Docker Desktop Linux containers。下面的命令在本项目目录执行。不需要在 Windows 全局安装 Java/Spark。

## 最短离线演示

下面只生成合成样本，不访问网络数据源、不调用 API。

```powershell
docker compose build
docker compose run --rm --no-deps cli run-demo
docker compose run --rm --no-deps cli inspect --release-id synthetic-r001
```

合成测试集预期：9 条输入，2 条隔离，1 条源记录重复，2 条跨文章精确重复，保留 4 篇。重复执行同一命令复用相同批次和已发布版本，不追加重复记录。

### 四个主业务命令

| 命令 | 输入 | 实际产物 |
|---|---|---|
| `ingest-clean` | 本地 Parquet 文件、source kind、scope | 清洗分区、隔离分区、CLEANED 批次 |
| `build-release` | release ID、批次 ID 列表 | 累计精确去重快照、重复映射、暂存 manifest、发布 context |
| `validate-publish` | release ID 对应的 context | 质量验证、不可变目录、DuckDB READY 登记 |
| `audit-near-duplicates` | 已登记 READY 的 release ID | 最多 2,000 篇的候选审计、最多 50 对人工复核清单、40 对合成测试结果 |

`download`、`make-fixtures`、`inspect`、`run-demo`、`benchmark` 是这四个阶段所需的下载、演示或验证辅助命令，不是新增业务功能。

分阶段合成演示：

```powershell
docker compose run --rm --no-deps cli make-fixtures
docker compose run --rm --no-deps cli ingest-clean --input /opt/airflow/data/fixtures/synthetic-00.parquet --source-kind synthetic
docker compose run --rm --no-deps cli ingest-clean --input /opt/airflow/data/fixtures/synthetic-01.parquet --source-kind synthetic
docker compose run --rm --no-deps cli build-release --release-id synthetic-r002 --batches synthetic-synthetic-00-full-recipe_v1 synthetic-synthetic-01-full-recipe_v1
docker compose run --rm --no-deps cli validate-publish --release-id synthetic-r002
docker compose run --rm --no-deps cli audit-near-duplicates --release-id synthetic-r002
```

## 固定公开数据：显式下载后再处理

来源为 [Wikimedia Wikipedia](https://huggingface.co/datasets/wikimedia/wikipedia) 的 `20231101.zh`；固定 Git revision：

`5428a5d9ebb8d68732fc1227ebc5e8d5cec28592`

[六分片目录](https://huggingface.co/datasets/wikimedia/wikipedia/tree/main/20231101.zh)中的文件名为 `train-00000-of-00006.parquet` 到 `train-00005-of-00006.parquet`。来源数据卡列出 1,384,748 行、约 1.72 GB 压缩下载；本项目的实际输入量、发布量与运行条件另见实测记录。先使用约 127 MB 的 `train-00002-of-00006.parquet`。

```powershell
docker compose run --rm --no-deps cli download --shards train-00002-of-00006.parquet
docker compose run --rm --no-deps cli ingest-clean --input /opt/airflow/data/raw/5428a5d9ebb8d68732fc1227ebc5e8d5cec28592/20231101.zh/train-00002-of-00006.parquet --scope 10k
docker compose run --rm --no-deps cli build-release --release-id wiki-zh-10k-r001 --batches wikimedia-train-00002-of-00006-10k-recipe_v1
docker compose run --rm --no-deps cli validate-publish --release-id wiki-zh-10k-r001
```

`10k`/`100k` 是**每个选定分片**按 `id,url,title,text` 的固定升序（null 在前）取前 N 行，不是随机抽样，也不代表全文主题分布。小样本迭代只选上述单分片；完整发布对六个分片都使用 `full`。六个文件是历史文件批次，不能称为真实实时增量或不同日期的文章更新。

下载固定 revision 的具体文件，仅将成功传输的 `.part` 改名。`ingest-clean` 记录文件 SHA-256；同批次输入文件变化会被拒绝。不可从临时网络失败推断数据质量。

### 数据许可与发布边界

核验日期：2026-09-09。HF 数据卡标注 CC BY-SA 3.0/GFDL；[Wikimedia 当前许可说明](https://dumps.wikimedia.org/legal.html)描述文本默认 CC BY-SA 4.0/GFDL，并提示例外。保留来源 URL、revision 与许可声明，不把代码许可证当作文本许可证。

本仓库**不提交原文、清洗后的全文、人工复核的文本对、下载文件或私有材料**。所有这些内容只保存在本项目数据卷。可公开代码、来源清单、合成测试样本及不含原文的聚合结果；另行分发真实全文前须按实际来源核对适用许可、署名与同方式共享条件。`data/`、Parquet/JSONL 等已列入本项目 `.gitignore`。

## 数据语义与边界

```text
固定源文件 → 分片清洗 / 隔离 → 累计精确去重 → 质量校验
                                            ↓
暂存目录 → 同文件系统 rename → DuckDB 事务登记 READY → 消费 / 审计
```

- 稳定文章 ID 为 `wikipedia:zh:<原始id>`。批次 ID 包含 source kind、分片名、scope、recipe version；manifest 另外锁定 source revision、文件 SHA-256 和 Python 实现指纹。
- 必填字段 `id/url/title/text` 必须为字符串。缺列或类型错误使任务失败；行级缺值或空白正文隔离。隔离记录保留原文在本地，不写入普通任务日志。
- 清洗只做 NFC、换行统一、删除行尾空白、压缩连续空白行。保留行首缩进、行内空格、繁简差异、大小写、标点与数学符号。
- 长度 `<100`、`>100000` 和重复非空行比例 `>0.5` 只作审计标记，**不删除文章**。重复行比例为 `(非空行数－不同非空行数)/非空行数`，空行忽略、非空行内容按清洗后原样比较；阈值是审计起点，不是经过模型效果验证的质量门槛。
- 同一文章 ID 的完全相同源记录计作源重复；同一 ID 的不同源记录阻止发布，不自动取最后一次写入。检查跨所选全部批次执行。
- 精确去重按 `(SHA-256, clean_text)` 分组，核对正文相等，而非仅相信哈希相等。每组保留字典序最小的文章 ID，保留移除文章到代表文章的映射。
- 内容哈希前 8 位转整数模 100，小于 5 的进入 validation，其余进入 train。比例是确定性交付规则，不保证小样本恰好 95/5。新增批次改变代表 ID 时，相同正文的 split 不变。
- 只对新增分片执行清洗；发布时对累计已清洗分区重建去重快照。**不是完全增量去重、跨快照 CDC，也没有文章修订或删除语义。**
- 相同 release ID 不允许对应不同输入/实现。规则或实现变更应升 `RECIPE` 版本、重清洗，并使用新 release ID；旧版本不覆盖。纯重跑不改规则。

### 发布恢复

文件与 DuckDB 不是一个分布式事务。发布先完成 Parquet 和 manifest、质量验证，再在同一文件系统将 staging 改名为不可变 release 目录，最后以 DuckDB 事务登记 `READY` 和质量指标。

消费者只能调用 `read_ready()` / `inspect` 查询 `READY` 清单，不能 glob `releases/` 或把 Spark `_SUCCESS` 当作业务成功。

- rename 前中断：旧 READY 不受影响，从该 context 重试。
- rename 后、数据库提交前中断：形成未登记目录；核对原 context 与 manifest 的身份一致、重新验证后补登记。
- 数据库提交后响应丢失：重试是幂等登记，不复制记录。
- 同 ID 的 manifest 身份不一致：拒绝，不覆盖、自动回退或猜测哪份数据正确。

处理未完成的 staging 目录不会被消费者读取。失败运行暂存用于诊断；没有自动递归清理命令。仅保留需要诊断的运行和已发布版本，避免占满 20 GB 开发磁盘预算。

### 最小账本

DuckDB保存 `batches`、`releases`、`quality_metrics`。正文、隔离记录与重复映射使用 Parquet。质量守恒：

1. 输入＝有效＋隔离。
2. 有效＝源唯一记录＋源重复。
3. 源唯一记录＝保留＋精确重复。
4. 保留＝train＋validation。

验证还检查文章 ID 唯一、无相等正文残留、内容哈希正确、重复映射代表存在、split 符合固定规则。精确重复隔离**不证明**没有近重复、语义泄漏或外部基准污染；没有开展模型质量和隐私脱敏效果评估。

## 有限近重复审计

从 READY 版本里选 100–2000 字符的正文，按文章 ID 的 SHA-256 排序取最多 2,000 篇。字符 5-gram → HashingTF → MinHashLSH（8表、seed42）生成候选，使用原始 shingle 集合重算 Jaccard，仅输出 `>=0.8` 的候选。

`candidate_pairs` 留在本地；`manual_review.csv` 至多50对，`human_label/notes`由候选人填写。阈值0.8是审计起点，不是自动删除规则。另有20对轻微扰动、20对无关文本的合成测试；合成检出结果与人工核验结果分开解释。没有测量全语料真实召回率，也不对已发布数据做任何近重复删除。

## Airflow：参数化的实际阶段

```powershell
docker compose up -d
```

本地界面：<http://localhost:18081>。DAG 为 `wiki_corpus_release`，默认手动触发、合成数据，无自动公开数据下载。参数：

- `source_kind`: `synthetic` 或显式选择 `wikimedia`。
- `shards`: 固定文件名列表，默认只选最小分片。
- `scope`: `10k` / `100k` / `full`。
- `release_id`: 此次不可变版本名。
- `run_audit`: 默认 false，主线通过后可选择 true。

任务依次调用真实 `prepare → ingest → build → publish → audit` 实现；仅用 XCom 传小清单，不传正文。单活动 DAG run、单活动任务、单 Spark JVM，DuckDB单写。IO失败可重试；数据规则错误重试不会自动修好，应先查明原因。

Compose中的固定凭据仅供这个不对外暴露的本地演示。不要部署到公网，不要把真实令牌写入源代码或日志。停止服务保留数据卷：

```powershell
docker compose stop
```

## 验证命令与证据

先聚焦，再完整；所有 Spark 集成测试启动真实 JVM，不用 stub 替代。

```powershell
docker compose run --rm --no-deps --entrypoint python cli -m pytest -q tests/test_contracts.py
docker compose run --rm --no-deps --entrypoint python cli -m pytest -q tests/test_pipeline.py -k "quality_and_exact_equality or publication_recovery or source_id_conflict"
docker compose run --rm --no-deps --entrypoint python cli -m pytest -q
```

覆盖：保留缩进/NFC、缺字段隔离、重复行审计不删除、哈希碰撞不能合并不同正文、跨批冲突、清洗复用、旧版本不变、改名/事务边界失败恢复、READY消费门槛、40对真实Spark MinHash合成测试、物理行序颠倒但10k样本不变。

固定10万条、`local[1]` / `local[2]`各三次的辅助性能实验：

```powershell
docker compose run --rm --no-deps cli benchmark --input /opt/airflow/data/raw/5428a5d9ebb8d68732fc1227ebc5e8d5cec28592/20231101.zh/train-00002-of-00006.parquet
```

每次使用新 worker/JVM和独立目录，测量清洗→构建→验证发布的端到端耗时；不清空操作系统页缓存，报告中明确包含 JVM 启动和全部IO。Linux `/proc` 每0.2秒采样 worker 进程树 RSS，可能重复计算共享页或错过瞬时峰值，**不称为整机峰值或精确容器内存**。报告写明实际行数；如果输入不足10万条，不把目标行数写成实际规模。无强制“快30%”指标。

源码只代表已实现的行为；来源总量、未运行的 benchmark、手工未标注的候选，不能写成已完成的个人成绩。对外简历数字应来自本机保存的验证结果。

## 参考与独立贡献

- [Data-Juicer 文本示例](https://github.com/datajuicer/data-juicer/tree/main/demos/process_cft_zh_data)：参考分析/处理/追踪的组织方式。
- [Data-Juicer 分布式去重工具](https://github.com/datajuicer/data-juicer/tree/main/tools/distributed_deduplication)：参考 PySpark/MinHash 的候选思路；不复制其默认64 GB配置或全局聚类/driver collect。
- [PySpark 3.5.9官方安装与依赖](https://spark.apache.org/docs/3.5.9/api/python/getting_started/install.html)。本项目PyArrow用于原生Parquet合成数据，不使用pandas-on-Spark/Arrow UDF。

独立贡献是固定输入合同、确定性清洗、正文等值验证、跨批质量对账、READY发布/恢复及可复现实验。框架原有算子、原始公开语料、未执行的规模实验不属于个人成果。
