# 已发布语料的审计与人工复核

`corpus_governance` 读取已有 READY 版本，生成独立的本地审计与复核产物。它不会重新清洗、删除正文、修改 train/validation 划分或覆盖已发布版本。原始 `corpus_pipeline` 入口继续保留；本说明使用独立治理入口完成跨集合统计与人工复核。

下面的 PowerShell 7 命令在项目根目录执行。需要 Docker Desktop Linux containers，以及数据卷中已存在的 READY 版本。示例版本是 `wiki-full-v2-20261102`；使用其他版本时替换该值。

```powershell
docker compose build cli
$releaseId = 'wiki-full-v2-20261102'
```

## 1. 生成近重复审计与复核队列

```powershell
docker compose run --rm --no-deps --entrypoint python cli -m corpus_governance audit-near-duplicates --release-id $releaseId
```

命令返回 `output_dir`。记录这次运行的完整目录，后续导出和汇总都使用同一目录，例如在 PowerShell 中粘贴实际输出：

```powershell
$nearAuditDir = Read-Host '粘贴近重复审计返回的 output_dir'
```

目录内包含：

| 文件 | 用途 |
|---|---|
| `summary.json` | 审计身份、来源版本、采样参数、候选总数、同集合与跨集合计数、待复核队列数 |
| `candidate_pairs/` | 本地候选 Parquet，含正文、两侧 split、`cross_split` 和稳定 `pair_id` |
| `manual_review.csv` | 最多 50 对候选的人工标签模板，不含正文 |
| `review_context.json` | 实际进入队列的正文对，以及可用的标题和来源 URL；按 `pair_id` 对照 CSV 阅读 |

审计在清洗后长度为 100–2000 字符的文章中，按 `SHA256(doc_id), doc_id` 排序选最多 2,000 篇。候选使用字符 5-gram、HashingTF、MinHashLSH，再计算原始 shingle 集合的 Jaccard。复核队列按 Jaccard 降序及文章 ID 排序，最多选 50 对，不代表全量候选都已复核。

`train_train_count + validation_validation_count + cross_split_count = candidate_count`。跨集合候选表示需要进一步判断的风险，不自动等于已确认泄漏。`review_queue_count` 是待复核队列大小，不是人工完成数；历史原始审计中的 `manual_review_count` 不再用于新治理汇总。

## 2. 生成质量规则的分层复核样本

```powershell
docker compose run --rm --no-deps --entrypoint python cli -m corpus_governance prepare-quality-review --release-id $releaseId --per-stratum 20
$qualityReviewDir = Read-Host '粘贴质量复核返回的 output_dir'
```

对下列 5 个风险标记分别取最多 20 条命中样本和 20 条未命中样本，每层按 `SHA256(doc_id), doc_id` 排序。某层不足 20 条时使用实际可用数量；`--per-stratum` 支持 1–100。

| 规则 | 人工判断的问题 |
|---|---|
| `short_text` | 文章是否缺少足以独立理解的上下文？短本身不算问题。 |
| `long_text` | 是否有大量填充或无关重复，影响正文使用？长本身不算问题。 |
| `repeated_lines` | 重复行是冗余噪声，还是列表、格式等合理结构？ |
| `low_chinese_ratio` | 是否不适合以中文为主的语料集合？中外文混排不自动算问题。 |
| `language_undetermined` | 缺少字母类内容是否使文章不适合作为语言训练文本？ |

`control_chars_removed` 记录已发生的处理事件，不作为待判定质量缺陷。

目录中 `quality_review.csv` 是标签模板，`review_context.json` 的 `rows` 与 `documents` 分别提供文章—规则关系和可读正文，`summary.json` 记录每层总体量、样本量与独立文章数。通过 `doc_id` 查阅正文，通过 `sample_id` 定位要标注的行。

同一篇文章可能出现在多个规则下，每行是一次“文章—规则”判断。不要把队列行数当作不同文章数量，也不要合并或删除重复出现的 `doc_id`。

## 3. 把工作包复制到本地并填写标签

下面使用已有的 `scheduler` 容器访问同一个数据卷；容器停止但仍存在时也可以 `docker cp`。先确认 `docker compose ps --all scheduler` 能列出该容器。本流程不要求为复核启动 Airflow 服务。

```powershell
$reviewContainer = docker compose ps --all --quiet scheduler
New-Item -ItemType Directory -Force -Path '.\data\reviews' | Out-Null
docker cp "${reviewContainer}:${nearAuditDir}" '.\data\reviews\'
docker cp "${reviewContainer}:${qualityReviewDir}" '.\data\reviews\'
$nearLocalDir = Join-Path '.\data\reviews' (Split-Path $nearAuditDir -Leaf)
$qualityLocalDir = Join-Path '.\data\reviews' (Split-Path $qualityReviewDir -Leaf)
```

本地目录分别与两次命令输出的目录名一致。用文本编辑器查看 `review_context.json`，编辑相邻 CSV。只填写以下 4 列：

| 列 | 填写要求 |
|---|---|
| `human_label` | 使用下表对应的标签；尚未复核留空 |
| `reviewer` | 实际复核人；填写标签后必须非空 |
| `reviewed_at` | 实际复核日期或时间，使用 ISO 格式，例如 `2026-11-02` 或 `2026-11-02T15:30:00+08:00` |
| `notes` | 具体判断依据，可留空 |

| 模板 | 正向结论 | 反向结论 | 不能确定 |
|---|---|---|---|
| `manual_review.csv` | `near_duplicate`：人工确认内容近重复 | `distinct`：内容有实质区别 | `uncertain` |
| `quality_review.csv` | `issue`：按该行规则的判断问题，确认存在缺陷 | `acceptable`：按该规则判断可接受 | `uncertain` |

保留所有列和所有行，不修改 release、配对／样本 ID、文章 ID、split、规则、flagged 或 Jaccard。保持这些元数据的原始文本；表格软件自动改变数字或日期格式可能导致校验拒绝。未完成的行保留空标签，不能删行。`uncertain` 是已做判断但无法定论，与尚未复核的空标签不同。

CSV、正文上下文和候选文件都留在本地 `data/` 或 Docker 数据卷；不要提交 Git。工具不会上传人工标签，也不会根据候选分数自动填写人工结论。

## 4. 导入标签并生成汇总

将填写后的 CSV 复制回同一审计／复核目录，使用新文件名保留最初的空模板：

```powershell
docker cp (Join-Path $nearLocalDir 'manual_review.csv') "${reviewContainer}:${nearAuditDir}/manual_review.labels.csv"
docker cp (Join-Path $qualityLocalDir 'quality_review.csv') "${reviewContainer}:${qualityReviewDir}/quality_review.labels.csv"

docker compose run --rm --no-deps --entrypoint python cli -m corpus_governance summarize-near-review --audit-dir $nearAuditDir --labels "$nearAuditDir/manual_review.labels.csv"
docker compose run --rm --no-deps --entrypoint python cli -m corpus_governance summarize-quality-review --review-dir $qualityReviewDir --labels "$qualityReviewDir/quality_review.labels.csv"
```

两条命令分别写入各自目录的 `review-summary.json`，并在终端打印汇总。`--labels` 可指向容器内另一份已填写 CSV；省略时读取该目录原来的模板文件。再次填写更多标签后，可用同一命令更新汇总，无需重新运行审计或抽样。

导入会核对上下文身份、模板字段、全部队列行和元数据。未知标签、缺少复核人／日期、重复或丢失行，以及身份字段被修改都会导致失败；应修正 CSV，不要用另一版本的模板替换上下文。

需要取回汇总时：

```powershell
docker cp "${reviewContainer}:${nearAuditDir}/review-summary.json" (Join-Path $nearLocalDir 'review-summary.json')
docker cp "${reviewContainer}:${qualityReviewDir}/review-summary.json" (Join-Path $qualityLocalDir 'review-summary.json')
```

## 5. 正确解释人工结果

- `pending_count` 是空标签数量；`reviewed_count` 包括明确结论与 `uncertain`；`adjudicated_count` 只包括正向和反向明确结论。
- 近重复汇总的 `positive_fraction_among_adjudicated` 为 `near_duplicate / (near_duplicate + distinct)`，另列跨集合队列的同口径结果。它是实际复核候选中的确认比例，不是全语料召回率。
- 质量汇总在每个规则、每个命中／未命中层分别计算 `issue / (issue + acceptable)`。`uncertain` 和未标注行不进入分母。
- 没有明确人工结论时，比例是 `null`，不能写成 0、准确率或已经完成的评估。只有 `uncertain` 标签时同样没有可计算的明确结论比例。
- 质量复核的总队列只统计进度与判断数量，不提供跨规则混合比例。各层有固定上限，而且同一文章可出现多次；分层问题比例不能直接当作全语料准确率、召回率或模型训练收益。

可对外展示经过核对且不含正文的聚合结果、采样范围、标注人数／进度和判断标准；保留未知与未完成项的真实状态。
