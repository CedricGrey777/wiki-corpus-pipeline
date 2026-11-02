# 比较两个 READY 版本

`corpus_governance` 是独立的只读治理包。版本比较使用 DuckDB 扫描已发布 Parquet，不启动 Spark、不改变清洗规则、不修改正文或发布状态；原 `corpus_pipeline` 的源码指纹不因新增治理模块而变化。

在能够读取现有数据卷的容器内执行：

```bash
python -m corpus_governance --root /opt/airflow/data compare-releases --left-release-id wiki-full-20260909 --right-release-id wiki-full-v2-20261102
```

输出为可保存的聚合 JSON，包含两个发布 ID、对应 manifest identity 中的批次/规则/实现标识，以及下列比较结果。入口通过 `read_ready()` 查询两个版本；只有文件目录而没有 READY 登记的版本会被拒绝。

## 文章与划分口径

- `documents.left_count` / `right_count`：直接读取各版本已发布 Parquet 得到的代表文章数。
- `common_count` / `added_count` / `removed_count`：按 `doc_id` 连接后的交集、仅右版存在、仅左版存在的文章数。这些是发布代表文章集合的变化，不是源文章修订或删除历史；去重代表 ID 的变化也可能改变集合成员。
- `common_text_changed_count` / `common_text_unchanged_count`：仅比较共同 `doc_id` 的 `clean_text` 等值关系，不依赖两版总量相等或仅凭哈希判断。
- `common_split_transitions`：共同文章的四种实际迁移计数，分别为 train→train、train→validation、validation→train、validation→validation。`common_split_changed_count` 为两个跨集合方向之和，不包含新增/移除文章，也不是 train 或 validation 总数之差。

两个方向的迁移可以相互抵消，因此“训练集总量不变”并不意味着“没有文章迁移”。测试中的合成例子专门覆盖这一情形；合成测试不代表真实语料观测结果。

## 来源与质量指标

`source_comparison.same_source_revisions` 比较 manifest 中的 source kind、revision 和 snapshot；`same_source_inputs` 进一步比较来源文件名、记录的 SHA-256 与 scope。不同 recipe 可以比较，来源是否一致由结果明确报告，不作默认假设。此处比较的是发布时保存的元数据，不重新读取原始下载文件计算 SHA-256。

`reported_metric_deltas` 使用两版发布报告中的整数指标。`delta = right - left` 仅在两版都记录过该项时计算。未测指标的值和差值均为 `null`；例如 v1 没有中文占比标记，不能将未测当成零再声称 v2 新增了相应数量的语言问题。质量标记总数仍沿用各版本报告的最终代表文章口径，不能直接用于说明同一批文章质量改善。

输出不包含正文、文本对、标题、URL 或单篇文章 ID 列表，只保留聚合计数和关联发布来源的 manifest 标识。真实 v1→v2 结果应由上述命令产生，不以合成测试或两个 split 总数之差代替。

## 2026-11-02 的真实 v1→v2 比较

[聚合证据](../evidence/2026-11-02-governance/version-comparison.json) 比较 `wiki-full-20260909` 与 `wiki-full-v2-20261102`。两版 manifest 的来源 revision、来源文件 SHA-256 和 scope 均一致，recipe 不同；此处没有重新下载或重新散列原始文件。

两版均有 **1,379,178 篇**代表文章，全部 `doc_id` 对应，新增与移除均为 **0**。共同 ID 中 **1,364,883 篇正文发生变化**、**14,295 篇不变**，说明有限宽度等规则产生了实际文本差异，不代表已经证明质量提升。

| v1 集合 → v2 集合 | train | validation | v1 合计 |
|---|---:|---:|---:|
| train | 1,245,597 | 65,202 | 1,310,799 |
| validation | 64,243 | 4,136 | 68,379 |
| v2 合计 | 1,309,840 | 69,338 | 1,379,178 |

实际发生集合迁移的是 **65,202 + 64,243 = 129,445 篇**；validation 的净增加仅为 **65,202 − 64,243 = 959 篇**。后者是两个方向相抵后的总量变化，不能替代前者。该比较衡量规则升级对已发布正文和确定性划分的影响，不衡量模型效果、语言质量或泄漏修复率。
