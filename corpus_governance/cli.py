from __future__ import annotations

import argparse
import json
import os


def main(argv=None):
    parser = argparse.ArgumentParser(description="Audit and review existing READY releases; never modify released text")
    parser.add_argument("--root", default=os.environ.get("CORPUS_DATA_ROOT", "data"))
    commands = parser.add_subparsers(dest="command", required=True)
    audit = commands.add_parser("audit-near-duplicates")
    audit.add_argument("--release-id", required=True)
    near = commands.add_parser("summarize-near-review")
    near.add_argument("--audit-dir", required=True)
    near.add_argument("--labels")
    prepare = commands.add_parser("prepare-quality-review")
    prepare.add_argument("--release-id", required=True)
    prepare.add_argument("--per-stratum", type=int, default=20)
    quality = commands.add_parser("summarize-quality-review")
    quality.add_argument("--review-dir", required=True)
    quality.add_argument("--labels")
    comparison = commands.add_parser("compare-releases")
    comparison.add_argument("--left-release-id", required=True)
    comparison.add_argument("--right-release-id", required=True)
    args = parser.parse_args(argv)
    if args.command == "audit-near-duplicates":
        from .audit import audit_near_duplicates
        result = audit_near_duplicates(args.root, args.release_id)
    elif args.command == "summarize-near-review":
        from .review import summarize_near_review
        result = summarize_near_review(args.audit_dir, args.labels)
    elif args.command == "prepare-quality-review":
        from .quality import prepare_quality_review
        result = prepare_quality_review(args.root, args.release_id, args.per_stratum)
    elif args.command == "summarize-quality-review":
        from .quality import summarize_quality_review
        result = summarize_quality_review(args.review_dir, args.labels)
    else:
        from .comparison import compare_releases
        result = compare_releases(args.root, args.left_release_id, args.right_release_id)
    print(json.dumps(result, ensure_ascii=False, indent=2))
    return 0
