"""CLI entry point for pipewatch."""

import argparse
import json
import sys

from pipewatch.collector import MetricCollector
from pipewatch.alerts import AlertEngine, AlertRule
from pipewatch.metrics import PipelineStatus
from pipewatch.reporter import Reporter


def build_default_engine() -> AlertEngine:
    rules = [
        AlertRule(name="high_error_rate", threshold=0.1, status=PipelineStatus.WARNING),
        AlertRule(name="critical_error_rate", threshold=0.25, status=PipelineStatus.CRITICAL),
    ]
    return AlertEngine(rules=rules)


def cmd_report(args, collector: MetricCollector, reporter: Reporter):
    report = reporter.generate(args.pipeline_id)
    if args.format == "json":
        print(json.dumps(report.to_dict(), indent=2))
    else:
        print(report.summary())


def cmd_list(args, collector: MetricCollector, reporter: Reporter):
    pipelines = collector.pipeline_ids()
    if not pipelines:
        print("No pipelines tracked.")
        return
    for pid in pipelines:
        report = reporter.generate(pid)
        print(f"{pid:30s} {report.status.value.upper():10s} error_rate={report.error_rate:.2%}")


def main(argv=None):
    parser = argparse.ArgumentParser(prog="pipewatch", description="Monitor ETL pipeline health.")
    sub = parser.add_subparsers(dest="command")

    report_p = sub.add_parser("report", help="Show report for a pipeline")
    report_p.add_argument("pipeline_id")
    report_p.add_argument("--format", choices=["text", "json"], default="text")

    sub.add_parser("list", help="List all tracked pipelines")

    args = parser.parse_args(argv)

    collector = MetricCollector()
    engine = build_default_engine()
    reporter = Reporter(collector=collector, alert_engine=engine)

    if args.command == "report":
        cmd_report(args, collector, reporter)
    elif args.command == "list":
        cmd_list(args, collector, reporter)
    else:
        parser.print_help()
        sys.exit(1)


if __name__ == "__main__":
    main()
