"""Benchmark the presidio text and dictionary masking operations (pandas).

Times, for each selected case:
  * text: MaskPresidio on the free-text column of the sample data
  * dict: MaskDictOperation on the JSON report column of the sample data
  * deep: MaskDictOperation on synthetic nested documents, with blank spaCy
    models to isolate the cost of walking the documents from the NLP cost

Example:
-------
    python scripts/benchmark_presidio.py
    python scripts/benchmark_presidio.py --cases deep --depth 10 --width 2 --rows 50
    python scripts/benchmark_presidio.py --models de=de_core_news_lg en=en_core_web_trf --batch-size 64

"""

import argparse
import json
import random
import statistics
import time
from collections.abc import Callable
from pathlib import Path

import pandas as pd
import spacy
from masking.mask.operations.operation_presidio import MaskPresidio
from masking.mask.operations.operation_presidio_dict import MaskDictOperation
from masking.utils.entity_detection import PresidioMultilingualAnalyzer

DATA = Path(__file__).resolve().parents[1] / "data"
SAMPLES = (("MaskingData_de.csv", "Beschrieb"), ("MaskingData_en.csv", "Description"))


def load_samples() -> tuple[list[str], list[str]]:
    texts, reports = [], []
    for file, text_col in SAMPLES:
        data = pd.read_csv(DATA / file)
        texts += data[text_col].dropna().unique().tolist()
        reports += data["Report"].dropna().unique().tolist()
    return texts, reports


def make_document(depth: int, width: int, rnd: random.Random) -> dict:
    if depth == 0:
        return (
            f"Visit {rnd.randint(0, 50)} with john.doe{rnd.randint(0, 5)}@example.com"
        )
    return {f"key{i}": make_document(depth - 1, width, rnd) for i in range(width)}


def measure(fn: Callable[[], object], repeat: int) -> dict:
    times = []
    for _ in range(repeat):
        start = time.perf_counter()
        fn()
        times.append(time.perf_counter() - start)
    return {"median": statistics.median(times), "min": min(times)}


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__.split("\n")[0])
    parser.add_argument("--cases", nargs="+", default=["text", "dict", "deep"])
    parser.add_argument(
        "--models",
        nargs="+",
        default=["de=de_core_news_lg", "en=en_core_web_trf"],
        help="language=spacy_model pairs",
    )
    parser.add_argument("--batch-size", type=int, default=32)
    parser.add_argument("--entities", nargs="+", default=["PERSON"])
    parser.add_argument("--depth", type=int, default=8)
    parser.add_argument("--width", type=int, default=3)
    parser.add_argument("--rows", type=int, default=50)
    parser.add_argument("--repeat", type=int, default=3)
    parser.add_argument("--output", type=Path, help="write the results as JSON")
    args = parser.parse_args()

    kwargs = {"pii_entities": args.entities, "batch_size": args.batch_size}
    results = {}

    if {"text", "dict"} & set(args.cases):
        models = dict(m.split("=", 1) for m in args.models)
        start = time.perf_counter()
        analyzer = PresidioMultilingualAnalyzer(models=models).analyzer
        print(f"models loaded in {time.perf_counter() - start:.1f}s: {models}")
        texts, reports = load_samples()

        if "text" in args.cases:
            op = MaskPresidio(col_name="t", analyzer=analyzer, **kwargs)
            results["text"] = measure(lambda: op(pd.Series(texts)), args.repeat)
            results["text"]["rows"] = len(texts)

        if "dict" in args.cases:
            op = MaskDictOperation(col_name="r", analyzer=analyzer, **kwargs)
            results["dict"] = measure(lambda: op(pd.Series(reports)), args.repeat)
            results["dict"]["rows"] = len(reports)

    if "deep" in args.cases:
        blank = {lang: spacy.blank(lang) for lang in ("de", "en")}
        analyzer = PresidioMultilingualAnalyzer(models=blank).analyzer
        rnd = random.Random(0)  # noqa: S311
        documents = [
            json.dumps(make_document(args.depth, args.width, rnd))
            for _ in range(args.rows)
        ]
        op = MaskDictOperation(
            col_name="r",
            analyzer=analyzer,
            **{**kwargs, "pii_entities": ["EMAIL_ADDRESS"]},
        )
        results["deep"] = measure(lambda: op(pd.Series(documents)), args.repeat)
        results["deep"].update(rows=args.rows, leaves_per_row=args.width**args.depth)

    for case, r in results.items():
        details = ", ".join(
            f"{k}={v}" for k, v in r.items() if k not in {"median", "min"}
        )
        print(
            f"{case:<5} median {r['median']:7.2f}s  min {r['min']:7.2f}s  ({details})"
        )

    if args.output:
        args.output.write_text(json.dumps(results, indent=2))


if __name__ == "__main__":
    main()
