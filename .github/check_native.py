"""Gate an explicit release binary against fixture, dev and generated CSV answers.

python check_native.py BINARY STARTER_DIR DEV_DIR DEV_ANSWER_DIR \
    --case GENERATED_INPUT ORACLE_OUTPUT [--case ...]
Requires the official starter's Python dependencies.
"""
import argparse
import os
from pathlib import Path
import subprocess
import sys
from tempfile import TemporaryDirectory


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    for name in ("binary", "starter", "dev", "answer"):
        parser.add_argument(name, type=Path)
    parser.add_argument("--case", action="append", nargs=2, type=Path, required=True)
    args = parser.parse_args()
    binary = args.binary.resolve(strict=True)
    sys.path.insert(0, str((args.starter / "scripts").resolve(strict=True)))
    from run_fixtures import SCHEMAS, load_actual, load_expected

    def check(data, answer, csv=False):
        with TemporaryDirectory() as out:
            env = dict(os.environ, DATA_DIR=str(data), OUTPUT_DIR=out)
            for key in ("WRITE_CSV", "PARQUET_WRITER"):
                env.pop(key, None)
            if csv:
                env["WRITE_CSV"] = "1"
            p = subprocess.run([binary], env=env, check=True, stdout=subprocess.PIPE, timeout=180)
            if p.stdout:
                raise ValueError(f"{data}: unexpected stdout")
            for table, schema in SCHEMAS.items():
                keys = list(schema)
                want = load_expected(answer / f"{table}.csv", schema).select(keys).sort(keys)
                if csv:
                    got = load_expected(Path(out) / f"{table}.csv", schema)
                else:
                    got = load_actual(Path(out) / table, schema)
                got = got.select(keys).sort(keys)
                if not got.equals(want):
                    raise ValueError(f"{data}: {table} differs from {answer}")

    fixtures = sorted(d for d in (args.starter / "fixtures").iterdir() if d.is_dir())
    if len(fixtures) != 36:
        raise ValueError(f"expected 36 official fixtures, found {len(fixtures)}")
    for csv, label in ((False, "Parquet"), (True, "CSV")):
        for d in fixtures:
            check(d / ("data" if (d / "data").is_dir() else "input"), d / "expected", csv)
        print(f"{label}: {len(fixtures)} fixtures MATCH", flush=True)
        check(args.dev, args.answer, csv)
        print(f"{label}: dev MATCH", flush=True)
        for data, answer in args.case:
            check(data, answer, csv)
        print(f"{label}: {len(args.case)} differential cases MATCH", flush=True)


if __name__ == "__main__":
    main()
