import argparse
import json


class LamiAnalysis():
    def __init__(self, metadata):
        self._LARGE_TIME = 0x1000000000000
        self._metadata = metadata
        self._init_parser()
        self._parse_args()

    def _init_parser(self):
        self._parser = argparse.ArgumentParser()
        # LAMI version phase
        self._parser.add_argument("--mi-version", action="store_true")
        # Metadata phase
        self._parser.add_argument("--metadata", action="store_true")
        # Compatibility test phase
        self._parser.add_argument("--test-compatibility", type=str)
        # Results phase
        self._parser.add_argument("--begin", type=int)
        self._parser.add_argument("--end", type=int)
        self._parser.add_argument("trace", nargs='?')

    def _parse_args(self):
        self.args, _ = self._parser.parse_known_args()
        if self.args.mi_version:
            print("1.0")
            exit(0)
        if self.args.metadata:
            print(json.dumps(self._metadata))
            exit(0)
        if self.args.test_compatibility:
            exit(0)
        if not self.args.trace:
            print("No trace to analyze!")
            exit(-1)

    def run_analysis(self): raise NotImplementedError

    def print_results(self, tables, datas):
        print(json.dumps({"results": [
            {
                "time-range": {
                    "class": "time-range",
                    "begin": {
                        "class": "timestamp",
                        "value": self.args.begin or 0
                    },
                    "end": {
                        "class": "timestamp",
                        "value": self.args.end or self._LARGE_TIME
                    }
                },
                "class": table,
                "data": data
            } for table, data in zip(tables, datas)
        ]}, indent=2))
