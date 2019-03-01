#!/usr/bin/python3
import ijson
from lami_analysis import LamiAnalysis


class KernelStatistics(LamiAnalysis):
    def __init__(self):
        super().__init__({
            "mi-version": {
                "major": 1,
                "minor": 0
            },
            "version": {
                "major": 1,
                "minor": 0,
                "patch": 0,
                "extra": "dev"
            },
            "title": "Kernel Statistics",
            "authors": [
                "Arnaud Fiorini"
            ],
            "description": "Provides statistics about the execution of kernels.",
            "url": "https://github.com/arfio/PythonTracingThings.git",
            "tags": [
                "gpu",
                "stats",
                "rocm"
            ],
            "table-classes": {
                "kernel-statistics": {
                    "title": "Kernel Execution Statistics",
                    "column-descriptions": [
                        {"title": "Kernel name", "type": "string"},
                        {"title": "Total Duration", "type": "duration", "unit": "ms"},
                        {"title": "Average Duration", "type": "duration", "unit": "ms"},
                        {"title": "Calls", "type": "number", "unit": ""},
                        {"title": "Percentage", "type": "ratio", "unit": "%"},
                    ]
                }
            }
        })

    def run_analysis(self):
        with open(self.args.trace) as json_trace:
            # Getting Kernel events and names
            events = ijson.items(json_trace, "traceEvents.item")
            kernel_events = [
                event
                for event in events
                if "args" in event and
                    "KernelName" in event["args"] and
                    int(event["args"]["BeginNs"]) > (self.args.begin or 0) and 
                    int(event["args"]["EndNs"]) < (self.args.end or self._LARGE_TIME)
            ]
            kernel_list = set([kernel_event["args"]["KernelName"] for kernel_event in kernel_events])
            kernel_statistics = {
                kernel: {
                    "Total Duration": 0.,
                    "Average Duration": 0.,
                    "Percentage": 0.,
                    "Calls": 0.
                } for kernel in kernel_list
            }
            kernel_time = 0.
            for kernel_event in kernel_events:
                kernel_time += float(kernel_event["args"]["DurationNs"])
                kernel_statistics[kernel_event["args"]["KernelName"]]["Total Duration"] += (
                    float(kernel_event["args"]["DurationNs"])
                )
                kernel_statistics[kernel_event["args"]["KernelName"]]["Calls"] += 1.
            
            for kernel in kernel_statistics:
                kernel_statistics[kernel]["Average Duration"] = (
                    kernel_statistics[kernel]["Total Duration"] / kernel_statistics[kernel]["Calls"]
                )
                kernel_statistics[kernel]["Percentage"] = 100 * (
                    kernel_statistics[kernel]["Total Duration"] / kernel_time
                )

            self.results = kernel_statistics

    def print_results(self):
        super().print_results(
            ["kernel-statistics"],
            [
                # kernel-statistics
                [
                    [
                        {"class": "string", "value": kernel},
                        {"class": "duration", "value": self.results[kernel]["Total Duration"]},
                        {"class": "duration", "value": self.results[kernel]["Average Duration"]},
                        {"class": "number", "value": int(self.results[kernel]["Calls"])},
                        {"class": "ratio", "value": self.results[kernel]["Percentage"]/100}
                    ]
                    for kernel in self.results
                ]
            ]
        )


if __name__ == "__main__":
    lami_analysis = KernelStatistics()
    lami_analysis.run_analysis()
    lami_analysis.print_results()
