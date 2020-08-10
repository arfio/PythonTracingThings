import bt2
import os
import re
import sys
import time
from heapq import heappush, heappop


def parse_compute_kernel_hsa_line(line):
    pass

def parse_hcc_ops_line(line):
    line_re = re.compile(r"(\d+):(\d+)\s(\d+):(\d+)\s(.+)")
    result = line_re.match(line)
    return (
        int(result.group(1)), # begin time
        int(result.group(2)), # end time
        {
            "name": result.group(5)
        }
    )

def parse_async_copy_line(line):
    line_re = re.compile(r"(\d+):(\d+)\s(\w+)")
    result = line_re.match(line)
    return (
        int(result.group(1)), # begin time
        int(result.group(2)), # end time
        {
            "name": result.group(3)
        }
    )

def parse_api_line(line):
    line_re = re.compile(r"(\d+):(\d+)\s(\d+):(\d+)\s(\w+)(.+)")
    result = line_re.match(line)
    return (
        int(result.group(1)), # begin time
        int(result.group(2)), # end time
        {
            "tid": int(result.group(3)),
            "name": result.group(5),
            "args": result.group(6)
        }
    )

def parse_roctx_line(line):
    line_re = re.compile(r"(\d+) (\d+):(\d+) (\d+):(.*)")
    result = line_re.match(line)
    return (
        int(result.group(1)), # begin time
        -1,
        {
            "pid": int(result.group(2)),
            "tid": int(result.group(3)),
            "cid": int(result.group(4)),
            "message": result.group(5),
        }
    )

event_types = {
    "compute_kernels_hsa": {
        "file_input": "results.txt",
        "parse_func": parse_compute_kernel_hsa_line,
        "fields": {}
    },
    "hcc_ops": {
        "file_input": "hcc_ops_trace.txt",
        "parse_func": parse_hcc_ops_line,
        "fields": { "name": "string" }
    },
    "async_copy": {
        "file_input": "async_copy_trace.txt",
        "parse_func": parse_async_copy_line,
        "fields": { "name": "string" }
    },
    "hsa_api": {
        "file_input": "hsa_api_trace.txt",
        "parse_func": parse_api_line,
        "fields": {
            "tid": "unsigned_integer",
            "name": "string",
            "args": "string",
        }
    },
    "hip_api": {
        "file_input": "hip_api_trace.txt",
        "parse_func": parse_api_line,
        "fields": {
            "tid": "unsigned_integer",
            "name": "string",
            "args": "string"
        }
    },
    "kfd_api": {
        "file_input": "kfd_api_trace.txt",
        "parse_func": parse_api_line,
        "fields": {
            "tid": "unsigned_integer",
            "name": "string",
            "args": "string" 
        }
    },
    "roctx": {
        "file_input": "roctx_trace.txt",
        "parse_func": parse_roctx_line,
        "fields": {
            "pid": "unsigned_integer",
            "tid": "unsigned_integer",
            "cid": "unsigned_integer",
            "message": "string"
        }
    },
}


def detect_input_files(input_path, event_types_detected={}):
    # input path can be a file or a directory
    if os.path.isfile(input_path):
        if input_path[:-4] == ".txt":
            event_types_detected["compute_kernels_hsa"] = {
                **event_types["compute_kernels_hsa"],
                "file_path": input_path
            }
        input_path = os.path.dirname(os.path.abspath(input_path))
    else:
        input_path = os.path.abspath(input_path)
    # add every valid filename to the list of input files
    for event_type in event_types:
        if event_type == "compute_kernels_hsa": continue
        file_path = os.path.join(input_path, event_types[event_type]["file_input"])
        if os.path.isfile(file_path):
            event_types_detected[event_type] = {
                **event_types[event_type],
                "file_path": file_path
            }
    return event_types_detected


def get_payload_class(fields, trace_class, payload_class):
    for field in fields:
        if fields[field] == "string":
            payload_class += [(field, trace_class.create_string_field_class())]
        elif fields[field] == "unsigned_integer":
            payload_class += [(field, trace_class.create_unsigned_integer_field_class())]


class RocmAPIMessageIterator(bt2._UserMessageIterator):
    def __init__(self, config, self_output_port):
        self._trace = self_output_port.user_data["trace"]
        self._event_type = self_output_port.user_data["event_type"]
        
        # Initializes the data objects for trace parsing
        self._stream = self._trace.create_stream(self._event_type["stream_class"])
        self._file = open(str(self._event_type["file_path"]), "r")

        # Because events are stored in a begin:end fashion, some end events occur after the
        # start of the next event. We store the event messages to keep the events ordered
        self._buffer = []
        self._size_buffer = 15000
        self._insert_buffer_begin_end()
        # heappush and heappop will compare against the first element of the tuple. In this case,
        # this element is the timestamp. However, when the timestamps are equal, it will compare against
        # the second element, so to manage this case, we put a rotating integer.
        self._integer = 0

    def _insert_buffer_begin_end(self):
        heappush(
            self._buffer,
            (0, 0, self._create_stream_beginning_message(self._stream))
        )
        heappush(
            self._buffer,
            (sys.maxsize, sys.maxsize, self._create_stream_end_message(self._stream))
        )

    def _parse_one_line(self, line):
        # Parsing the line to get payload and timestamp information
        (time_begin, time_end, fields) = self._event_type["parse_func"](line)
        # Create event message
        def fill_and_push_msg(time, fields, name_suffix):
            msg = self._create_event_message(
                self._event_type["event_class"],
                self._stream,
                default_clock_snapshot=time
            )
            for field in fields:
                if field == "name":
                    msg.event.payload_field[field] = fields[field] + name_suffix
                else:
                    msg.event.payload_field[field] = fields[field]
            heappush(self._buffer, (time, self._integer, msg))
            self._integer += 1
        
        fill_and_push_msg(time_begin, fields, "_enter")
        # Some events have no end time
        if time_end >= 0:
            fill_and_push_msg(time_end, fields, "_exit")

    def __next__(self):
        # Reading from the current event type file if the queue buffer is empty
        try:
            if len(self._buffer) < self._size_buffer:
                line = next(self._file)
            else:
                msg_send = heappop(self._buffer)[2]
                return msg_send
        except StopIteration:
            # Empty buffer
            while len(self._buffer) > 0:
                msg_send = heappop(self._buffer)[2]
                return msg_send
            self._file.close()
            raise StopIteration
        # Fill the buffer to its capacity
        while len(self._buffer) < self._size_buffer:
            self._parse_one_line(line)
        
        msg_send = heappop(self._buffer)[2]
        return msg_send


@bt2.plugin_component_class
class RocmSource(bt2._UserSourceComponent, message_iterator_class=RocmAPIMessageIterator):
    def __init__(self, config, params, obj):
        # Checks what types of event are available
        event_types_available = {}
        for input_path in params["inputs"]:
            # input_path type is no longer str due to bt2 implementation
            event_types_available = detect_input_files(str(input_path), event_types_available)
        
        # Event type not yet supported
        if "compute_kernels_hsa" in event_types_available.keys(): del event_types_available["compute_kernels_hsa"]

        # Initiliazes the metadata objects of the trace
        rocm_trace = self._create_trace_class()
        clock_class = self._create_clock_class(
            frequency=1000000000, # 1 GHz
            precision=1, # Nanosecond precision
            origin_is_unix_epoch=True
        )
        for event_type in event_types_available:
            # Stream classes
            event_types_available[event_type]["stream_class"] = (
                rocm_trace.create_stream_class(default_clock_class=clock_class)
            )
            # Field classes
            payload_class = rocm_trace.create_structure_field_class()
            event_types_available[event_type]["payload_class"] = payload_class
            get_payload_class(event_types_available[event_type]["fields"], rocm_trace, payload_class)
            # Event classes
            event_types_available[event_type]["event_class"] = (
                event_types_available[event_type]["stream_class"].create_event_class(
                    name=event_type,
                    payload_field_class=event_types_available[event_type]["payload_class"])
            )
        # Same trace object for all ports
        trace = rocm_trace()
        for event_type in event_types_available:
            self._add_output_port(
                "out_" + event_type,
                {
                    "trace": trace,
                    "event_type": event_types_available[event_type]
                }
            )


bt2.register_plugin(
    module_name=__name__,
    name="rocm",
    description="rocprofiler/roctracer format",
    author="Arnaud Fiorini"
)