import csv
from datetime import datetime, timezone
from typing import Protocol, List, Union


class MetricStorage(Protocol):
    def save_metrics(self, records: List[List[Union[str, int]]]) -> None:
        ...

    def setup_storage(self) -> None:
        ...


class CsvMetricStorage:
    def __init__(self, path: str):
        self.path = path
        self.setup_storage()

    def save_metrics(self, records: List[List[Union[str, int]]]) -> None:
        with open(self.path, "a", newline="") as file:
            writer = csv.writer(file, delimiter=";")
            writer.writerows(records)

    def setup_storage(self) -> None:
        with open(self.path, "w", newline="") as file:
            writer = csv.writer(file, delimiter=";")
            writer.writerow(["timestamp", "metric_name", "metric_value"])


class TextMetricStorage:
    def __init__(self, path: str):
        self.path = path
        self.setup_storage()

    def save_metrics(self, records: List[List[Union[str, int]]]) -> None:
        with open(self.path, "a") as file:
            for record in records:
                file.write(f"{record[0]} {record[1]} {record[2]}\n")

    def setup_storage(self) -> None:
        open(self.path, "w").close()


class Statsd:
    def __init__(self, buffer_size: int, storage: MetricStorage):
        self.buffer_size = buffer_size
        self.storage = storage
        self.metric_buffer = []

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.flush_metrics()

    def log_metric(self, metric: str, value: int) -> None:
        self.metric_buffer.append([get_current_utc_timestamp(), metric, value])
        if len(self.metric_buffer) >= self.buffer_size:
            self.flush_metrics()

    def flush_metrics(self) -> None:
        if self.metric_buffer:
            self.storage.save_metrics(self.metric_buffer)
            self.metric_buffer.clear()

    def incr(self, metric_name: str) -> None:
        self.log_metric(metric_name, 1)

    def decr(self, metric_name: str) -> None:
        self.log_metric(metric_name, -1)


def get_current_utc_timestamp() -> str:
    return datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%S%z")


def get_txt_statsd(path: str, buffer_size: int = 10) -> Statsd:
    if not path.endswith(".txt"):
        raise ValueError("Файл должен быть с расширением '.txt'")
    return Statsd(buffer_size, TextMetricStorage(path))


def get_csv_statsd(path: str, buffer_size: int = 10) -> Statsd:
    if not path.endswith(".csv"):
        raise ValueError("Файл должен быть с расширением '.csv'")
    return Statsd(buffer_size, CsvMetricStorage(path))
