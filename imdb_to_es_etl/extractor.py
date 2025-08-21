import gzip
import csv
from typing import Generator

class IMDBExtractor:
    def __init__(self, filepath: str, batch_size: int = 1000):
        self.filepath = filepath
        self.batch_size = batch_size

    def extract(self) -> Generator[list[dict], None, None]:
        with gzip.open(self.filepath, 'rt', encoding='utf-8') as f:
            reader = csv.DictReader(f, delimiter='\t')
            batch = []
            for row in reader:
                batch.append(row)
                if len(batch) >= self.batch_size:
                    yield batch
                    batch = []
            if batch:
                yield batch