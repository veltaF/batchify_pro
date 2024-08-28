from array import array
from typing import Callable, Iterable, TypeVar

Record = TypeVar("Record", bound=str | bytes | array)

def batchify(
    records: Iterable[Record],
    record_max_size: int = 1024**2,  # 1 MB
    batch_max_size: int = 5 * 1024**2,  # 5 MB
    batch_max_records: int = 500,
    size_func: Callable[[Record], int] = len,
) -> Iterable[list[Record]]:
    """
    Create iterable of batches of records from iterable of records.

    Keyword arguments:
    record_max_size -- output record max size (default 1MB)
    batch_max_size -- max size of output batch as sum of batch record sizes (default 5MB)
    batch_max_records -- maximum number of records in a single batch (default 500)
    size_func -- function to determine the size of a record. This allows for custom size function.
    """
    assert record_max_size <= batch_max_size
    
    # Discard oversize records from input stream
    records = filter(lambda r: size_func(r) <= record_max_size, records)
    
    # Init generator
    batch: list[Record] = []
    record_count: int = 0
    batch_size: int = 0
    
    for record in records:
        if (
            record_count + 1 > batch_max_records
            or batch_size + size_func(record) > batch_max_size
        ):
            # Yield current batch and reset
            yield batch
            batch = []
            record_count = 0
            batch_size = 0
        
        # Add record to current batch
        batch.append(record)
        record_count += 1
        batch_size += size_func(record)
    
    # Yield the last batch if it has records
    if batch:
        yield batch

# Example usage:
if __name__ == "__main__":
    # Each "record" string is 1 MB in size (1024 * 1024 bytes)
    records = ["a" * (1024 * 1024) for _ in range(7)]  # 7 records, each 1 MB
    batches = list(batchify(records))
    for i, batch in enumerate(batches, 1):
        print(f"Batch {i} contains {len(batch)} records")