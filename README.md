# Batchify Function

This function takes in an array of records of variable size and splits the input into batches of records (array of arrays) suitably sized for delivery to a system with the following limits:

- Maximum size of an output record is **1 MB**. Larger records will be discarded.
- Maximum size of an output batch is **5 MB**.
- Maximum number of records in an output batch is **500**.

### Input:
The input for the function is an array of records:
```python
[<record1>, <record2>, <record3>, ... , <recordn>]
```

### Output:
The output is an array of batches:
```python
[<batch1>, <batch2>, ..., <batchn>]
```
Each batch is an array of records, maintaining the order in which they arrive.

The records are assumed to be strings of variable length, and they must pass intact through the system, retaining their original order.

## Running Tests

   To run the unit tests, use the following command:

   ```sh
   python3 -m unittest test_batchify.py
   ```
   