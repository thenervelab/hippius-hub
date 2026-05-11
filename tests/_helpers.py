import hashlib


def write_test_file(path, size, seed=b"hippius"):
    """Write `size` bytes of deterministic, non-repeating content to `path`.
    Each 32-byte stride is a fresh SHA256 of the previous stride, so any
    chunk-offset misalignment in the downloader produces a hash mismatch.
    Returns the SHA256 hex of the file content."""
    h_file = hashlib.sha256()
    state = hashlib.sha256(seed).digest()
    written = 0
    block_size = 1024 * 1024
    with open(path, "wb") as f:
        while written < size:
            target = min(block_size, size - written)
            block = bytearray()
            while len(block) < target:
                state = hashlib.sha256(state).digest()
                block.extend(state)
            chunk = bytes(block[:target])
            f.write(chunk)
            h_file.update(chunk)
            written += target
    return h_file.hexdigest()


def sha256_of_file(path):
    h = hashlib.sha256()
    with open(path, "rb") as f:
        for chunk in iter(lambda: f.read(1024 * 1024), b""):
            h.update(chunk)
    return h.hexdigest()
