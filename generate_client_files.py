import os
import string
import random
from pathlib import Path


# Root = this file's directory
ROOT_DIR = Path(__file__).resolve().parent
CLIENT_FILES_DIR = ROOT_DIR / "client_files"


def ensure_dir(path: Path) -> None:
    path.mkdir(parents=True, exist_ok=True)


def random_text_bytes(size: int) -> bytes:
    """
    Return ~size bytes of ASCII text (roughly).
    """
    chars = string.ascii_letters + string.digits + " \n"
    s = "".join(random.choice(chars) for _ in range(size))
    return s.encode("utf-8")


def random_binary_bytes(size: int) -> bytes:
    """
    Return size bytes of random binary data.
    """
    return os.urandom(size)


def write_file(path: Path, data: bytes) -> None:
    ensure_dir(path.parent)
    with path.open("wb") as f:
        f.write(data)
    print(f"Wrote {path.name:20s}  size={len(data):>10,d} bytes")


def main() -> None:
    ensure_dir(CLIENT_FILES_DIR)
    print(f"Writing files into: {CLIENT_FILES_DIR}")

    # ---- Small text files: ~5 KB – 50 KB ----
    small_sizes = [5_000, 10_000, 20_000, 30_000, 40_000, 50_000]
    for i, sz in enumerate(small_sizes, start=1):
        path = CLIENT_FILES_DIR / f"small_text_{i}.txt"
        write_file(path, random_text_bytes(sz))

    # ---- Medium text / binary files: ~100 KB – 1 MB ----
    medium_sizes = [100_000, 250_000, 500_000, 1_000_000]
    for i, sz in enumerate(medium_sizes, start=1):
        # mix text and binary just to vary content
        if i % 2 == 1:
            path = CLIENT_FILES_DIR / f"medium_text_{i}.txt"
            data = random_text_bytes(sz)
        else:
            path = CLIENT_FILES_DIR / f"medium_bin_{i}.bin"
            data = random_binary_bytes(sz)
        write_file(path, data)

    # ---- Large binary files: ~5 MB – 100 MB ----
    large_sizes = [
        5_000_000,     # 5 MB
        10_000_000,    # 10 MB
        20_000_000,    # 20 MB
        50_000_000,    # 50 MB
        75_000_000,    # 75 MB
        100_000_000,   # 100 MB
    ]
    for i, sz in enumerate(large_sizes, start=1):
        path = CLIENT_FILES_DIR / f"large_{i}.bin"
        write_file(path, random_binary_bytes(sz))

    # ---- Optional: keep your existing big_test.bin for SD consistency ----
    big_test_path = CLIENT_FILES_DIR / "big_test.bin"
    if not big_test_path.exists():
        write_file(big_test_path, random_binary_bytes(20_000_000))

    print("\nDone. Client file set ready.")


if __name__ == "__main__":
    main()
