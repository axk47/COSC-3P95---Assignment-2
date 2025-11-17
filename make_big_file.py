from pathlib import Path
import os

client_dir = Path("client_files")
client_dir.mkdir(exist_ok=True)

p = client_dir / "big_test.bin"
size = 20 * 1024 * 1024  # 20 MB

with p.open("wb") as f:
    f.write(os.urandom(size))

print("Created", p, "size", size, "bytes")
