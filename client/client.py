from pathlib import Path
import time
import httpx
import zlib
import gzip
import hashlib
import csv
import os

from telemetry.otel_setup import setup_tracing, setup_metrics
from opentelemetry.instrumentation.httpx import HTTPXClientInstrumentor

SERVER_URL = "http://localhost:8000/upload"

PROJECT_ROOT = Path(__file__).resolve().parent.parent
CLIENT_DIR = PROJECT_ROOT / "client_files"

SD_DIR = PROJECT_ROOT / "sd"
SD_DIR.mkdir(exist_ok=True)
SD_CSV = SD_DIR / "sd_data.csv"


# --------- OpenTelemetry setup ---------

CLIENT_SAMPLING = "always_on"  
#CLIENT_SAMPLING = "0.2"

tracer = setup_tracing("file-transfer-client", sampling=CLIENT_SAMPLING)
meter = setup_metrics("file-transfer-client")

files_sent = meter.create_counter(
    "client_files_sent_total",
    description="Total number of files sent by the client",
)

file_transfer_latency = meter.create_histogram(
    "client_file_transfer_latency_ms",
    description="Latency of file upload from client perspective",
)

HTTPXClientInstrumentor().instrument()

def append_sd_row(
    file_name: str,
    original_size: int,
    compressed_size: int,
    compression_ratio: float,
    latency_ms: float,
    is_large_file: bool,
    bug_triggered: bool,
    checksum_ok: bool,
):
    """
    Append a row to sd_data.csv for Statistical Debugging.
    One row per file transfer.
    """
    file_exists = SD_CSV.exists()

    with SD_CSV.open("a", newline="") as f:
        writer = csv.writer(f)

        if not file_exists:
            # header row
            writer.writerow([
                "file_name",
                "original_size",
                "compressed_size",
                "compression_ratio",
                "latency_ms",
                "is_large_file",
                "bug_triggered",
                "checksum_ok",
                "failed",
            ])

        failed = not checksum_ok

        writer.writerow([
            file_name,
            original_size,
            compressed_size,
            compression_ratio,
            round(latency_ms, 3),
            int(is_large_file),
            int(bug_triggered),
            int(checksum_ok),
            int(failed),
        ])


def read_and_compress(file_path: Path, chunk_size: int = 1024 * 1024):
    """
    Read the file in chunks, compute checksum incrementally, and compress incrementally
    using zlib in gzip mode.
    """
    sha = hashlib.sha256()

    # gzip mode: wbits = 16 + MAX_WBITS
    compressor = zlib.compressobj(
        level=6,
        method=zlib.DEFLATED,
        wbits=16 + zlib.MAX_WBITS,
    )

    original_size = 0
    chunk_count = 0
    compressed_parts = []

    with file_path.open("rb") as f:
        while True:
            chunk = f.read(chunk_size)
            if not chunk:
                break

            chunk_count += 1
            original_size += len(chunk)

            sha.update(chunk)

            compressed_part = compressor.compress(chunk)
            if compressed_part:
                compressed_parts.append(compressed_part)

    # flush compressor
    flushed = compressor.flush()
    if flushed:
        compressed_parts.append(flushed)

    compressed = b"".join(compressed_parts)
    compressed_size = len(compressed)

    compression_ratio = (
        compressed_size / original_size if original_size > 0 else 1.0
    )

    checksum = sha.hexdigest()

    return (
        compressed,
        checksum,
        original_size,
        compressed_size,
        compression_ratio,
        chunk_count,
    )




def send_file(file_path: Path):
    (
        compressed,
        checksum,
        original_size,
        compressed_size,
        compression_ratio,
        chunk_count,
    ) = read_and_compress(file_path)

    with tracer.start_as_current_span("client_send_file") as span:
        span.set_attribute("file.name", file_path.name)
        span.set_attribute("file.original_size", original_size)
        span.set_attribute("file.compressed_size", compressed_size)
        span.set_attribute("file.compression_ratio", compression_ratio)
        span.set_attribute("file.chunk_count", chunk_count)

        print(
            f"Sending {file_path.name}: "
            f"orig={original_size} bytes, "
            f"compressed={compressed_size} bytes, "
            f"ratio={compression_ratio:.2f}, "
            f"chunks={chunk_count}"
        )

        params = {
            "filename": file_path.name,
            "checksum": checksum,
            "compression_ratio": str(compression_ratio),
        }

        start = time.perf_counter()
        span.add_event("upload_started", {"chunk_count": chunk_count})

        response = httpx.post(
            SERVER_URL,
            params=params,
            content=compressed,
            timeout=120.0,
        )

        span.add_event("upload_finished", {"status_code": response.status_code})
        elapsed_ms = (time.perf_counter() - start) * 1000.0

        # metrics
        file_transfer_latency.record(elapsed_ms, {"file.name": file_path.name})
        files_sent.add(1, {"success": response.status_code == 200})

        response.raise_for_status()
        resp_json = response.json()
        print(f"Server response for {file_path.name}: {resp_json}")

        # Extract SD-related values from server response
        checksum_ok = bool(resp_json.get("checksum_ok", True))
        is_large_file = bool(resp_json.get("is_large_file", False))
        bug_triggered = bool(resp_json.get("bug_triggered", False))

        # Log SD row
        append_sd_row(
            file_name=file_path.name,
            original_size=original_size,
            compressed_size=compressed_size,
            compression_ratio=compression_ratio,
            latency_ms=elapsed_ms,
            is_large_file=is_large_file,
            bug_triggered=bug_triggered,
            checksum_ok=checksum_ok,
        )



def main():
    if not CLIENT_DIR.exists():
        raise RuntimeError(f"Client folder does not exist: {CLIENT_DIR}")

    files = sorted(CLIENT_DIR.iterdir())
    if not files:
        print(f"No files found in {CLIENT_DIR}. Add some test files first.")
        return

    for p in files:
        if p.is_file():
            send_file(p)


if __name__ == "__main__":
    main()
