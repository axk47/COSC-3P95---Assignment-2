from fastapi import FastAPI, Request, BackgroundTasks
from pathlib import Path
import gzip
import zlib
import hashlib
from concurrent.futures import ThreadPoolExecutor
import time
import random

from telemetry.otel_setup import setup_tracing, setup_metrics
from opentelemetry.instrumentation.fastapi import FastAPIInstrumentor

# ----------------- Paths / basic setup -----------------

PROJECT_ROOT = Path(__file__).resolve().parent.parent
OUTPUT_DIR = PROJECT_ROOT / "server_files"
OUTPUT_DIR.mkdir(exist_ok=True)

app = FastAPI(title="File Transfer Server")

# Explicit thread pool
executor = ThreadPoolExecutor(max_workers=8)

# ----------------- OpenTelemetry setup -----------------

SERVER_SAMPLING = "always_on"
#SERVER_SAMPLING = "0.2"

tracer = setup_tracing("file-transfer-server", sampling=SERVER_SAMPLING)
meter = setup_metrics("file-transfer-server")

files_processed = meter.create_counter(
    "server_files_processed_total",
    description="Total number of files processed by the server",
)

file_write_latency = meter.create_histogram(
    "server_file_write_latency_ms",
    description="Time from receiving compressed body to scheduling write",
)

FastAPIInstrumentor().instrument_app(app)

# Bug configuration for Statistical Debugging
BUG_ENABLED = True
BUG_PROBABILITY = 0.3  # 30% of large-file uploads get corrupted
LARGE_FILE_THRESHOLD = 10 * 1024 * 1024  # 10 MB
CORRUPT_TAIL_BYTES = 1024  # how much of the end of the file we zero out



def save_bytes_task(data: bytes, dest_path: Path):
    """
    Runs in a worker thread.
    Writes the given bytes to the destination file.
    """
    with dest_path.open("wb") as f:
        f.write(data)


@app.post("/upload")
async def upload_file(request: Request, background_tasks: BackgroundTasks):
    """
    Receives compressed file data in the request body and metadata in query params.

    Query params:
      - filename
      - checksum  (SHA-256 of original, uncompressed data)
      - compression_ratio (string float, informational)
    Body:
      - gzip-compressed file bytes
    """
    start = time.perf_counter()

    # Create a custom span for this handler
    with tracer.start_as_current_span("server_handle_upload") as span:
        # --- Read query parameters ---
        params = request.query_params
        filename = params.get("filename")
        checksum = params.get("checksum")
        compression_ratio_str = params.get("compression_ratio")

        if not filename or not checksum:
            span.set_attribute("error", True)
            span.add_event("missing_metadata", {"filename": filename, "checksum_present": bool(checksum)})
            return {"status": "error", "reason": "missing filename or checksum"}

        span.set_attribute("file.name", filename)
        if compression_ratio_str is not None:
            try:
                span.set_attribute("file.compression_ratio", float(compression_ratio_str))
            except ValueError:
                span.set_attribute("file.compression_ratio_parse_error", True)

        # --- Read compressed body ---
        compressed_body = await request.body()
        span.set_attribute("file.compressed_size", len(compressed_body))

        # --- Decompress ---
        span.add_event("decompression_started")
        try:
           original_data = zlib.decompress(compressed_body, wbits=16 + zlib.MAX_WBITS)
        except Exception as e:
            span.set_attribute("error", True)
            span.add_event("decompression_failed", {"exception": str(e)})
            return {"status": "error", "reason": f"decompression_failed: {e}"}
        span.add_event("decompression_finished")

        original_size = len(original_data)
        span.set_attribute("file.original_size", original_size)

        # --- BUG: random corruption for large files ---
        is_large_file = original_size > LARGE_FILE_THRESHOLD
        bug_triggered = False

        if BUG_ENABLED and is_large_file and random.random() < BUG_PROBABILITY:
            bug_triggered = True
            span.add_event(
                "bug_corruption_triggered",
                {
                    "file.name": filename,
                    "original_size": original_size,
                    "corrupt_tail_bytes": CORRUPT_TAIL_BYTES,
                },
            )
            # Corrupt the last CORRUPT_TAIL_BYTES bytes
            if original_size >= CORRUPT_TAIL_BYTES:
                corrupted = bytearray(original_data)
                corrupted[-CORRUPT_TAIL_BYTES:] = b"\x00" * CORRUPT_TAIL_BYTES
                original_data = bytes(corrupted)

        # expose predicates as span attributes for SD later
        span.set_attribute("predicate.is_large_file", is_large_file)
        span.set_attribute("predicate.bug_triggered", bug_triggered)


        # --- Verify checksum ---
        sha = hashlib.sha256()
        sha.update(original_data)
        server_checksum = sha.hexdigest()
        checksum_ok = (server_checksum == checksum)

        span.set_attribute("file.checksum_ok", checksum_ok)
        if not checksum_ok:
            span.add_event(
                "checksum_mismatch",
                {
                    "file.name": filename,
                    "expected": checksum,
                    "actual": server_checksum,
                },
            )

        # --- Schedule saving to disk using thread pool ---
        dest_path = OUTPUT_DIR / filename
        executor_future = executor.submit(save_bytes_task, original_data, dest_path)
        # we don't wait for future here; FastAPI BackgroundTasks is enough
        # but to satisfy pattern, we record that we've scheduled it
        background_tasks.add_task(lambda: executor_future)

        elapsed_ms = (time.perf_counter() - start) * 1000.0

        # record metrics
        file_write_latency.record(elapsed_ms, {"file.name": filename, "checksum_ok": checksum_ok})
        files_processed.add(1, {"checksum_ok": checksum_ok})

        return {
            "status": "ok",
            "filename": filename,
            "checksum_ok": checksum_ok,
            "server_checksum": server_checksum,
            "compression_ratio": compression_ratio_str,
            "original_size": original_size,
            "compressed_size": len(compressed_body),
            "is_large_file": is_large_file,
            "bug_triggered": bug_triggered,
        }
