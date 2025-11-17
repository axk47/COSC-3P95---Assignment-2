# FILE TRANSFER PROJECT – RUN & TEST INSTRUCTIONS
**COSC 3P95 – Assignment 2**

## 1. Project Structure
```
FILE_TRANSFER_PROJECT/
    client/
        client.py
        client_files/
    server/
        server.py
        server_files/
    telemetry/
        otel_setup.py
    sd/
        analyze_sd.py
        sd_data.csv
    docker-compose.yml
    otel-collector-config.yaml
    requirements.txt
    README.md
```

## 2. Requirements
- Python 3.10+
- Pip
- Docker Desktop

Install dependencies:
```
pip install -r requirements.txt
```

## 3. Start OpenTelemetry + Jaeger
```
docker-compose up -d
```
Jaeger UI: http://localhost:16686

## 4. Start the Server
```
cd server
uvicorn server:app --reload --host 127.0.0.1 --port 8000
```

## 5. Run the Client
```
cd client
python -m client
```

## 6. View Traces (Jaeger)
Open http://localhost:16686 and select:
- file-transfer-client
- file-transfer-server

## 7. Test Sampling Modes
Set in server.py:
```
SERVER_SAMPLING = "always_on"
SERVER_SAMPLING = "0.2"
```

## 8. Verify Metrics
Metrics appear in terminal.

Client:
- client_file_transfer_latency_ms
- client_files_sent_total

Server:
- server_file_write_latency_ms
- server_files_processed_total

## 9. Statistical Debugging
Enable bug:
```
BUG_ENABLED = True
```
Run multiple clients:
```
python -m client
```

## 10. Shut Down
```
docker-compose down
```

## 11. Marker Checklist
- File transfer pipeline
- Compression, encryption, chunking
- OTEL instrumentation
- Custom spans/events
- Sampling modes
- SD bug isolation

## 12. Summary
Complete instructions to run and test the system.
