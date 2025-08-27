# Geyser gRPC Latency Tester

To test Latency of Geyser gRPC stream. 
If GRPC provider needs API key, add it at line 134 in `main.go`. You can also switch between listening blocks vs transactions vs account updates.

## Building
builds are created in `build/` directory.

```bash
make build # build local os binary
make build-linux # build linux binary
```
