# Laptop 2 — Secondary NameNode + DataNode — Rack A
# Replace <LAPTOP1_IP> with Laptop 1's IP address

$env:NODE_ID = "SecondaryNN"
$env:MY_RACK = "RackA"
$env:LISTEN_PORT = "9001"
$env:NAMENODE_ADDR = "<LAPTOP1_IP>:8000"

Write-Host "Environment set for Laptop 2 (Secondary NameNode). Starting..."
go run ./cmd/secondary/
