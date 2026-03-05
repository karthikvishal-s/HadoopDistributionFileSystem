# Laptop 4 — DataNode 2 — Rack B
# Replace <LAPTOP1_IP> with Laptop 1's IP address

$env:NODE_ID = "DataNode2"
$env:MY_RACK = "RackB"
$env:LISTEN_PORT = "9003"
$env:NAMENODE_ADDR = "<LAPTOP1_IP>:8000"

Write-Host "Environment set for Laptop 4 (DataNode 2). Starting..."
go run ./cmd/datanode2/
