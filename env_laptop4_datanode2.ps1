# Laptop 2 — Secondary NameNode + DataNode — Rack A
# Replace <LAPTOP1_IP> with Laptop 1's IP address

$env:NODE_ID = "DataNode2"
$env:MY_RACK = "RackB"
$env:LISTEN_PORT = "9003"
$env:NAMENODE_ADDR = "10.253.4.56:8000"

Write-Host "Environment set for Laptop 4 (Secondary NameNode). Starting..."
go run ./cmd/datanode2/