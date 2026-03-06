# Laptop 4 — DataNode 2 — Rack B
$env:NODE_ID = "DataNode2"
$env:MY_RACK = "RackB"
$env:LISTEN_PORT = "9003"
$env:NAMENODE_ADDR = "10.253.4.158:8000"

Write-Host "Environment set for Laptop 4 (DataNode 2). Starting..."
go run ./cmd/datanode2/
