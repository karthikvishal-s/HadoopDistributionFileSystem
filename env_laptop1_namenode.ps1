# Laptop 1 — NameNode (Master) — Rack A
# Replace the IPs below with your teammates' actual IPs (run ipconfig on each laptop)

$env:NAMENODE_PORT = "8000"
$env:EXPECTED_DATANODES = "3"
$env:DATANODE_ADDRS = "SecondaryNN=<LAPTOP2_IP>:9001,DataNode1=<LAPTOP3_IP>:9002,DataNode2=<LAPTOP4_IP>:9003"

Write-Host "Environment set for Laptop 1 (NameNode). Starting..."
go run ./cmd/namenode/
