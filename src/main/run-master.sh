go build -buildmode=plugin ../mrapps/wc.go
rm mr-*
echo "running coordinator"
go run mrcoordinator.go pg-*.txt