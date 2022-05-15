#
# lab1
#
.EXPORT_ALL_VARIABLES:
GO111MODULE=off

build-plugin:
	cd main && go build -buildmode=plugin ../mrapps/wc.go

run-demo: # build-plugin
	echo "simple sequential mapreduce implementation"
	cd main && rm mr-out* || true && go run mrsequential.go wc.so pg*.txt
	# cat main/mr-out-0

run-master: build-plugin
	cd main && go run mrmaster.go pg-*.txt

run-worker: build-plugin
	cd main && go run mrworker.go wc.so