build:
	@go build -o bin/main main.go

pprof:
	@go tool pprof -http=":8000" ./bin/main ./bin/measurements_billion-profile.pb.gz

.PHONY: build pprof

