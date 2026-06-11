.PHONY: run fresh clean build

run:
	go run .

fresh: clean run

clean:
	rm -f data.db data.db-wal data.db-shm

build:
	go build -o streaming-backend .
