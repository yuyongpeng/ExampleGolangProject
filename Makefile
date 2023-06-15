
define rmobj
	rm -rf esImport_*
	rm -rf esService_*
	rm -rf soldierImportService_*
endef

define build
	go build -gcflags "all=-N -l" -o esImport_${getLastCommitId} main/esImport.go
	go build -gcflags "all=-N -l" -o esService_${getLastCommitId} main/esService.go
	go build -gcflags "all=-N -l" -o soldierImportService_${getLastCommitId} main/soldierImportService.go
endef

getLastCommitId = ${shell git log --pretty=format:"%h" | head -1  | awk '{print $1}'}

export CGO_ENABLED=0

#linux: export GOOS=linux
#linux: export GOARCH=amd64
#linux:
#	${call rmobj}
#	${build}
#
#mac: export GOOS=darwin
#mac: export GOARCH=amd64
#mac:
#	${call rmobj}
#	${build}

initgo: export GO111MODULE=on
initgo:
	go mod download

define pub
	mkdir -p "build"
	go build -o build/pub mqtt/pub/main/main.go
endef

define sub
	mkdir -p "build"
	go build -o build/sub mqtt/sub/main/main.go
endef

define clean
	rm -rf ./build
endef

pub:
	$(pub)
sub:
	$(sub)
all:
	$(clean)
	$(pub)
	$(sub)
clean:
	$(clean)
