# zep-exporter

A zeppelin prometheus exporter

## dependence

[zeppelin-client](https://github.com/Qihoo360/zeppelin-client/tree/master/manager)

```
git clone https://github.com/Qihoo360/zeppelin-client
cd zeppelin-client/third
git submodule init
git submodule update
cd ..
make
```

## usage

### build

```
go get github.com/tinytub/zep-exporter
cd $GOPATH/src/github.com/tinytub/zep-exporter
go build
```
### start deamon

default read zp_checkup and zp_info generated json file

```
./zep-exporter json
```

get metrics

```
curl localhost:9128/metrics
```
