#!/bin/bash
arg1=$1
arg2=$2
arg3=$3

cd tickers
go run main.go
cd ..

cd aggregates
go run main.go $arg1 $arg2
cd ..

cd breakouthist
go run main.go $arg1 $arg2
cd ..

cd changepct
go run main.go $arg1 $arg2
cd ..

cd changepctall
go run main.go $arg3 $arg2
cd ..

cd trades
go run main.go $arg1 $arg2
cd ..

cd lob
go run main.go $arg1 $arg2
cd ..

cd lov
go run main.go $arg1 $arg2
cd ..

cd short
go run main.go $arg1 $arg2
cd ..

cd stock_split
go run main.go $arg2
cd ..

cd average_volume
go run main.go $arg2
cd ..

cd pattern_features
go run main.go $arg2
cd ..