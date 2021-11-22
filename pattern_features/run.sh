#!/bin/bash
arg1=$1
export PGPASSWORD='P`AgD!9g!%~hz3M<'
psql -h 52.116.150.66 -d stockmarket -U postgres -c "\copy pattern_features (ticker,date,co,value20_days_change_pct,above200_ma)  from $arg1 with delimiter as ','"