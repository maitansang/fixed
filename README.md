# 1. Documentation for automated scheduler script
1/ Connect to server 
#### server: devuser@52.116.150.66 

```bash
# password: UpworkDataGuy!@#123
```
#### sudo ssh devuser@52.116.150.66

2/ Start or stop service go-cronjob
#### start service
```bash
# start service
```
#### sudo systemctl start go-cronjob

#### stop service
```bash
# stop service
```
#### sudo systemctl stop go-cronjob

# 2. Documentation running short transactions 
1/ Go to terminal of short_sale

```bash
# go to folder fixed
```
#### cd fixed

```bash
# go to folder short_sale
```
#### cd short_sale

```bash
#  run command to get short transations from finra and save to database
```
#### go run main.go date-month
. example get short transactions data for September 2021 
#### go run main.go 2021-09
