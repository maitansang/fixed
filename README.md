# Documentation for automated scheduler script
1/ Connect to server 
#### server: devuser@52.116.150.66 

```bash
# password: UpworkDataGuy!@#123
sudo ssh devuser@52.116.150.66
```
2/ Start or stop service go-cronjob

```bash
# start service
sudo systemctl start go-cronjob
```

```bash
# stop service
sudo systemctl stop go-cronjob
```

# 2. Documentation running short transactions 
1/ Connect to server 
#### server: devuser@52.116.150.66 

```bash
# password: UpworkDataGuy!@#123
sudo ssh devuser@52.116.150.66
```
2/
```bash
# go to folder fixed
cd fixed
```
3/
```bash
# go to folder short_sale
cd short_sale
```
4/ go run main.go YYYY-MM
```bash
#  run command to get short transations from finra and save to database example get short transactions data for September 2021 
go run main.go 2021-09
```