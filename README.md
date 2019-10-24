# AWS Elasticache Snapshot Downloader
[![MIT licensed](https://img.shields.io/badge/license-MIT-blue.svg)](https://raw.githubusercontent.com/taherbs/aws-elasticache-snapshot-downloader/master/LICENSE)

This script will download the latest listed Elasticache/Redis clusters snapshots on you local machine.

## Prerequisites
* Install prerequisites by running the below command:
```bash
pip3 install -r requirements.txt
```

## Usage:
* create the params.yaml file based on the use [params.yaml.sample](./params.yaml.sample)
* Run the below command:
```bash
# Download the latest version of the listed Elasticache/Redis clusters snapshots
python3 dnld_es_snapshots.py
```
