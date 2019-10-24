import os
import logging
import yaml
import boto3


class EsSnapshot():
    s3 = None
    es = None

    def __init__(self, region):
        self.s3 = boto3.client('s3')
        self.es = boto3.client('elasticache', region)
        self.es_s3_snapshots_posfix = "-0001.rdb"

    def get_es_snapshots(self, cluster_name):
        try:
            response = self.es.describe_snapshots(
                CacheClusterId=cluster_name,
                ShowNodeGroupConfig=False
            )
            logging.info("Extract the list snapshots")
            return response["Snapshots"]
        except Exception as error_msg:
            logging.error(error_msg)
            raise Exception("method: get_es_snapshots - {}".format(error_msg))

    def get_last_es_snapshot(self, cluster_snapshots_list):
        try:
            latest_es_snapshot = cluster_snapshots_list[0]
            for num in range(1, len(cluster_snapshots_list)):
                if cluster_snapshots_list[num]["NodeSnapshots"][0]["SnapshotCreateTime"] > latest_es_snapshot["NodeSnapshots"][0]["SnapshotCreateTime"]:
                    latest_es_snapshot = cluster_snapshots_list[num]
            logging.info("Getting the lastest snapshot name/id: {}".format(latest_es_snapshot["SnapshotName"]))
            return latest_es_snapshot["SnapshotName"]
        except Exception as error_msg:
            logging.error(error_msg)
            raise Exception("method: get_last_es_snapshot - {}".format(error_msg))

    def copy_snapshot_to_s3(self, snapshot_id, target_bucket):
        try:
            self.es.copy_snapshot(
                SourceSnapshotName=snapshot_id,
                TargetSnapshotName=snapshot_id,
                TargetBucket=target_bucket
            )
            logging.info("Snapshot copy triggered")
            return True
        except Exception as error_msg:
            logging.error(error_msg)
            raise Exception("method: copy_snapshot_to_s3 - {}".format(error_msg))

    def wait_for_snapshot_copy(self, snapshot_id, target_bucket, timeout, max_attempts):
        try:
            self.s3.get_waiter('object_exists').wait(
                Bucket=target_bucket,
                Key="{}{}".format(snapshot_id, self.es_s3_snapshots_posfix),
                WaiterConfig={
                    'Delay': timeout,
                    'MaxAttempts': max_attempts
                }
            )
            logging.info("Snapshot copied to S3")
            return True
        except Exception as error_msg:
            logging.error(error_msg)
            raise Exception("method: copy_snapshot_to_s3 - {}".format(error_msg))

    def download_snapshots_from_s3(self, bucket_name, snapshot_id):
        try:
            self.s3.download_file(
                Bucket=bucket_name,
                Key="{}{}".format(snapshot_id, self.es_s3_snapshots_posfix),
                Filename="{}/downloads/{}.rdb".format(os.getcwd(), snapshot_id)
            )
            logging.info("Snapshot downloaded")
            return True
        except Exception as error_msg:
            logging.error(error_msg)
            raise Exception("method: download_snapshots_from_s3 - {}".format(error_msg))

    def clean_s3_snapshot(self, bucket_name, snapshot_id):
        try:
            self.s3.delete_objects(
                Bucket=bucket_name,
                Delete={
                    'Objects': [
                        {
                            'Key': "{}{}".format(snapshot_id, self.es_s3_snapshots_posfix)
                        }
                    ],
                    'Quiet': False
                }
            )
            logging.info("S3 bucket cleaned")
            return True
        except Exception as error_msg:
            logging.error(error_msg)
            raise Exception("method: clean_s3_snapshot - {}".format(error_msg))

def main():
    try:
        # load config from yaml file
        logging.basicConfig(level=logging.INFO)
        logging.info("Loading config from yaml file")
        conf_file = open("params.yaml")
        config = yaml.safe_load(conf_file)
        conf_file.close()

        es_snapshot = EsSnapshot(config['region'])
        for cluster_id in config['cluster_ids']:
            logging.info("Get snapshot for cluster: {}".format(cluster_id))
            latest_snapshot_id = es_snapshot.get_last_es_snapshot(es_snapshot.get_es_snapshots(cluster_id))
            es_snapshot.clean_s3_snapshot(config['target_bucket'], latest_snapshot_id)
            es_snapshot.copy_snapshot_to_s3(latest_snapshot_id, config['target_bucket'])
            es_snapshot.wait_for_snapshot_copy(latest_snapshot_id, config['target_bucket'], config['timeout'], config['max_attempts'])
            es_snapshot.download_snapshots_from_s3(config['target_bucket'], latest_snapshot_id)
            es_snapshot.clean_s3_snapshot(config['target_bucket'], latest_snapshot_id)
    except Exception as error_msg:
        raise Exception("Error - Something bad happened - {}.".format(error_msg))

# main entry point
if __name__ == "__main__":
    main()
