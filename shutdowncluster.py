import os
import configparser
import boto3


def main():
    config = configparser.ConfigParser()
    config.read_file(open('dwh.cfg'))

    # Some parameters are defined as Environment Variable
    KEY = os.environ.get("DWH_AWS_KEY")
    SECRET = os.environ.get("DWH_AWS_SECRET")

    # load parameters from config file
    DWH_CLUSTER_IDENTIFIER = config.get("CLUSTER", "CLUSTER_IDENTIFIER")

    redshift = boto3.client('redshift', region_name="us-west-2",
                            aws_access_key_id=KEY,
                            aws_secret_access_key=SECRET)

    redshift.delete_cluster(ClusterIdentifier=DWH_CLUSTER_IDENTIFIER,
                            SkipFinalClusterSnapshot=True)


if __name__ == "__main__":
    main()
