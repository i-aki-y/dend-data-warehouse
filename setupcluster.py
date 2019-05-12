import os
import pandas as pd
import configparser

import boto3


def main():
    """Setup redshift cluster by using the configuration file"""
    config = configparser.ConfigParser()
    config.read_file(open('dwh.cfg'))

    # Some parameters are defined as Environment Variable
    KEY = os.environ.get("DWH_AWS_KEY")
    SECRET = os.environ.get("DWH_AWS_SECRET")
    IAM_ROLE = os.environ.get("DWH_AWS_ROLE_ARN")

    # load parameters from config file
    DWH_CLUSTER_TYPE = config.get("CLUSTER", "CLUSTER_TYPE")
    DWH_NUM_NODES = config.get("CLUSTER", "NUM_NODES")
    DWH_NODE_TYPE = config.get("CLUSTER", "NODE_TYPE")
    DWH_CLUSTER_IDENTIFIER = config.get("CLUSTER", "CLUSTER_IDENTIFIER")
    DWH_DB = config.get("CLUSTER", "DB_NAME")
    DWH_DB_USER = config.get("CLUSTER", "DB_USER")
    DWH_DB_PASSWORD = config.get("CLUSTER", "DB_PASSWORD")
    DWH_PORT = config.get("CLUSTER", "DB_PORT")

    settings = pd.DataFrame({
        "Param": [
            "DWH_CLUSTER_TYPE", "DWH_NUM_NODES", "DWH_NODE_TYPE",
            "DWH_CLUSTER_IDENTIFIER", "DWH_DB", "DWH_DB_USER",
            "DWH_DB_PASSWORD", "DWH_PORT", "IAM_ROLE"],
        "Value": [
            DWH_CLUSTER_TYPE, DWH_NUM_NODES, DWH_NODE_TYPE,
            DWH_CLUSTER_IDENTIFIER, DWH_DB, DWH_DB_USER,
            DWH_DB_PASSWORD, DWH_PORT, IAM_ROLE]
    })

    # show configs
    for _, row in settings.iterrows():
        print(row["Param"], row["Value"])

    redshift = boto3.client('redshift', region_name="us-west-2",
                            aws_access_key_id=KEY,
                            aws_secret_access_key=SECRET)

    # Create cluster
    try:
        response = redshift.create_cluster(
            ClusterType=DWH_CLUSTER_TYPE,
            NodeType=DWH_NODE_TYPE,
            NumberOfNodes=int(DWH_NUM_NODES),
            DBName=DWH_DB,
            ClusterIdentifier=DWH_CLUSTER_IDENTIFIER,
            MasterUsername=DWH_DB_USER,
            MasterUserPassword=DWH_DB_PASSWORD,
            IamRoles=[IAM_ROLE]
        )
    except Exception as e:
        print(e)

    # put resoponse
    print(response)


if __name__ == "__main__":
    main()
