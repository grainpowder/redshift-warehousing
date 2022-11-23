import time
import logging
import boto3

from configparser import ConfigParser


def create_cluster(
    parser: ConfigParser, 
    logger: logging.Logger, 
    session: boto3.Session,
    db_password: str
) -> ConfigParser:
    redshift_client = session.client("redshift")

    # Cluster subnet group
    logger.info("Create subnet group")
    redshift_client.create_cluster_subnet_group(
        ClusterSubnetGroupName=parser.get("cluster.subnet.group", "name"),
        Description=parser.get("cluster.subnet.group", "desc"),
        SubnetIds=[
            parser.get("network.subnet.a", "id"),
            parser.get("network.subnet.c", "id")
        ]
    )
    
    # Cluster
    logger.info("Create redshift cluster")
    response = redshift_client.create_cluster(
        ClusterIdentifier=parser.get("cluster", "identifier"),
        Port=parser.getint("cluster", "db_port"),
        DBName=parser.get("cluster", "db_name"),
        MasterUsername=parser.get("DEFAULT", "admin_profile"),
        MasterUserPassword=db_password,
        ClusterType="multi-node",
        NodeType=parser.get("cluster", "node_type"),
        NumberOfNodes=parser.getint("cluster", "node_count"),
        ClusterSubnetGroupName=parser.get("cluster.subnet.group", "name"),
        AvailabilityZone=parser.get("network.subnet.a", "az"),
        PubliclyAccessible=True,
        EnhancedVpcRouting=False,
        VpcSecurityGroupIds=[parser.get("network.vpc", "sg_id")],
    )
    
    logger.info("Waiting for cluster to be available")
    cluster_status = response["Cluster"]["ClusterStatus"]
    while cluster_status != "available":
        cluster_info = redshift_client.describe_clusters(
            ClusterIdentifier=parser.get("cluster", "identifier")
        )["Clusters"][0]
        cluster_status = cluster_info["ClusterStatus"]
        time.sleep(5)

    logger.info("Associate IAM role to the cluster")
    redshift_client.modify_cluster_iam_roles(
        ClusterIdentifier=parser.get("cluster", "identifier"), 
        AddIamRoles=[parser.get("iam.role", "arn")]
    )
    
    logger.info("Save DB host information in configuration file")
    parser["cluster"]["db_password"] = db_password
    parser["cluster"]["db_host"] = cluster_info["Endpoint"]["Address"]

    return parser


def delete_cluster(
    parser: ConfigParser, 
    logger: logging.Logger, 
    session: boto3.Session,
) -> None:
    redshift_client = session.client("redshift")

    # Cluster
    logger.info("Delete Redshift cluster")
    redshift_client.delete_cluster(
        ClusterIdentifier=parser.get("cluster", "identifier"),
        SkipFinalClusterSnapshot=True
    )
    logger.info("Waiting for cluster to be deleted")
    cluster_status = "deleting"
    while cluster_status == "deleting":
        time.sleep(5)
        try:
            redshift_client.describe_clusters(
                ClusterIdentifier=parser.get("cluster", "identifier")
            )
        except redshift_client.exceptions.ClusterNotFoundFault:
            cluster_status = "deleted" 
    
    # Cluster subnet group
    logger.info("Delete corresponding subnet group")
    redshift_client.delete_cluster_subnet_group(
        ClusterSubnetGroupName=parser.get("cluster.subnet.group", "name")
    )
