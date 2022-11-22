import boto3
import os
import typer
import json
import time

from configparser import ConfigParser, ExtendedInterpolation
from typer import Typer
from logger import make_logger


CONFIG_FILE_PATH = f"{os.getcwd()}/dwh.cfg"
with open(f"{os.getcwd()}/default_config.json", "r") as file:
    DEFAULT_CONFIG = json.load(file)
app = Typer()
cp = ConfigParser(interpolation=ExtendedInterpolation())
logger = make_logger(__name__)


@app.command("create-config")
def create_config():
    if os.path.exists(CONFIG_FILE_PATH):
        os.remove(CONFIG_FILE_PATH)
    cp.read_dict(DEFAULT_CONFIG)
    with open(CONFIG_FILE_PATH, "w") as file:
        cp.write(file)

    logger.info(f"Successfully created configuration file in working directory")


@app.command("create-vpc")
def create_vpc():
    if not os.path.exists(CONFIG_FILE_PATH):
        raise FileNotFoundError(
            "Configuration file not found. Execute create-config command first."
        )
    else:        
        cp.read(CONFIG_FILE_PATH)
        if cp.has_option("network.vpc", "id"):
            logger.info("Terminate process since VPC is already created")
            exit()

    session = boto3.Session(
        profile_name=cp.get("DEFAULT", "admin_profile"), 
        region_name=cp.get("DEFAULT", "region")
    )
    ec2_client = session.client("ec2")

    # VPC
    logger.info("Create VPC")
    vpc_info = ec2_client.create_vpc(
        CidrBlock=cp.get("network.vpc", "cidr"),
        TagSpecifications=[
            {
                "ResourceType": "vpc", 
                "Tags":[{"Key": "Name", "Value": cp.get("network.vpc", "name")}]
            }
        ]
    )["Vpc"]

    # Security group
    logger.info("Create security group with ingress policy")
    sg_info = ec2_client.create_security_group(
        GroupName=cp.get("network.vpc", "sg_name"),
        Description="traffic rules over Redshift cluster",
        VpcId=vpc_info["VpcId"]
    )
    ec2_client.authorize_security_group_ingress(
        CidrIp="0.0.0.0/0",  # better to allow connection by predefined VPN, but skip for simplicity
        IpProtocol="tcp",
        GroupId=sg_info["GroupId"],
        FromPort=cp.getint("cluster", "db_port"),
        ToPort=cp.getint("cluster", "db_port")
    )

    # Internet gateway
    logger.info("Create internet gateway and attach it to VPC")
    igw_info = ec2_client.create_internet_gateway(
        TagSpecifications=[
            {
                "ResourceType": "internet-gateway",
                "Tags":[{"Key": "Name", "Value": cp.get("network.vpc", "igw_name")}]
            }
        ]
    )["InternetGateway"]
    ec2_client.attach_internet_gateway(
        InternetGatewayId=igw_info["InternetGatewayId"], 
        VpcId=vpc_info["VpcId"]
    )

    # Route table
    logger.info("Create route table and define route from subnet to internet")
    rt_info = ec2_client.create_route_table(
        VpcId=vpc_info["VpcId"],
        TagSpecifications=[
            {
                "ResourceType": "route-table", 
                "Tags":[{"Key": "Name", "Value": cp.get("network.subnet.a", "rt_name")}]
            }
        ]
    )["RouteTable"]
    ec2_client.create_route(
        DestinationCidrBlock="0.0.0.0/0",
        GatewayId=igw_info["InternetGatewayId"],
        RouteTableId=rt_info["RouteTableId"]
    )

    # Subnets
    logger.info("Create 2 subnets and attach each to route table")
    subnet_a_info = ec2_client.create_subnet(
        CidrBlock=cp.get("network.subnet.a", "cidr"),
        VpcId=vpc_info["VpcId"],
        AvailabilityZone=cp.get("network.subnet.a", "az"),
        TagSpecifications=[
            {
                "ResourceType": "subnet",
                "Tags": [{"Key": "Name", "Value": cp.get("network.subnet.a", "name")}]
            }
        ]
    )["Subnet"]
    subnet_c_info = ec2_client.create_subnet(
        CidrBlock=cp.get("network.subnet.c", "cidr"),
        VpcId=vpc_info["VpcId"],
        AvailabilityZone=cp.get("network.subnet.c", "az"),
        TagSpecifications=[
            {
                "ResourceType": "subnet",
                "Tags": [{"Key": "Name", "Value": cp.get("network.subnet.c", "name")}]
            }
        ]
    )["Subnet"]
    association_a = ec2_client.associate_route_table(
        RouteTableId=rt_info["RouteTableId"], 
        SubnetId=subnet_a_info["SubnetId"],
    )
    association_c = ec2_client.associate_route_table(
        RouteTableId=rt_info["RouteTableId"], 
        SubnetId=subnet_c_info["SubnetId"],
    )

    # Configuration
    logger.info("Add resource IDs into configuration file")
    network_resource_info = {
        "network.vpc": {
            "id": vpc_info["VpcId"],
            "igw_id": igw_info["InternetGatewayId"],
            "sg_id": sg_info["GroupId"],
        },
        "network.subnet.a": {
            "id": subnet_a_info["SubnetId"],
            "rt_id": rt_info["RouteTableId"],
            "rt_asc_id": association_a["AssociationId"]
        },
        "network.subnet.c": {
            "id": subnet_c_info["SubnetId"],
            "rt_id": rt_info["RouteTableId"],
            "rt_asc_id": association_c["AssociationId"]
        }
    }
    cp.read_dict(network_resource_info)
    with open(CONFIG_FILE_PATH, "w") as file:
        cp.write(file)


@app.command("create-cluster")
def create_redshift_cluster(
    db_password: str = typer.Argument(...)
):
    cp.read(CONFIG_FILE_PATH)
    assert cp.has_option("network.vpc", "id"), \
        "This process requires predefined subnets. Execute create-vpc command first."

    if cp.has_option("cluster", "id"):
        logger.info("Terminate process since Redshift cluster is already created")
        exit()

    session = boto3.Session(
        profile_name=cp.get("DEFAULT", "admin_profile"), 
        region_name=cp.get("DEFAULT", "region")
    )
    redshift_client = session.client("redshift")

    logger.info("Create subnet group")
    redshift_client.create_cluster_subnet_group(
        ClusterSubnetGroupName=cp.get("cluster.subnet.group", "name"),
        Description=cp.get("cluster.subnet.group", "desc"),
        SubnetIds=[
            cp.get("network.subnet.a", "id"),
            cp.get("network.subnet.c", "id")
        ]
    )
    
    logger.info("Create redshift cluster")
    response = redshift_client.create_cluster(
        ClusterIdentifier=cp.get("cluster", "identifier"),
        Port=cp.getint("cluster", "db_port"),
        DBName=cp.get("cluster", "db_name"),
        MasterUsername=cp.get("DEFAULT", "admin_profile"),
        MasterUserPassword=db_password,
        ClusterType="multi-node",
        NodeType=cp.get("cluster", "node_type"),
        NumberOfNodes=cp.getint("cluster", "node_count"),
        ClusterSubnetGroupName=cp.get("cluster.subnet.group", "name"),
        AvailabilityZone=cp.get("network.subnet.a", "az"),
        PubliclyAccessible=True,
        EnhancedVpcRouting=False,
        VpcSecurityGroupIds=[cp.get("network.vpc", "sg_id")],
    )
    
    logger.info("Waiting for cluster to be available")
    cluster_status = response["Cluster"]["ClusterStatus"]
    while cluster_status != "available":
        time.sleep(5)
        cluster_info = redshift_client.describe_clusters(
            ClusterIdentifier=cp.get("cluster", "identifier")
        )["Clusters"][0]
        cluster_status = cluster_info["ClusterStatus"]
    
    logger.info("Save DB host information in configuration file")
    cp["cluster"]["db_password"] = db_password
    cp["cluster"]["db_host"] = cluster_info["Endpoint"]["Address"]
    with open(CONFIG_FILE_PATH, "w") as file:
        cp.write(file)


@app.command("create-iam")
def create_iam_role():
    cp.read(CONFIG_FILE_PATH)
    if cp.has_option("iam.role", "arn"):
        logger.info("Terminate process since S3 read role for Redshift is already defined")
        exit()

    session = boto3.Session(
        profile_name=cp.get("DEFAULT", "admin_profile"), 
        region_name=cp.get("DEFAULT", "region")
    )
    iam_client = session.client("iam")

    logger.info("Create IAM role for Redshift cluster")
    trusted_entity = {
        "Version": "2012-10-17",
        "Statement": [
            {
                "Effect": "Allow",
                "Action": ["sts:AssumeRole"],
                "Principal": {"Service": ["redshift.amazonaws.com"]}
            }
        ]
    }
    role_info = iam_client.create_role(
        Path=f"/{cp.get('cluster', 'identifier')}/",
        RoleName=cp.get("iam.role", "name"),
        AssumeRolePolicyDocument=json.dumps(trusted_entity),
        Description="Allow Redshift to read S3 buckets"
    )["Role"]

    logger.info("Attach S3 read policy to created role")
    iam_client.attach_role_policy(
        RoleName=cp.get("iam.role", "name"),
        PolicyArn=cp.get("iam.role", "policy")
    )

    logger.info("Save ARN of created Redshift role into configuration file")
    cp["iam.role"]["arn"] = role_info["Arn"]
    with open(CONFIG_FILE_PATH, "w") as file:
        cp.write(file)


@app.command("delete-vpc")
def delete_vpc():
    cp.read(CONFIG_FILE_PATH)
    if not cp.has_option("network.vpc", "id"):
        logger.info("VPC to be deleted is not defined in configuration file")
        exit()
    session = boto3.Session(
        profile_name=cp.get("DEFAULT", "admin_profile"), 
        region_name=cp.get("DEFAULT", "region")
    )
    ec2_client = session.client("ec2")

    # Subnets
    logger.info("Disassociate route table from subnets and delete 2 subnets")
    ec2_client.disassociate_route_table(
        AssociationId=cp.get("network.subnet.a", "rt_asc_id")
    )
    ec2_client.disassociate_route_table(
        AssociationId=cp.get("network.subnet.c", "rt_asc_id")
    )
    ec2_client.delete_subnet(SubnetId=cp.get("network.subnet.a", "id"))
    ec2_client.delete_subnet(SubnetId=cp.get("network.subnet.c", "id"))

    # Route table
    logger.info("Delete route table")
    ec2_client.delete_route_table(
        RouteTableId=cp.get("network.subnet.a", "rt_id")
    )

    # Internet gateway
    logger.info("Detach internet gateway from VPC and delete it")
    ec2_client.detach_internet_gateway(
        InternetGatewayId=cp.get("network.vpc", "igw_id"), 
        VpcId=cp.get("network.vpc", "id")
    )
    ec2_client.delete_internet_gateway(
        InternetGatewayId=cp.get("network.vpc", "igw_id")
    )

    # Security group
    logger.info("Delete security group defined for current VPC")
    ec2_client.delete_security_group(GroupId=cp.get("network.vpc", "sg_id"))

    # VPC
    logger.info("Delete VPC")
    ec2_client.delete_vpc(VpcId=cp.get("network.vpc", "id"))

    # Configuration
    logger.info("Delete resource IDs from configuration file")
    cp.remove_section("network.vpc")
    cp.remove_section("network.subnet.a")
    cp.remove_section("network.subnet.c")
    with open(CONFIG_FILE_PATH, "w") as file:
        cp.write(file)


@app.command("delete-cluster")
def delete_redshift_cluster():
    cp.read(CONFIG_FILE_PATH)
    if not cp.has_option("cluster", "db_host"):
        logger.info("Cluster to be deleted is not defined in configuration file")
        exit()
    session = boto3.Session(
        profile_name=cp.get("DEFAULT", "admin_profile"), 
        region_name=cp.get("DEFAULT", "region")
    )
    redshift_client = session.client("redshift")

    # Cluster
    logger.info("Delete Redshift cluster")
    redshift_client.delete_cluster(
        ClusterIdentifier=cp.get("cluster", "identifier"),
        SkipFinalClusterSnapshot=True
    )
    logger.info("Waiting for cluster to be deleted")
    cluster_status = "deleting"
    while cluster_status == "deleting":
        time.sleep(5)
        try:
            redshift_client.describe_clusters(
                ClusterIdentifier=cp.get("cluster", "identifier")
            )
        except redshift_client.exceptions.ClusterNotFoundFault:
            cluster_status = "deleted" 
    
    # Cluster subnet group
    logger.info("Delete corresponding subnet group")
    redshift_client.delete_cluster_subnet_group(
        ClusterSubnetGroupName=cp.get("cluster.subnet.group", "name")
    )

    # Configuration
    logger.info("Delete resource IDs from configuration file")
    cp.remove_section("cluster")
    cp.remove_section("cluster.subnet.group")
    with open(CONFIG_FILE_PATH, "w") as file:
        cp.write(file)


@app.command("delete-iam")
def delete_iam():
    cp.read(CONFIG_FILE_PATH)
    if not cp.has_option("iam.role", "arn"):
        logger.info("IAM role to be deleted is not defined in configuration file")
        exit()

    session = boto3.Session(
        profile_name=cp.get("DEFAULT", "admin_profile"), 
        region_name=cp.get("DEFAULT", "region")
    )
    iam_client = session.client("iam")

    logger.info("Detach S3 read policy from IAM role")
    iam_client.detach_role_policy(
        RoleName=cp.get("iam.role", "name"),
        PolicyArn=cp.get("iam.role", "policy")
    )

    logger.info("Delete IAM role for Redshift S3 read access")
    iam_client.delete_role(RoleName=cp.get("iam.role", "name"))

    logger.info("Delete role ARN from configuration file")
    cp.remove_section("iam.role")
    with open(CONFIG_FILE_PATH, "w") as file:
        cp.write(file)


@app.command("delete-config")
def delete_config():
    logger.info("Delete configuration file from project folder")
    os.remove(CONFIG_FILE_PATH)


if __name__ == "__main__":
    app()