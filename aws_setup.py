import boto3
import os

from configparser import ConfigParser
from typer import Typer
from logger import make_logger


CONFIG_FILE_PATH = f"{os.getcwd()}/dwh.cfg"
CONFIG = {
    "DEFAULT": {
        "admin_profile": "admin.kim",
        "region": "ap-northeast-2",
        "dw_port": 5439,
    },
    "s3": {
        "log_data": "s3://udacity-dend/log-data",
        "log_jsonpath": "s3://udacity-dend/log_json_path.json",
        "song_data": "s3://udacity-dend/song-data",
    },
    "network.vpc": {
        "cidr": "172.10.0.0/16",
        "name": "prod",
        "igw_name": "prod-igw",
        "sg_name": "redshift",
    },
    "network.subnet": {
        "cidr": "172.10.100.0/24",
        "name": "dw-pub-c",
        "rt_name": "rt-pub-c",
        "az": "%(region)sc",
    },
}
app = Typer()
config = ConfigParser()
logger = make_logger(__name__)


@app.command("create-config")
def create_config():
    if os.path.exists(CONFIG_FILE_PATH):
        os.remove(CONFIG_FILE_PATH)
    config.read_dict(CONFIG)
    with open(CONFIG_FILE_PATH, "w") as file:
        config.write(file)

    logger.info(f"Successfully created configuration file in working directory")


@app.command("create-vpc")
def create_vpc():
    if not os.path.exists(CONFIG_FILE_PATH):
        raise FileNotFoundError(
            "Configuration file not found. Execute create-config command in advance."
        )
    else:        
        config.read(CONFIG_FILE_PATH)
        if config.has_option("network.vpc", "id"):
            logger.info("Terminate process since VPC is already created")
            exit()

    session = boto3.Session(
        profile_name=config.get("DEFAULT", "admin_profile"), 
        region_name=config.get("DEFAULT", "region")
    )
    ec2_client = session.client("ec2")

    logger.info("Create VPC and attach internet gateway")
    vpc_info = ec2_client.create_vpc(
        CidrBlock=config.get("network.vpc", "cidr"),
        TagSpecifications=[
            {
                "ResourceType": "vpc", 
                "Tags":[{"Key": "Name", "Value": config.get("network.vpc", "name")}]
            }
        ]
    )["Vpc"]
    igw_info = ec2_client.create_internet_gateway(
        TagSpecifications=[
            {
                "ResourceType": "internet-gateway",
                "Tags":[{"Key": "Name", "Value": config.get("network.vpc", "igw_name")}]
            }
        ]
    )["InternetGateway"]
    ec2_client.attach_internet_gateway(
        InternetGatewayId=igw_info["InternetGatewayId"], 
        VpcId=vpc_info["VpcId"]
    )

    logger.info("Create security group and traffic ingress policy")
    sg_info = ec2_client.create_security_group(
        GroupName=config.get("network.vpc", "sg_name"),
        Description="traffic rules over Redshift cluster",
        VpcId=vpc_info["VpcId"]
    )
    ec2_client.authorize_security_group_ingress(
        CidrIp="0.0.0.0/0",  # better to allow connection by predefined VPN, but skip for simplicity
        IpProtocol="tcp",
        GroupId=sg_info["GroupId"],
        FromPort=config.getint("DEFAULT", "dw_port"),
        ToPort=config.getint("DEFAULT", "dw_port")
    )

    logger.info("Create subnet and attach route table")
    subnet_info = ec2_client.create_subnet(
        CidrBlock=config.get("network.subnet", "cidr"),
        VpcId=vpc_info["VpcId"],
        AvailabilityZone=config.get("network.subnet", "az"),
        TagSpecifications=[
            {
                "ResourceType": "subnet",
                "Tags": [{"Key": "Name", "Value": config.get("network.subnet", "name")}]
            }
        ]
    )["Subnet"]
    rt_info = ec2_client.create_route_table(
        VpcId=vpc_info["VpcId"],
        TagSpecifications=[
            {
                "ResourceType": "route-table", 
                "Tags":[{"Key": "Name", "Value": config.get("network.subnet", "rt_name")}]
            }
        ]
    )["RouteTable"]

    logger.info("Associate route table to subnet and create route to internet gateway")
    associtaion = ec2_client.associate_route_table(
        RouteTableId=rt_info["RouteTableId"], 
        SubnetId=subnet_info["SubnetId"],
    )
    ec2_client.create_route(
        DestinationCidrBlock="0.0.0.0/0",
        GatewayId=igw_info["InternetGatewayId"],
        RouteTableId=rt_info["RouteTableId"]
    )

    logger.info("Add resource IDs into configuration file")
    network_resource_info = {
        "network.vpc": {
            "id": vpc_info["VpcId"],
            "igw_id": igw_info["InternetGatewayId"],
            "sg_id": sg_info["GroupId"],
        },
        "network.subnet": {
            "id": subnet_info["SubnetId"],
            "rt_id": rt_info["RouteTableId"],
            "rt_asc_id": associtaion["AssociationId"]
        }
    }
    config.read_dict(network_resource_info)
    with open(CONFIG_FILE_PATH, "w") as file:
        config.write(file)


@app.command("create-cluster")
def create_redshift_cluster():
    pass


@app.command("create-iam")
def create_iam():
    pass


@app.command("delete-vpc")
def delete_vpc():
    config.read(CONFIG_FILE_PATH)
    if not config.has_option("network.vpc", "id"):
        logger.info("VPC to be deleted is not defined in configuration file")
        exit()
    session = boto3.Session(
        profile_name=config.get("DEFAULT", "admin_profile"), 
        region_name=config.get("DEFAULT", "region")
    )
    ec2_client = session.client("ec2")

    logger.info("Disassociate route table from subnet")
    ec2_client.disassociate_route_table(
        AssociationId=config.get("network.subnet", "rt_asc_id")
    )

    logger.info("Delete route table and corresponding subnet")
    ec2_client.delete_route_table(
        RouteTableId=config.get("network.subnet", "rt_id")
    )
    ec2_client.delete_subnet(SubnetId=config.get("network.subnet", "id"))

    logger.info("Delete security group defined for current VPC")
    ec2_client.delete_security_group(GroupId=config.get("network.vpc", "sg_id"))

    logger.info("Detach internet gateway and delete VPC")
    ec2_client.detach_internet_gateway(
        InternetGatewayId=config.get("network.vpc", "igw_id"), 
        VpcId=config.get("network.vpc", "id")
    )
    ec2_client.delete_internet_gateway(
        InternetGatewayId=config.get("network.vpc", "igw_id")
    )    
    ec2_client.delete_vpc(VpcId=config.get("network.vpc", "id"))

    logger.info("Delete resource IDs from configuration file")
    config.remove_option("network.vpc", "id")
    config.remove_option("network.vpc", "igw_id")
    config.remove_option("network.vpc", "sg_id")
    config.remove_option("network.subnet", "id")
    config.remove_option("network.subnet", "rt_id")
    config.remove_option("network.subnet", "rt_asc_id")
    with open(CONFIG_FILE_PATH, "w") as file:
        config.write(file)


if __name__ == "__main__":
    app()