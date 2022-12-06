import os
import logging
import boto3

from configparser import ConfigParser


def create_vpc(
    parser: ConfigParser, 
    logger: logging.Logger, 
    session: boto3.Session
) -> ConfigParser:
    """
    create VPC and corresponding subnets to create clusters within
    """
    ec2_client = session.client("ec2")

    # VPC
    logger.info("Create VPC")
    vpc_info = ec2_client.create_vpc(
        CidrBlock=parser.get("network.vpc", "cidr"),
        TagSpecifications=[
            {
                "ResourceType": "vpc", 
                "Tags":[{"Key": "Name", "Value": parser.get("network.vpc", "name")}]
            }
        ]
    )["Vpc"]

    # Security group
    logger.info("Create security group with ingress policy")
    sg_info = ec2_client.create_security_group(
        GroupName=parser.get("network.vpc", "sg_name"),
        Description="traffic rules over Redshift cluster",
        VpcId=vpc_info["VpcId"]
    )
    ec2_client.authorize_security_group_ingress(
        CidrIp="0.0.0.0/0",  # better to allow connection by predefined VPN, but skip for simplicity
        IpProtocol="tcp",
        GroupId=sg_info["GroupId"],
        FromPort=parser.getint("cluster", "db_port"),
        ToPort=parser.getint("cluster", "db_port")
    )

    # Internet gateway
    logger.info("Create internet gateway and attach it to VPC")
    igw_info = ec2_client.create_internet_gateway(
        TagSpecifications=[
            {
                "ResourceType": "internet-gateway",
                "Tags":[{"Key": "Name", "Value": parser.get("network.vpc", "igw_name")}]
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
                "Tags":[{"Key": "Name", "Value": parser.get("network.subnet.a", "rt_name")}]
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
        CidrBlock=parser.get("network.subnet.a", "cidr"),
        VpcId=vpc_info["VpcId"],
        AvailabilityZone=parser.get("network.subnet.a", "az"),
        TagSpecifications=[
            {
                "ResourceType": "subnet",
                "Tags": [{"Key": "Name", "Value": parser.get("network.subnet.a", "name")}]
            }
        ]
    )["Subnet"]
    subnet_c_info = ec2_client.create_subnet(
        CidrBlock=parser.get("network.subnet.c", "cidr"),
        VpcId=vpc_info["VpcId"],
        AvailabilityZone=parser.get("network.subnet.c", "az"),
        TagSpecifications=[
            {
                "ResourceType": "subnet",
                "Tags": [{"Key": "Name", "Value": parser.get("network.subnet.c", "name")}]
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
    parser.read_dict(network_resource_info)
    
    return parser


def delete_vpc( 
    parser: ConfigParser, 
    logger: logging.Logger, 
    session: boto3.Session
) -> None:
    """
    delete subnets and VPC generated to initiate Redshift cluster
    """
    ec2_client = session.client("ec2")

    # Subnets
    logger.info("Disassociate route table from subnets and delete 2 subnets")
    ec2_client.disassociate_route_table(
        AssociationId=parser.get("network.subnet.a", "rt_asc_id")
    )
    ec2_client.disassociate_route_table(
        AssociationId=parser.get("network.subnet.c", "rt_asc_id")
    )
    ec2_client.delete_subnet(SubnetId=parser.get("network.subnet.a", "id"))
    ec2_client.delete_subnet(SubnetId=parser.get("network.subnet.c", "id"))

    # Route table
    logger.info("Delete route table")
    ec2_client.delete_route_table(
        RouteTableId=parser.get("network.subnet.a", "rt_id")
    )

    # Internet gateway
    logger.info("Detach internet gateway from VPC and delete it")
    ec2_client.detach_internet_gateway(
        InternetGatewayId=parser.get("network.vpc", "igw_id"), 
        VpcId=parser.get("network.vpc", "id")
    )
    ec2_client.delete_internet_gateway(
        InternetGatewayId=parser.get("network.vpc", "igw_id")
    )

    # Security group
    logger.info("Delete security group defined for current VPC")
    ec2_client.delete_security_group(GroupId=parser.get("network.vpc", "sg_id"))

    # VPC
    logger.info("Delete VPC")
    ec2_client.delete_vpc(VpcId=parser.get("network.vpc", "id"))
