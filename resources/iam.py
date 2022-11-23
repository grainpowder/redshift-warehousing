import logging
import boto3
import json

from configparser import ConfigParser


def create_iam_role(
    parser: ConfigParser, 
    logger: logging.Logger, 
    session: boto3.Session,
):
    iam_client = session.client("iam")

    logger.info("Create IAM role for Redshift cluster")
    # Reference: https://docs.aws.amazon.com/IAM/latest/UserGuide/reference_policies_elements_principal.html
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
        Path=f"/{parser.get('cluster', 'identifier')}/",
        RoleName=parser.get("iam.role", "name"),
        AssumeRolePolicyDocument=json.dumps(trusted_entity),
        Description="Allow Redshift to read S3 buckets"
    )["Role"]

    logger.info("Attach S3 read policy to created role")
    iam_client.attach_role_policy(
        RoleName=parser.get("iam.role", "name"),
        PolicyArn=parser.get("iam.role", "policy")
    )

    logger.info("Save ARN of created Redshift role into configuration file")
    parser["iam.role"]["arn"] = role_info["Arn"]
    
    return parser


def delete_iam_role(
    parser: ConfigParser, 
    logger: logging.Logger, 
    session: boto3.Session,
) -> None:
    iam_client = session.client("iam")

    logger.info("Detach S3 read policy from IAM role")
    iam_client.detach_role_policy(
        RoleName=parser.get("iam.role", "name"),
        PolicyArn=parser.get("iam.role", "policy")
    )

    logger.info("Delete IAM role for Redshift S3 read access")
    iam_client.delete_role(RoleName=parser.get("iam.role", "name"))
