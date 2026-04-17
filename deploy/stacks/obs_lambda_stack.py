from constructs import Construct
from aws_cdk.aws_ecr_assets import Platform
from aws_cdk import (
    Stack,
    aws_lambda as _lambda,
    aws_iam as iam,
    aws_ec2 as ec2,
    aws_events as events,
    aws_events_targets as targets,
    aws_logs as logs,
    aws_sqs as sqs,
    aws_lambda_event_sources as event_sources,
    Duration,
)


class ObsLambdaStack(Stack):
    """
    CDK stack for the Germany WSV observation ingestion Lambda.

    Provisions:
    - Docker-based Lambda function (ARM64) running the obs handler
    - IAM role with least-privilege S3, EC2 networking, and Secrets Manager access
    - VPC placement via provided subnet IDs
    - Event source: CloudWatch rate-based schedule (default) or SQS queue trigger
    - CloudWatch log group with 1-month retention

    Required config keys:
        vpc_id, subnet_ids, s3_bucket_arns,
        obs_memory_mb, obs_timeout_min,
        obs_event_source          — "schedule" or "queue"
        obs_schedule_minutes      — (schedule mode) polling interval
        obs_queue_url             — (queue mode) full SQS queue URL
        obs_queue_region          — (queue mode) SQS queue region
        obs_queue_batch           — (queue mode, optional) batch size, default 1
        obs_concurrency           — reserved concurrency (None = unrestricted)
    """

    def __init__(self, scope: Construct, id: str, *, lambda_env: dict,
                 config: dict, ingest_name: str, display_name: str = None, **kwargs) -> None:
        super().__init__(scope, id,
                         description=f"{ingest_name} obs Lambda",
                         **kwargs)
        display_name = display_name or ingest_name

        # ── Networking ─────────────────────────────────────────
        vpc = ec2.Vpc.from_lookup(self, "Vpc",
                                  vpc_id=config["vpc_id"],
                                  is_default=False)
        subnet_filter = ec2.SubnetFilter.by_ids(config["subnet_ids"])

        # ── IAM role ───────────────────────────────────────────
        role = iam.Role(self, "ObsRole",
            assumed_by=iam.ServicePrincipal("lambda.amazonaws.com"),
            description=f"{ingest_name} observation Lambda execution role",
            managed_policies=[
                iam.ManagedPolicy.from_aws_managed_policy_name(
                    "service-role/AWSLambdaBasicExecutionRole"),
            ],
        )

        # S3 access — ingest bucket + cache bucket (seen_obs, station_meta, raw cache)
        role.add_to_policy(iam.PolicyStatement(
            sid="S3IngestBucketAccess",
            actions=[
                "s3:GetObject",
                "s3:PutObject",
                "s3:HeadObject",
                "s3:ListBucket",
                "s3:DeleteObject",
            ],
            resources=config["s3_bucket_arns"],
        ))

        # EC2 networking — required for Lambda in VPC
        role.add_to_policy(iam.PolicyStatement(
            sid="LambdaVpcNetworking",
            actions=[
                "ec2:CreateNetworkInterface",
                "ec2:DescribeNetworkInterfaces",
                "ec2:DeleteNetworkInterface",
                "ec2:CreateTags",
            ],
            resources=["*"],
        ))

        # Secrets Manager — for any API credentials stored as secrets
        role.add_to_policy(iam.PolicyStatement(
            sid="SecretsManagerReadAccess",
            actions=[
                "secretsmanager:GetSecretValue",
                "secretsmanager:DescribeSecret",
            ],
            resources=["*"],
        ))

        # ── Lambda function ────────────────────────────────────
        fn = _lambda.DockerImageFunction(self, "ObsFn",
            function_name=f"{display_name}ObsLambda",
            description=(
                f"Fetches current gauge measurements from Pegelonline and "
                f"ingests observations for the {display_name} network"
            ),
            memory_size=config["obs_memory_mb"],
            timeout=Duration.minutes(config["obs_timeout_min"]),
            reserved_concurrent_executions=config.get("obs_concurrency"),
            role=role,
            architecture=_lambda.Architecture.ARM_64,
            environment=lambda_env,
            tracing=_lambda.Tracing.ACTIVE,
            code=_lambda.DockerImageCode.from_image_asset(
                directory="../",
                file="deploy/Dockerfile",
                build_ssh="default",
                platform=Platform.LINUX_ARM64,
                exclude=[
                    "deploy/cdk.out",
                    "deploy/.venv",
                    "env",
                    ".venv",
                    "dev",
                    ".git",
                    "__pycache__",
                    "tests",
                    "legacy",
                ],
                cmd=["handlers.obs_handler.lambda_handler"],
            ),
            vpc=vpc,
            vpc_subnets=ec2.SubnetSelection(subnet_filters=[subnet_filter]),
            allow_public_subnet=True,
            log_group=logs.LogGroup(self, "ObsLogGroup",
                log_group_name=f"/aws/lambda/{display_name}ObsLambda",
                retention=logs.RetentionDays.ONE_MONTH,
            ),
        )

        # ── Event source ───────────────────────────────────────
        obs_event_source = config.get("obs_event_source", "schedule")

        if obs_event_source == "schedule":
            # Rate-based schedule — Pegelonline updates measurements every ~15 min
            rule = events.Rule(self, "ObsSchedule",
                rule_name=f"{display_name}ObsSchedule",
                description=(
                    f"Triggers {display_name} obs Lambda every "
                    f"{config['obs_schedule_minutes']} minute(s)"
                ),
                schedule=events.Schedule.rate(
                    Duration.minutes(config["obs_schedule_minutes"])
                ),
            )
            rule.add_target(targets.LambdaFunction(fn, retry_attempts=0))

        elif obs_event_source == "queue":
            # SQS-triggered mode — derive queue ARN from the provided URL
            queue_url = config["obs_queue_url"]
            queue_region = config["obs_queue_region"]
            # URL format: https://sqs.<region>.amazonaws.com/<account_id>/<queue_name>
            url_parts = queue_url.rstrip("/").split("/")
            account_id = url_parts[-2]
            queue_name = url_parts[-1]

            queue = sqs.Queue.from_queue_arn(self, "ObsQueue",
                queue_arn=f"arn:aws:sqs:{queue_region}:{account_id}:{queue_name}",
            )
            fn.add_event_source(event_sources.SqsEventSource(
                queue,
                batch_size=config.get("obs_queue_batch", 1),
            ))
            role.add_to_policy(iam.PolicyStatement(
                sid="SqsObsQueueAccess",
                actions=[
                    "sqs:ReceiveMessage",
                    "sqs:DeleteMessage",
                    "sqs:GetQueueAttributes",
                ],
                resources=[queue.queue_arn],
            ))

        else:
            raise ValueError(
                f"Unsupported obs_event_source: '{obs_event_source}'. "
                "Must be 'schedule' or 'queue'."
            )