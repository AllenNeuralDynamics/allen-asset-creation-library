"""Automation capsule job"""

import json
import logging
import os
from functools import cached_property
from urllib.request import urlopen

import boto3
from aind_data_access_api.document_db import MetadataDbClient
from codeocean import CodeOcean
from codeocean.components import (
    EveryoneRole,
    GroupPermissions,
    GroupRole,
    Permissions,
)
from codeocean.computation import Computation, ComputationEndStatus
from codeocean.data_asset import (
    AWSS3Target,
    ComputationSource,
    DataAssetParams,
    DataAssetState,
    Source,
    Target,
)
from pydantic import AliasChoices, Field, SecretStr
from pydantic_settings import BaseSettings

logger = logging.getLogger(__name__)
logger.setLevel(os.getenv("LOG_LEVEL", "INFO"))


class JobSettings(BaseSettings):
    """Job Settings"""

    codeocean_token: SecretStr = Field(
        ...,
        description="Code Ocean token.",
        validation_alias=AliasChoices(
            "codeocean_token", "CODEOCEAN_TOKEN", "CUSTOM_KEY"
        ),
    )
    codeocean_domain: str = Field(
        default="https://codeocean.allenneuraldynamics.org",
        description="CodeOcean domain.",
    )
    docdb_host: str = Field(
        default="api.allenneuraldynamics.org",
        description="Host name for DocDB API Gateway",
    )
    docdb_collection_version: str = Field(
        default="v1", description="Version of Metadata Index"
    )
    destination_bucket: str = Field(
        ..., description="S3 bucket to capture results to."
    )
    asset_permissions: Permissions = Field(
        default=Permissions(
            everyone=EveryoneRole.Viewer,
            groups=[
                GroupPermissions(
                    group="AIND Data Administrators", role=GroupRole.Owner
                )
            ],
        ),
        description="Add additional users and groups.",
    )
    co_source_computation_id: str = Field(
        ...,
        description=(
            "The id for the pipeline that is triggering the automation. "
            "It is passed into the capsule as an env var."
        ),
    )
    co_source_exit_code: int = Field(
        ...,
        description=(
            "The exit code of the pipeline that is triggering the automation. "
            "It is passed into the capsule as an env var."
        ),
    )


class CaptureResultsJob:
    """Job to capture results and register the data asset"""

    def __init__(self, job_settings: JobSettings):
        """Class constructor"""
        self.job_settings = job_settings
        self.co_client = CodeOcean(
            domain=self.job_settings.codeocean_domain,
            token=self.job_settings.codeocean_token.get_secret_value(),
        )
        self.docdb_client = MetadataDbClient(
            host=self.job_settings.docdb_host,
            version=self.job_settings.docdb_collection_version,
        )

    @cached_property
    def source_computation(self) -> Computation:
        """Fetch computation information from Code Ocean and cache it."""
        return self.co_client.computations.get_computation(
            self.job_settings.co_source_computation_id
        )

    def _check_pipeline_end_status(self):
        """Checks if the pipeline finished successfully."""
        src_computation_id = self.job_settings.co_source_computation_id
        src_computation_exit_code = self.job_settings.co_source_exit_code
        if src_computation_exit_code != 0:
            raise Exception(
                f"The input computation: {src_computation_id} has an error! "
                f"Error code: {src_computation_exit_code}"
            )
        end_status = self.source_computation.end_status
        if end_status == ComputationEndStatus.Failed:
            raise Exception(
                f"The input computation: {src_computation_id} has an error! "
                f"End Status: {end_status}"
            )
        if end_status == ComputationEndStatus.Stopped:
            raise Exception(
                f"The input computation: {src_computation_id} was stopped! "
                f"End Status: {end_status}"
            )
        else:
            logger.info(
                f"The input computation {src_computation_id} finished without "
                f"a reported error. "
                f"End Status: {end_status}."
            )

    def _get_data_description(self) -> dict:
        """Download the data description file from the results folder."""
        file_urls = self.co_client.computations.get_result_file_urls(
            computation_id=self.job_settings.co_source_computation_id,
            path="data_description.json",
        )
        with urlopen(file_urls.download_url) as f:
            contents = f.read().decode("utf-8")
        data_description = json.loads(contents)
        return data_description

    @staticmethod
    def _check_if_target_already_exists(bucket: str, prefix: str) -> bool:
        """Check if the s3 bucket and prefix already exists."""
        s3 = boto3.client("s3")
        response = s3.list_objects_v2(
            Bucket=bucket, Prefix=prefix + "/", MaxKeys=1
        )
        return response.get("KeyCount", 0) > 0

    def _capture_results(self, data_description: dict) -> str:
        """Capture the pipeline run results as a data asset."""
        default_tags = set()
        custom_metadata = dict()
        subject_id = data_description.get("subject_id")
        data_level = data_description.get("data_level")
        name = data_description["name"].strip("/")
        if subject_id is not None:
            default_tags.add(subject_id)
            custom_metadata["subject id"] = subject_id
        if data_level is not None:
            default_tags.add(data_level)
            custom_metadata["data level"] = data_level
        data_description_tags = set(data_description.get("tags", []))
        tags = list(default_tags.union(data_description_tags))
        tags.sort()
        source = Source(
            computation=ComputationSource(
                id=self.job_settings.co_source_computation_id
            )
        )
        target = Target(
            aws=AWSS3Target(
                bucket=self.job_settings.destination_bucket, prefix=name
            )
        )
        asset_params = DataAssetParams(
            name=data_description["name"],
            mount=data_description["name"],
            tags=tags,
            description=data_description.get("data_summary"),
            custom_metadata=custom_metadata,
            source=source,
            target=target,
        )
        data_asset = self.co_client.data_assets.create_data_asset(
            data_asset_params=asset_params
        )
        return data_asset

    @staticmethod
    def _send_notification(e: Exception):
        """Send a notification if an error occurs."""
        logger.exception(e)

    def run_job(self):
        """
        Main job runner.
        - Checks pipeline status
        - Get the data description from the results folder
        - Check if the s3 bucket and prefix already exists
        - Capture the results as a data asset
        - Register the data asset with DocDB
        - Update the asset permissions
        """
        try:
            self._check_pipeline_end_status()
            data_description = self._get_data_description()
            s3_bucket = self.job_settings.destination_bucket
            s3_prefix = data_description["name"].strip("/")
            if self._check_if_target_already_exists(
                bucket=s3_bucket, prefix=s3_prefix
            ):
                raise FileExistsError(
                    f"S3 Target s3://{s3_bucket}/{s3_prefix} already exists!"
                )
            captured_data_asset = self._capture_results(
                data_description=data_description
            )
            data_asset = self.co_client.data_assets.wait_until_ready(
                data_asset=captured_data_asset,
                polling_interval=10,
                timeout=600,
            )
            if data_asset.state != DataAssetState.Ready:
                raise Exception(f"Data asset creation failed! {data_asset}")
            self.co_client.data_assets.update_permissions(
                data_asset_id=data_asset.id,
                permissions=self.job_settings.asset_permissions,
            )
            docdb_response = self.docdb_client.register_asset(
                s3_location=f"s3://{s3_bucket}/{s3_prefix}"
            )
            logger.info(docdb_response)
            logger.info("Finished capturing asset!")
        except Exception as e:
            self._send_notification(e)
            raise e
