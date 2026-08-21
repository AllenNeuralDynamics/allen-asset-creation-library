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

    # Each tuple is a group of interchangeable schema files. Exactly one
    # file from every group must be present in the results folder. The
    # ``rig``/``instrument`` and ``session``/``acquisition`` groups capture
    # the v1 -> v2 renames of those schemas.
    SCHEMA_FILE_GROUPS = (
        ("rig.json", "instrument.json"),
        ("session.json", "acquisition.json"),
        ("data_description.json",),
        ("procedures.json",),
        ("subject.json",),
    )

    # DocDB collection versions the metadata API actually serves. Schema major
    # versions below the minimum predate the v1 schemas and belong in the v1
    # collection; there is no v0 collection to route them to.
    MIN_COLLECTION_VERSION = 1
    MAX_COLLECTION_VERSION = 2

    def __init__(self, job_settings: JobSettings):
        """Class constructor"""
        self.job_settings = job_settings
        self.co_client = CodeOcean(
            domain=self.job_settings.codeocean_domain,
            token=self.job_settings.codeocean_token.get_secret_value(),
        )

    @cached_property
    def source_computation(self) -> Computation:
        """Fetch computation information from Code Ocean and cache it."""
        return self.co_client.computations.get_computation(
            self.job_settings.co_source_computation_id
        )

    @classmethod
    def _collection_version_from_schema(cls, schemas: dict) -> str:
        """
        Determine the DocDB collection version from the tracked schema files.

        Every group in :attr:`SCHEMA_FILE_GROUPS` must be represented by at
        least one present file, otherwise the metadata is incomplete and the
        collection cannot be determined. The collection version is the lowest
        schema major version found across the present files (major version
        ``N`` -> ``vN``): if any file is still on v1 the asset belongs in the
        v1 collection, and only when every file is on v2 does it belong in the
        v2 collection.

        A schema major version is not itself a collection version. Legacy
        metadata predating the v1 schemas reports major version 0 -- a stale
        ``rig.json`` at ``0.3.2`` alongside v1 files is the common case -- and
        that metadata belongs in the v1 collection, so majors below
        :attr:`MIN_COLLECTION_VERSION` are raised to it.

        Fails loudly when a required file is missing, a schema version is
        absent or cannot be parsed, or the resolved collection is one the
        metadata API does not serve, rather than guessing which collection the
        metadata belongs to.

        Parameters
        ----------
        schemas : dict
            Mapping of schema file name (e.g. ``"data_description.json"``) to
            its parsed contents. Only files present in the results folder
            should appear as keys.

        Returns
        -------
        str
            The DocDB collection version, e.g. ``"v2"``.

        Raises
        ------
        ValueError
            If a required schema file group has no present file, a present
            file is missing a ``schema_version`` or its major version cannot
            be parsed as an integer, or the resolved major version is above
            :attr:`MAX_COLLECTION_VERSION`.
        """
        missing = [
            " or ".join(group)
            for group in cls.SCHEMA_FILE_GROUPS
            if not any(name in schemas for name in group)
        ]
        if missing:
            raise ValueError(
                "Cannot determine the DocDB collection version; the results "
                "are missing required schema file(s): "
                f"{'; '.join(missing)}."
            )
        majors = []
        for name, contents in schemas.items():
            schema_version = contents.get("schema_version")
            if not schema_version:
                raise ValueError(
                    f"'{name}' is missing a 'schema_version'; cannot "
                    "determine the DocDB collection version."
                )
            major = str(schema_version).split(".")[0]
            if not major.isdigit():
                raise ValueError(
                    "Could not parse a major version from schema_version "
                    f"'{schema_version}' in '{name}'; cannot determine the "
                    "DocDB collection version."
                )
            majors.append(int(major))
        resolved = max(min(majors), cls.MIN_COLLECTION_VERSION)
        if resolved > cls.MAX_COLLECTION_VERSION:
            raise ValueError(
                f"Resolved DocDB collection version 'v{resolved}' from schema "
                f"majors {sorted(set(majors))}, but the metadata API serves "
                f"v{cls.MIN_COLLECTION_VERSION} through "
                f"v{cls.MAX_COLLECTION_VERSION}; update "
                "MAX_COLLECTION_VERSION once the API serves it."
            )
        return f"v{resolved}"

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

    def _list_result_file_names(self) -> set:
        """
        List the file names at the root of the computation results folder.

        Returns
        -------
        set
            The names of the files (not sub-folders) in the results folder.
        """
        folder = self.co_client.computations.list_computation_results(
            computation_id=self.job_settings.co_source_computation_id
        )
        return {item.name for item in folder.items if item.type == "file"}

    def _get_result_file(self, path: str) -> dict:
        """
        Download and parse a JSON file from the computation results folder.

        Parameters
        ----------
        path : str
            The path of the file within the results folder, e.g.
            ``"data_description.json"``.

        Returns
        -------
        dict
            The parsed JSON contents of the file.
        """
        file_urls = self.co_client.computations.get_result_file_urls(
            computation_id=self.job_settings.co_source_computation_id,
            path=path,
        )
        with urlopen(file_urls.download_url) as f:
            contents = f.read().decode("utf-8")
        return json.loads(contents)

    def _get_schemas(self) -> dict:
        """
        Download every tracked schema file present in the results folder.

        Returns
        -------
        dict
            Mapping of schema file name to its parsed contents, containing
            only the files from :attr:`SCHEMA_FILE_GROUPS` that are present.
        """
        present = self._list_result_file_names()
        tracked = {
            name for group in self.SCHEMA_FILE_GROUPS for name in group
        }
        return {
            name: self._get_result_file(name)
            for name in sorted(tracked)
            if name in present
        }

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

        data_description_tags = (
            set()
            if not data_description.get("tags")
            else set(data_description["tags"])
        )
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
        - Get the tracked schema files from the results folder
        - Determine the DocDB collection version from the schemas
        - Check if the s3 bucket and prefix already exists
        - Capture the results as a data asset
        - Register the data asset with DocDB
        - Update the asset permissions
        """
        try:
            self._check_pipeline_end_status()
            schemas = self._get_schemas()
            collection_version = self._collection_version_from_schema(schemas)
            data_description = schemas["data_description.json"]
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
                polling_interval=300,
                timeout=129600,
            )
            if data_asset.state != DataAssetState.Ready:
                raise Exception(f"Data asset creation failed! {data_asset}")
            self.co_client.data_assets.update_permissions(
                data_asset_id=data_asset.id,
                permissions=self.job_settings.asset_permissions,
            )
            docdb_client = MetadataDbClient(
                host=self.job_settings.docdb_host,
                version=collection_version,
            )
            docdb_response = docdb_client.register_asset(
                s3_location=f"s3://{s3_bucket}/{s3_prefix}"
            )
            logger.info(docdb_response)
            logger.info("Finished capturing asset!")
        except Exception as e:
            self._send_notification(e)
            raise e
