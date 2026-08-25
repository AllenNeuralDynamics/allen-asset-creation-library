"""Module to test classes and methods in job module"""

import json
import os
import unittest
from copy import deepcopy
from pathlib import Path
from unittest.mock import MagicMock, PropertyMock, patch

from codeocean.components import (
    EveryoneRole,
    Permissions,
    UserPermissions,
    UserRole,
)
from codeocean.computation import (
    Computation,
    ComputationEndStatus,
)
from codeocean.data_asset import (
    AWSS3Target,
    ComputationSource,
    DataAsset,
    DataAssetParams,
    Source,
    Target,
)
from pydantic import SecretStr

from allen_asset_creation_library.job import CaptureResultsJob, JobSettings

RESOURCES_DIR = Path(os.path.dirname(os.path.realpath(__file__))) / "resources"


class TestJobSettings(unittest.TestCase):
    """Test the JobSettings class"""

    @patch.dict(
        os.environ,
        {
            "CO_SOURCE_COMPUTATION_ID": "123-456",
            "CO_SOURCE_EXIT_CODE": "0",
            "CUSTOM_KEY": "abc-def",
            "CODEOCEAN_DOMAIN": "https://example.com",
            "DOCDB_HOST": "example.com",
            "DESTINATION_BUCKET": "example",
            "ASSET_PERMISSIONS": (
                '{"users": [{"email": "abc@example.com", '
                '"role": "owner"}], "everyone": "viewer"}'
            ),
        },
        clear=True,
    )
    def test_job_settings_env_vars(self):
        """Tests that job settings are configured correctly"""

        job_settings = JobSettings()
        expected_settings = JobSettings(
            codeocean_token=SecretStr("abc-def"),
            codeocean_domain="https://example.com",
            docdb_host="example.com",
            destination_bucket="example",
            asset_permissions=Permissions(
                users=[
                    UserPermissions(
                        email="abc@example.com", role=UserRole.Owner
                    )
                ],
                everyone=EveryoneRole.Viewer,
            ),
            co_source_computation_id="123-456",
            co_source_exit_code=0,
        )
        self.assertEqual(expected_settings, job_settings)


class TestCaptureResultsJob(unittest.TestCase):
    """Tests methods in CaptureResultsJob class"""

    @classmethod
    def setUpClass(cls) -> None:
        """Set up the class with common resources"""
        base_job_settings = {
            "codeocean_token": "abc-123",
            "codeocean_domain": "https://example.com",
            "docdb_host": "example.com",
            "destination_bucket": "example",
            "asset_permissions": {
                "users": [{"email": "abc@example.com", "role": "owner"}],
                "everyone": "viewer",
            },
            "co_source_computation_id": "123-456",
            "co_source_exit_code": 0,
        }
        base_input_computation = {
            "id": "123-456",
            "created": 1769559830,
            "name": "Run 0000001",
            "run_time": 20,
            "owner": "Person 1",
            "state": "completed",
            "processes": [{"name": "test_capsule", "capsule_id": "00a"}],
            "end_status": "succeeded",
            "exit_code": 0,
            "has_results": True,
        }
        with open(RESOURCES_DIR / "example_data_description.json", "r") as f:
            base_data_description = json.load(f)
        with open(
            RESOURCES_DIR / "example_capture_result_response.json", "r"
        ) as f:
            base_capture_result_response = json.load(f)
        base_wait_until_ready_response = deepcopy(base_capture_result_response)
        base_wait_until_ready_response["state"] = "ready"
        failed_job_settings = deepcopy(base_job_settings)
        failed_job_settings["co_source_exit_code"] = 1
        failed_input_computation = deepcopy(base_input_computation)
        failed_input_computation["end_status"] = ComputationEndStatus.Failed
        failed_input_computation["exit_code"] = 1
        stopped_job_settings = deepcopy(base_job_settings)
        stopped_input_computation = deepcopy(base_input_computation)
        stopped_input_computation["end_status"] = ComputationEndStatus.Stopped
        cls.co_patcher = patch("allen_asset_creation_library.job.CodeOcean")
        cls.mock_codeocean_client = cls.co_patcher.start()
        (
            cls.mock_codeocean_client.return_value.computations.get_computation
        ).return_value = Computation(**base_input_computation)
        (
            cls.mock_codeocean_client.return_value.computations
        ).get_result_file_download_url.return_value = MagicMock(
            url="https://example.com/data.json"
        )
        (
            cls.mock_codeocean_client.return_value.data_assets
        ).create_data_asset.return_value = DataAsset(
            **base_capture_result_response
        )
        cls.source_computation = Computation(**base_input_computation)
        cls.job = CaptureResultsJob(
            job_settings=JobSettings(**base_job_settings)
        )
        cls.failed_source_computation = Computation(**failed_input_computation)
        cls.failed_job = CaptureResultsJob(
            job_settings=JobSettings(**failed_job_settings)
        )
        cls.stopped_source_computation = Computation(
            **stopped_input_computation
        )
        cls.stopped_job = CaptureResultsJob(
            job_settings=JobSettings(**stopped_job_settings)
        )
        cls.data_description = base_data_description
        # A complete set of v2 schema files: one file per required group,
        # each on schema major version 2.
        cls.base_schemas = {
            "instrument.json": {"schema_version": "2.0.0"},
            "acquisition.json": {"schema_version": "2.0.0"},
            "data_description.json": base_data_description,
            "procedures.json": {"schema_version": "2.0.0"},
            "subject.json": {"schema_version": "2.0.0"},
        }
        cls.base_wait_until_ready_response = base_wait_until_ready_response
        cls.base_capture_result_response = base_capture_result_response

    @classmethod
    def tearDownClass(cls):
        """Stop the patchers."""
        cls.co_patcher.stop()

    def test_check_pipeline_end_status(self):
        """Tests _check_pipeline_end_status method."""
        with self.assertLogs(level="INFO") as captured:
            self.job._check_pipeline_end_status()
        self.assertEqual(1, len(captured.output))
        self.assertIn("End Status: succeeded.", captured.output[0])

    @patch(
        "allen_asset_creation_library.job.CaptureResultsJob"
        ".source_computation",
        new_callable=PropertyMock,
    )
    def test_check_pipeline_end_failed(
        self, mock_source_computation: MagicMock
    ):
        """Tests _check_pipeline_end_status method when pipeline failed."""
        mock_source_computation.return_value = self.failed_source_computation
        with self.assertRaises(Exception) as e:
            self.failed_job._check_pipeline_end_status()
        self.assertIn("Error code: 1", str(e.exception))

    @patch(
        "allen_asset_creation_library.job.CaptureResultsJob"
        ".source_computation",
        new_callable=PropertyMock,
    )
    def test_check_pipeline_end_failed_edge_case(
        self, mock_source_computation: MagicMock
    ):
        """
        Tests _check_pipeline_end_status method when pipeline failed but exit
        code is zero for some reason.
        """
        mock_source_computation.return_value = self.failed_source_computation
        with self.assertRaises(Exception) as e:
            self.job._check_pipeline_end_status()
        self.assertIn("End Status: failed", str(e.exception))

    @patch(
        "allen_asset_creation_library.job.CaptureResultsJob"
        ".source_computation",
        new_callable=PropertyMock,
    )
    def test_check_pipeline_end_stopped(
        self, mock_source_computation: MagicMock
    ):
        """Tests _check_pipeline_end_status method when pipeline stopped."""
        mock_source_computation.return_value = self.stopped_source_computation
        with self.assertRaises(Exception) as e:
            self.stopped_job._check_pipeline_end_status()
        self.assertIn("End Status: stopped", str(e.exception))

    @patch("allen_asset_creation_library.job.urlopen")
    def test_get_result_file(
        self,
        mock_urlopen: MagicMock,
    ):
        """Tests _get_result_file method."""

        (
            mock_urlopen.return_value.__enter__.return_value.read
        ).return_value = b'{"key": "value"}'
        contents = self.job._get_result_file("data_description.json")
        self.assertEqual({"key": "value"}, contents)

    def test_list_result_file_names(self):
        """Tests _list_result_file_names filters to files only."""
        folder = MagicMock()
        folder.items = [
            MagicMock(name="data_description.json", type="file"),
            MagicMock(name="subject.json", type="file"),
            MagicMock(name="a_subfolder", type="folder"),
        ]
        # MagicMock's ``name`` kwarg is special, so set it explicitly.
        folder.items[0].name = "data_description.json"
        folder.items[1].name = "subject.json"
        folder.items[2].name = "a_subfolder"
        (
            self.mock_codeocean_client.return_value.computations
        ).list_computation_results.return_value = folder
        self.assertEqual(
            {"data_description.json", "subject.json"},
            self.job._list_result_file_names(),
        )

    @patch(
        "allen_asset_creation_library.job.CaptureResultsJob._get_result_file"
    )
    @patch(
        "allen_asset_creation_library.job.CaptureResultsJob"
        "._list_result_file_names"
    )
    def test_get_schemas(
        self,
        mock_list_result_file_names: MagicMock,
        mock_get_result_file: MagicMock,
    ):
        """Downloads only the tracked schema files that are present."""
        mock_list_result_file_names.return_value = {
            "instrument.json",
            "acquisition.json",
            "data_description.json",
            "procedures.json",
            "subject.json",
            "processing.json",  # not a tracked schema; should be ignored
        }
        mock_get_result_file.return_value = {"schema_version": "2.0.0"}
        schemas = self.job._get_schemas()
        self.assertEqual(
            {
                "instrument.json",
                "acquisition.json",
                "data_description.json",
                "procedures.json",
                "subject.json",
            },
            set(schemas),
        )

    def test_collection_version_from_schema(self):
        """All v2 schemas map to the v2 DocDB collection."""
        self.assertEqual(
            "v2",
            self.job._collection_version_from_schema(self.base_schemas),
        )

    def test_collection_version_from_schema_any_v1(self):
        """A single v1 schema forces the v1 DocDB collection."""
        schemas = deepcopy(self.base_schemas)
        # Swap in the v1 file names/versions for one group.
        del schemas["acquisition.json"]
        schemas["session.json"] = {"schema_version": "1.0.0"}
        self.assertEqual(
            "v1",
            self.job._collection_version_from_schema(schemas),
        )

    def test_collection_version_from_schema_legacy(self):
        """A legacy 0.x schema maps to the v1 collection, not v0."""
        schemas = deepcopy(self.base_schemas)
        # Swap in the pre-v1 rig file for the instrument group.
        del schemas["instrument.json"]
        schemas["rig.json"] = {"schema_version": "0.3.2"}
        self.assertEqual(
            "v1",
            self.job._collection_version_from_schema(schemas),
        )

    def test_collection_version_from_schema_all_legacy(self):
        """Metadata entirely on 0.x schemas maps to the v1 collection."""
        schemas = {
            name: {"schema_version": "0.3.2"} for name in self.base_schemas
        }
        self.assertEqual(
            "v1",
            self.job._collection_version_from_schema(schemas),
        )

    def test_collection_version_from_schema_required_missing(self):
        """Fails loudly when a required schema file group is missing."""
        schemas = deepcopy(self.base_schemas)
        del schemas["procedures.json"]
        with self.assertRaises(ValueError) as e:
            self.job._collection_version_from_schema(schemas)
        self.assertIn("missing required schema file(s)", str(e.exception))
        self.assertIn("procedures.json", str(e.exception))

    def test_collection_version_from_schema_alternate_missing(self):
        """Fails when neither file in an interchangeable group is present."""
        schemas = deepcopy(self.base_schemas)
        del schemas["instrument.json"]
        with self.assertRaises(ValueError) as e:
            self.job._collection_version_from_schema(schemas)
        self.assertIn("rig.json or instrument.json", str(e.exception))

    def test_collection_version_from_schema_version_missing(self):
        """Fails loudly when a present file has no schema_version."""
        schemas = deepcopy(self.base_schemas)
        schemas["subject.json"] = {}
        with self.assertRaises(ValueError) as e:
            self.job._collection_version_from_schema(schemas)
        self.assertIn("missing a 'schema_version'", str(e.exception))
        self.assertIn("subject.json", str(e.exception))

    def test_collection_version_from_schema_malformed(self):
        """Fails loudly when a schema_version cannot be parsed."""
        schemas = deepcopy(self.base_schemas)
        schemas["subject.json"] = {"schema_version": "not-a-version"}
        with self.assertRaises(ValueError) as e:
            self.job._collection_version_from_schema(schemas)
        self.assertIn("Could not parse a major version", str(e.exception))

    @patch("allen_asset_creation_library.job.boto3.client")
    def test_check_if_target_already_exists_true(
        self, mock_boto_client: MagicMock
    ):
        """Tests _check_if_target_already_exists method when True."""
        mock_s3_client = MagicMock()
        mock_boto_client.return_value = mock_s3_client
        mock_response = {
            "Contents": [
                {"Key": "data_description.json", "Size": 100},
            ],
            "KeyCount": 1,
            "ResponseMetadata": {"HTTPStatusCode": 200},
        }
        mock_s3_client.list_objects_v2.return_value = mock_response
        s3_check = self.job._check_if_target_already_exists(
            bucket="example", prefix="prefix"
        )
        self.assertTrue(s3_check)

    @patch("allen_asset_creation_library.job.boto3.client")
    def test_check_if_target_already_exists_false(
        self, mock_boto_client: MagicMock
    ):
        """Tests _check_if_target_already_exists method when False."""
        mock_s3_client = MagicMock()
        mock_boto_client.return_value = mock_s3_client
        mock_response = {
            "Contents": [],
            "KeyCount": 0,
            "ResponseMetadata": {"HTTPStatusCode": 200},
        }
        mock_s3_client.list_objects_v2.return_value = mock_response
        s3_check = self.job._check_if_target_already_exists(
            bucket="example", prefix="prefix"
        )
        self.assertFalse(s3_check)

    def test_capture_results(self):
        """Tests _capture_results method when successful."""

        data_description = deepcopy(self.data_description)
        self.job._capture_results(data_description=data_description)
        asset_name = "123456_2025-01-28_16-48-50_processed_2025-01-29_13-04-34"
        (
            self.mock_codeocean_client.return_value.data_assets
        ).create_data_asset.assert_called_once_with(
            data_asset_params=DataAssetParams(
                name=asset_name,
                tags=["123456", "custom_tag", "derived"],
                mount=asset_name,
                description="example description",
                source=Source(computation=ComputationSource(id="123-456")),
                target=Target(
                    aws=AWSS3Target(bucket="example", prefix=asset_name)
                ),
                custom_metadata={
                    "subject id": "123456",
                    "data level": "derived",
                },
            )
        )

    def test_send_notification(self):
        """Tests _send_notification method."""

        with self.assertLogs(level="ERROR") as captured:
            self.job._send_notification(Exception("Something went wrong!"))
        self.assertEqual(1, len(captured.output))
        self.assertIn("Something went wrong!", captured.output[0])

    @patch(
        "allen_asset_creation_library.job.CaptureResultsJob"
        "._check_pipeline_end_status"
    )
    @patch(
        "allen_asset_creation_library.job.CaptureResultsJob._get_schemas"
    )
    @patch(
        "allen_asset_creation_library.job.CaptureResultsJob"
        "._check_if_target_already_exists"
    )
    @patch(
        "allen_asset_creation_library.job.CaptureResultsJob"
        "._capture_results"
    )
    @patch("allen_asset_creation_library.job.MetadataDbClient")
    def test_run_job_success(
        self,
        mock_docdb_client: MagicMock,
        mock_capture_results: MagicMock,
        mock_check_if_target_already_exists: MagicMock,
        mock_get_schemas: MagicMock,
        mock_check_pipeline_end_status: MagicMock,
    ):
        """Tests run_job method when successful."""

        (
            self.mock_codeocean_client.return_value.data_assets
        ).wait_until_ready.return_value = DataAsset(
            **self.base_wait_until_ready_response
        )
        mock_check_pipeline_end_status.return_value = None
        mock_get_schemas.return_value = self.base_schemas
        mock_check_if_target_already_exists.return_value = False
        mock_capture_results.return_value = DataAsset(
            **self.base_capture_result_response
        )
        mock_register_asset = mock_docdb_client.return_value.register_asset
        mock_register_asset.return_value = {"message": "success"}
        with self.assertLogs(level="INFO") as captured:
            self.job.run_job()
        expected_logs = [
            "INFO:allen_asset_creation_library.job:" "{'message': 'success'}",
            "INFO:allen_asset_creation_library.job:"
            "Finished capturing asset!",
        ]
        self.assertEqual(expected_logs, captured.output)
        (
            self.mock_codeocean_client.return_value.data_assets
        ).update_permissions.asset_called_once_with(
            data_asset_id="124-abc",
            permissions=Permissions(
                users=[
                    UserPermissions(
                        email="abc@example.com", role=UserRole.Owner
                    )
                ],
                groups=None,
                everyone=EveryoneRole.Viewer,
            ),
        )
        # The collection version is derived from schema_version "2.2.0".
        mock_docdb_client.assert_called_once_with(
            host="example.com", version="v2"
        )
        mock_register_asset.assert_called_once_with(
            s3_location=(
                "s3://example/123456_2025-01-28_16-48-50"
                "_processed_2025-01-29_13-04-34"
            )
        )

    @patch(
        "allen_asset_creation_library.job.CaptureResultsJob"
        "._check_pipeline_end_status"
    )
    @patch(
        "allen_asset_creation_library.job.CaptureResultsJob._get_schemas"
    )
    @patch(
        "allen_asset_creation_library.job.CaptureResultsJob"
        "._check_if_target_already_exists"
    )
    @patch(
        "allen_asset_creation_library.job.CaptureResultsJob"
        "._capture_results"
    )
    @patch("allen_asset_creation_library.job.MetadataDbClient.register_asset")
    @patch(
        "allen_asset_creation_library.job.CaptureResultsJob"
        "._send_notification"
    )
    def test_run_job_target_exists_failure(
        self,
        mock_send_notification: MagicMock,
        mock_register_asset: MagicMock,
        mock_capture_results: MagicMock,
        mock_check_if_target_already_exists: MagicMock,
        mock_get_schemas: MagicMock,
        mock_check_pipeline_end_status: MagicMock,
    ):
        """Tests run_job method failed when target already exists."""

        mock_check_pipeline_end_status.return_value = None
        mock_get_schemas.return_value = self.base_schemas
        mock_check_if_target_already_exists.return_value = True
        with self.assertRaises(Exception) as e:
            self.job.run_job()
        mock_send_notification.assert_called_once_with(e.exception)
        mock_register_asset.assert_not_called()
        mock_capture_results.assert_not_called()

    @patch(
        "allen_asset_creation_library.job.CaptureResultsJob"
        "._check_pipeline_end_status"
    )
    @patch(
        "allen_asset_creation_library.job.CaptureResultsJob._get_schemas"
    )
    @patch(
        "allen_asset_creation_library.job.CaptureResultsJob"
        "._check_if_target_already_exists"
    )
    @patch(
        "allen_asset_creation_library.job.CaptureResultsJob"
        "._capture_results"
    )
    @patch(
        "allen_asset_creation_library.job.MetadataDbClient" ".register_asset"
    )
    @patch(
        "allen_asset_creation_library.job.CaptureResultsJob"
        "._send_notification"
    )
    def test_run_job_results_capture_failure(
        self,
        mock_send_notification: MagicMock,
        mock_register_asset: MagicMock,
        mock_capture_results: MagicMock,
        mock_check_if_target_already_exists: MagicMock,
        mock_get_schemas: MagicMock,
        mock_check_pipeline_end_status: MagicMock,
    ):
        """Tests run_job method failed when capture failed."""

        mock_check_pipeline_end_status.return_value = None
        mock_get_schemas.return_value = self.base_schemas
        mock_check_if_target_already_exists.return_value = False
        failed_capture = deepcopy(self.base_wait_until_ready_response)
        failed_capture["state"] = "failed"
        failed_capture_response = DataAsset(**failed_capture)
        (
            self.mock_codeocean_client.return_value.data_assets
        ).wait_until_ready.return_value = failed_capture_response
        with self.assertRaises(Exception) as e:
            self.job.run_job()
        mock_send_notification.assert_called_once_with(e.exception)
        mock_register_asset.assert_not_called()
        mock_capture_results.assert_called_once()


if __name__ == "__main__":
    unittest.main()
