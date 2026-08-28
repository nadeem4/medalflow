"""The storage seam and DatalakeClient's error handling (Phase 3, task 4).

`delete` pre-checked `exists`, and `exists` returned False for literally any
failure -- so an expired credential or a 429 turned `delete` into a silent
no-op that logged only at debug. `synapse.py` calls it to clear an output
location before writing, so a swallowed delete leaves stale data behind.
"""

import pytest
from medalflow.common.exceptions import CTEError
from medalflow.constants.datalake import LakeType
from medalflow.datalake.client import DatalakeClient
from medalflow.protocols import StorageClient
from medalflow.protocols import storage as storage_protocol


class NotFound(Exception):
    status_code = 404


class Forbidden(Exception):
    status_code = 403


class PathIsDirectory(Exception):
    status_code = 409


class _PathClient:
    def __init__(self, path, error, deleted):
        self._path = path
        self._error = error
        self._deleted = deleted

    def _act(self):
        if self._error is not None:
            raise self._error
        self._deleted.append(self._path)

    delete_file = _act
    delete_directory = _act
    get_file_properties = _act
    get_directory_properties = _act


class _FakeFileSystem:
    """Stands in for the Azure file system client."""

    def __init__(self, file_error=None, dir_error=None):
        self.file_error = file_error
        self.dir_error = dir_error
        self.deleted = []

    def get_file_client(self, path):
        return _PathClient(path, self.file_error, self.deleted)

    def get_directory_client(self, path):
        return _PathClient(path, self.dir_error, self.deleted)


@pytest.fixture
def client(offline_settings, monkeypatch):
    lake_client = DatalakeClient(LakeType.PROCESSED)
    monkeypatch.setattr(lake_client, "_get_fs_client", lambda: lake_client._fake)
    return lake_client


def _with_fs(client, **kwargs):
    client._fake = _FakeFileSystem(**kwargs)
    return client._fake


# --- 4a: a protocol sized to actual use --------------------------------------


def test_storage_protocol_covers_only_the_methods_production_uses():
    declared = {name for name in vars(StorageClient) if not name.startswith("_")}

    assert declared == {"delete", "read_csv"}


def test_datalake_client_satisfies_the_storage_protocol(offline_settings):
    assert isinstance(DatalakeClient(LakeType.PROCESSED), StorageClient)


def test_storage_protocol_does_not_pull_pandas_into_layer_zero():
    assert not hasattr(storage_protocol, "pd")


# --- 4b: bound, logged, narrowed error handling ------------------------------


def test_delete_logs_the_file_failure_before_falling_back_to_a_directory(client, caplog):
    fs = _with_fs(client, file_error=PathIsDirectory("path is a directory"))

    with caplog.at_level("DEBUG"):
        client.delete("outputs/table")

    assert fs.deleted == ["dev/outputs/table"]
    assert "path is a directory" in caplog.text


def test_exists_raises_on_a_failure_that_is_not_a_missing_path(client):
    _with_fs(client, file_error=Forbidden("denied"), dir_error=Forbidden("denied"))

    with pytest.raises(CTEError):
        client.exists("outputs/table")


def test_exists_returns_false_for_a_missing_path(client):
    _with_fs(client, file_error=NotFound("nope"), dir_error=NotFound("nope"))

    assert client.exists("outputs/table") is False


def test_exists_returns_true_for_a_present_path(client):
    _with_fs(client)

    assert client.exists("outputs/table") is True


# --- 4c: no exists-precheck, no silent no-op ---------------------------------


def test_delete_does_not_pre_check_existence(client, monkeypatch):
    fs = _with_fs(client)
    checked = []
    monkeypatch.setattr(client, "exists", lambda path: checked.append(path) or True)

    client.delete("outputs/table")

    assert checked == []
    assert fs.deleted == ["dev/outputs/table"]


def test_delete_still_deletes_when_an_existence_check_would_have_said_no(client, monkeypatch):
    fs = _with_fs(client)
    monkeypatch.setattr(client, "exists", lambda path: False)

    client.delete("outputs/table")

    assert fs.deleted == ["dev/outputs/table"]


def test_delete_treats_a_missing_path_as_success(client):
    _with_fs(client, file_error=NotFound("nope"), dir_error=NotFound("nope"))

    client.delete("outputs/table")


def test_delete_raises_rather_than_silently_leaving_stale_data(client):
    _with_fs(client, file_error=Forbidden("denied"), dir_error=Forbidden("denied"))

    with pytest.raises(CTEError):
        client.delete("outputs/table")


# --- 4d: the span reports the file system actually used ----------------------


def test_span_attributes_report_the_file_system_in_use(client, offline_settings):
    attributes = client._span_attributes(operation="delete", path="outputs/table")

    assert attributes["storage.file_system"] == offline_settings.datasource_file_system
