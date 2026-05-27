"""Regression: upload_folder must accept max_workers kwarg (L3-Py)."""
import inspect
from hippius_hub.file_upload import upload_folder


def test_upload_folder_signature_includes_max_workers():
    sig = inspect.signature(upload_folder)
    assert "max_workers" in sig.parameters, "upload_folder must accept max_workers kwarg"
    assert sig.parameters["max_workers"].default == 8, "default must be 8"


def test_upload_folder_threads_max_workers():
    src = inspect.getsource(upload_folder)
    # The kwarg must be threaded into ThreadPoolExecutor.
    assert "max_workers=max_workers" in src, (
        "upload_folder must pass its max_workers kwarg into ThreadPoolExecutor"
    )
