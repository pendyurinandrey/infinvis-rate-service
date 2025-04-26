import os
import shutil

import pytest
from wiremock.constants import Config
from wiremock.resources.mappings.resource import Mappings
from wiremock.testing.testcontainer import wiremock_container


@pytest.fixture(scope="module")
def wm_server():
    with wiremock_container(secure=False) as wm:
        Config.base_url = wm.get_url("__admin") # (2
        yield wm
        Mappings.delete_all_mappings()


@pytest.fixture
def datadir(tmpdir, request):
    """
    Fixture responsible for searching a folder with the same name of test
    module and, if available, moving all contents to a temporary directory so
    tests can use them freely.
    """
    filename = request.module.__file__
    test_dir, _ = os.path.splitext(filename)

    if os.path.isdir(test_dir):
        shutil.copytree(test_dir, tmpdir.strpath, dirs_exist_ok=True)

    return tmpdir.strpath
