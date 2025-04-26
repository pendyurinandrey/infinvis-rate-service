import os
import shutil

import pytest
from wiremock.constants import Config
from wiremock.server import WireMockServer


@pytest.fixture(scope="session")
def wm_server():
    with WireMockServer(root_dir='./build') as wm:
        Config.base_url = 'http://localhost:{}/__admin'.format(wm.port)
        yield wm


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
