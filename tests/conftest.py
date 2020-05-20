import pytest
import os
import shutil
from pathlib import Path
import tempfile

DATA_ROOT = Path(__file__).parent / 'data'


@pytest.fixture(scope='function')
def template_file():
    def get_template_file(filename):
        fd, path = tempfile.mkstemp(suffix='.yml')
        os.close(fd)
        src_path = os.path.join(DATA_ROOT, filename)
        shutil.copy(src_path, path)

        return path

    return get_template_file