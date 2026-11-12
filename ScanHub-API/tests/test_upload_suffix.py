import pytest

from scan_api import safe_upload_suffix


@pytest.mark.parametrize(
    ("filename", "expected"),
    [
        ("report.pdf", ".pdf"),
        ("archive.tar.gz", ".gz"),
        ("report." + "a" * 32, "." + "a" * 32),
        ("report." + "a" * 33, ""),
        ("report.txt\\..\\payload", ""),
        ("report.<bad>", ""),
        ("report.txt\n", ""),
        ("report", ""),
        (None, ""),
    ],
)
def test_upload_suffix_only_allows_safe_extensions(filename, expected):
    assert safe_upload_suffix(filename) == expected
