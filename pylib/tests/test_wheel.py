"""Verify the built wheel contains the expected packages.

Regression test: the monorepo restructure (crates/, cmd/, pylib/) broke
the NAS deploy because pyproject.toml paths didn't match the deployed
directory structure. This test ensures the wheel always contains both
etp_commands and etp_lib at the top level.
"""

import subprocess
import tempfile
import zipfile
from pathlib import Path


def test_wheel_contains_packages():
    """Build the wheel and verify etp_commands and etp_lib are present."""
    with tempfile.TemporaryDirectory() as tmp:
        result = subprocess.run(
            ["uv", "build", "--wheel", "--out-dir", tmp],
            capture_output=True,
            text=True,
        )
        assert result.returncode == 0, f"uv build failed: {result.stderr}"

        wheels = list(Path(tmp).glob("*.whl"))
        assert len(wheels) == 1, f"expected 1 wheel, got {len(wheels)}"

        with zipfile.ZipFile(wheels[0]) as zf:
            names = zf.namelist()

        # Both packages must be at the wheel root (no pylib/ or cmd/ prefix)
        assert any(n.startswith("etp_commands/") for n in names), (
            f"etp_commands/ not found in wheel: {names}"
        )
        assert any(n.startswith("etp_lib/") for n in names), (
            f"etp_lib/ not found in wheel: {names}"
        )

        # Key entry points must exist
        assert "etp_commands/dispatcher.py" in names
        assert "etp_commands/anime.py" in names
        assert "etp_commands/catalog.py" in names
        assert "etp_lib/paths.py" in names
