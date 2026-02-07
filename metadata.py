import hashlib
import mimetypes
import stat
from dataclasses import dataclass, asdict
from datetime import datetime
from pathlib import Path


DELTA_REQUIRED_COLUMNS = ["Directory", "Dataset Repo", "SF Table", "Filename"]


@dataclass
class DeltaInfo:
    directory: str
    dataset_repo: str
    sf_table: str
    filename: str

    def to_dict(self) -> dict:
        return asdict(self)


@dataclass
class FileMetadata:
    name: str
    extension: str
    full_path: str
    relative_path: str
    size_bytes: int
    created: datetime
    modified: datetime
    permissions: str
    owner: str
    mime_type: str
    sha256: str

    def compute_sha256(self) -> None:
        """Compute SHA256 hash lazily (reads entire file)."""
        self.sha256 = _compute_sha256(Path(self.full_path))

    def to_dict(self) -> dict:
        d = asdict(self)
        d.pop("relative_path", None)
        d["created"] = self.created.strftime("%Y-%m-%d %H:%M:%S")
        d["modified"] = self.modified.strftime("%Y-%m-%d %H:%M:%S")
        return d


def _compute_sha256(file_path: Path) -> str:
    """Compute SHA256 in 8KB chunks."""
    sha256 = hashlib.sha256()
    try:
        with open(file_path, "rb") as f:
            while chunk := f.read(8192):
                sha256.update(chunk)
    except (PermissionError, OSError):
        return ""
    return sha256.hexdigest()


def _get_owner(file_path: Path) -> str:
    """Get file owner cross-platform."""
    try:
        return file_path.owner()
    except (NotImplementedError, OSError):
        return "N/A"


def extract_metadata(file_path: Path, base_dir: Path) -> FileMetadata:
    """Extract all metadata from a single file."""
    file_stat = file_path.stat()

    return FileMetadata(
        name=file_path.name,
        extension=file_path.suffix,
        full_path=str(file_path.resolve()),
        relative_path=str(file_path.relative_to(base_dir)),
        size_bytes=file_stat.st_size,
        created=datetime.fromtimestamp(file_stat.st_ctime),
        modified=datetime.fromtimestamp(file_stat.st_mtime),
        permissions=stat.filemode(file_stat.st_mode),
        owner=_get_owner(file_path),
        mime_type=mimetypes.guess_type(str(file_path))[0] or "unknown",
        sha256="",
    )
