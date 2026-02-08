import hashlib
import os
import stat

try:
    import magic
    _magic_instance = magic.Magic(mime=True)
except ImportError:
    _magic_instance = None
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
    ctime: datetime
    mtime: datetime
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
        d["ctime"] = self.ctime.strftime("%Y-%m-%d %H:%M:%S")
        d["mtime"] = self.mtime.strftime("%Y-%m-%d %H:%M:%S")
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


def _detect_mime(file_path: Path) -> str:
    """Detect MIME type via file header bytes, fall back to extension."""
    if _magic_instance:
        try:
            return _magic_instance.from_file(str(file_path))
        except (PermissionError, OSError):
            pass
    import mimetypes
    return mimetypes.guess_type(str(file_path))[0] or "unknown"


def _get_owner(file_path: Path) -> str:
    """Get file owner cross-platform."""
    try:
        return file_path.owner()
    except (NotImplementedError, OSError):
        return "N/A"


def extract_metadata_stat(
    file_path: Path, base_dir: Path, file_stat: os.stat_result
) -> FileMetadata:
    """Build FileMetadata from a pre-computed stat result.

    Owner and mime_type are set to placeholders; call enrich_metadata()
    to fill them in after cheaper filters have passed.
    """
    return FileMetadata(
        name=file_path.name,
        extension=file_path.suffix,
        full_path=str(file_path.resolve()),
        relative_path=str(file_path.relative_to(base_dir)),
        size_bytes=file_stat.st_size,
        ctime=datetime.fromtimestamp(file_stat.st_ctime),
        mtime=datetime.fromtimestamp(file_stat.st_mtime),
        permissions=stat.filemode(file_stat.st_mode),
        owner="",
        mime_type="",
        sha256="",
    )


def enrich_metadata(metadata: FileMetadata, file_path: Path) -> FileMetadata:
    """Fill in expensive fields (owner, mime_type) on an existing FileMetadata."""
    metadata.owner = _get_owner(file_path)
    metadata.mime_type = _detect_mime(file_path)
    return metadata


def extract_metadata(file_path: Path, base_dir: Path) -> FileMetadata:
    """Extract all metadata from a single file (convenience wrapper)."""
    file_stat = file_path.stat()
    metadata = extract_metadata_stat(file_path, base_dir, file_stat)
    return enrich_metadata(metadata, file_path)
