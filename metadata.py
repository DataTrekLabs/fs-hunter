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
from loguru import logger


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
    md5: str

    def compute_hash(self) -> None:
        """Compute MD5 hash lazily (reads entire file)."""
        self.md5 = _compute_md5(Path(self.full_path))

    def to_dict(self) -> dict:
        d = asdict(self)
        d["ctime"] = self.ctime.strftime("%Y-%m-%d %H:%M:%S")
        d["mtime"] = self.mtime.strftime("%Y-%m-%d %H:%M:%S")
        return d


def _compute_md5(file_path: Path) -> str:
    """Compute MD5 in 8KB chunks."""
    md5 = hashlib.md5()
    try:
        with open(file_path, "rb") as f:
            while chunk := f.read(8192):
                md5.update(chunk)
    except (PermissionError, OSError) as e:
        logger.debug("_compute_md5 error | file={} error={}", file_path, e)
        return ""
    return md5.hexdigest()


def _detect_mime(file_path: Path) -> str:
    """Detect MIME type via file header bytes, fall back to extension."""
    if _magic_instance:
        try:
            return _magic_instance.from_file(str(file_path))
        except (PermissionError, OSError) as e:
            logger.debug("_detect_mime magic failed, falling back | file={} error={}", file_path, e)
    import mimetypes
    return mimetypes.guess_type(str(file_path))[0] or "unknown"


def _get_owner(file_path: Path) -> str:
    """Get file owner cross-platform."""
    try:
        return file_path.owner()
    except (NotImplementedError, OSError) as e:
        logger.debug("_get_owner error | file={} error={}", file_path, e)
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
        md5="",
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
