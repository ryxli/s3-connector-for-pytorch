#  Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
#  // SPDX-License-Identifier: BSD
import errno
import io
import os
import stat
from types import SimpleNamespace
from typing import Optional
from pathlib_abc import PathBase, ParserBase, UnsupportedOperation
import posixpath
from urllib.parse import urlparse


from ._s3client import S3Client, S3ClientConfig
from s3torchconnectorclient import S3Exception


def _get_bucket_region(region: Optional[str] = None):
    if region is not None:
        return region
    for var in ["BUCKET_REGION", "AWS_REGION", "REGION"]:
        if var in os.environ:
            return os.environ[var]
    return "us-east-1"


def _get_throughput_target_gbps():
    for var in ["S3_CRT_THROUGHPUT_TARGET_GPBS"]:
        if var in os.environ:
            return float(os.environ[var])
    return 400.0


def _get_part_size():
    for var in ["S3_CRT_PART_SIZE_MB"]:
        if var in os.environ:
            return int(os.environ[var]) * 1024 * 1024
    return 64 * 1024 * 1024


class S3Parser(ParserBase):
    @classmethod
    def _unsupported_msg(cls, attribute):
        return f"{cls.__name__}.{attribute} is unsupported"

    @property
    def sep(self):
        return "/"

    def join(self, path, *paths):
        return posixpath.join(path, *paths)

    def split(self, path):
        scheme, bucket, prefix, _, _, _ = urlparse(path)
        parent, _, name = prefix.lstrip("/").rpartition("/")
        if not bucket:
            return bucket, name
        return (scheme + "://" + bucket + "/" + parent, name)

    def splitdrive(self, path):
        scheme, bucket, prefix, _, _, _ = urlparse(path)
        drive = f"{scheme}://{bucket}"
        return drive, prefix.lstrip("/")

    def splitext(self, path):
        return posixpath.splitext(path)

    def normcase(self, path):
        return posixpath.normcase(path)

    def isabs(self, path):
        s = os.fspath(path)
        scheme_tail = s.split("://", 1)
        return len(scheme_tail) == 2


class S3Path(PathBase):
    __slots__ = ("_region", "_s3_client_config", "_client", "_raw_path")
    parser = S3Parser()

    def __init__(
        self,
        *pathsegments,
        client: Optional[S3Client] = None,
        region=None,
        s3_client_config=None,
    ):
        super().__init__(*pathsegments)
        self._region = region or _get_bucket_region(region)
        self._s3_client_config = s3_client_config or S3ClientConfig(
            throughput_target_gbps=_get_throughput_target_gbps(),
            part_size=_get_part_size(),
        )
        self._client = client or S3Client(
            region=self._region,
            s3client_config=self._s3_client_config,
        )

    def __repr__(self):
        return f"{type(self).__name__}({str(self)!r})"

    def __hash__(self):
        return hash(str(self))

    def __eq__(self, other):
        if not isinstance(other, S3Path):
            return NotImplemented
        return str(self) == str(other)

    def with_segments(self, *pathsegments):
        path = str("/".join(pathsegments))
        if path.startswith("/"):
            path = path[1:]
        if not path.startswith(self.anchor):
            path = f"{self.anchor}{path}"
        return type(self)(
            path,
            client=self._client,
            region=self._region,
            s3_client_config=self._s3_client_config,
        )

    @property
    def bucket(self):
        if self.is_absolute() and self.drive.startswith("s3://"):
            return self.drive[5:]
        return ""

    @property
    def key(self):
        if self.is_absolute() and len(self.parts) > 1:
            return self.parser.sep.join(self.parts[1:])
        return ""

    def open(self, mode="r", buffering=-1, encoding=None, errors=None, newline=None):
        if buffering != -1 or not self.is_absolute():
            raise UnsupportedOperation()
        action = "".join(c for c in mode if c not in "btU")
        if action == "r":
            try:
                fileobj = self._client.get_object(self.bucket, self.key)
            except S3Exception:
                raise FileNotFoundError(errno.ENOENT, "Not found", str(self)) from None
            except:
                raise
        elif action == "w":
            try:
                fileobj = self._client.put_object(self.bucket, self.key)
            except S3Exception:
                raise
            except:
                raise
        else:
            raise UnsupportedOperation()
        if "b" not in mode:
            fileobj = io.TextIOWrapper(fileobj, encoding, errors, newline)
        return fileobj

    def stat(self, *, follow_symlinks=True):
        try:
            info = self._client.head_object(self.bucket, self.key)
            mode = stat.S_IFDIR if info.key.endswith("/") else stat.S_IFREG
        except S3Exception as e:
            error_msg = f"No stats available for {self}; it may not exist."
            try:
                info = self._client.head_object(self.bucket, self.key + "/")
                mode = stat.S_IFDIR
            except S3Exception:
                try:
                    listobjs = list(self._client.list_objects(self.bucket, self.key))
                    if len(listobjs) > 0 and len(listobjs[0].object_info) > 0:
                        info = SimpleNamespace(
                            size=0,
                            last_modified=None,
                        )
                        mode = stat.S_IFDIR
                    else:
                        raise ValueError(error_msg) from e
                except S3Exception:
                    raise ValueError(error_msg) from e
        return os.stat_result(
            (
                mode,  # mode
                None,  # ino
                "s3://",  # dev,
                None,  # nlink,
                None,  # uid,
                None,  # gid,
                info.size,  # size,
                None,  # atime,
                info.last_modified,  # mtime,
                None,  # ctime,
            )
        )

    def iterdir(self):
        if not self.is_dir():
            raise NotADirectoryError("not a s3 folder")
        try:
            key = self.key if self.key.endswith("/") else self.key + "/"
            for page in iter(
                self._client.list_objects(self.bucket, key, delimiter="/")
            ):
                for prefix in page.common_prefixes:
                    # yield directories first
                    yield self.with_segments(prefix.rstrip("/"))
                for info in page.object_info:
                    if info.key == key:
                        pass
                    else:
                        yield self.with_segments(info.key)
        except S3Exception:
            pass

    def mkdir(self, mode=0o777, parents=False, exist_ok=False):
        if self.is_dir():
            if exist_ok:
                return
            raise FileExistsError("s3 folder already exists")
        writer = None
        try:
            writer = self._client.put_object(
                self.bucket, self.key if self.key.endswith("/") else self.key + "/"
            )
        finally:
            if writer is not None:
                writer.close()

    def unlink(self, missing_ok=False):
        if self.is_dir():
            if missing_ok:
                return
            raise Exception(
                f"Path {self} is a directory; call rmdir instead of unlink."
            )
        self._client.delete_object(self.bucket, self.key)

    def rmdir(self):
        try:
            print(next(self.iterdir()))
            raise Exception(f"Path {self} is not empty")
        except NotADirectoryError:
            self._client.delete_object(self.bucket, self.key)

    def glob(self, pattern, *, case_sensitive=None, recurse_symlinks=True):
        if ".." in pattern:
            raise NotImplementedError(
                "Relative paths with '..' not supported in glob patterns"
            )
        if pattern.startswith(self.anchor) or pattern.startswith("/"):
            raise NotImplementedError("Non-relative patterns are unsupported")

        parts = list(PurePosixPath(pattern).parts)
        select = self._glob_selector(parts, case_sensitive, recurse_symlinks)
        return select(self)

    def with_name(self, name):
        """Return a new path with the file name changed."""
        split = self.parser.split
        if split(name)[0]:
            raise ValueError(f"Invalid name {name!r}")
        return self.with_segments(split(self._raw_path)[0], name)

    def __getstate__(self):
        state = {
            slot: getattr(self, slot, None)
            for cls in self.__class__.__mro__
            for slot in getattr(cls, "__slots__", [])
            if slot
            not in [
                "_client",
            ]
        }
        return (None, state)

    def __setstate__(self, state):
        _, state_dict = state
        for slot, value in state_dict.items():
            if slot not in ["client", "_client"]:
                setattr(self, slot, value)
        if not hasattr(self, "_region"):
            self._region = _get_bucket_region()
        if not hasattr(self, "_s3_client_config"):
            self._s3_client_config = S3ClientConfig(
                throughput_target_gbps=_get_throughput_target_gbps(),
                part_size=_get_part_size(),
            )
        self._client = S3Client(
            region=self._region,
            s3client_config=self._s3_client_config,
        )
