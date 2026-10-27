"""Tiny file helper that goes through Spark's Hadoop FileSystem.

Why not plain Python I/O: the run ledger must land next to the data, which is a
local directory on a laptop and a gs:// bucket on Dataproc. Hadoop's FileSystem
already speaks both, and it is on the classpath wherever Spark is. No extra
Python dependency (gcsfs, google-cloud-storage) is needed on either side.
"""

from __future__ import annotations

from pyspark.sql import SparkSession


class HadoopFS:
    def __init__(self, spark: SparkSession):
        self._jvm = spark._jvm
        self._conf = spark._jsc.hadoopConfiguration()

    def _path(self, p: str):
        return self._jvm.org.apache.hadoop.fs.Path(p)

    def _fs(self, p):
        return p.getFileSystem(self._conf)

    def write_text(self, path: str, text: str) -> None:
        p = self._path(path)
        out = self._fs(p).create(p, True)
        try:
            out.write(bytearray(text.encode("utf-8")))
        finally:
            out.close()

    def read_text(self, path: str) -> str:
        p = self._path(path)
        stream = self._fs(p).open(p)
        try:
            return self._jvm.org.apache.commons.io.IOUtils.toString(stream, "UTF-8")
        finally:
            stream.close()

    def glob(self, pattern: str) -> list[str]:
        p = self._path(pattern)
        statuses = self._fs(p).globStatus(p)
        if statuses is None:
            return []
        return sorted(s.getPath().toString() for s in statuses)

    def exists(self, path: str) -> bool:
        p = self._path(path)
        return bool(self._fs(p).exists(p))

    def mkdirs(self, path: str) -> None:
        p = self._path(path)
        self._fs(p).mkdirs(p)
