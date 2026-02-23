# [Ref: 03_原子目标与规约/_共享规约/07_数据版本控制规约]
# [Ref: diting-infra schemas/sql/02_l2_data_versions.sql]
# 写入 L2 表 data_versions：(data_type, version_id, timestamp, file_path, file_size, checksum)

import logging
from datetime import datetime
from typing import Optional

logger = logging.getLogger(__name__)


def write_data_version(
    conn,
    data_type: str,
    version_id: str,
    timestamp: datetime,
    file_path: str,
    file_size: Optional[int] = None,
    checksum: Optional[str] = None,
) -> None:
    """
    写入一条 data_versions 记录。UNIQUE(data_type, version_id)，冲突时忽略或更新由调用方决定。
    此处采用 ON CONFLICT DO NOTHING 避免重复写入同版本。
    """
    sql = """
    INSERT INTO data_versions (data_type, version_id, timestamp, file_path, file_size, checksum)
    VALUES (%s, %s, %s, %s, %s, %s)
    ON CONFLICT (data_type, version_id) DO NOTHING
    """
    cur = conn.cursor()
    try:
        cur.execute(
            sql,
            (data_type, version_id, timestamp, file_path, file_size or 0, checksum or ""),
        )
        conn.commit()
        if cur.rowcount:
            logger.info("write_data_version: data_type=%s version_id=%s", data_type, version_id)
    finally:
        cur.close()
