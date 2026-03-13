# [Ref: 01_语义分类器_实践] [Ref: 09_ Module A] ClassifierOutput 写入 L2 表 classifier_output_snapshot
# 分类完成后同一批次内写入，batch_id/correlation_id 一致，供 Module B 按约定读取

import json
import logging
import uuid
from typing import Any, List

logger = logging.getLogger(__name__)

# DomainTag 枚举值 -> primary_tag 字符串（与 L2 表 primary_tag 约定一致）
_DOMAIN_TAG_TO_STR = {
    0: "UNSPECIFIED",
    1: "AGRI",
    2: "TECH",
    3: "GEO",
    4: "UNKNOWN",
    5: "CUSTOM",
}


def _output_to_row(output: Any, batch_id: str, correlation_id: str) -> tuple:
    """将单条 ClassifierOutput 转为 (batch_id, symbol, primary_tag, primary_confidence, tags_json, correlation_id)."""
    primary_tag = "UNKNOWN"
    primary_confidence = 0.0
    tags_list = []
    if getattr(output, "tags", None):
        for t in output.tags:
            tag_val = getattr(t, "domain_tag", 4)
            conf = getattr(t, "confidence", 0.0)
            label = getattr(t, "domain_label", None) or ""
            tags_list.append({"domain_tag": tag_val, "confidence": conf, "domain_label": label})
        if tags_list:
            t0 = tags_list[0]
            tag_val = t0["domain_tag"]
            primary_tag = _DOMAIN_TAG_TO_STR.get(tag_val, "UNKNOWN")
            if tag_val == 5 and t0.get("domain_label"):
                primary_tag = (t0["domain_label"] or "")[:16] or "CUSTOM"
            primary_confidence = t0.get("confidence", 0.0)
    tags_json = json.dumps(tags_list, ensure_ascii=False) if tags_list else None
    symbol = getattr(output, "symbol", "") or ""
    return (batch_id, symbol, primary_tag, primary_confidence, tags_json, correlation_id)


def write_classifier_output_snapshot(
    dsn: str,
    outputs: List[Any],
    batch_id: str = "",
    correlation_id: str = "",
) -> int:
    """
    将本批 ClassifierOutput 写入 L2 表 classifier_output_snapshot。
    :param dsn: PG L2 连接串
    :param outputs: 本批分类结果列表
    :param batch_id: 本批唯一标识，空则自动生成
    :param correlation_id: 全链路请求 ID
    :return: 写入行数
    """
    if not outputs:
        return 0
    batch_id = batch_id or str(uuid.uuid4())
    correlation_id = correlation_id or batch_id

    try:
        import psycopg2
    except ImportError:
        logger.warning("psycopg2 未安装，跳过写入 L2 classifier_output_snapshot")
        return 0

    rows = [_output_to_row(o, batch_id, correlation_id) for o in outputs]

    try:
        conn = psycopg2.connect(dsn)
        try:
            cur = conn.cursor()
            cur.executemany(
                """
                INSERT INTO classifier_output_snapshot
                (batch_id, symbol, primary_tag, primary_confidence, tags_json, correlation_id)
                VALUES (%s, %s, %s, %s, %s::jsonb, %s)
                """,
                rows,
            )
            conn.commit()
            n = len(rows)
            logger.info("ClassifierOutput 写入 L2 表 classifier_output_snapshot: batch_id=%s, 行数=%s", batch_id, n)
            return n
        finally:
            conn.close()
    except Exception as e:
        logger.warning("写入 classifier_output_snapshot 失败（表可能未创建）: %s", e)
        return 0
