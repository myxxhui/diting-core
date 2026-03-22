# [Ref: 01_语义分类器_实践] [Ref: 09_ Module A] ClassifierOutput 写入 L2 表 classifier_output_snapshot
# 分类完成后同一批次内写入，batch_id/correlation_id 一致，供 Module B 按约定读取

import json
import logging
import uuid
from typing import Any, List

logger = logging.getLogger(__name__)

# DomainTag 枚举值 -> primary_tag 字符串（与 L2 表 primary_tag 约定一致；以中文为主便于理解与过滤）
_DOMAIN_TAG_TO_STR = {
    0: "未指定",
    1: "农业",
    2: "科技",
    3: "宏观",
    4: "未知",
    5: "自定义",
}


def _output_to_row(output: Any, batch_id: str, correlation_id: str) -> tuple:
    """将单条 ClassifierOutput 转为行元组（含 segment_shares_json）。"""
    primary_tag = "未知"
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
            primary_tag = _DOMAIN_TAG_TO_STR.get(tag_val, "未知")
            if tag_val == 5 and t0.get("domain_label"):
                primary_tag = ((t0["domain_label"] or "")[:64] or "自定义")
            primary_confidence = t0.get("confidence", 0.0)
    tags_json = json.dumps(tags_list, ensure_ascii=False) if tags_list else None
    symbol = getattr(output, "symbol", "") or ""
    seg_shares = getattr(output, "segment_shares", None) or []
    def _seg_row(s: Any) -> dict:
        sid = getattr(s, "segment_id", "") or ""
        row = {
            "segment_id": sid,
            "revenue_share": float(getattr(s, "revenue_share", 0) or 0),
            "is_primary": bool(getattr(s, "is_primary", False)),
            # 与 L2 主营表是否一致：seg_no_disclosure 表示无 symbol_business_profile 行
            "disclosure_present": sid != "seg_no_disclosure",
        }
        return row

    segment_shares_json = json.dumps(
        [_seg_row(s) for s in seg_shares],
        ensure_ascii=False,
    ) if seg_shares else "[]"
    return (batch_id, symbol, primary_tag, primary_confidence, tags_json, segment_shares_json, correlation_id)


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

    insert_sql = """
                INSERT INTO classifier_output_snapshot
                (batch_id, symbol, primary_tag, primary_confidence, tags_json, segment_shares_json, correlation_id)
                VALUES (%s, %s, %s, %s, %s::jsonb, %s::jsonb, %s)
                """

    def _missing_segment_column(err: BaseException) -> bool:
        msg = str(err).lower()
        return "segment_shares_json" in msg and (
            "does not exist" in msg or "undefinedcolumn" in msg.replace(" ", "")
        )

    try:
        conn = psycopg2.connect(dsn)
        try:
            cur = conn.cursor()
            try:
                cur.executemany(insert_sql, rows)
            except Exception as e:
                if _missing_segment_column(e):
                    logger.info(
                        "classifier_output_snapshot 缺 segment_shares_json 列，执行 ADD COLUMN 后重试写入"
                    )
                    cur.execute(
                        """
                        ALTER TABLE classifier_output_snapshot
                        ADD COLUMN IF NOT EXISTS segment_shares_json JSONB;
                        """
                    )
                    conn.commit()
                    cur.executemany(insert_sql, rows)
                else:
                    raise
            conn.commit()
            n = len(rows)
            logger.info("ClassifierOutput 写入 L2 表 classifier_output_snapshot: batch_id=%s, 行数=%s", batch_id, n)
            return n
        finally:
            conn.close()
    except Exception as e:
        logger.warning("写入 classifier_output_snapshot 失败（表可能未创建）: %s", e)
        return 0
