"""
manufacturersテーブル用のクエリ定義。

manufacturersテーブルは単独で完結するテーブルです。
"""

from typing import Any, Dict, List, Tuple


def build_query(record_ids: List[int]) -> Tuple[str, List[Any]]:
    """
    manufacturersテーブル用のクエリを構築する。
    
    Parameters
    ----------
    record_ids: List[int]
        取得対象のIDリスト
    
    Returns
    -------
    Tuple[str, List[Any]]
        クエリ文字列とパラメータのタプル
    """
    # headers.pyで指定されているフィールドのみを取得
    # statusは0が有効、1が無効として表示
    query = """
        SELECT 
            m.name,
            m.code,
            CASE 
                WHEN m.status = 0 THEN '有効'
                WHEN m.status = 1 THEN '無効'
                ELSE CAST(m.status AS VARCHAR)
            END AS status,
            m.updated_at
        FROM manufacturers m
    """
    
    if not record_ids:
        raise ValueError("record_ids is required.")

    placeholders = ",".join(["%s"] * len(record_ids))
    query += f" WHERE m.id IN ({placeholders})"
    params: List[Any] = record_ids
    
    query += " ORDER BY m.id"
    
    return query, params


def transform_row(row: Dict[str, Any]) -> Dict[str, Any]:
    """
    取得した行データを変換する。
    
    Parameters
    ----------
    row: Dict[str, Any]
        データベースから取得した行データ
    
    Returns
    -------
    Dict[str, Any]
        変換後の行データ
    """
    # 必要に応じてデータ変換を行う
    # 現時点ではそのまま返す
    return row

