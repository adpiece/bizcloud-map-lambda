"""
categoriesテーブル用のクエリ定義。

categoriesテーブルは単独で完結するテーブルです。
"""

from typing import Any, Dict, List, Tuple


def build_query(record_ids: List[int], is_all_record: bool) -> Tuple[str, List[Any]]:
    """
    categoriesテーブル用のクエリを構築する。
    
    Parameters
    ----------
    record_ids: List[int]
        取得対象のIDリスト
    is_all_record: bool
        True の場合、ID指定を無視して全件取得する
    
    Returns
    -------
    Tuple[str, List[Any]]
        クエリ文字列とパラメータのタプル
    """
    # headers.pyで指定されているフィールドのみを取得
    # statusは0が有効、1が無効として表示
    query = """
        SELECT 
            c.name,
            c.code,
            c.label_color,
            CASE 
                WHEN c.status = 0 THEN '有効'
                WHEN c.status = 1 THEN '無効'
                ELSE CAST(c.status AS VARCHAR)
            END AS status,
            c.updated_at
        FROM categories c
    """
    
    params: List[Any] = []
    
    if not is_all_record:
        if not record_ids:
            # 空のリストの場合は空の結果を返す
            query = "SELECT * FROM categories WHERE 1=0"
            return query, params
        
        placeholders = ",".join(["%s"] * len(record_ids))
        query += f" WHERE c.id IN ({placeholders})"
        params = record_ids
    
    query += " ORDER BY c.id"
    
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

