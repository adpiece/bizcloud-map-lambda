"""
productテーブル用のクエリ定義。

productテーブルはcategoriesとmanufacturersテーブルと関連している。
"""

from typing import Any, Dict, List, Tuple


def build_query(record_ids: List[int]) -> Tuple[str, List[Any]]:
    """
    productテーブル用のクエリを構築する。
    
    Parameters
    ----------
    record_ids: List[int]
        取得対象のIDリスト
    
    Returns
    -------
    Tuple[str, List[Any]]
        クエリ文字列とパラメータのタプル
    """
    # productsテーブルとcategories、manufacturersテーブルをJOIN
    # headers.pyで指定されているフィールドのみを取得
    query = """
        SELECT 
            c.name AS category_name,
            p.product_name,
            p.product_code,
            m.name AS manufacturer_name,
            p.updated_at
        FROM products p
        LEFT JOIN categories c ON p.category_id = c.id
        LEFT JOIN manufacturers m ON p.manufacturer_id = m.id
    """
    
    if not record_ids:
        raise ValueError("record_ids is required.")

    placeholders = ",".join(["%s"] * len(record_ids))
    query += f" WHERE p.id IN ({placeholders})"
    params: List[Any] = record_ids
    
    query += " ORDER BY p.id"
    
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

