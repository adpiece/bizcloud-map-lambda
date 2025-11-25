"""
usersテーブル用のクエリ定義。

usersテーブルはusers_roles（中間テーブル）を通じてrolesテーブルと関連している。
statusはconfirmed_atが入力されていれば"有効"、なければ"無効"として表示する。
"""

from typing import Any, Dict, List, Tuple


def build_query(record_ids: List[int]) -> Tuple[str, List[Any]]:
    """
    usersテーブル用のクエリを構築する。
    
    Parameters
    ----------
    record_ids: List[int]
        取得対象のIDリスト
    
    Returns
    -------
    Tuple[str, List[Any]]
        クエリ文字列とパラメータのタプル
    """
    # usersテーブルとusers_roles、rolesテーブルをJOIN
    # roleは複数ある可能性があるため、STRING_AGGで結合
    query = """
        SELECT 
            u.id,
            u.name,
            u.email,
            COALESCE(STRING_AGG(r.name, ', ' ORDER BY r.name), '') AS role,
            CASE 
                WHEN u.confirmed_at IS NOT NULL THEN '有効'
                ELSE '無効'
            END AS status,
            u.updated_at
        FROM users u
        LEFT JOIN users_roles ur ON u.id = ur.user_id
        LEFT JOIN roles r ON ur.role_id = r.id
    """
    
    if not record_ids:
        raise ValueError("record_ids is required.")

    placeholders = ",".join(["%s"] * len(record_ids))
    query += f" WHERE u.id IN ({placeholders})"
    params: List[Any] = record_ids
    
    # GROUP BYでroleを集約
    query += " GROUP BY u.id, u.name, u.email, u.confirmed_at, u.updated_at"
    query += " ORDER BY u.id"
    
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

