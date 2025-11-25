"""
file_typeごとのクエリ定義。

各file_typeに対応するクエリモジュールをインポートし、
build_query関数を提供する。
"""

from typing import Any, Callable, Dict, List, Tuple

from .users import build_query as build_users_query, transform_row as transform_users_row
from .product import build_query as build_product_query, transform_row as transform_product_row
from .categories import build_query as build_categories_query, transform_row as transform_categories_row
from .manufacturers import build_query as build_manufacturers_query, transform_row as transform_manufacturers_row

# file_type -> (build_query関数, transform_row関数) のマッピング
QUERY_BUILDERS: Dict[str, Tuple[Callable[[List[int]], Tuple[str, List[Any]]], Callable[[Dict[str, Any]], Dict[str, Any]]]] = {
    "users": (build_users_query, transform_users_row),
    "products": (build_product_query, transform_product_row),
    "categories": (build_categories_query, transform_categories_row),
    "manufacturers": (build_manufacturers_query, transform_manufacturers_row),
}


def get_query_builder(file_type: str) -> Tuple[Callable[[List[int]], Tuple[str, List[Any]]], Callable[[Dict[str, Any]], Dict[str, Any]]]:
    """
    file_typeに対応するクエリビルダーと行変換関数を取得する。
    
    Parameters
    ----------
    file_type: str
        ファイルタイプ（例: "users", "product"）
    
    Returns
    -------
    Tuple[build_query関数, transform_row関数]
        クエリビルダーと行変換関数のタプル
    
    Raises
    ------
    KeyError
        指定されたfile_typeに対応するクエリビルダーが存在しない場合
    """
    if file_type not in QUERY_BUILDERS:
        raise KeyError(f"No query builder found for file_type: {file_type}")
    return QUERY_BUILDERS[file_type]

