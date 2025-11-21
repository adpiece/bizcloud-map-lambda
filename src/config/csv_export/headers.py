"""
テーブルごとのCSVヘッダー設定。

TABLE_EXPORT_CONFIG は以下のような構造:
{
    "<table_name>": {
        "field_order": ["id", "name", ...],  # オプション: 欲しいカラムと順序
        "labels": {                          # オプション: カラム名→ヘッダー名
            "id": "ID",
            "name": "氏名",
        },
    },
}

必要なテーブルごとに追記してください。未設定の場合は
デフォルトでDBのカラム名をそのまま使用します。
"""

from typing import Dict, List, Mapping

TableConfig = Mapping[str, object]

TABLE_EXPORT_CONFIG: Dict[str, TableConfig] = {
    "users": {
        "field_order": [
            "id",
            "provider",
            "uid",
            "name",
            "email",
            "created_at",
            "updated_at",
        ],
        "labels": {
            "id": "ID",
            "provider": "プロバイダ",
            "uid": "UID",
            "name": "氏名",
            "email": "メールアドレス",
            "created_at": "作成日時",
            "updated_at": "更新日時",
        },
    },
    # 他のテーブルを追加したい場合はここに追記してください。
}

