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
            "name",
            "email",
            "role",
            "status",
            "updated_at",
        ],
        "labels": {
            "name": "アカウント名",
            "email": "メールアドレス",
            "role": "ロール",
            "status": "ステータス",
            "updated_at": "更新日時",
        },
    },
    "products": {
        "field_order": [
            "category_name",
            "product_name",
            "product_code",
            "manufacturer_name",
            "updated_at",
        ],
        "labels": {
            "category_name": "カテゴリ",
            "product_name": "品名",
            "product_code": "品番",
            "manufacturer_name": "メーカー",
            "updated_at": "更新日時",
        },
    },
    "categories": {
        "field_order": [
            "name",
            "code",
            "label_color",
            "status",
            "updated_at",
        ],
        "labels": {
            "name": "カテゴリ名",
            "code": "コード",
            "label_color": "ラベル色",
            "status": "ステータス",
            "updated_at": "更新日時",
        },
    },
    "manufacturers": {
        "field_order": [
            "name",
            "code",
            "status",
            "updated_at",
        ],
        "labels": {
            "name": "メーカー名",
            "code": "コード",
            "status": "ステータス",
            "updated_at": "更新日時",
        },
    },
    "locations": {
        "field_order": [
            "name",
            "code",
            "status",
            "updated_at",
        ],
        "labels": {
            "name": "保管場所名",
            "code": "コード",
            "status": "ステータス",
            "updated_at": "更新日時",
        },
    },
    "supplies": {
        "field_order": [
            "category_name",
            "supply_name",
            "supply_code",
            "manufacturer_name",
            "location_name",
            "updated_at",
        ],
        "labels": {
            "category_name": "カテゴリ",
            "supply_name": "品名",
            "supply_code": "品番",
            "manufacturer_name": "メーカー",
            "location_name": "保管場所",
            "updated_at": "更新日時",
        },
    },
    # 他のテーブルを追加したい場合はここに追記してください。
}

