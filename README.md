# bizcloud-map-lambda

## ローカル実行メモ

1. 依存ライブラリをローカルの `vendor/` にインストール  
   ```bash
   pip install -r requirements.txt -t vendor
   ```
2. `.env` を用意（`env.sample` からコピー）
3. `docker compose run --rm lambda`

※ `vendor/` が存在しないとコンテナから `pg8000` などを読み込めません。
