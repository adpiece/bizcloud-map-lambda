# bizcloud-map-lambda

## 事前準備

bizcloud-map-apiのDBへ接続をするため"docker-compose up -d"で起動しておく必要があります。

### 環境変数の設定
.envファイルを作成して、以下の環境変数を設定してください。

```bash
cp env.sample .env
```

## 開発環境
```bash
docker-compose build
```

```bash
docker compose run --rm lambda
```

実行する関数は"docker-compose.yml"に記載されている
entrypointで指定されているので、実行関数に合わせファイル名を変更してください。