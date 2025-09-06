# Cyberpunk 2077 DB Seeder (Fandom API → DB)

対象: **Night Cityの全サブディストリクト**, **OS/Arms系サイバーウェア**, **主要消耗品**  
方式: **Fandom MediaWiki API** を礼節レートで叩き、**pageid/revid**を保存して差分更新。

## 使い方

### 1) 依存のインストール
```bash
pip install requests sqlalchemy psycopg2-binary python-dotenv mwparserfromhell
```

### 2) DBの指定
PostgreSQL 推奨。環境変数 `DATABASE_URL` が未設定なら SQLite にフォールバックします。
```bash
# Postgres 例
export DATABASE_URL='postgresql+psycopg2://user:pass@localhost:5432/cpunk'
```

### 3) 実行
```bash
python seed_cyberpunk_db.py --all
# or
python seed_cyberpunk_db.py --subdistricts
python seed_cyberpunk_db.py --os
python seed_cyberpunk_db.py --arms
python seed_cyberpunk_db.py --consumables
```

### 4) 出力
- DB テーブル群: `sources`, `pages`, `subdistricts`, `cyberware`, `cyberware_variants`, `items`, `item_stats`
- JSON スナップショット: `./out/subdistricts.json`, `./out/cyberware_os.json`, `./out/cyberware_arms.json`, `./out/consumables.json`, `./out/manifest.json`

## 注意・運用
- Fandom 利用規約に従い、**MediaWiki API**を使用・**レート制限**を厳守します。
- `pages.revid` で差分検知を行い、更新コストを最小化します。
- `mwparserfromhell` があれば wikitext を軽く整形して `summary` に格納します。
- 詳細な数値表（効果・持続・価格など）の**完全抽出**はページごとに差があるため、
  抽出強化は `upsert_*` 関数群を拡張して段階的に行ってください。

## 既知の限界
- ページ構造の揺れにより、自動抽出精度は 100% にはなりません。半自動運用を推奨します。
- 公式パッチで数値が変わる場合があります。定期ジョブで `revid` の変化を監視してください。

---

© Fandom contributors, CC BY-SA 3.0 — Data retrieved via MediaWiki API.
