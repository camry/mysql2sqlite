# mysql2sqlite

MySQL convert to SQLite3

## 使用

```bash
rm -f game_base.db sqlite_game_base.sql && \
mysql2sqlite --server user:password@host:port --db game_base > sqlite_game_base.sql && \
sqlite3 game_base.db < sqlite_game_base.sql
# 忽略表和字段配置
rm -f game_base.db sqlite_game_base.sql && \
mysql2sqlite --server user:password@host:port --db game_base --config config/ignore.yaml > sqlite_game_base.sql && \
sqlite3 game_base.db < sqlite_game_base.sql
```
