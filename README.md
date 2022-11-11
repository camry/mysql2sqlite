# mysql2sqlite

MySQL convert to SQLite3

## 使用

```bash
mysql2sqlite --server user:password@host:port --db game_base > sqlite_game_base.sql && \
sqlite3 game_base.db < sqlite_game_base.sql
```