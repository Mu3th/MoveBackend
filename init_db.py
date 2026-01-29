import sqlite3

conn = sqlite3.connect("app.db")

with open("schema.sql", "r", encoding="utf-8") as f:
    conn.executescript(f.read())

conn.commit()
conn.close()

print("Database initialized ✔")
