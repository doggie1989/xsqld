-- HTAP P0 outbox changelog + capture triggers (installed via xsqld pipeline).
-- Fires inside xsqld's own write transaction => atomic & durable with business data.

CREATE TABLE IF NOT EXISTS xsqld_changelog(
  seq INTEGER PRIMARY KEY AUTOINCREMENT,
  tbl TEXT NOT NULL,
  op  TEXT NOT NULL,            -- I / U / D
  pk  INTEGER NOT NULL,
  payload TEXT,                 -- I/U: new row JSON; D: NULL
  ts  INTEGER NOT NULL          -- unixepoch() at capture, for lag measurement
);

CREATE TRIGGER IF NOT EXISTS orders_ai AFTER INSERT ON orders BEGIN
  INSERT INTO xsqld_changelog(tbl,op,pk,payload,ts)
  VALUES('orders','I',NEW.id,
    json_object('id',NEW.id,'user_id',NEW.user_id,'amount_cents',NEW.amount_cents,
                'status',NEW.status,'channel',NEW.channel,'created_at',NEW.created_at),
    unixepoch());
END;

CREATE TRIGGER IF NOT EXISTS orders_au AFTER UPDATE ON orders BEGIN
  INSERT INTO xsqld_changelog(tbl,op,pk,payload,ts)
  VALUES('orders','U',NEW.id,
    json_object('id',NEW.id,'user_id',NEW.user_id,'amount_cents',NEW.amount_cents,
                'status',NEW.status,'channel',NEW.channel,'created_at',NEW.created_at),
    unixepoch());
END;

CREATE TRIGGER IF NOT EXISTS orders_ad AFTER DELETE ON orders BEGIN
  INSERT INTO xsqld_changelog(tbl,op,pk,payload,ts)
  VALUES('orders','D',OLD.id,NULL,unixepoch());
END;