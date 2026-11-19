CREATE TABLE IF NOT EXISTS xsqld_changelog(
  seq INTEGER PRIMARY KEY AUTOINCREMENT,
  tbl TEXT NOT NULL,
  op TEXT NOT NULL,
  pk TEXT NOT NULL,
  payload TEXT,
  ts INTEGER NOT NULL
);
CREATE TABLE IF NOT EXISTS xsqld_htap_meta(
  id INTEGER PRIMARY KEY CHECK(id=1),
  source_id TEXT NOT NULL,
  pruned_seq INTEGER NOT NULL DEFAULT 0,
  schema_fingerprint TEXT NOT NULL DEFAULT ''
);
INSERT OR IGNORE INTO xsqld_htap_meta(id,source_id) VALUES(1,lower(hex(randomblob(16))));
UPDATE sqlite_sequence SET seq=max(seq,0) WHERE name='xsqld_changelog';
INSERT INTO sqlite_sequence(name,seq) SELECT 'xsqld_changelog',0
WHERE NOT EXISTS(SELECT 1 FROM sqlite_sequence WHERE name='xsqld_changelog');
