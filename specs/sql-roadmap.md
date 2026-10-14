# AeroDB SQL roadmap — doğruluk önce

**Dayanak:** `specs/sql-conformance.md`, HEAD `675fdcd7f7f2eeb3f6f86bcd043af2b5fa94168e`, 14 Ekim 2026. Bu belge önerilen ayrı geliştirme işidir; bu audit hiçbir maddede implementasyon yapmadı. 92 vaka / 309 SQL/API denemesi; 28 bug vaka, 28 unsupported vaka. Bunlar toplam standart yüzdesi değildir.

## P0 — yanlış sonuç ve bütünlük riski

| Sıra | İş / feature alanı | Kabul kriteri / mevcut kanıt |
|---|---|---|
| 1 | Parser fail-closed; E051, E071, F302 | Bütün tokenlar tüketilir. UNION/EXCEPT/INTERSECT ve bilinmeyen suffix, implementasyon yoksa açık unsupported/parse error. `bad_sql_tail`, `union_second_branch`, `union_all`, `except`, `intersect` yeniden üretilebilir. İlk SELECT'i başarı diye döndürmek yasak. |
| 2 | Typed expression/value ve SQL truth model; E131, E061 | NULL UNKNOWN olarak kalır; WHERE yalnız TRUE seçer; NULL arithmetic NULL döndürür; nullable UPDATE çalışır. Equals/NotEquals, AND/OR ve IN subquery NULL testleri yeşil. Division by zero kontrollü hata, 0 sonucu değil. |
| 3 | Logical PK/FK storage identity; E141, F031-13, F701 | FK tüm kolon/target-key ile kontrol edilir, NULL muafiyeti, geçerli composite pair'ler korunur. Invalid composite FK reddedilir; RESTRICT dependent tabloyu silmez; ON UPDATE CASCADE uygulanır veya declaration açıkça reddedilir. Hem statement hem commit recheck aynı kuralları kullanır. |
| 4 | ORDER BY executor; F850, E121 ilişkili alan | Parser'ın order node'u her SELECT yolunda uygulanır; DESC ve non-projected column testleri yeşil. Expr/multiple-key/null-placement yoksa açık syntax error, sessiz no-op değil. |
| 5 | Aggregate ve scalar subquery doğruluğu; E091, F471 | COUNT(expr) NULL dışlar; AVG denominator NULL'suz; boş global group bir row üretir; empty/all-NULL sums NULL; DISTINCT doğru. Scalar 0 row=NULL, >1 row/cardinality error. |

P0 bitiş kapısı: `audit/run_probes.py` sonuçlarındaki bu bug repro'ları için davranışa özgü integration regression testleri önce RED, sonra GREEN; tüm eski testler ve yeni suite tekrar çalıştırılır. Sadece status etiketini değiştirerek veya oracle gevşeterek başarı sayılmaz. Isolated temp DB kullanılır. Exact numeric, key equality ve NULL tipi açısından string fallback kaldırılması tasarım önkoşuludur.

## P1 — günlük ilişkisel SQL'in Core omurgası

1. **Exact numeric + character:** E011-03 DECIMAL/NUMERIC precision/scale/overflow, E021-02 VARCHAR ve CHAR uzunluk/encoding sınırları. Testler: finansal exact decimal addition/rounding/overflow; INSERT ve UPDATE aynı conversion kuralları. Double'ı DECIMAL alias'ı yapmak kabul edilmez.
2. **Predicates/projection:** E051-01 DISTINCT, E061-03 IN list, E061-04 LIKE, E061-06 IS NULL, F201 CAST, F261 CASE/COALESCE. NULL dahil tam truth-table sonuçlarını test et.
3. **Constraints:** E141-02 UNIQUE (NOT NULL core subset), E141-06 CHECK; INSERT/UPDATE/multirow ve transaction rollback üzerinde doğrula. CHECK FALSE reddeder, UNKNOWN geçer.[7] UNIQUE nullable varyantların feature ayrımını ayrıca doğrula; tüm UNIQUE davranışlarını tek Core alt koduna sıkıştırma.
4. **DML / DDL:** E101 searched UPDATE expression ve unconditional DELETE, F031-02 view, F031-04 ALTER ADD COLUMN. Default, dependency/catalog persistence ve reopen/regression testleri gerekir. SQL semantics'i engine public path ve subquery path arasında eşitle.
5. **Metadata/error surface:** F021 TABLES/COLUMNS/constraint metadata; E171 deterministik SQLSTATE eşlemesi. Type/category ve mesajı ayır. Typed SELECT-result API, SQL/CLI veya network-driver iddiası değildir.
6. **Yorumlar ve parser:** E161 inline/full-line `--` lexer kuralları; SELECT DISTINCT'i kolon adı sanma; desteklenmeyen function/window için özgül unsupported hata. SQL cursor E121 bu audit'te unsupported; top-level ORDER BY desteklenmesi cursor uyumu değildir.

P1 kapısı: önce açıkça ilan edilmiş destek kapsamı ve beklenen sonuçları içeren genişletilmiş suite; Core feature/subfeature başlıklarını tek bir passing smoke ile implemented ilan etme. Kabul edilen syntax'ın hiç çalışmamasıyla syntax unsupported'u JSON'da ayrı tut.

## P2 — transaction semantiği ve interoperability

- E151 COMMIT/ROLLBACK omurgasını koru. E152-01 SERIALIZABLE ve E152-02 READ ONLY/WRITE için gerçek policy/işlem kontrolü olmadan parser ekleme. Snapshot ile serializable'ı eşitleme; write-skew/phantom ve iki bağımsız transaction litmus testleri ekle.
- T271 savepoints Optional. Undo/rollback sınırı ve nested savepoint, RELEASE, rollback-to sonrası transaction yaşam döngüsü gerçek veriyle test edilir.[1]
- Durability/reopen, error sonrası explicit transaction policy, DDL concurrency, deferred/composite constraint races: bu audit genel doğruluğunu kanıtlamadı. Baseline MVCC testleri başlangıç kanıtıdır, certification değil.
- JDBC/ODBC/SQL/CLI ayrı proje/kapsam; mevcut Rust API eksiklerini SQL:2023 feature sayısına uydurma kodlarla ekleme.

## P3 — Optional genişlik, P0/P1 sonrasında

T121 CTE, T611 window/ROW_NUMBER ve T821 JSON_VALUE; incremental olarak semantics + type + NULL/path/window partition tests. Full outer join F406 ve INTERSECT F302 Optional'dır; FULL join örneği geçti, INTERSECT şu anda sessiz suffix bug'ıdır (fail-closed P0, doğru operator implementasyonu P1/P3 seçimine bağlı).[1] Recursive CTE, advanced JSON, XML, spatial, graph ve analytic fonksiyonlar bu audit'te **unverified/out of scope**; unsupported diye tahmin edilmedi.

## Sürüm / teslim disiplini

Her değişiklik ayrı, küçük kapsam ve acceptance tests ile gelir. Bu roadmap süre/versiyon/commit/push sözü vermez. Feature matrisi HEAD, örnek SQL, beklenen/gerçek sonuç, source-code pointer ve source edition ile güncellenir. Eski yeşil testler + seçilmiş yeni yeşil örnekler üzerinden genel SQL:2023 uyumluluk yüzdesi üretilmez; Core/Optional tabloyu bağımsız doğrula.[3]

## Sources

[1] https://www.postgresql.org/docs/18/features-sql-standard.html
[3] https://www.postgresql.org/docs/18/features.html
[7] https://www.postgresql.org/docs/18/ddl-constraints.html
