# AeroDB 0.5.1 — SQL:2023 uyumluluk denetimi

**Tarih:** 14 Ekim 2026. **Sürüm:** `0.5.1`. **Git HEAD:** `675fdcd7f7f2eeb3f6f86bcd043af2b5fa94168e`.

## Sonuç ve kapsam

AeroDB temel bir SQL depolama/transaction omurgasına sahip; fakat sorgu doğruluğu ve veri bütünlüğünde önemli açıklar var. Bu çalışma eksik özellikleri uygulamaz; `src/`, sürüm ve commit değişmemiştir. Bir sertifikasyon, tüm SQL:2023 dökümü veya genel uyumluluk yüzdesi değildir.

İzole worktree üzerinde `cargo test --all-targets`: **227 test çalıştırması geçti, 0 başarısız, 0 ignored**; 40 sonuç bloğu, 37 integration binary. Bazı unit testleri lib ve binary tarafından iki kez çalıştırılır. Derleyici uyarıları vardı. Mevcut testlerin yeşil olması aşağıdaki SQL semantik problemlerini dışlamıyor.

Yeni suite: **92 bağımsız vaka**, **309 SQL/API denemesi** (fixture dahil): 255 OK, 28 execution error, 26 syntax error. OK sayısı doğru sonuç sayısı değildir; beklenen constraint hataları da başarılı vaka olabilir. Vaka dağılımı: **34 implemented**, **28 bug**, **28 unsupported**, **1 partial**, **1 unverified**. Bunlar ayrı SQL özellik sayıları veya ayrı kök neden sayıları değildir.

Her vaka ayrı süreç ve `tempfile` veritabanı kullanır. Rust adapter gerçek `Engine::execute` yolunu çalıştırır; sonuçları stdout üzerinden kaydeder, parse/execute hatalarını ayrı tutar. `execute_select_statement` helper'ını tek başına kullanmaz: helper joinleri reddederken gerçek Engine join yolu çalışır. Parser acceptance değil; row multiset/order, beklenen hata ve hata kategorisi kontrol edilir. Setup başarısızlığı açıkça işaretlenir; sonraki eksik tablo hatası özellik desteği sayılmaz. Kaydedilen `!` fixture adımı kasıtlı hatalı INSERT'tür ve beklenen hata kontrol edilir.

## SQL:2023 sınıflandırma yöntemi

SQL:2023, ISO/IEC 9075:2023'tür; Core/Optional ayrımı özellik tabanlıdır. PostgreSQL 18'in açıklaması Core için 177 zorunlu özellik sayar ve kendi listesinin bile yaklaşık olduğunu açıkça belirtir.[3] Burada resmi feature ID ve Core flag'leri PostgreSQL 18'in iki birincil tablosundan doğrulandı; PostgreSQL'in destek durumu AeroDB'ye taşınmadı.[1][2] Tablo **68 seçilmiş feature/subfeature satırı** içerir (58 Core, 10 Optional); parent feature ve subfeature satırları örtüşür, dolayısıyla bu sayı herhangi bir standardın paydası değildir.

- **implemented:** yalnızca listelenen temsilî davranışlar doğrulandı; bütün feature için destek ilanı değil.
- **partial:** olumlu alt küme var, başka temel davranışlar yok veya runtime sınırlı.
- **unsupported:** syntax reddi ya da implementasyon bulunmadığı doğrulanan runtime hatası; error türü JSON'da ayrı.
- **bug:** kabul edilen sorgu yanlış sonuç/hatasız geçiş üretiyor veya ilan edilen kısıt geçerli veriyi reddediyor.
- **unverified:** oracle/API sınırı nedeniyle genel destek kararı verilemiyor.

`E121-02/-03` resmi olarak cursor subfeature'leridir. İlgili top-level ORDER BY regresyonları eşlenmiştir; cursor SQL desteği iddia edilmez (`E121-01` syntax reddi). `F850` top-level ORDER BY alanıdır; NULLS FIRST ayrıca denenmiştir, uydurma bağımsız feature kodu verilmemiştir. Boolean, FULL JOIN, INTERSECT, savepoint, CTE, window ve JSON burada Optional olarak işaretlidir; popüler olmaları onları Core yapmaz.[1]

## Öncelikli doğruluk ve bütünlük bulguları

### P0 — yanlış sonucu / geçersiz bütünlüğü sessizce kabul etme

1. **NULL / üç değerli mantık bozuk.** `v = NULL` NULL satırını döndürüyor; `v <> NULL` diğerlerini döndürüyor; NULL+1 sonucu `1`; UNKNOWN OR/AND davranışı yanlış. SQL ordinary comparison NULL operandında UNKNOWN üretmelidir.[6] `IS NULL` syntax olarak yok. `UPDATE v = NULL` nullable INTEGER üzerinde `Invalid INTEGER` ile reddediliyor. Kanıt: `null_eq`, `null_neq`, `null_arithmetic`, `three_value_or`, `three_value_and`, `update_null`. Kod: `src/sql/ast.rs:193-285`, `src/execution/runtime.rs:169-178`.
2. **ORDER BY kabul edilip uygulanmıyor.** `ORDER BY id DESC` gerçekte 1,2,3,4; seçilmeyen kolonla sıralama da bozuk. Parser order bilgisini AST'ye koyuyor; Engine SELECT döngüsü sıralamayı kullanmıyor. `order_desc`, `order_nonselected`; `src/sql/parser.rs:1053-1094`, `src/execution/runtime.rs:1159-1292`.
3. **Set operator suffix'leri yok sayılıyor.** Farklı ikinci branch eklenen UNION sadece ilk SELECT'i döndürüyor; UNION ALL duplicate kaybediyor; EXCEPT ve INTERSECT ilk tabloyu döndürüyor. UNION ikinci sorguyu birleştirmeli; ALL duplicate'leri korumalıdır.[8] İlk `union`/duplicate elimination smoke'ları tesadüfen geçer, adversarial `union_second_branch` desteğin bulunmadığını ortaya çıkarır. `src/sql/ast.rs:134-184` set-op node içermiyor; SELECT parser sonunda token tüketimi kontrol edilmiyor (`src/sql/parser.rs:1121`). `bad_sql_tail` da trailing NONSENSE'i sessizce kabul ediyor. Desteklenmeyen syntax önce güvenli biçimde reddedilmelidir.
4. **Aggregate NULL/empty/DISTINCT yanlış.** COUNT(v)=4, beklenen 3; AVG(v)=12.5, NULL'suz 50/3 gerekir; boş global aggregate hiç row üretmiyor; tüm-NULL group sonucu 1,0,0,0,0; COUNT(DISTINCT v)=4, beklenen 2. COUNT(expr) NULL'ları saymaz; AVG NULL'ları dışlar; empty SUM/AVG/MIN/MAX NULL üretir.[4] `aggregate_count`, `aggregate_avg`, `aggregate_empty`, `all_null_aggregate`, `aggregate_distinct`; `src/execution/runtime.rs:879-908,969-1024`.
5. **Scalar subquery cardinality kontrolü yok.** Çok satırda hata yerine ilk değeri alıyor; sıfır satırda NULL yerine boş string dönüyor. Scalar subquery bir row/column olmalı; çok row hata, sıfır row NULL'dır.[5] `scalar_cardinality`, `scalar_empty`; `src/execution/runtime.rs:1814-1818,1891-1895`. IN(subquery) NULL eşleşmesini gerçek eşitlik sayıyor (`in_subquery_null`).
6. **Composite / nullable / hedef-kolon FK sorunu.** Parent (1,2) varken child (1,999) kabul ediliyor; nullable FK'nin NULL değeri reddediliyor; FK parent'ın ilk fiziksel kolonundan farklı bir PRIMARY KEY kolonunu hedeflediğinde geçerli referans reddediliyor. Composite referans tüm ilgili kolonlarla karşılaştırılmalıdır; normal nullable FK NULL ile referans şartından muaf olur.[7] `fk_composite_invalid`, `fk_nullable`, `fk_nonphysical_parent_key`. `src/constraints/foreign_key.rs:20-43` yalnızca FK listesinin ilk kolonu ve Btree integer key kullanıyor; commit recheck aynı varsayımı tekrar ediyor (`src/catalog/mod.rs:305-333`).
7. **DROP TABLE RESTRICT dependent FK'ye rağmen parent'ı siliyor.** `drop_restrict_dependency` hatasız geçiyor; `drop_table` dependent olmayan smoke'u geçse de RESTRICT desteğini kanıtlamaz. Parser RESTRICT'i ayrı modellemiyor (`src/sql/parser.rs:1123-1140`), catalog drop dependency kontrolü yapmıyor (`src/catalog/mod.rs:871-912`).

### P1 — temel dil / tip eksikleri ve dar alt kümeler

- `DECIMAL/NUMERIC`, `VARCHAR`, `CAST`, `CASE`, `COALESCE`, `SELECT DISTINCT`, `IN(value-list)`, `LIKE`, yorumlar, unconditional DELETE, UPDATE expression, ALTER ADD COLUMN ve CREATE VIEW eksik. DISTINCT ve information_schema syntax'tan sonra bulunmayan column/table hatasına düşüyor; bu doğru sonuç üretmiş bir özellik değil.
- INTEGER ilk kolon fiziksel storage key şartı; CHAR PRIMARY KEY table yaratılabiliyor ancak INSERT reddediliyor (`pk_text`, partial). Composite PK duplicate çifti reddetse de aynı ilk kolonla farklı ikinci kolon `(1,2),(1,3)` geçerli olduğu halde fiziksel duplicate key nedeniyle reddediliyor (`composite_pk_distinct`, bug). Logical SQL key'leri ile fiziksel row identity ayrılmalı.
- CHAR padding, DATE string storage ve invalid date reddi geçti. DATE/TIMESTAMP typed literal syntax yok; bu yüzden tam F051 uyumu yok. Decimal yerine DOUBLE geçirmek exact numeric çözümü değildir.
- NOT NULL, basit PRIMARY KEY duplicate/null, FK insert/delete/update NO ACTION, tek seviye DELETE CASCADE, DEFAULT ve filtreli INSERT/UPDATE/DELETE örnekleri geçti. UNIQUE ve CHECK syntax yok. ON UPDATE CASCADE declaration kabul ediliyor fakat geçerli parent update delete-no-action kontrolünde reddediliyor (`fk_update_cascade`). Recursive cascade/deferred constraints bu suite'te **unverified**.

### Transaction ve API: birbirinden ayrı

COMMIT/ROLLBACK ve çok-row INSERT hatasında statement atomicity probe'ları geçti. Baseline concurrent testleri snapshot isolation, aborted work visibility ve first-committer-wins davranışlarını test etti (`tests/mvcc_concurrent.rs`). Bu **SERIALIZABLE desteği değildir**: `src/transaction/state.rs:26-38` yalnızca Snapshot enum'u içerir; SET TRANSACTION SERIALIZABLE / READ ONLY syntax yok. Savepoint Optional syntax yok.[1]

`E171` public error module incelemesinde SQLSTATE mapping/field yok (`src/error.rs:4-32`); DbError/mesaj ile standardın beş karakterli SQLSTATE'i aynı şey değil. Bu bir SQL statement probe'u değil, API bulgusudur. Rust `Database::query_all` bütün tabloyu Row olarak döndürür; Engine genel SELECT sonuçlarını stdout'a basar (`src/db.rs:85-99`, `src/engine/mod.rs:20-23`). ODBC/JDBC/SQL/CLI sürücü uyumluluğu bu audit'in dışında; API/sürücü eksikliğiyle SQL syntax/semantik hatalarını tek toplamda birleştirmiyoruz.

## Feature matrisi

Her satır yalnızca belirtilen kanıt kapsamı içindir. Ayrıntılı query, fixture, beklenen/gerçek row ve parse/runtime error: `audit/results-0.5.1.json`. Makine matrisi: `audit/sql-feature-matrix.json`.

| Feature ID | Standart kapsam | Gözlenen durum | Probe ID / kanıt | Kod konumu |
|---|---|---|---|---|
| E011-03 | Core | unsupported | `decimal` | `src/sql/ast.rs:193-225`; `src/storage/row.rs:6-29` |
| E011-04 | Core | bug | `arithmetic`, `divide_zero` | `src/sql/ast.rs:193-225`; `src/storage/row.rs:6-29` |
| E021-01 | Core | implemented | `char_padding` | `src/storage/row.rs:6-133` |
| E021-02 | Core | unsupported | `varchar` | `src/storage/row.rs:6-133` |
| E051 | Core | bug | `bad_sql_tail` | `src/sql/ast.rs:5-29`; `src/sql/ast.rs:134-184` |
| E051-01 | Core | unsupported | `distinct` | `src/sql/ast.rs:5-29`; `src/sql/ast.rs:134-184` |
| E051-06 | Core | implemented | `having` | `src/execution/runtime.rs:846-1025` |
| E061-01 | Core | implemented | `select_filter` | `src/sql/ast.rs:193-287`; `src/execution/runtime.rs:1749-1908` |
| E061-02 | Core | implemented | `between` | `src/sql/ast.rs:193-287`; `src/execution/runtime.rs:1749-1908` |
| E061-03 | Core | unsupported | `in_list` | `src/sql/ast.rs:193-287`; `src/execution/runtime.rs:1749-1908` |
| E061-04 | Core | unsupported | `like` | `src/sql/ast.rs:193-287`; `src/execution/runtime.rs:1749-1908` |
| E061-06 | Core | unsupported | `null_predicate` | `src/sql/ast.rs:193-287`; `src/execution/runtime.rs:1749-1908` |
| E061-08 | Core | implemented | `exists_uncorrelated` | `src/sql/ast.rs:193-287`; `src/execution/runtime.rs:1749-1908` |
| E061-11 | Core | bug | `in_subquery`, `in_subquery_null` | `src/sql/ast.rs:193-287`; `src/execution/runtime.rs:1749-1908` |
| E061-13 | Core | implemented | `correlated_exists` | `src/sql/ast.rs:193-287`; `src/execution/runtime.rs:1749-1908` |
| E061-14 | Core | bug | `three_value_or`, `three_value_and` | `src/sql/ast.rs:193-287`; `src/execution/runtime.rs:1749-1908` |
| E071-01 | Core | bug | `union`, `union_second_branch`, `union_duplicate_elimination` | `src/sql/ast.rs:134-184`; `src/sql/parser.rs:1121` |
| E071-02 | Core | bug | `union_all` | `src/sql/ast.rs:134-184`; `src/sql/parser.rs:1121` |
| E071-03 | Core | bug | `except` | `src/sql/ast.rs:134-184`; `src/sql/parser.rs:1121` |
| E091 | Core | bug | `aggregate_empty`, `all_null_aggregate` | `src/execution/runtime.rs:846-1025` |
| E091-01 | Core | bug | `aggregate_avg` | `src/execution/runtime.rs:846-1025` |
| E091-02 | Core | bug | `aggregate_count` | `src/execution/runtime.rs:846-1025` |
| E091-05 | Core | implemented | `aggregate_sum` | `src/execution/runtime.rs:846-1025` |
| E091-07 | Core | bug | `aggregate_distinct` | `src/execution/runtime.rs:846-1025` |
| E101-01 | Core | implemented | `multi_insert_atomic` | `src/sql/ast.rs:5-29`; `src/sql/ast.rs:134-184` |
| E101-03 | Core | partial | `update_expression`, `update` | `src/sql/ast.rs:5-29`; `src/sql/ast.rs:134-184` |
| E101-04 | Core | partial | `delete`, `delete_all` | `src/sql/ast.rs:5-29`; `src/sql/ast.rs:134-184` |
| E121-01 | Core | unsupported | `cursor` | `src/sql/ast.rs:5-29`; `src/sql/ast.rs:134-184` |
| E121-02 | Core | bug | `order_nonselected` | `src/sql/parser.rs:1053-1121`; `src/execution/runtime.rs:1159-1292` |
| E121-03 | Core | unsupported | `order_expr` | `src/sql/parser.rs:1053-1121`; `src/execution/runtime.rs:1159-1292` |
| E131 | Core | bug | `null_eq`, `null_neq`, `null_arithmetic`, `update_null` | `src/sql/ast.rs:193-287`; `src/execution/runtime.rs:1749-1908` |
| E141-01 | Core | implemented | `not_null` | `src/sql/ast.rs:5-29`; `src/sql/ast.rs:134-184` |
| E141-02 | Core | unsupported | `unique` | `src/sql/ast.rs:5-29`; `src/sql/ast.rs:134-184` |
| E141-03 | Core | bug | `pk_duplicate`, `composite_pk`, `pk_text`, `composite_pk_distinct` | `src/constraints/primary_key.rs:12-47`; `src/execution/runtime.rs:557-595` |
| E141-04 | Core | bug | `fk_insert`, `fk_delete_no_action`, `fk_update_no_action`, `fk_nullable`, `fk_composite_invalid`, `fk_nonphysical_parent_key` | `src/constraints/foreign_key.rs:20-43`; `src/constraints/foreign_key.rs:46-111` |
| E141-06 | Core | unsupported | `check` | `src/sql/ast.rs:5-29`; `src/sql/ast.rs:134-184` |
| E141-07 | Core | implemented | `default` | `src/sql/ast.rs:5-29`; `src/sql/ast.rs:134-184` |
| E141-08 | Core | implemented | `pk_null` | `src/constraints/primary_key.rs:12-47`; `src/execution/runtime.rs:557-595` |
| E151-01 | Core | implemented | `commit` | `src/transaction/state.rs:26-38`; `src/transaction/manager.rs:226-291` |
| E151-02 | Core | implemented | `rollback` | `src/transaction/state.rs:26-38`; `src/transaction/manager.rs:226-291` |
| E152-01 | Core | unsupported | `serializable` | `src/transaction/state.rs:26-38`; `src/transaction/manager.rs:226-291` |
| E152-02 | Core | unsupported | `readonly` | `src/transaction/state.rs:26-38`; `src/transaction/manager.rs:226-291` |
| E161 | Core | unsupported | `comment`, `inline_comment` | `src/sql/ast.rs:5-29`; `src/sql/ast.rs:134-184` |
| E171 | Core | unsupported | static API inspection | `src/error.rs:4-32` |
| F021-02 | Core | unsupported | `information_schema` | `src/sql/ast.rs:5-29`; `src/sql/ast.rs:134-184` |
| F031-01 | Core | partial | `pk_text`, `select_filter` | `src/sql/ast.rs:135-141`; `src/execution/runtime.rs:1090-1128` |
| F031-02 | Core | unsupported | `view` | `src/sql/ast.rs:5-29`; `src/sql/ast.rs:134-184` |
| F031-04 | Core | unsupported | `alter_add` | `src/sql/ast.rs:5-29`; `src/sql/ast.rs:134-184` |
| F031-13 | Core | bug | `drop_table`, `drop_restrict_dependency` | `src/sql/parser.rs:1123-1140`; `src/catalog/mod.rs:871-912` |
| F041-01 | Core | implemented | `inner_join` | `src/execution/runtime.rs:700-845`; `src/execution/runtime.rs:1276-1291` |
| F041-03 | Core | implemented | `left_join` | `src/execution/runtime.rs:700-845`; `src/execution/runtime.rs:1276-1291` |
| F041-04 | Core | implemented | `right_join` | `src/execution/runtime.rs:700-845`; `src/execution/runtime.rs:1276-1291` |
| F051-01 | Core | partial | `date_storage`, `date_literal`, `bad_date` | `src/storage/row.rs:6-133` |
| F051-03 | Core | unsupported | `timestamp_literal` | `src/storage/row.rs:6-133` |
| F191 | Optional | implemented | `fk_delete_cascade` | `src/constraints/foreign_key.rs:20-43`; `src/constraints/foreign_key.rs:46-111` |
| F201 | Core | unsupported | `cast` | `src/sql/ast.rs:5-29`; `src/sql/ast.rs:134-184` |
| F261-02 | Core | unsupported | `case` | `src/sql/ast.rs:5-29`; `src/sql/ast.rs:134-184` |
| F261-04 | Core | unsupported | `coalesce` | `src/sql/ast.rs:5-29`; `src/sql/ast.rs:134-184` |
| F302 | Optional | bug | `intersect` | `src/sql/ast.rs:134-184`; `src/sql/parser.rs:1121` |
| F406 | Optional | implemented | `full_join` | `src/execution/runtime.rs:700-845`; `src/execution/runtime.rs:1276-1291` |
| F471 | Core | bug | `scalar_one`, `scalar_empty`, `scalar_cardinality` | `src/execution/runtime.rs:1810-1824`; `src/execution/runtime.rs:1883-1896` |
| F701 | Optional | bug | `fk_update_cascade` | `src/constraints/foreign_key.rs:20-43`; `src/constraints/foreign_key.rs:46-111` |
| F850 | Optional | bug | `order_desc`, `order_nulls` | `src/sql/parser.rs:1053-1121`; `src/execution/runtime.rs:1159-1292` |
| T031 | Optional | implemented | `boolean_type` | `src/sql/ast.rs:5-29`; `src/sql/ast.rs:134-184` |
| T121 | Optional | unsupported | `cte` | `src/sql/ast.rs:5-29`; `src/sql/ast.rs:134-184` |
| T271 | Optional | unsupported | `savepoint` | `src/transaction/state.rs:26-38`; `src/transaction/manager.rs:226-291` |
| T611 | Optional | unsupported | `window` | `src/sql/ast.rs:5-29`; `src/sql/ast.rs:134-184` |
| T821 | Optional | unsupported | `json_value` | `src/sql/ast.rs:5-29`; `src/sql/ast.rs:134-184` |

## Tekrarlama ve kanıtlar

```bash
python3 audit/run_probes.py "$PWD" "$HOME/.hermes/cache/scratch/aerodb-sql-audit-recheck"
```

Cargo/Rust 2024 ve yerel dependency cache gerekir; audit native runner'ı geçici bir Cargo projesinde `--offline` derler. Repo `src/` veya examples'a dosya yazmaz. Her DB tempfile içinde açılır; teardown ile silinir. İlk cargo dependency fetch'i gerekiyorsa kullanıcı normal dependency setup'ını yapmalıdır; araç fixture sonuçları uydurmaz. stdout text API sınırlaması: native result-type metadata, text 'NULL' ile NULL ayrımı, driver protocols/collation/Unicode standard semantics ve performans ölçülmedi. Sorting testleri yalnızca ORDER BY olan vakalarda order-sensitive, diğerleri duplicate koruyan multiset karşılaştırmasıdır. Suite bulguları yüzünden exit 0 dönebilir: bu audit tamamlandı demektir, conformance geçti demek değildir. Process/build başarısızlıkları saklanmaz.

Kalıcı repo artifact'ları: `audit/run_probes.py`, `audit/sql_audit.rs`, `audit/cases.json`, `audit/results-0.5.1.json`, `audit/sql-feature-matrix.json`, `audit/audit-summary.json`, `audit/sources.json`. Ham loglar, kaynak browser snapshot'ları ve baseline: `/home/urunsiyabend/.hermes/cache/scratch/aerodb-sql-audit`. Audit scope'a yayılmayan kaynak dosyaları, sürüm, branch, remote değişmemiştir. Roadmap: `specs/sql-roadmap.md`.

## Sources

[1] https://www.postgresql.org/docs/18/features-sql-standard.html
[2] https://www.postgresql.org/docs/18/unsupported-features-sql-standard.html
[3] https://www.postgresql.org/docs/18/features.html
[4] https://www.postgresql.org/docs/18/functions-aggregate.html
[5] https://www.postgresql.org/docs/18/sql-expressions.html
[6] https://www.postgresql.org/docs/18/functions-comparison.html
[7] https://www.postgresql.org/docs/18/ddl-constraints.html
[8] https://www.postgresql.org/docs/18/queries-union.html

