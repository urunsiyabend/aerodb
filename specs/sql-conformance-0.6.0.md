# AeroDB 0.6.0 — SQL uyumluluk denetimi (0.5.1 audit'inin yeniden koşumu)

**Tarih:** 14 Ekim 2026. **Sürüm:** `0.6.0`. **Dal:** `release/sql-0.6.0`.

**Git HEAD:**
- Kaynak kod (`src/`, `tests/`) en son `fd7985824cd71b185fe16345a54230d74fb04013` commit'inde değişti.
- Sonraki commit'ler yalnız dokümantasyon ve audit artifact'ı ekler.
- Audit, bu belgeyi ekleyen commit üzerinde çalıştırıldı. Kesin HEAD, `audit/audit-summary-0.6.0.json` içindeki `head` alanındadır.

**Taban:** 0.5.1 denetimi (`specs/sql-conformance.md`, HEAD `675fdcd`).

## Sonuç ve kapsam

Bu belge 0.5.1 için yazılan 92 vakalık audit suite'inin 0.6.0 üzerindeki yeniden koşumudur.

**Bu belge ne değildir:**
- Bir SQL:2023 sertifikasyonu değildir.
- Standardın tüm özelliklerinin dökümü değildir.
- Bir genel uyumluluk yüzdesi vermez ve hiçbir sayı yüzdeye çevrilmemelidir.

`implemented`, yalnızca listelenen temsilî probe'ların ve testlerin doğrulandığı anlamına gelir; ilgili feature'ın tamamı için destek ilanı değildir. Satır sayıları herhangi bir standardın paydası değildir.

Audit, `audit/` altındaki dosyalara dokunulmadan çalıştırıldı: `run_probes.py`, `sql_audit.rs`, `cases.json` ve oracle desenleri 0.5.1'dekiyle aynıdır.

- **Ham sonuçlar:** `audit/results-0.6.0.json`.
- **Özet:** `audit/audit-summary-0.6.0.json`.
- **`"version": "0.5.1"` etiketi:** Runner'ın özetine yazdığı bu değer, değişmez 0.5.1 harness'ında sabit kodlu bir metindir. Derlenen sürümü göstermez; derlenen paket `0.6.0`'dır.

### Test sayıları

`cargo test --all-targets` ve `cargo test --release --all-targets`, kod HEAD'i `fd79858` üzerinde koşuldu:
- **49 sonuç bloğu, 558 test geçti, 0 başarısız, 0 ignored.** Debug ve release aynıdır.
- 0.5.1 tabanı 40 blok ve 227 testti. Ancak 0.5.1'de `main.rs` kütüphane modüllerini yeniden derlediği için lib unit testleri iki kez sayılıyordu. 0.6.0'da `main.rs` kütüphaneye bağlanır ve unit testler bir kez çalışır, dolayısıyla sayılar birebir karşılaştırılamaz.
- Testlerin yeşil olması aşağıda adı geçen sınırlamaları ortadan kaldırmaz.

### Audit durum sayıları

`audit/results-0.6.0.json` üzerinden hesaplanmıştır:

| Durum | 0.5.1 | 0.6.0 |
|---|---|---|
| implemented | 34 | **83** |
| bug | 28 | **0** |
| unsupported | 28 | **8** |
| partial | 1 | **0** |
| unverified | 1 | **1** |
| toplam vaka | 92 | 92 |

**Sebep dağılımı (0.6.0):**
- `result_matches` 67
- `expected_error_enforced` 15
- `syntax_rejected` 7
- `setup_parse_error` 1
- `empty_statement_api` 1
- `invalid_syntax_rejected` 1

**SQL/API denemeleri:** 309 (fixture dahil). Bunların 282'si OK, 16'sı execution error, 11'i parse error. Beklenen constraint ve hata vakaları da hata adımı içerir; OK sayısı doğru sonuç sayısı değildir.

**Geçişler (0.5.1 → 0.6.0):**
- bug → implemented: 28
- unsupported → implemented: 20
- partial → implemented: 1
- implemented → implemented: 34
- unsupported → unsupported: 8
- unverified → unverified: 1

Hiçbir vaka geriye gitmedi.

### Ara koşu ve ileti uyumluluğu düzeltmesi

HEAD `32a4197` üzerindeki ara koşuda tek bir `bug` kalmıştı: `drop_table`, `wrong_error` sebebiyle.
- **Neden:** Oracle deseni 0.5.1 metni `No such table` idi; 0.6.0 ise `table 't' not found` yazıyordu.
- **Düzeltme:** Oracle değiştirilmedi. `DbError::TableNotFound` mesajı 0.5.1 ifadesine döndürüldü: `no such table: t`. SQLSTATE `42P01` aynı kaldı.
- **Test:** `error::tests::table_not_found_message_says_no_such_table`.

## Yöntem

Yöntem 0.5.1 denetimiyle aynıdır:
- Her vaka ayrı süreçte ve ayrı `tempfile` veritabanında çalışır.
- Adapter, gerçek `Engine::execute` yolunu `parse_statement` ile kullanır.
- Kontrol edilenler: row multiset veya sıralı karşılaştırma, beklenen hata ve hata kategorisi (oracle deseni).

**Durum tanımları:**
- **implemented:** Listelenen temsilî davranışlar doğrulandı. Bu, feature'ın tamamı için destek ilanı değildir.
- **partial:** Olumlu bir alt küme var, ancak başka temel davranışlar eksik.
- **unsupported:** Syntax reddi, ya da implementasyonun bulunmadığını doğrulayan bir runtime hatası. 0.6.0'da bunların hepsi açık reddir: `0A000` veya `42883`.
- **bug:** Kabul edilen sorgu yanlış sonuç üretiyor, hata vermeden geçiyor ya da yanlış hata veriyor.
- **unverified:** Oracle veya API sınırı nedeniyle karar verilemiyor.

Feature ID'leri ve Core/Optional işaretleri 0.5.1 tablosundakilerle aynıdır; kaynaklar oradadır. Tabloda 68 satır vardır: 58 Core, 10 Optional. Parent feature ve subfeature satırları örtüşür.

`E171`, bir SQL probe'u değil, statik bir API incelemesidir; audit durum sayılarına dahil değildir.

## Feature matrisi

**Sütunlar:**
- "Probe": vaka kimliği ve 0.6.0 sonucu.
- "0.6.0 kanıtı": repo içindeki test dosyaları ve temsilî test adları.

Tüm `tests/sql_*.rs` dosyaları 0.6.0'da eklendi; `tests/storage_format.rs` ve `tests/mvcc_rollback_reopen.rs` de öyle.

| Feature ID | Standart kapsam | 0.5.1 | 0.6.0 gözlenen durum | Probe (0.6.0 sonucu) | 0.6.0 kanıtı (testler) |
|---|---|---|---|---|---|
| E011-03 | Core | unsupported | implemented | `decimal` (result_matches) | `tests/sql_identity_types.rs` (decimal_rounding_on_insert_and_update, decimal_overflow_is_22003, decimal_expression_assignment_is_exact, decimal_survives_reopen); `tests/sql_semantics.rs` (exact_numeric_arithmetic, decimal_sum_and_avg_are_exact); `src/sql/decimal.rs` unit |
| E011-04 | Core | bug | implemented | `arithmetic` (result_matches), `divide_zero` (expected_error_enforced) | `tests/sql_semantics.rs` (divide_zero, null_arithmetic, exact_numeric_arithmetic, integer_sum_overflow_is_an_error); `src/sql/value.rs` unit (int_division_truncates_toward_zero, division_by_zero_in_every_kind, int_overflow_is_detected, double_overflow_is_an_error) |
| E021-01 | Core | implemented | implemented | `char_padding` (result_matches) | `tests/sql_identity_types.rs` (char_multibyte_is_padded_by_characters, text_fk_matches_char_pad_space); `tests/char_type.rs` |
| E021-02 | Core | unsupported | implemented | `varchar` (result_matches) | `tests/sql_identity_types.rs` (varchar_length_counts_characters); `tests/sql_ddl.rs` (decimal_and_varchar_types_survive_reopen) |
| E051 | Core | bug | implemented | `bad_sql_tail` (invalid_syntax_rejected) | `tests/sql_parser.rs` (bad_sql_tail, rejects_tail_in_subquery, rejects_tail_after_create_table, rejects_tail_after_semicolon) |
| E051-01 | Core | unsupported | implemented | `distinct` (result_matches) | `tests/sql_semantics.rs` (distinct, distinct_order_by_must_use_select_list); `tests/sql_parser.rs` (select_distinct_is_not_a_column) |
| E051-06 | Core | implemented | implemented | `having` (result_matches) | `tests/sql_semantics.rs` (having, group_by_violation); `tests/having.rs` |
| E061-01 | Core | implemented | implemented | `select_filter` (result_matches) | `tests/sql_semantics.rs` (null_comparisons_are_unknown); `tests/operators.rs` |
| E061-02 | Core | implemented | implemented | `between` (result_matches) | `tests/sql_semantics.rs` (between_with_null) |
| E061-03 | Core | unsupported | implemented | `in_list` (result_matches) | `tests/sql_semantics.rs` (in_list, in_list_with_null_member) |
| E061-04 | Core | unsupported | implemented | `like` (result_matches) | `tests/sql_semantics.rs` (like, like_underscore_and_escape); `src/execution/eval.rs` unit (like_patterns) |
| E061-06 | Core | unsupported | implemented | `null_predicate` (result_matches) | `tests/sql_semantics.rs` (null_predicate) |
| E061-08 | Core | implemented | implemented | `exists_uncorrelated` (result_matches) | `tests/sql_semantics.rs` (correlated_exists); `tests/nested_queries.rs` |
| E061-11 | Core | bug | implemented | `in_subquery`, `in_subquery_null` (result_matches) | `tests/sql_semantics.rs` (in_subquery, in_subquery_null, not_in_subquery_containing_null_keeps_no_rows, null_in_empty_subquery_is_false) |
| E061-13 | Core | implemented | implemented | `correlated_exists` (result_matches) | `tests/sql_semantics.rs` (correlated_exists, correlated_reference_binds_in_innermost_matching_scope, correlated_scalar_subquery_in_projection) |
| E061-14 | Core | bug | implemented | `three_value_or`, `three_value_and` (result_matches) | `tests/sql_semantics.rs` (truth_tables, not_excludes_unknown_rows); `src/execution/eval.rs` unit (three_valued_logic) |
| E071-01 | Core | bug | implemented | `union`, `union_second_branch`, `union_duplicate_elimination` (result_matches) | `tests/sql_semantics.rs` (union, union_second_branch, union_duplicate_elimination, union_arity_mismatch, set_operation_order_by_applies_to_whole_query); `tests/sql_parser.rs` (union_parses_to_setop) |
| E071-02 | Core | bug | implemented | `union_all` (result_matches) | `tests/sql_semantics.rs` (union_all) |
| E071-03 | Core | bug | implemented | `except` (result_matches) | `tests/sql_semantics.rs` (except, except_all_and_intersect_all_use_multiplicities) |
| E091 | Core | bug | implemented | `aggregate_empty`, `all_null_aggregate` (result_matches) | `tests/sql_semantics.rs` (aggregate_empty, all_null_aggregate); `tests/aggregate.rs` |
| E091-01 | Core | bug | implemented | `aggregate_avg` (result_matches) | `tests/sql_semantics.rs` (aggregate_avg, decimal_sum_and_avg_are_exact) |
| E091-02 | Core | bug | implemented | `aggregate_count` (result_matches) | `tests/sql_semantics.rs` (aggregate_count) |
| E091-05 | Core | implemented | implemented | `aggregate_sum` (result_matches) | `tests/sql_semantics.rs` (aggregate_sum, integer_sum_overflow_is_an_error) |
| E091-07 | Core | bug | implemented | `aggregate_distinct` (result_matches) | `tests/sql_semantics.rs` (aggregate_distinct, sum_and_avg_distinct) |
| E101-01 | Core | implemented | implemented | `multi_insert_atomic` (result_matches) | `tests/sql_constraints.rs` (autocommit_multi_insert_is_still_atomic, check_multi_row_insert_with_one_bad_row_inserts_nothing); `tests/insert.rs` |
| E101-03 | Core | partial | implemented | `update_expression`, `update` (result_matches) | `tests/sql_semantics.rs` (update_expression, update_null); `tests/sql_constraints.rs` (update_creating_unique_duplicate_is_rejected, update_swapping_unique_values_succeeds) |
| E101-04 | Core | partial | implemented | `delete`, `delete_all` (result_matches) | `tests/sql_semantics.rs` (delete_all) |
| E121-01 | Core | unsupported | unsupported | `cursor` (syntax_rejected, 0A000) | `tests/sql_parser.rs` (cursor_is_unsupported) |
| E121-02 | Core | bug | implemented | `order_nonselected` (result_matches) | `tests/sql_semantics.rs` (order_nonselected, order_by_on_join_with_aggregate) |
| E121-03 | Core | unsupported | implemented | `order_expr` (result_matches) | `tests/sql_semantics.rs` (order_expr, order_by_alias, order_by_ordinal, order_by_name_ignores_derived_output_names) |
| E131 | Core | bug | implemented | `null_eq`, `null_neq`, `null_arithmetic`, `update_null` (result_matches) | `tests/sql_semantics.rs` (null_eq, null_neq, null_arithmetic, update_null, null_comparisons_are_unknown); `src/sql/value.rs` unit (null_propagates_through_arithmetic) |
| E141-01 | Core | implemented | implemented | `not_null` (expected_error_enforced) | `tests/not_null.rs`, `tests/nullability.rs`; `tests/sql_ddl.rs` (add_not_null_without_default_on_nonempty_table_fails, add_not_null_with_default) |
| E141-02 | Core | unsupported | implemented | `unique` (expected_error_enforced) | `tests/sql_constraints.rs` (unique_audit_probe, nullable_unique_allows_two_nulls, composite_unique_with_partial_null_allowed_twice, unique_duplicate_within_multi_row_insert_has_no_effect, database_concurrent_unique_commit_sees_latest_state) |
| E141-03 | Core | bug | implemented | `pk_duplicate`, `composite_pk` (expected_error_enforced), `pk_text`, `composite_pk_distinct` (result_matches) | `tests/sql_identity_types.rs` (pk_text, composite_pk_distinct, non_first_column_primary_key, rowid_allocation_survives_reopen, commit_recheck_rejects_concurrent_duplicate_text_key); `tests/storage_format.rs` (legacy_nonfirst_integer_primary_key_keeps_identity); `tests/primary_key.rs` |
| E141-04 | Core | bug | implemented | `fk_insert`, `fk_delete_no_action`, `fk_update_no_action`, `fk_composite_invalid` (expected_error_enforced), `fk_nullable`, `fk_nonphysical_parent_key` (result_matches) | `tests/sql_identity_types.rs` (fk_composite_invalid, fk_nullable, fk_nonphysical_parent_key, fk_must_reference_primary_key, fk_arity_mismatch, fk_type_mismatch, self_referencing_fk, commit_recheck_rejects_child_of_concurrently_deleted_parent); `tests/sql_constraints.rs` (on_update_restrict_rejects_key_swap, on_update_no_action_accepts_key_swap, on_delete_restrict_*, database_stale_fk_references_are_rejected_at_commit); `tests/sql_metadata.rs` (commit_time_foreign_key_violation_is_23503); `tests/foreign_keys.rs` |
| E141-06 | Core | unsupported | implemented | `check` (expected_error_enforced) | `tests/sql_constraints.rs` (check_audit_probe, check_null_is_unknown_and_passes, table_check_across_two_columns, check_with_subquery_or_aggregate_is_0a000, unique_and_check_survive_reopen); `tests/sql_binding.rs` (check_expression_must_be_boolean) |
| E141-07 | Core | implemented | implemented | `default` (result_matches) | `tests/default_values.rs`; `tests/sql_semantics.rs` (default_expression_survives_reopen); `tests/sql_binding.rs` (default_expressions_are_bound) |
| E141-08 | Core | implemented | implemented | `pk_null` (expected_error_enforced) | `tests/primary_key.rs`; `tests/sql_identity_types.rs` |
| E151-01 | Core | implemented | implemented | `commit` (result_matches) | `tests/mvcc_rollback.rs`; `tests/sql_constraints.rs` (failed_statement_aborts_explicit_tx, database_failed_statement_aborts_transaction) |
| E151-02 | Core | implemented | implemented | `rollback` (result_matches) | `tests/mvcc_rollback.rs`, `tests/mvcc_rollback_reopen.rs`; `tests/sql_constraints.rs` (rollback_after_failure_then_new_begin_works) |
| E152-01 | Core | unsupported | unsupported | `serializable` (syntax_rejected, 0A000) | `tests/sql_parser.rs` (set_transaction_is_unsupported) |
| E152-02 | Core | unsupported | unsupported | `readonly` (syntax_rejected, 0A000) | `tests/sql_parser.rs` (set_transaction_is_unsupported) |
| E161 | Core | unsupported | implemented (`inline_comment`) / unverified (`comment`) | `inline_comment` (result_matches), `comment` (empty_statement_api) | `tests/sql_parser.rs` (inline_comment, block_comment, comment_only_script_is_empty); `src/sql/lexer.rs` unit (comments_are_skipped) |
| E171 | Core | unsupported | implemented — yalnız Rust API, statik inceleme (probe yok, sayılara dahil değil) | — | `src/error.rs` unit (every_variant_has_its_sqlstate, table_not_found_message_says_no_such_table); `tests/sql_metadata.rs` (engine_errors_carry_their_sqlstate: 23 deyim) |
| F021-02 | Core | unsupported | implemented (alt küme) | `information_schema` (result_matches) | `tests/sql_metadata.rs` (14 test: tables, columns, table_constraints, key_column_usage, referential_constraints, check_constraints, views, salt-okunurluk, reopen) |
| F031-01 | Core | partial | implemented | `pk_text`, `select_filter` (result_matches) | `tests/sql_identity_types.rs` (pk_text, table_without_primary_key_accepts_duplicate_rows); `tests/sql_ddl.rs` (create_table_duplicate_column_follows_identifier_rule) |
| F031-02 | Core | unsupported | implemented | `view` (result_matches) | `tests/sql_ddl.rs` (view_audit_probe, view_with_column_list, view_over_view, reopen_keeps_view, dml_on_view_is_unsupported, view_star_keeps_definition_time_columns) |
| F031-04 | Core | unsupported | implemented | `alter_add` (result_matches) | `tests/sql_ddl.rs` (alter_add, add_column_with_default, add_column_check_validated_against_existing_rows, alter_in_transaction_rollback_restores_shape_and_data, reopen_after_alter_keeps_column_and_values) |
| F031-13 | Core | bug | implemented | `drop_table`, `drop_restrict_dependency` (expected_error_enforced) | `tests/sql_ddl.rs` (drop_restrict_dependency, drop_child_then_parent_works, drop_cascade_is_unsupported, drop_table_referenced_by_view_is_rejected, drop_wrong_object_type); `src/error.rs` unit (table_not_found_message_says_no_such_table) |
| F041-01 | Core | implemented | implemented | `inner_join` (result_matches) | `tests/sql_semantics.rs` (inner_join, ambiguous_column_in_join); `tests/multi_join.rs` |
| F041-03 | Core | implemented | implemented | `left_join` (result_matches) | `tests/sql_semantics.rs` (left_join) |
| F041-04 | Core | implemented | implemented | `right_join` (result_matches) | `tests/sql_semantics.rs` (right_join) |
| F051-01 | Core | partial | implemented | `date_storage`, `date_literal` (result_matches), `bad_date` (expected_error_enforced) | `tests/sql_binding.rs` (character_literal_next_to_a_date_is_converted_at_bind_time); `src/sql/value.rs` unit (cast_to_temporal, compare_text_literal_with_temporal) |
| F051-03 | Core | unsupported | unsupported | `timestamp_literal` (syntax_rejected, 0A000) | `TIMESTAMP '…'` / `TIME '…'` literal'leri bilinçli olarak reddedilir |
| F191 | Optional | implemented | implemented | `fk_delete_cascade` (result_matches) | `tests/sql_identity_types.rs` (on_delete_cascade_with_composite_key, on_delete_cascade_self_referencing_cycle); `tests/foreign_keys.rs` |
| F201 | Core | unsupported | implemented | `cast` (result_matches) | `tests/sql_semantics.rs` (cast); `src/sql/value.rs` unit (cast_text_to_integer, cast_numeric_to_integer_rounds_half_away, cast_to_decimal_and_text, cast_to_char_and_varchar) |
| F261-02 | Core | unsupported | implemented | `case` (result_matches) | `tests/sql_semantics.rs` (case); `src/execution/eval.rs` unit (functions_and_case) |
| F261-04 | Core | unsupported | implemented | `coalesce` (result_matches) | `tests/sql_semantics.rs` (coalesce); `tests/sql_binding.rs` |
| F302 | Optional | bug | implemented | `intersect` (result_matches) | `tests/sql_semantics.rs` (intersect, except_all_and_intersect_all_use_multiplicities); `tests/sql_parser.rs` (intersect_binds_tighter) |
| F406 | Optional | implemented | implemented | `full_join` (result_matches) | `tests/sql_semantics.rs` (full_join) |
| F471 | Core | bug | implemented | `scalar_one`, `scalar_empty` (result_matches), `scalar_cardinality` (expected_error_enforced) | `tests/sql_semantics.rs` (scalar_one, scalar_empty, scalar_cardinality, correlated_scalar_subquery_in_projection) |
| F701 | Optional | bug | implemented | `fk_update_cascade` (result_matches) | `tests/sql_constraints.rs` (fk_update_cascade, composite_on_update_cascade, on_update_cascade_chain_grandparent_parent_child, cascade_into_child_whose_check_rejects_fails_the_whole_update) |
| F850 | Optional | bug | implemented | `order_desc`, `order_nulls` (result_matches) | `tests/sql_semantics.rs` (order_desc, order_nulls, order_by_multiple_keys, limit_and_offset_apply_after_order) |
| T031 | Optional | implemented | implemented | `boolean_type` (result_matches) | `tests/sql_semantics.rs` (truth_tables); `src/execution/eval.rs` unit (three_valued_logic) |
| T121 | Optional | unsupported | unsupported | `cte` (syntax_rejected, 0A000) | `tests/sql_parser.rs` (cte_is_unsupported) |
| T271 | Optional | unsupported | unsupported | `savepoint` (setup_parse_error, 0A000) | `tests/sql_parser.rs` (savepoint_is_unsupported) |
| T611 | Optional | unsupported | unsupported | `window` (syntax_rejected, 0A000) | `tests/sql_parser.rs` (window_is_unsupported) |
| T821 | Optional | unsupported | unsupported | `json_value` (syntax_rejected, 42883) | `tests/sql_parser.rs` (rejects_unknown_function) |

## Kalan sınırlamalar / named limitations

### Uygulanmamış probe'lar

8 unsupported ve 1 unverified probe kaldı:

| Probe | Feature | Durum | Gözlenen hata | Neden |
|---|---|---|---|---|
| `timestamp_literal` | F051-03 | unsupported | `feature not supported: TIMESTAMP/TIME typed literals` (0A000) | Typed TIMESTAMP/TIME literal sözdizimi yok. Değer bir string literal'den atanabilir (`'2024-01-02 03:04:05'`); `DATE '…'` desteklenir. |
| `savepoint` | T271 | unsupported | `feature not supported: SAVEPOINT`, `… ROLLBACK TO SAVEPOINT` (0A000) | Savepoint ve transaction içinde kısmi geri alma yok. |
| `serializable` | E152-01 | unsupported | `feature not supported: SET TRANSACTION` (0A000) | Tek izolasyon seviyesi snapshot isolation'dır; SERIALIZABLE değildir. |
| `readonly` | E152-02 | unsupported | `feature not supported: SET TRANSACTION` (0A000) | READ ONLY transaction modu yok. |
| `cte` | T121 | unsupported | `feature not supported: WITH (common table expressions)` (0A000) | CTE yok. |
| `window` | T611 | unsupported | `feature not supported: window functions (OVER)` (0A000) | Window fonksiyonları ve `WINDOW` yok. |
| `json_value` | T821 | unsupported | `function JSON_VALUE does not exist` (42883) | JSON fonksiyonları yok. |
| `cursor` | E121-01 | unsupported | `feature not supported: DECLARE (cursors)` (0A000) | SQL cursor yok. |
| `comment` | E161 | unverified | `syntax error: empty statement` (42601) | Ayrıntısı aşağıda. |

**`comment` probe'unun ayrıntısı:**
- Probe, yalnız bir tam satır yorumunu (`-- audit comment SELECT 99`) tek bir deyim olarak gönderir.
- Harness'ın boş deyim için bir API'si yoktur: `parse_statement` tek bir deyim bekler ve boş girdi 42601'dir.
- Script düzeyinde aynı yorum yok sayılır: `parse_script("-- …")` boş bir liste (`[]`) döndürür; test `tests/sql_parser.rs::comment_only_script_is_empty`.
- Satır içi yorumlar (`inline_comment`) ve blok yorumlar çalışır.
- Bu durum 0.5.1'de de unverified idi.

### 0A000 ile reddedilen diğer sözdizimleri

Bunlar audit'te probe'lanmadı; hepsi açıkça reddedilir, sessizce yok sayılmaz:
- `RELEASE SAVEPOINT`, `INTERVAL` literal'leri.
- `INSERT … SELECT`, sorgu olarak `VALUES`.
- `NATURAL JOIN`, `JOIN … USING`, parantezli join'ler, `FETCH FIRST`.
- `IS [NOT] DISTINCT FROM`, `ANY`/`SOME`/`ALL`, row value constructor, `SIMILAR TO`, `BETWEEN SYMMETRIC`.
- Aggregate `FILTER` / `WITHIN GROUP`.
- `ON DELETE/UPDATE SET NULL / SET DEFAULT`, `DROP TABLE/VIEW … CASCADE`, `CREATE OR REPLACE VIEW`.
- `ADD COLUMN` dışındaki `ALTER TABLE` biçimleri, `ADD CONSTRAINT`.
- `CREATE UNIQUE INDEX`, çok kolonlu indeksler.
- View üzerinde DML.
- Katalog ve şema nitelemeli kolon referansları.

### Belgelenmiş sapmalar (standarttan veya beklentiden)

**İzolasyon ve DDL:**
- **İzolasyon:** Yalnız snapshot isolation vardır; SERIALIZABLE değildir. Write skew engellenmez.
- **DDL snapshot'lanmaz:**
  - Eşzamanlı `Database` transaction'ları birbirinin DDL'ini hemen görür; `information_schema` dahil.
  - Eski bir snapshot, yeni eklenen kolonu NULL olarak okur.
  - `ALTER TABLE ADD COLUMN`, eşzamanlı commit edilmemiş transaction'ların satırlarını yeniden yazmaz; bu satırlar DEFAULT yerine NULL okunur.
  - Ayrı `Database` transaction'larındaki eşzamanlı DDL birbirinden izole değildir: bir commit, başka bir transaction'ın commit edilmemiş katalog sayfasını diske yazabilir. DDL eşzamanlılığı roadmap'te P2'dir ve doğrulanmamıştır.
- **Sözdizimi hatası ve failed transaction:** Açık transaction içindeki bir sözdizimi hatası, transaction'ı failed duruma (25P02) yalnız `Engine::execute_sql` / `Engine::query` yolunda geçirir. CLI ve `parse_statement` + `Engine::execute` kullanan çağıranlar (audit adapter'ı dahil) parse hatasını raporlar, transaction'ı bozmaz.

**FK ve atama kuralları:**
- **RESTRICT:**
  - `ON DELETE/UPDATE RESTRICT` anında kontrol edilir.
  - Deyimin kendisinin silmediği veya yeniden bağlamadığı bir child satır eski anahtara hâlâ referans veriyorsa 23503 döner.
  - Anahtar takası RESTRICT altında reddedilir, NO ACTION altında kabul edilir.
- **Atama:**
  - Her değer CHAR/VARCHAR/TEXT kolonuna atanabilir. Bu PostgreSQL davranışıdır ve standarttan daha gevşektir.
  - Character literal'ler bind sırasında kolon tipine çevrilir.
  - Kategoriler arası diğer atamalar 42804 ile reddedilir.
  - Yalnız string sabitlerinden oluşan `CASE` / `COALESCE` ifadesinin sayısal veya boolean kolona atanması bind'da değil, değer başına çalışma anında kontrol edilir.
- **FK hedefi:** Hedef, parent'ın PRIMARY KEY'i ya da bir UNIQUE kolon kümesi olmalıdır (42830). 0.5.1'de PK olmayan kolona verilmiş FK içeren eski kataloglar yine de yüklenir.

**Tipler ve ifadeler:**
- **Tamsayı bölmesi:** `INTEGER / INTEGER` sıfıra doğru keser. INTEGER kolonları 32 bittir; ifadeler 64 bitte hesaplanır ve atama sırasında taşma 22003'tür.
- **Tarih:** Yalnız tarih içeren bir string TIMESTAMP/DATETIME için kabul edilmez.
- **Kabul edilen standart dışı sözdizimi:**
  - `a = b = c` gibi zincirli karşılaştırmalar kabul edilir.
  - `BEGIN IMMEDIATE/DEFERRED` ifadesindeki kelime transaction adı olarak okunur.
- **Gruplama (fail-closed):**
  - `GROUP BY -1` / `GROUP BY 1.0` ordinal sayılmaz (42803).
  - Gruplanmış dış sorgu içinde yalnız dış kolonlara uygulanan bir aggregate 0A000 değil 42803 verir.
  - İç içe grup kontrolü bütün ifadeleri değil kolon referanslarını karşılaştırır.
- **Sonuç başlığı:** Hesaplanmış DECIMAL sütunlarının başlıktaki precision/scale değeri ilk değerden türetilir; örneğin `SUM(total) DECIMAL(5,2)`.
- **Sonuç başlığı tip etiketleri** 0.5.1'den farklı olabilir; örneğin DOUBLE üzerindeki `SUM` artık `DOUBLE` gösterir.

**İsimler ve meta veri:**
- **Şema niteleyicileri:**
  - `public.t` 42P01 verir, oysa `information_schema` `table_schema = 'public'` raporlar.
  - Şema nitelemeli kolon referansları 0A000'dır.
  - Kullanılabilen tek şema niteleyicisi `information_schema`'dır.
- **İsim eşleme:** `DROP TABLE` / `DROP VIEW` / `DROP INDEX` isimleri birebir (büyük/küçük harf duyarlı) eşler; diğer referanslar ise tırnaksız tanımlayıcı kuralını izler.
- **Meta veri:**
  - `unique_constraint_name`, aynı kolon kümesinde PK'yi tercih eder.
  - PK ihlali mesajı constraint adını içermez.
  - 0.5.1'de yazılmış default'lar tırnaklı literal olarak gösterilir (`'5'`).
  - Bozuk view açıklama yolu (`view V cannot be described`) testsizdir; SQL ile bugün ulaşılamaz.

**Depolama ve performans:**
- **Satır boyutu:** Bir satır tek bir 4 KiB sayfaya sığmalıdır (yaklaşık 4 KB serileştirilmiş veri). Daha büyük satırlar yazmadan önce 54000 ile reddedilir.
- **İndeksler:** İkincil indeksler reopen'dan sonra kalıcı değildir (0.5.1'den beri). Tek kolonlu eşitlik içindir.
- **Performans:**
  - FK kontrolleri parent satırlarını tarar (O(n·m)).
  - `UPDATE/DELETE … WHERE`, PK ve UNIQUE kontrolleri görünür satırları tarar.
  - Aynı indeks değerine sahip çok satır tek transaction'da karesel büyür.
- **Dayanıklılık maliyeti:**
  - ROLLBACK, iptal edilen transaction'ın sayfalarını kalıcı yazar (commit gibi fsync).
  - İptal edilen transaction'ların veya root pinleme sonucu terk edilen sayfalar yeniden kullanılmaz; free-page listesi yoktur.
- **Çökme pencereleri:**
  - Yepyeni bir veritabanının ilk transaction'ı, abort kaydı ile meta yazımı arasında çökerse transaction id'leri 1'den yeniden başlayabilir.
  - Bir sayfa imajı, başka bir canlı transaction'ın commit edilmemiş split'ini içerebilir.
- **Depolama formatı:**
  - Format sürümü 2'dir.
  - 0.5.x dosyaları okunur ve yazılır; ilk yazım dosyayı sürüm 2 olarak işaretler.
  - 0.5.x, yeni özellik kullanan v2 dosyalarında açık hatayla durur: view satırında "root page not int", yeni tip kodlarında "bad type", yeni değer etiketlerinde "Unknown type tag".
  - Bilinmeyen gelecek sürümler, dosyalar değiştirilmeden reddedilir.

**API ve CLI:**
- **Rust API:**
  - `Engine::query` / `Engine::execute_sql` / `Database::query` gömülü Rust API'leridir. SQL/CLI, ODBC veya JDBC değildir.
  - Kütüphane, CLI'dan kalma durum satırlarını stdout'a basar.
  - `Engine::new` açılamayan dosyada panic eder; bunun yerine `Engine::open` kullanılmalıdır.
- **CLI:** CLI sabit `data.aerodb` yolunu kullanır, satır başına bir deyim kabul eder ve SQLSTATE göstermez; SQLSTATE yalnız Rust API'de `DbError::sqlstate()` ile alınır.

Ayrıntılı liste: `CHANGELOG.md` → `[0.6.0]` → "Changed / breaking" ve "Known limitations".

## Tekrarlama

```bash
python3 audit/run_probes.py "$PWD" "$PWD/audit/results"
```

- `audit/results/` git tarafından yok sayılır (`audit/.gitignore`).
- Runner, aerodb'yi geçici bir Cargo projesinde `--offline` derler.
- Her veritabanı bir tempfile dizininde açılır.
- Exit kodu 0 olması "audit tamamlandı" demektir, "conformance geçti" demek değildir.

Kalıcı artifact'lar:
- `audit/results-0.6.0.json`: bu koşunun `results.json`'ının birebir kopyası.
- `audit/audit-summary-0.6.0.json`: özet, HEAD ve sürüm notu.

0.5.1 artifact'ları (`audit/results-0.5.1.json`, `audit/audit-summary.json`, `specs/sql-conformance.md`) değiştirilmedi.
