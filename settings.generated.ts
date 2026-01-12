// AUTO-GENERATED from ClickHouse v26.1.1.1-new Settings.cpp
// Run: make update-settings
// Do not edit manually.

/**
 * Typed ClickHouse settings interface.
 * Generated from official ClickHouse source code.
 */
export interface ClickHouseSettings {
  /**
   * Write add http CORS header.
   */
  add_http_cors_header?: boolean;

  /**
   * An additional filter expression to apply to the result of `SELECT` query. This setting is not applied to any subquery. **Example** ```sql INSERT INTO table_1 VALUES (1, 'a'), (2, 'bb'), (3, 'ccc'), (4, 'dddd'); SElECT * FROM table_1; ``` ```response ┌─x─┬─y────┐ │ 1 │ a │ │ 2 │ bb │ │ 3 │ ccc │ │ 4 │ dddd │ └───┴──────┘ ``` ```sql SELECT * FROM table_1 SETTINGS additional_result_filter = 'x != 2' ``` ```response ┌─x─┬─y────┐ │ 1 │ a │ │ 3 │ ccc │ │ 4 │ dddd │ └───┴──────┘ ```
   */
  additional_result_filter?: string;

  /**
   * An additional filter expression that is applied after reading from the specified table. **Example** ```sql INSERT INTO table_1 VALUES (1, 'a'), (2, 'bb'), (3, 'ccc'), (4, 'dddd'); SELECT * FROM table_1; ``` ```response ┌─x─┬─y────┐ │ 1 │ a │ │ 2 │ bb │ │ 3 │ ccc │ │ 4 │ dddd │ └───┴──────┘ ``` ```sql SELECT * FROM table_1 SETTINGS additional_table_filters = {'table_1': 'x != 2'} ``` ```response ┌─x─┬─y────┐ │ 1 │ a │ │ 3 │ ccc │ │ 4 │ dddd │ └───┴──────┘ ```
   */
  additional_table_filters?: Record<string, string>;

  /**
   * Format for AggregateFunction input during INSERT operations. Possible values: - `state` — Binary string with the serialized state (the default). This is the default behavior where AggregateFunction values are expected as binary data. - `value` — The format expects a single value of the argument of the aggregate function, or in the case of multiple arguments, a tuple of them. They will be deserialized using the corresponding IDataType or DataTypeTuple and then aggregated to form the state. - `array` — The format expects an Array of values, as described in the `value` option above. All elements of the array will be aggregated to form the state. **Examples** For a table with structure: ```sql CREATE TABLE example ( user_id UInt64, avg_session_length AggregateFunction(avg, UInt32) ); ``` With `aggregate_function_input_format = 'value'`: ```sql INSERT INTO example FORMAT CSV 123,456 ``` With `aggregate_function_input_format = 'array'`: ```sql INSERT INTO example FORMAT CSV 123,"[456,789,101]" ``` Note: The `value` and `array` formats are slower than the default `state` format as they require creating and aggregating values during insertion.
   * @since 26.1
   */
  aggregate_function_input_format?: 'state' | 'value' | 'array';

  /**
   * Enables or disables rewriting all aggregate functions in a query, adding [-OrNull](/sql-reference/aggregate-functions/combinators#-ornull) suffix to them. Enable it for SQL standard compatibility. It is implemented via query rewrite (similar to [count_distinct_implementation](#count_distinct_implementation) setting) to get consistent results for distributed queries. Possible values: - 0 — Disabled. - 1 — Enabled. **Example** Consider the following query with aggregate functions: ```sql SELECT SUM(-1), MAX(0) FROM system.one WHERE 0; ``` With `aggregate_functions_null_for_empty = 0` it would produce: ```text ┌─SUM(-1)─┬─MAX(0)─┐ │ 0 │ 0 │ └─────────┴────────┘ ``` With `aggregate_functions_null_for_empty = 1` the result would be: ```text ┌─SUMOrNull(-1)─┬─MAXOrNull(0)─┐ │ NULL │ NULL │ └───────────────┴──────────────┘ ```
   */
  aggregate_functions_null_for_empty?: boolean;

  /**
   * Maximal size of block in bytes accumulated during aggregation in order of primary key. Lower block size allows to parallelize more final merge stage of aggregation.
   */
  aggregation_in_order_max_block_bytes?: number;

  /**
   * Number of threads to use for merge intermediate aggregation results in memory efficient mode. When bigger, then more memory is consumed. 0 means - same as 'max_threads'.
   */
  aggregation_memory_efficient_merge_threads?: number;

  /**
   * Enable independent aggregation of partitions on separate threads when partition key suits group by key. Beneficial when number of partitions close to number of cores and partitions have roughly the same size
   */
  allow_aggregate_partitions_independently?: boolean;

  /**
   * File/S3 engines/table function will parse paths with '::' as `<archive> :: <file>` if the archive has correct extension.
   */
  allow_archive_path_syntax?: boolean;

  /**
   * Use background I/O pool to read from MergeTree tables. This setting may increase performance for I/O bound queries
   */
  allow_asynchronous_read_from_io_pool_for_merge_tree?: boolean;

  /**
   * If it's enabled, in hedged requests we can start new connection until receiving first data packet even if we have already made some progress (but progress haven't updated for `receive_data_timeout` timeout), otherwise we disable changing replica after the first time we made progress.
   */
  allow_changing_replica_until_first_data_packet?: boolean;

  /**
   * Allow CREATE INDEX query without TYPE. Query will be ignored. Made for SQL compatibility tests.
   */
  allow_create_index_without_type?: boolean;

  /**
   * Enable custom error code in function throwIf(). If true, thrown exceptions may have unexpected error codes.
   */
  allow_custom_error_code_in_throwif?: boolean;

  /**
   * If it is set to true, then a user is allowed to executed DDL queries.
   */
  allow_ddl?: boolean;

  /**
   * Allow to create databases with deprecated Ordinary engine
   */
  allow_deprecated_database_ordinary?: boolean;

  /**
   * Allow usage of deprecated error prone window functions (neighbor, runningAccumulate, runningDifferenceStartingWithFirstValue, runningDifference)
   */
  allow_deprecated_error_prone_window_functions?: boolean;

  /**
   * Functions `snowflakeToDateTime`, `snowflakeToDateTime64`, `dateTimeToSnowflake`, and `dateTime64ToSnowflake` are deprecated and disabled by default. Please use functions `snowflakeIDToDateTime`, `snowflakeIDToDateTime64`, `dateTimeToSnowflakeID`, and `dateTime64ToSnowflakeID` instead. To re-enable the deprecated functions (e.g., during a transition period), please set this setting to `true`.
   */
  allow_deprecated_snowflake_conversion_functions?: boolean;

  /**
   * Allow to create *MergeTree tables with deprecated engine definition syntax
   */
  allow_deprecated_syntax_for_merge_tree?: boolean;

  /**
   * If it is set to true, then a user is allowed to executed distributed DDL queries.
   */
  allow_distributed_ddl?: boolean;

  /**
   * Allow ALTER TABLE ... DROP DETACHED PART[ITION] ... queries
   */
  allow_drop_detached?: boolean;

  /**
   * Allows using Dynamic type in JOIN keys. Added for compatibility. It's not recommended to use Dynamic type in JOIN keys because comparison with other types may lead to unexpected results.
   * @since 25.11
   */
  allow_dynamic_type_in_join_keys?: boolean;

  /**
   * Allow execute multiIf function columnar
   */
  allow_execute_multiif_columnar?: boolean;

  /**
   * Allow to create table with the Alias engine.
   * @since 25.12
   */
  allow_experimental_alias_table_engine?: boolean;

  /**
   * Allow new query analyzer.
   */
  allow_experimental_analyzer?: boolean;

  /**
   * If it is set to true, allow to specify experimental compression codecs (but we don't have those yet and this option does nothing).
   */
  allow_experimental_codecs?: boolean;

  /**
   * Allow to execute correlated subqueries.
   * @since 25.6
   */
  allow_experimental_correlated_subqueries?: boolean;

  /**
   * Allow experimental database engine DataLakeCatalog with catalog_type = 'glue'
   * @since 25.4
   */
  allow_experimental_database_glue_catalog?: boolean;

  /**
   * Allow experimental database engine DataLakeCatalog with catalog_type = 'hms'
   * @since 25.6
   */
  allow_experimental_database_hms_catalog?: boolean;

  /**
   * Allow experimental database engine DataLakeCatalog with catalog_type = 'iceberg'
   * @since 25.1
   */
  allow_experimental_database_iceberg?: boolean;

  /**
   * Allow to create database with Engine=MaterializedPostgreSQL(...).
   */
  allow_experimental_database_materialized_postgresql?: boolean;

  /**
   * Allow experimental database engine DataLakeCatalog with catalog_type = 'unity'
   * @since 25.4
   */
  allow_experimental_database_unity_catalog?: boolean;

  /**
   * Allow experimental delta-kernel-rs implementation.
   * @since 25.6
   */
  allow_experimental_delta_kernel_rs?: boolean;

  /**
   * Enables delta-kernel writes feature.
   * @since 25.10
   */
  allow_experimental_delta_lake_writes?: boolean;

  /**
   * Enable experimental functions for funnel analysis.
   */
  allow_experimental_funnel_functions?: boolean;

  /**
   * Enable experimental hash functions
   */
  allow_experimental_hash_functions?: boolean;

  /**
   * Allow to explicitly use 'OPTIMIZE' for iceberg tables.
   * @since 25.9
   */
  allow_experimental_iceberg_compaction?: boolean;

  /**
   * Allow to execute `insert` queries into iceberg.
   * @since 25.8
   */
  allow_experimental_insert_into_iceberg?: boolean;

  /**
   * If it is set to true, and the conditions of `join_to_sort_minimum_perkey_rows` and `join_to_sort_maximum_table_rows` are met, rerange the right table by key to improve the performance in left or inner hash join.
   */
  allow_experimental_join_right_table_sorting?: boolean;

  /**
   * Allow experimental feature to store Kafka related offsets in ClickHouse Keeper. When enabled a ClickHouse Keeper path and replica name can be specified to the Kafka table engine. As a result instead of the regular Kafka engine, a new type of storage engine will be used that stores the committed offsets primarily in ClickHouse Keeper
   */
  allow_experimental_kafka_offsets_storage_in_keeper?: boolean;

  /**
   * Enable Kusto Query Language (KQL) - an alternative to SQL.
   * @since 25.2
   */
  allow_experimental_kusto_dialect?: boolean;

  /**
   * Allows to use the MaterializedPostgreSQL table engine. Disabled by default, because this feature is experimental
   */
  allow_experimental_materialized_postgresql_table?: boolean;

  /**
   * Enable experimental functions for natural language processing.
   */
  allow_experimental_nlp_functions?: boolean;

  /**
   * Use up to `max_parallel_replicas` the number of replicas from each shard for SELECT query execution. Reading is parallelized and coordinated dynamically. 0 - disabled, 1 - enabled, silently disable them in case of failure, 2 - enabled, throw an exception in case of failure
   */
  allow_experimental_parallel_reading_from_replicas?: number;

  /**
   * Enable PRQL - an alternative to SQL.
   * @since 25.2
   */
  allow_experimental_prql_dialect?: boolean;

  /**
   * Allows creation of [QBit](../../sql-reference/data-types/qbit.md) data type.
   * @since 25.11
   */
  allow_experimental_qbit_type?: boolean;

  /**
   * Experimental data deduplication for SELECT queries based on part UUIDs
   */
  allow_experimental_query_deduplication?: boolean;

  /**
   * Allows defining columns with [statistics](../../engines/table-engines/mergetree-family/mergetree.md/#table_engine-mergetree-creating-a-table) and [manipulate statistics](../../engines/table-engines/mergetree-family/mergetree.md/#column-statistics).
   */
  allow_experimental_statistics?: boolean;

  /**
   * Experimental timeSeries* aggregate functions for Prometheus-like timeseries resampling, rate, delta calculation.
   * @since 25.7
   */
  allow_experimental_time_series_aggregate_functions?: boolean;

  /**
   * Allows creation of tables with the [TimeSeries](../../engines/table-engines/integrations/time-series.md) table engine. Possible values: - 0 — the [TimeSeries](../../engines/table-engines/integrations/time-series.md) table engine is disabled. - 1 — the [TimeSeries](../../engines/table-engines/integrations/time-series.md) table engine is enabled.
   */
  allow_experimental_time_series_table?: boolean;

  /**
   * Enable WINDOW VIEW. Not mature enough.
   */
  allow_experimental_window_view?: boolean;

  /**
   * Experimental dictionary source for integration with YTsaurus.
   * @since 25.9
   */
  allow_experimental_ytsaurus_dictionary_source?: boolean;

  /**
   * Experimental table engine for integration with YTsaurus.
   * @since 25.9
   */
  allow_experimental_ytsaurus_table_engine?: boolean;

  /**
   * Experimental table engine for integration with YTsaurus.
   * @since 25.9
   */
  allow_experimental_ytsaurus_table_function?: boolean;

  /**
   * Allows a more general join planning algorithm that can handle more complex conditions, but only works with hash join. If hash join is not enabled, then the usual join planning algorithm is used regardless of the value of this setting.
   * @since 25.2
   */
  allow_general_join_planning?: boolean;

  /**
   * Allow to use the function `getClientHTTPHeader` which lets to obtain a value of an the current HTTP request's header. It is not enabled by default for security reasons, because some headers, such as `Cookie`, could contain sensitive info. Note that the `X-ClickHouse-*` and `Authentication` headers are always restricted and cannot be obtained with this function.
   */
  allow_get_client_http_header?: boolean;

  /**
   * Allow functions that use Hyperscan library. Disable to avoid potentially long compilation times and excessive resource usage.
   */
  allow_hyperscan?: boolean;

  /**
   * Enables or disables [introspection functions](../../sql-reference/functions/introspection.md) for query profiling. Possible values: - 1 — Introspection functions enabled. - 0 — Introspection functions disabled. **See Also** - [Sampling Query Profiler](../../operations/optimizing-performance/sampling-query-profiler.md) - System table [trace_log](/operations/system-tables/trace_log)
   */
  allow_introspection_functions?: boolean;

  /**
   * Allow CREATE MATERIALIZED VIEW with SELECT query that references nonexistent tables or columns. It must still be syntactically valid. Doesn't apply to refreshable MVs. Doesn't apply if the MV schema needs to be inferred from the SELECT query (i.e. if the CREATE has no column list and no TO table). Can be used for creating MV before its source table.
   */
  allow_materialized_view_with_bad_select?: boolean;

  /**
   * Allow named collections' fields override by default.
   */
  allow_named_collection_override_by_default?: boolean;

  /**
   * Allow to execute alters which affects not only tables metadata, but also data on disk
   */
  allow_non_metadata_alters?: boolean;

  /**
   * Allow non-const timezone arguments in certain time-related functions like toTimeZone(), fromUnixTimestamp*(), snowflakeToDateTime*(). This setting exists only for compatibility reasons. In ClickHouse, the time zone is a property of the data type, respectively of the column. Enabling this setting gives the wrong impression that different values within a column can have different timezones. Therefore, please do not enable this setting.
   */
  allow_nonconst_timezone_arguments?: boolean;

  /**
   * User-level setting that allows mutations on replicated tables to make use of non-deterministic functions such as `dictGet`. Given that, for example, dictionaries, can be out of sync across nodes, mutations that pull values from them are disallowed on replicated tables by default. Enabling this setting allows this behavior, making it the user's responsibility to ensure that the data used is in sync across all nodes. **Example** ```xml <profiles> <default> <allow_nondeterministic_mutations>1</allow_nondeterministic_mutations> <!-- ... --> </default> <!-- ... --> </profiles> ```
   */
  allow_nondeterministic_mutations?: boolean;

  /**
   * Allow nondeterministic (like `rand` or `dictGet`, since later has some caveats with updates) functions in sharding key. Possible values: - 0 — Disallowed. - 1 — Allowed.
   */
  allow_nondeterministic_optimize_skip_unused_shards?: boolean;

  /**
   * Prefer prefetched threadpool if all parts are on local filesystem
   */
  allow_prefetched_read_pool_for_local_filesystem?: boolean;

  /**
   * Prefer prefetched threadpool if all parts are on remote filesystem
   */
  allow_prefetched_read_pool_for_remote_filesystem?: boolean;

  /**
   * Allows push predicate on AST level for distributed subqueries with enabled anlyzer
   * @since 25.2
   */
  allow_push_predicate_ast_for_distributed_subqueries?: boolean;

  /**
   * Allows push predicate when subquery contains WITH clause
   */
  allow_push_predicate_when_subquery_contains_with?: boolean;

  /**
   * When moving conditions from WHERE to PREWHERE, allow reordering them to optimize filtering
   */
  allow_reorder_prewhere_conditions?: boolean;

  /**
   * Control whether `SETTINGS` after `FORMAT` in `INSERT` queries is allowed or not. It is not recommended to use this, since this may interpret part of `SETTINGS` as values. Example: ```sql INSERT INTO FUNCTION null('foo String') SETTINGS max_threads=1 VALUES ('bar'); ``` But the following query will work only with `allow_settings_after_format_in_insert`: ```sql SET allow_settings_after_format_in_insert=1; INSERT INTO FUNCTION null('foo String') VALUES ('bar') SETTINGS max_threads=1; ``` Possible values: - 0 — Disallow. - 1 — Allow. :::note Use this setting only for backward compatibility if your use cases depend on old syntax. :::
   */
  allow_settings_after_format_in_insert?: boolean;

  /**
   * Allow using simdjson library in 'JSON*' functions if AVX2 instructions are available. If disabled rapidjson will be used.
   */
  allow_simdjson?: boolean;

  /**
   * Allows to output columns with special serialization kinds like Sparse and Replicated without converting them to full column representation. It helps to avoid unnecessary data copy during formatting.
   * @since 25.12
   */
  allow_special_serialization_kinds_in_output_formats?: boolean;

  /**
   * Allows using statistics to optimize queries
   */
  allow_statistics_optimize?: boolean;

  /**
   * If it is set to true, allow to specify meaningless compression codecs.
   */
  allow_suspicious_codecs?: boolean;

  /**
   * In CREATE TABLE statement allows creating columns of type FixedString(n) with n > 256. FixedString with length >= 256 is suspicious and most likely indicates a misuse
   */
  allow_suspicious_fixed_string_types?: boolean;

  /**
   * Reject primary/secondary indexes and sorting keys with identical expressions
   */
  allow_suspicious_indices?: boolean;

  /**
   * Allows or restricts using [LowCardinality](../../sql-reference/data-types/lowcardinality.md) with data types with fixed size of 8 bytes or less: numeric data types and `FixedString(8_bytes_or_less)`. For small fixed values using of `LowCardinality` is usually inefficient, because ClickHouse stores a numeric index for each row. As a result: - Disk space usage can rise. - RAM consumption can be higher, depending on a dictionary size. - Some functions can work slower due to extra coding/encoding operations. Merge times in [MergeTree](../../engines/table-engines/mergetree-family/mergetree.md)-engine tables can grow due to all the reasons described above. Possible values: - 1 — Usage of `LowCardinality` is not restricted. - 0 — Usage of `LowCardinality` is restricted.
   */
  allow_suspicious_low_cardinality_types?: boolean;

  /**
   * Allow suspicious `PRIMARY KEY`/`ORDER BY` for MergeTree (i.e. SimpleAggregateFunction).
   */
  allow_suspicious_primary_key?: boolean;

  /**
   * Reject TTL expressions that don't depend on any of table's columns. It indicates a user error most of the time.
   */
  allow_suspicious_ttl_expressions?: boolean;

  /**
   * Allows or restricts using [Variant](../../sql-reference/data-types/variant.md) and [Dynamic](../../sql-reference/data-types/dynamic.md) types in GROUP BY keys.
   * @since 24.12
   */
  allow_suspicious_types_in_group_by?: boolean;

  /**
   * Allows or restricts using [Variant](../../sql-reference/data-types/variant.md) and [Dynamic](../../sql-reference/data-types/dynamic.md) types in ORDER BY keys.
   * @since 24.12
   */
  allow_suspicious_types_in_order_by?: boolean;

  /**
   * In CREATE TABLE statement allows specifying Variant type with similar variant types (for example, with different numeric or date types). Enabling this setting may introduce some ambiguity when working with values with similar types.
   */
  allow_suspicious_variant_types?: boolean;

  /**
   * Allow unrestricted (without condition on path) reads from system.zookeeper table, can be handy, but is not safe for zookeeper
   */
  allow_unrestricted_reads_from_keeper?: boolean;

  /**
   * Execute ALTER TABLE MOVE ... TO [DISK|VOLUME] asynchronously
   */
  alter_move_to_space_execute_async?: boolean;

  /**
   * Enables or disables the display of information about the parts to which the manipulation operations with partitions and parts have been successfully applied. Applicable to [ATTACH PARTITION|PART](/sql-reference/statements/alter/partition#attach-partitionpart) and to [FREEZE PARTITION](/sql-reference/statements/alter/partition#freeze-partition). Possible values: - 0 — disable verbosity. - 1 — enable verbosity. **Example** ```sql CREATE TABLE test(a Int64, d Date, s String) ENGINE = MergeTree PARTITION BY toYYYYMDECLARE(d) ORDER BY a; INSERT INTO test VALUES(1, '2021-01-01', ''); INSERT INTO test VALUES(1, '2021-01-01', ''); ALTER TABLE test DETACH PARTITION ID '202101'; ALTER TABLE test ATTACH PARTITION ID '202101' SETTINGS alter_partition_verbose_result = 1; ┌─command_type─────┬─partition_id─┬─part_name────┬─old_part_name─┐ │ ATTACH PARTITION │ 202101 │ 202101_7_7_0 │ 202101_5_5_0 │ │ ATTACH PARTITION │ 202101 │ 202101_8_8_0 │ 202101_6_6_0 │ └──────────────────┴──────────────┴──────────────┴───────────────┘ ALTER TABLE test FREEZE SETTINGS alter_partition_verbose_result = 1; ┌─command_type─┬─partition_id─┬─part_name────┬─backup_name─┬─backup_path───────────────────┬─part_backup_path────────────────────────────────────────────┐ │ FREEZE ALL │ 202101 │ 202101_7_7_0 │ 8 │ /var/lib/clickhouse/shadow/8/ │ /var/lib/clickhouse/shadow/8/data/default/test/202101_7_7_0 │ │ FREEZE ALL │ 202101 │ 202101_8_8_0 │ 8 │ /var/lib/clickhouse/shadow/8/ │ /var/lib/clickhouse/shadow/8/data/default/test/202101_8_8_0 │ └──────────────┴──────────────┴──────────────┴─────────────┴───────────────────────────────┴─────────────────────────────────────────────────────────────┘ ```
   */
  alter_partition_verbose_result?: boolean;

  /**
   * Allows to set up waiting for actions to be executed on replicas by [ALTER](../../sql-reference/statements/alter/index.md), [OPTIMIZE](../../sql-reference/statements/optimize.md) or [TRUNCATE](../../sql-reference/statements/truncate.md) queries. Possible values: - `0` — Do not wait. - `1` — Wait for own execution. - `2` — Wait for everyone. Cloud default value: `1`. :::note `alter_sync` is applicable to `Replicated` tables only, it does nothing to alters of not `Replicated` tables. :::
   */
  alter_sync?: number;

  /**
   * A mode for `ALTER` queries that have the `UPDATE` commands. Possible values: - `heavy` - run regular mutation. - `lightweight` - run lightweight update if possible, run regular mutation otherwise. - `lightweight_force` - run lightweight update if possible, throw otherwise.
   * @since 25.6
   */
  alter_update_mode?: 'heavy' | 'lightweight' | 'lightweight_force';

  /**
   * If a table has a space-filling curve in its index, e.g. `ORDER BY mortonEncode(x, y)` or `ORDER BY hilbertEncode(x, y)`, and the query has conditions on its arguments, e.g. `x >= 10 AND x <= 20 AND y >= 20 AND y <= 30`, use the space-filling curve for index analysis.
   */
  analyze_index_with_space_filling_curves?: boolean;

  /**
   * Allow to add compound identifiers to nested. This is a compatibility setting because it changes the query result. When disabled, `SELECT a.b.c FROM table ARRAY JOIN a` does not work, and `SELECT a FROM table` does not include `a.b.c` column into `Nested a` result.
   * @since 25.9
   */
  analyzer_compatibility_allow_compound_identifiers_in_unflatten_nested?: boolean;

  /**
   * Force to resolve identifier in JOIN USING from projection (for example, in `SELECT a + 1 AS b FROM t1 JOIN t2 USING (b)` join will be performed by `t1.a + 1 = t2.b`, rather then `t1.b = t2.b`).
   */
  analyzer_compatibility_join_using_top_level_identifier?: boolean;

  /**
   * Enables legacy ClickHouse server behaviour in `ANY INNER|LEFT JOIN` operations. :::note Use this setting only for backward compatibility if your use cases depend on legacy `JOIN` behaviour. ::: When the legacy behaviour is enabled: - Results of `t1 ANY LEFT JOIN t2` and `t2 ANY RIGHT JOIN t1` operations are not equal because ClickHouse uses the logic with many-to-one left-to-right table keys mapping. - Results of `ANY INNER JOIN` operations contain all rows from the left table like the `SEMI LEFT JOIN` operations do. When the legacy behaviour is disabled: - Results of `t1 ANY LEFT JOIN t2` and `t2 ANY RIGHT JOIN t1` operations are equal because ClickHouse uses the logic which provides one-to-many keys mapping in `ANY RIGHT JOIN` operations. - Results of `ANY INNER JOIN` operations contain one row per key from both the left and right tables. Possible values: - 0 — Legacy behaviour is disabled. - 1 — Legacy behaviour is enabled. See also: - [JOIN strictness](/sql-reference/statements/select/join#settings)
   */
  any_join_distinct_right_table_keys?: boolean;

  /**
   * Enables filtering out rows deleted with lightweight DELETE. If disabled, a query will be able to read those rows. This is useful for debugging and "undelete" scenarios
   */
  apply_deleted_mask?: boolean;

  /**
   * If true, mutations (UPDATEs and DELETEs) which are not materialized in data part will be applied on SELECTs.
   */
  apply_mutations_on_fly?: boolean;

  /**
   * If true, patch parts (that represent lightweight updates) are applied on SELECTs.
   * @since 25.6
   */
  apply_patch_parts?: boolean;

  /**
   * The number of buckets in the temporary cache for applying patch parts in Join mode.
   * @since 25.9
   */
  apply_patch_parts_join_cache_buckets?: number;

  /**
   * When enabled, PREWHERE conditions are applied after FINAL processing for ReplacingMergeTree and similar engines. This can be useful when PREWHERE references columns that may have different values across duplicate rows, and you want FINAL to select the winning row before filtering. When disabled, PREWHERE is applied during reading. Note: If apply_row_level_security_after_final is enabled and row policy uses non-sorting-key columns, PREWHERE will also be deferred to maintain correct execution order (row policy must be applied before PREWHERE).
   * @since 26.1
   */
  apply_prewhere_after_final?: boolean;

  /**
   * When enabled, row policies and PREWHERE are applied after FINAL processing for *MergeTree tables. (Especially for ReplacingMergeTree) When disabled, row policies are applied before FINAL, which can cause different results when the policy filters out rows that should be used for deduplication in ReplacingMergeTree or similar engines. If the row policy expression depends only on columns in ORDER BY, it will still be applied before FINAL as an optimization, since such filtering cannot affect the deduplication result. Possible values: - 0 — Row policy and PREWHERE are applied before FINAL (default). - 1 — Row policy and PREWHERE are applied after FINAL.
   * @since 26.1
   */
  apply_row_policy_after_final?: boolean;

  /**
   * Whether the client should accept settings from server. This only affects operations performed on the client side, in particular parsing the INSERT input data and formatting the query result. Most of query execution happens on the server and is not affected by this setting. Normally this setting should be set in user profile (users.xml or queries like `ALTER USER`), not through the client (client command line arguments, `SET` query, or `SETTINGS` section of `SELECT` query). Through the client it can be changed to false, but can't be changed to true (because the server won't send the settings if user profile has `apply_settings_from_server = false`). Note that initially (24.12) there was a server setting (`send_settings_to_client`), but latter it got replaced with this client setting, for better usability.
   * @since 25.3
   */
  apply_settings_from_server?: boolean;

  /**
   * Type of descriptor to use for Arrow Flight requests. 'path' sends the dataset name as a path descriptor. 'command' sends a SQL query as a command descriptor (required for Dremio). Possible values: - 'path' — Use FlightDescriptor::Path (default, works with most Arrow Flight servers) - 'command' — Use FlightDescriptor::Command with a SELECT query (required for Dremio)
   * @since 25.12
   */
  arrow_flight_request_descriptor_type?: 'path' | 'command';

  /**
   * Include [ALIAS](../../sql-reference/statements/create/table.md/#alias) columns for wildcard query (`SELECT *`). Possible values: - 0 - disabled - 1 - enabled
   */
  asterisk_include_alias_columns?: boolean;

  /**
   * Include [MATERIALIZED](/sql-reference/statements/create/view#materialized-view) columns for wildcard query (`SELECT *`). Possible values: - 0 - disabled - 1 - enabled
   */
  asterisk_include_materialized_columns?: boolean;

  /**
   * If true, data from INSERT query is stored in queue and later flushed to table in background. If wait_for_async_insert is false, INSERT query is processed almost instantly, otherwise client will wait until data will be flushed to table
   */
  async_insert?: boolean;

  /**
   * The exponential growth rate at which the adaptive asynchronous insert timeout decreases
   */
  async_insert_busy_timeout_decrease_rate?: string;

  /**
   * The exponential growth rate at which the adaptive asynchronous insert timeout increases
   */
  async_insert_busy_timeout_increase_rate?: string;

  /**
   * Maximum time to wait before dumping collected data per query since the first data appeared.
   */
  async_insert_busy_timeout_max_ms?: number;

  /**
   * If auto-adjusting is enabled through async_insert_use_adaptive_busy_timeout, minimum time to wait before dumping collected data per query since the first data appeared. It also serves as the initial value for the adaptive algorithm
   */
  async_insert_busy_timeout_min_ms?: number;

  /**
   * For async INSERT queries in the replicated table, specifies that deduplication of inserting blocks should be performed
   */
  async_insert_deduplicate?: boolean;

  /**
   * Maximum size in bytes of unparsed data collected per query before being inserted
   */
  async_insert_max_data_size?: number;

  /**
   * Maximum number of insert queries before being inserted. Only takes effect if setting [`async_insert_deduplicate`](#async_insert_deduplicate) is 1.
   */
  async_insert_max_query_number?: number;

  /**
   * Timeout for polling data from asynchronous insert queue
   */
  async_insert_poll_timeout_ms?: number;

  /**
   * If it is set to true, use adaptive busy timeout for asynchronous inserts
   */
  async_insert_use_adaptive_busy_timeout?: boolean;

  /**
   * Enables asynchronous connection creation and query sending while executing remote query. Enabled by default.
   */
  async_query_sending_for_remote?: boolean;

  /**
   * Enables asynchronous read from socket while executing remote query. Enabled by default.
   */
  async_socket_for_remote?: boolean;

  /**
   * Threshold of bytes to read per replica to enable parallel replicas automatically (applies only when `automatic_parallel_replicas_mode`=1). 0 means no threshold.
   * @since 26.1
   */
  automatic_parallel_replicas_min_bytes_per_replica?: number;

  /**
   * 🚨 HIGHLY EXPERIMENTAL 🚨 Enable automatic switching to execution with parallel replicas based on collected statistics. Requires enabling `parallel_replicas_local_plan` and providing `cluster_for_parallel_replicas`. 0 - disabled, 1 - enabled, 2 - only statistics collection is enabled (switching to execution with parallel replicas is disabled).
   * @since 26.1
   */
  automatic_parallel_replicas_mode?: number;

  /**
   * Use multiple threads for azure multipart upload.
   */
  azure_allow_parallel_part_upload?: boolean;

  /**
   * Check each uploaded object in azure blob storage to be sure that upload was successful
   * @since 24.12
   */
  azure_check_objects_after_upload?: boolean;

  /**
   * Connection timeout for host from azure disks.
   * @since 25.9
   */
  azure_connect_timeout_ms?: number;

  /**
   * Enables or disables creating a new file on each insert in azure engine tables
   */
  azure_create_new_file_on_insert?: boolean;

  /**
   * Ignore absence of file if it does not exist when reading certain keys. Possible values: - 1 — `SELECT` returns empty result. - 0 — `SELECT` throws an exception.
   */
  azure_ignore_file_doesnt_exist?: boolean;

  /**
   * Maximum number of files that could be returned in batch by ListObject request
   */
  azure_list_object_keys_size?: number;

  /**
   * Maximum number of blocks in multipart upload for Azure.
   */
  azure_max_blocks_in_multipart_upload?: number;

  /**
   * Max number of requests that can be issued simultaneously before hitting request per second limit. By default (0) equals to `azure_max_get_rps`
   * @since 25.9
   */
  azure_max_get_burst?: number;

  /**
   * Limit on Azure GET request per second rate before throttling. Zero means unlimited.
   * @since 25.9
   */
  azure_max_get_rps?: number;

  /**
   * The maximum number of a concurrent loaded parts in multipart upload request. 0 means unlimited.
   */
  azure_max_inflight_parts_for_one_file?: number;

  /**
   * Max number of requests that can be issued simultaneously before hitting request per second limit. By default (0) equals to `azure_max_put_rps`
   * @since 25.9
   */
  azure_max_put_burst?: number;

  /**
   * Limit on Azure PUT request per second rate before throttling. Zero means unlimited.
   * @since 25.9
   */
  azure_max_put_rps?: number;

  /**
   * Max number of azure redirects hops allowed.
   * @since 25.9
   */
  azure_max_redirects?: number;

  /**
   * The maximum size of object to copy using single part copy to Azure blob storage.
   */
  azure_max_single_part_copy_size?: number;

  /**
   * The maximum size of object to upload using singlepart upload to Azure blob storage.
   */
  azure_max_single_part_upload_size?: number;

  /**
   * The maximum number of retries during single Azure blob storage read.
   */
  azure_max_single_read_retries?: number;

  /**
   * The maximum number of retries in case of unexpected errors during Azure blob storage write
   */
  azure_max_unexpected_write_error_retries?: number;

  /**
   * The maximum size of part to upload during multipart upload to Azure blob storage.
   */
  azure_max_upload_part_size?: number;

  /**
   * The minimum size of part to upload during multipart upload to Azure blob storage.
   */
  azure_min_upload_part_size?: number;

  /**
   * Idleness timeout for sending and receiving data to/from azure. Fail if a single TCP read or write call blocks for this long.
   * @since 25.9
   */
  azure_request_timeout_ms?: number;

  /**
   * Maximum number of retries in azure sdk
   */
  azure_sdk_max_retries?: number;

  /**
   * Minimal backoff between retries in azure sdk
   */
  azure_sdk_retry_initial_backoff_ms?: number;

  /**
   * Maximal backoff between retries in azure sdk
   */
  azure_sdk_retry_max_backoff_ms?: number;

  /**
   * Enables or disables skipping empty files in S3 engine. Possible values: - 0 — `SELECT` throws an exception if empty file is not compatible with requested format. - 1 — `SELECT` returns empty result for empty file.
   */
  azure_skip_empty_files?: boolean;

  /**
   * The exact size of part to upload during multipart upload to Azure blob storage.
   */
  azure_strict_upload_part_size?: number;

  /**
   * Throw an error if matched zero files according to glob expansion rules. Possible values: - 1 — `SELECT` throws an exception. - 0 — `SELECT` returns empty result.
   */
  azure_throw_on_zero_files_match?: boolean;

  /**
   * Enables or disables truncate before insert in azure engine tables.
   */
  azure_truncate_on_insert?: boolean;

  /**
   * Multiply azure_min_upload_part_size by this factor each time azure_multiply_parts_count_threshold parts were uploaded from a single write to Azure blob storage.
   */
  azure_upload_part_size_multiply_factor?: number;

  /**
   * Each time this number of parts was uploaded to Azure blob storage, azure_min_upload_part_size is multiplied by azure_upload_part_size_multiply_factor.
   */
  azure_upload_part_size_multiply_parts_count_threshold?: number;

  /**
   * When set to `true` than for all azure requests first two attempts are made with low send and receive timeouts. When set to `false` than all attempts are made with identical timeouts.
   * @since 25.9
   */
  azure_use_adaptive_timeouts?: boolean;

  /**
   * Maximum size of batch for multi request to [Zoo]Keeper during backup or restore
   */
  backup_restore_batch_size_for_keeper_multi?: number;

  /**
   * Maximum size of batch for multiread request to [Zoo]Keeper during backup or restore
   */
  backup_restore_batch_size_for_keeper_multiread?: number;

  /**
   * If a host during a BACKUP ON CLUSTER or RESTORE ON CLUSTER operation doesn't recreate its ephemeral 'alive' node in ZooKeeper for this amount of time then the whole backup or restore is considered as failed. This value should be bigger than any reasonable time for a host to reconnect to ZooKeeper after a failure. Zero means unlimited.
   * @since 24.12
   */
  backup_restore_failure_after_host_disconnected_for_seconds?: number;

  /**
   * How long the initiator should wait for other host to react to the 'error' node and stop their work on the current BACKUP ON CLUSTER or RESTORE ON CLUSTER operation.
   * @since 24.12
   */
  backup_restore_finish_timeout_after_error_sec?: number;

  /**
   * Approximate probability of failure for a keeper request during backup or restore. Valid value is in interval [0.0f, 1.0f]
   */
  backup_restore_keeper_fault_injection_probability?: number;

  /**
   * 0 - random seed, otherwise the setting value
   */
  backup_restore_keeper_fault_injection_seed?: number;

  /**
   * Max retries for [Zoo]Keeper operations in the middle of a BACKUP or RESTORE operation. Should be big enough so the whole operation won't fail because of a temporary [Zoo]Keeper failure.
   */
  backup_restore_keeper_max_retries?: number;

  /**
   * Max retries for [Zoo]Keeper operations while handling an error of a BACKUP ON CLUSTER or RESTORE ON CLUSTER operation.
   * @since 24.12
   */
  backup_restore_keeper_max_retries_while_handling_error?: number;

  /**
   * Max retries for [Zoo]Keeper operations during the initialization of a BACKUP ON CLUSTER or RESTORE ON CLUSTER operation.
   * @since 24.12
   */
  backup_restore_keeper_max_retries_while_initializing?: number;

  /**
   * Initial backoff timeout for [Zoo]Keeper operations during backup or restore
   */
  backup_restore_keeper_retry_initial_backoff_ms?: number;

  /**
   * Max backoff timeout for [Zoo]Keeper operations during backup or restore
   */
  backup_restore_keeper_retry_max_backoff_ms?: number;

  /**
   * Maximum size of data of a [Zoo]Keeper's node during backup
   */
  backup_restore_keeper_value_max_size?: number;

  /**
   * Setting for Aws::Client::RetryStrategy, Aws::Client does retries itself, 0 means no retries. It takes place only for backup/restore.
   */
  backup_restore_s3_retry_attempts?: number;

  /**
   * Initial backoff delay in milliseconds before the first retry attempt during backup and restore. Each subsequent retry increases the delay exponentially, up to the maximum specified by `backup_restore_s3_retry_max_backoff_ms`
   * @since 25.9
   */
  backup_restore_s3_retry_initial_backoff_ms?: number;

  /**
   * Jitter factor applied to the retry backoff delay in Aws::Client::RetryStrategy during backup and restore operations. The computed backoff delay is multiplied by a random factor in the range [1.0, 1.0 + jitter], up to the maximum `backup_restore_s3_retry_max_backoff_ms`. Must be in [0.0, 1.0] interval
   * @since 25.9
   */
  backup_restore_s3_retry_jitter_factor?: number;

  /**
   * Maximum delay in milliseconds between retries during backup and restore operations.
   * @since 25.9
   */
  backup_restore_s3_retry_max_backoff_ms?: number;

  /**
   * When set to `true`, all threads executing S3 requests to the same backup endpoint are slowed down after any single S3 request encounters a retryable S3 error, such as 'Slow Down'. When set to `false`, each thread handles s3 request backoff independently of the others.
   * @since 25.9
   */
  backup_slow_all_threads_after_retryable_s3_error?: boolean;

  /**
   * Only has an effect in ClickHouse Cloud. Number of background threads for speculatively downloading new data parts into the filesystem cache, when [cache_populated_by_fetch](merge-tree-settings.md/#cache_populated_by_fetch) is enabled. Zero to disable.
   */
  cache_warmer_threads?: number;

  /**
   * Calculate text stack trace in case of exceptions during query execution. This is the default. It requires symbol lookups that may slow down fuzzing tests when a huge amount of wrong queries are executed. In normal cases, you should not disable this option.
   */
  calculate_text_stack_trace?: boolean;

  /**
   * Cancels HTTP read-only queries (e.g. SELECT) when a client closes the connection without waiting for the response. Cloud default value: `0`.
   */
  cancel_http_readonly_queries_on_client_close?: boolean;

  /**
   * CAST operator into IPv4, CAST operator into IPV6 type, toIPv4, toIPv6 functions will return default value instead of throwing exception on conversion error.
   */
  cast_ipv4_ipv6_default_on_conversion_error?: boolean;

  /**
   * Enables or disables keeping of the `Nullable` data type in [CAST](/sql-reference/functions/type-conversion-functions#CAST) operations. When the setting is enabled and the argument of `CAST` function is `Nullable`, the result is also transformed to `Nullable` type. When the setting is disabled, the result always has the destination type exactly. Possible values: - 0 — The `CAST` result has exactly the destination type specified. - 1 — If the argument type is `Nullable`, the `CAST` result is transformed to `Nullable(DestinationDataType)`. **Examples** The following query results in the destination data type exactly: ```sql SET cast_keep_nullable = 0; SELECT CAST(toNullable(toInt32(0)) AS Int32) as x, toTypeName(x); ``` Result: ```text ┌─x─┬─toTypeName(CAST(toNullable(toInt32(0)), 'Int32'))─┐ │ 0 │ Int32 │ └───┴───────────────────────────────────────────────────┘ ``` The following query results in the `Nullable` modification on the destination data type: ```sql SET cast_keep_nullable = 1; SELECT CAST(toNullable(toInt32(0)) AS Int32) as x, toTypeName(x); ``` Result: ```text ┌─x─┬─toTypeName(CAST(toNullable(toInt32(0)), 'Int32'))─┐ │ 0 │ Nullable(Int32) │ └───┴───────────────────────────────────────────────────┘ ``` **See Also** - [CAST](/sql-reference/functions/type-conversion-functions#CAST) function
   */
  cast_keep_nullable?: boolean;

  /**
   * Allows choosing a parser of the text representation of date and time during cast from String. Possible values: - `'best_effort'` — Enables extended parsing. ClickHouse can parse the basic `YYYY-MM-DD HH:MM:SS` format and all [ISO 8601](https://en.wikipedia.org/wiki/ISO_8601) date and time formats. For example, `'2018-06-08T01:02:03.000Z'`. - `'best_effort_us'` — Similar to `best_effort` (see the difference in [parseDateTimeBestEffortUS](../../sql-reference/functions/type-conversion-functions#parseDateTimeBestEffortUS) - `'basic'` — Use basic parser. ClickHouse can parse only the basic `YYYY-MM-DD HH:MM:SS` or `YYYY-MM-DD` format. For example, `2019-08-20 10:18:56` or `2019-08-20`. See also: - [DateTime data type.](../../sql-reference/data-types/datetime.md) - [Functions for working with dates and times.](../../sql-reference/functions/date-time-functions.md)
   * @since 25.7
   */
  cast_string_to_date_time_mode?: 'basic' | 'best_effort' | 'best_effort_us';

  /**
   * Use types inference during String to Dynamic conversion
   */
  cast_string_to_dynamic_use_inference?: boolean;

  /**
   * Use types inference during String to Variant conversion.
   * @since 25.5
   */
  cast_string_to_variant_use_inference?: boolean;

  /**
   * Defines the level of detail for the [CHECK TABLE](/sql-reference/statements/check-table) query result for `MergeTree` family engines . Possible values: - 0 — the query shows a check status for every individual data part of a table. - 1 — the query shows the general table check status.
   */
  check_query_single_value_result?: boolean;

  /**
   * Check that DDL query (such as DROP TABLE or RENAME) will not break referential dependencies
   */
  check_referential_table_dependencies?: boolean;

  /**
   * Check that DDL query (such as DROP TABLE or RENAME) will not break dependencies
   */
  check_table_dependencies?: boolean;

  /**
   * Validate checksums on reading. It is enabled by default and should be always enabled in production. Please do not expect any benefits in disabling this setting. It may only be used for experiments and benchmarks. The setting is only applicable for tables of MergeTree family. Checksums are always validated for other table engines and when receiving data over the network.
   */
  checksum_on_read?: boolean;

  /**
   * Cloud mode
   */
  cloud_mode?: boolean;

  /**
   * The database engine allowed in Cloud. 1 - rewrite DDLs to use Replicated database, 2 - rewrite DDLs to use Shared database
   */
  cloud_mode_database_engine?: number;

  /**
   * The engine family allowed in Cloud. - 0 - allow everything - 1 - rewrite DDLs to use *ReplicatedMergeTree - 2 - rewrite DDLs to use SharedMergeTree - 3 - rewrite DDLs to use SharedMergeTree except when explicitly passed remote disk is specified UInt64 to minimize public part
   */
  cloud_mode_engine?: number;

  /**
   * Cluster for a shard in which current server is located
   */
  cluster_for_parallel_replicas?: string;

  /**
   * If set to `true`, increases performance of processing archives in cluster functions. Should be set to `false` for compatibility and to avoid errors during upgrade to 25.7+ if using cluster functions with archives on earlier versions.
   * @since 25.8
   */
  cluster_function_process_archive_on_multiple_nodes?: boolean;

  /**
   * Defines the approximate size of a batch (in bytes) used in distributed processing of tasks in cluster table functions with `bucket` split granularity. The system accumulates data until at least this amount is reached. The actual size may be slightly larger to align with data boundaries.
   * @since 25.12
   */
  cluster_table_function_buckets_batch_size?: number;

  /**
   * Controls how data is split into tasks when executing a CLUSTER TABLE FUNCTION. This setting defines the granularity of work distribution across the cluster: - `file` — each task processes an entire file. - `bucket` — tasks are created per internal data block within a file (for example, Parquet row groups). Choosing finer granularity (like `bucket`) can improve parallelism when working with a small number of large files. For instance, if a Parquet file contains multiple row groups, enabling `bucket` granularity allows each group to be processed independently by different workers.
   * @since 25.12
   */
  cluster_table_function_split_granularity?: 'file' | 'bucket';

  /**
   * Enable collecting hash table statistics to optimize memory allocation
   */
  collect_hash_table_stats_during_aggregation?: boolean;

  /**
   * Enable collecting hash table statistics to optimize memory allocation
   */
  collect_hash_table_stats_during_joins?: boolean;

  /**
   * The `compatibility` setting causes ClickHouse to use the default settings of a previous version of ClickHouse, where the previous version is provided as the setting. If settings are set to non-default values, then those settings are honored (only settings that have not been modified are affected by the `compatibility` setting). This setting takes a ClickHouse version number as a string, like `22.3`, `22.8`. An empty value means that this setting is disabled. Disabled by default. :::note In ClickHouse Cloud, the service-level default compatibility setting must be set by ClickHouse Cloud support. Please [open a case](https://clickhouse.cloud/support) to have it set. However, the compatibility setting can be overridden at the user, role, profile, query, or session level using standard ClickHouse setting mechanisms such as `SET compatibility = '22.3'` in a session or `SETTINGS compatibility = '22.3'` in a query. :::
   */
  compatibility?: string;

  /**
   * Ignore AUTO_INCREMENT keyword in column declaration if true, otherwise return error. It simplifies migration from MySQL
   */
  compatibility_ignore_auto_increment_in_create_table?: boolean;

  /**
   * Compatibility ignore collation in create table
   */
  compatibility_ignore_collation_in_create_table?: boolean;

  /**
   * Compatibility: when enabled, folds pre-signed URL query parameters (e.g. X-Amz-*) into the S3 key (legacy behavior), so '?' acts as a wildcard in the path. When disabled (default), pre-signed URL query parameters are kept in the URL query to avoid interpreting '?' as a wildcard.
   * @since 26.1
   */
  compatibility_s3_presigned_url_query_in_path?: boolean;

  /**
   * Enables or disables JIT-compilation of aggregate functions to native code. Enabling this setting can improve the performance. Possible values: - 0 — Aggregation is done without JIT compilation. - 1 — Aggregation is done using JIT compilation. **See Also** - [min_count_to_compile_aggregate_expression](#min_count_to_compile_aggregate_expression)
   */
  compile_aggregate_expressions?: boolean;

  /**
   * Compile some scalar functions and operators to native code.
   */
  compile_expressions?: boolean;

  /**
   * Compile sort description to native code.
   */
  compile_sort_description?: boolean;

  /**
   * Connection timeout if there are no replicas.
   */
  connect_timeout?: number;

  /**
   * The timeout in milliseconds for connecting to a remote server for a Distributed table engine, if the 'shard' and 'replica' sections are used in the cluster definition. If unsuccessful, several attempts are made to connect to various replicas.
   */
  connect_timeout_with_failover_ms?: number;

  /**
   * Connection timeout for selecting first healthy replica (for secure connections).
   */
  connect_timeout_with_failover_secure_ms?: number;

  /**
   * The wait time in milliseconds for a connection when the connection pool is full. Possible values: - Positive integer. - 0 — Infinite timeout.
   */
  connection_pool_max_wait_ms?: number;

  /**
   * The maximum number of connection attempts with each replica for the Distributed table engine.
   */
  connections_with_failover_max_tries?: number;

  /**
   * When set to `true`, a `SELECT` query will be converted to conjuctive normal form (CNF). There are scenarios where rewriting a query in CNF may execute faster (view this [Github issue](https://github.com/ClickHouse/ClickHouse/issues/11749) for an explanation). For example, notice how the following `SELECT` query is not modified (the default behavior): ```sql EXPLAIN SYNTAX SELECT * FROM ( SELECT number AS x FROM numbers(20) ) AS a WHERE ((x >= 1) AND (x <= 5)) OR ((x >= 10) AND (x <= 15)) SETTINGS convert_query_to_cnf = false; ``` The result is: ```response ┌─explain────────────────────────────────────────────────────────┐ │ SELECT x │ │ FROM │ │ ( │ │ SELECT number AS x │ │ FROM numbers(20) │ │ WHERE ((x >= 1) AND (x <= 5)) OR ((x >= 10) AND (x <= 15)) │ │ ) AS a │ │ WHERE ((x >= 1) AND (x <= 5)) OR ((x >= 10) AND (x <= 15)) │ │ SETTINGS convert_query_to_cnf = 0 │ └────────────────────────────────────────────────────────────────┘ ``` Let's set `convert_query_to_cnf` to `true` and see what changes: ```sql EXPLAIN SYNTAX SELECT * FROM ( SELECT number AS x FROM numbers(20) ) AS a WHERE ((x >= 1) AND (x <= 5)) OR ((x >= 10) AND (x <= 15)) SETTINGS convert_query_to_cnf = true; ``` Notice the `WHERE` clause is rewritten in CNF, but the result set is the identical - the Boolean logic is unchanged: ```response ┌─explain───────────────────────────────────────────────────────────────────────────────────────────────────────────────┐ │ SELECT x │ │ FROM │ │ ( │ │ SELECT number AS x │ │ FROM numbers(20) │ │ WHERE ((x <= 15) OR (x <= 5)) AND ((x <= 15) OR (x >= 1)) AND ((x >= 10) OR (x <= 5)) AND ((x >= 10) OR (x >= 1)) │ │ ) AS a │ │ WHERE ((x >= 10) OR (x >= 1)) AND ((x >= 10) OR (x <= 5)) AND ((x <= 15) OR (x >= 1)) AND ((x <= 15) OR (x <= 5)) │ │ SETTINGS convert_query_to_cnf = 1 │ └───────────────────────────────────────────────────────────────────────────────────────────────────────────────────────┘ ``` Possible values: true, false
   */
  convert_query_to_cnf?: boolean;

  /**
   * Controls the kind of joins in the decorrelated query plan. The default value is `right`, which means that decorrelated plan will contain RIGHT JOINs with subquery input on the right side. Possible values: - `left` - Decorrelation process will produce LEFT JOINs and input table will appear on the left side. - `right` - Decorrelation process will produce RIGHT JOINs and input table will appear on the right side.
   * @since 25.12
   */
  correlated_subqueries_default_join_kind?: 'left' | 'right';

  /**
   * Use filter expressions to inference equivalent expressions and substitute them instead of creating a CROSS JOIN.
   * @since 25.8
   */
  correlated_subqueries_substitute_equivalent_expressions?: boolean;

  /**
   * Specifies which of the `uniq*` functions should be used to perform the [COUNT(DISTINCT ...)](/sql-reference/aggregate-functions/reference/count) construction. Possible values: - [uniq](/sql-reference/aggregate-functions/reference/uniq) - [uniqCombined](/sql-reference/aggregate-functions/reference/uniqcombined) - [uniqCombined64](/sql-reference/aggregate-functions/reference/uniqcombined64) - [uniqHLL12](/sql-reference/aggregate-functions/reference/uniqhll12) - [uniqExact](/sql-reference/aggregate-functions/reference/uniqexact)
   */
  count_distinct_implementation?: string;

  /**
   * Rewrite count distinct to subquery of group by
   */
  count_distinct_optimization?: boolean;

  /**
   * Stop counting once a pattern matches zero-length in the `countMatches` function.
   * @since 25.8
   */
  count_matches_stop_at_empty_match?: boolean;

  /**
   * Enable `IF NOT EXISTS` for `CREATE` statement by default. If either this setting or `IF NOT EXISTS` is specified and a table with the provided name already exists, no exception will be thrown.
   */
  create_if_not_exists?: boolean;

  /**
   * Ignore UNIQUE keyword in CREATE UNIQUE INDEX. Made for SQL compatibility tests.
   */
  create_index_ignore_unique?: boolean;

  /**
   * The probability of a fault injection during table creation after creating metadata in ZooKeeper
   */
  create_replicated_merge_tree_fault_injection_probability?: number;

  /**
   * Allow to create *MergeTree tables with empty primary key when ORDER BY and PRIMARY KEY not specified
   */
  create_table_empty_primary_key_by_default?: boolean;

  /**
   * Minimal size of block to compress in CROSS JOIN. Zero value means - disable this threshold. This block is compressed when any of the two thresholds (by rows or by bytes) are reached.
   */
  cross_join_min_bytes_to_compress?: number;

  /**
   * Minimal count of rows to compress block in CROSS JOIN. Zero value means - disable this threshold. This block is compressed when any of the two thresholds (by rows or by bytes) are reached.
   */
  cross_join_min_rows_to_compress?: number;

  /**
   * Allows data types without explicit modifiers [NULL or NOT NULL](/sql-reference/statements/create/table#null-or-not-null-modifiers) in column definition will be [Nullable](/sql-reference/data-types/nullable). Possible values: - 1 — The data types in column definitions are set to `Nullable` by default. - 0 — The data types in column definitions are set to not `Nullable` by default.
   */
  data_type_default_nullable?: boolean;

  /**
   * Adds a modifier `SYNC` to all `DROP` and `DETACH` queries. Possible values: - 0 — Queries will be executed with delay. - 1 — Queries will be executed without delay.
   */
  database_atomic_wait_for_drop_and_detach_synchronously?: boolean;

  /**
   * 0 - Don't allow to explicitly specify UUIDs for tables in Replicated databases. 1 - Allow. 2 - Allow, but ignore the specified UUID and generate a random one instead.
   */
  database_replicated_allow_explicit_uuid?: number;

  /**
   * Allow long-running DDL queries (CREATE AS SELECT and POPULATE) in Replicated database engine. Note that it can block DDL queue for a long time.
   */
  database_replicated_allow_heavy_create?: boolean;

  /**
   * Allow to create only Replicated tables in database with engine Replicated
   */
  database_replicated_allow_only_replicated_engine?: boolean;

  /**
   * 0 - Don't allow to explicitly specify ZooKeeper path and replica name for *MergeTree tables in Replicated databases. 1 - Allow. 2 - Allow, but ignore the specified path and use default one instead. 3 - Allow and don't log a warning.
   */
  database_replicated_allow_replicated_engine_arguments?: number;

  /**
   * Execute DETACH TABLE as DETACH TABLE PERMANENTLY if database engine is Replicated
   */
  database_replicated_always_detach_permanently?: boolean;

  /**
   * Enforces synchronous waiting for some queries (see also database_atomic_wait_for_drop_and_detach_synchronously, mutations_sync, alter_sync). Not recommended to enable these settings.
   */
  database_replicated_enforce_synchronous_settings?: boolean;

  /**
   * Sets how long initial DDL query should wait for Replicated database to process previous DDL queue entries in seconds. Possible values: - Positive integer. - 0 — Unlimited.
   */
  database_replicated_initial_query_timeout_sec?: number;

  /**
   * The delay in seconds before a dropped table is actually removed from a Shared database. This allows to recover the table within this time using `UNDROP TABLE` statement.
   * @since 25.12
   */
  database_shared_drop_table_delay_seconds?: number;

  /**
   * Check overflow of decimal arithmetic/comparison operations
   */
  decimal_check_overflow?: boolean;

  /**
   * Enables or disables the deduplication check for materialized views that receive data from Replicated* tables. Possible values: 0 — Disabled. 1 — Enabled. When enabled, ClickHouse performs deduplication of blocks in materialized views that depend on Replicated* tables. This setting is useful for ensuring that materialized views do not contain duplicate data when the insertion operation is being retried due to a failure. **See Also** - [NULL Processing in IN Operators](/guides/developer/deduplicating-inserts-on-retries#insert-deduplication-with-materialized-views)
   */
  deduplicate_blocks_in_dependent_materialized_views?: boolean;

  /**
   * Allows to set a default value for SQL SECURITY option when creating a materialized view. [More about SQL security](../../sql-reference/statements/create/view.md/#sql_security). The default value is `DEFINER`.
   */
  default_materialized_view_sql_security?: 'DEFINER' | 'INVOKER' | 'NONE';

  /**
   * Maximum size of right-side table if limit is required but `max_bytes_in_join` is not set.
   */
  default_max_bytes_in_join?: number;

  /**
   * Allows to set default `SQL SECURITY` option while creating a normal view. [More about SQL security](../../sql-reference/statements/create/view.md/#sql_security). The default value is `INVOKER`.
   */
  default_normal_view_sql_security?: 'DEFINER' | 'INVOKER' | 'NONE';

  /**
   * Default table engine to use when `ENGINE` is not set in a `CREATE` statement. Possible values: - a string representing any valid table engine name Cloud default value: `SharedMergeTree`. **Example** Query: ```sql SET default_table_engine = 'Log'; SELECT name, value, changed FROM system.settings WHERE name = 'default_table_engine'; ``` Result: ```response ┌─name─────────────────┬─value─┬─changed─┐ │ default_table_engine │ Log │ 1 │ └──────────────────────┴───────┴─────────┘ ``` In this example, any new table that does not specify an `Engine` will use the `Log` table engine: Query: ```sql CREATE TABLE my_table ( x UInt32, y UInt32 ); SHOW CREATE TABLE my_table; ``` Result: ```response ┌─statement────────────────────────────────────────────────────────────────┐ │ CREATE TABLE default.my_table ( `x` UInt32, `y` UInt32 ) ENGINE = Log └──────────────────────────────────────────────────────────────────────────┘ ```
   */
  default_table_engine?: string;

  /**
   * Same as [default_table_engine](#default_table_engine) but for temporary tables. In this example, any new temporary table that does not specify an `Engine` will use the `Log` table engine: Query: ```sql SET default_temporary_table_engine = 'Log'; CREATE TEMPORARY TABLE my_table ( x UInt32, y UInt32 ); SHOW CREATE TEMPORARY TABLE my_table; ``` Result: ```response ┌─statement────────────────────────────────────────────────────────────────┐ │ CREATE TEMPORARY TABLE default.my_table ( `x` UInt32, `y` UInt32 ) ENGINE = Log └──────────────────────────────────────────────────────────────────────────┘ ```
   */
  default_temporary_table_engine?: string;

  /**
   * Allows to set default `DEFINER` option while creating a view. [More about SQL security](../../sql-reference/statements/create/view.md/#sql_security). The default value is `CURRENT_USER`.
   */
  default_view_definer?: string;

  /**
   * Enables delta-kernel internal data pruning.
   * @since 25.9
   */
  delta_lake_enable_engine_predicate?: boolean;

  /**
   * Enables Test level logs of DeltaLake expression visitor. These logs can be too verbose even for test logging.
   * @since 25.9
   */
  delta_lake_enable_expression_visitor_logging?: boolean;

  /**
   * Defines a bytes limit for a single inserted data file in delta lake.
   * @since 25.10
   */
  delta_lake_insert_max_bytes_in_data_file?: number;

  /**
   * Defines a rows limit for a single inserted data file in delta lake.
   * @since 25.10
   */
  delta_lake_insert_max_rows_in_data_file?: number;

  /**
   * Enables logging delta lake metadata files into system table.
   * @since 25.11
   */
  delta_lake_log_metadata?: boolean;

  /**
   * End version of delta lake snapshot to read. Value -1 means to read latest version (value 0 is a valid snapshot version).
   * @since 26.1
   */
  delta_lake_snapshot_end_version?: number;

  /**
   * Start version of delta lake snapshot to read. Value -1 means to read latest version (value 0 is a valid snapshot version).
   * @since 26.1
   */
  delta_lake_snapshot_start_version?: number;

  /**
   * Version of delta lake snapshot to read. Value -1 means to read latest version (value 0 is a valid snapshot version).
   * @since 25.9
   */
  delta_lake_snapshot_version?: number;

  /**
   * Enables throwing an exception if there was an error when analyzing scan predicate in delta-kernel.
   * @since 25.9
   */
  delta_lake_throw_on_engine_predicate_error?: boolean;

  /**
   * If true, include only column names and types into result of DESCRIBE query
   */
  describe_compact_output?: boolean;

  /**
   * Enables describing subcolumns for a [DESCRIBE](../../sql-reference/statements/describe-table.md) query. For example, members of a [Tuple](../../sql-reference/data-types/tuple.md) or subcolumns of a [Map](/sql-reference/data-types/map#reading-subcolumns-of-map), [Nullable](../../sql-reference/data-types/nullable.md/#finding-null) or an [Array](../../sql-reference/data-types/array.md/#array-size) data type. Possible values: - 0 — Subcolumns are not included in `DESCRIBE` queries. - 1 — Subcolumns are included in `DESCRIBE` queries. **Example** See an example for the [DESCRIBE](../../sql-reference/statements/describe-table.md) statement.
   */
  describe_include_subcolumns?: boolean;

  /**
   * If true, virtual columns of table will be included into result of DESCRIBE query
   */
  describe_include_virtual_columns?: boolean;

  /**
   * Which dialect will be used to parse query
   */
  dialect?: 'clickhouse' | 'kusto' | 'prql' | 'promql';

  /**
   * Validate primary key type for dictionaries. By default id type for simple layouts will be implicitly converted to UInt64.
   */
  dictionary_validate_primary_key_type?: boolean;

  /**
   * Sets what happens when the amount of data exceeds one of the limits. Possible values: - `throw`: throw an exception (default). - `break`: stop executing the query and return the partial result, as if the source data ran out.
   */
  distinct_overflow_mode?: 'throw' | 'break';

  /**
   * Is the memory-saving mode of distributed aggregation enabled.
   */
  distributed_aggregation_memory_efficient?: boolean;

  /**
   * Enables/disables inserted data sending in batches. When batch sending is enabled, the [Distributed](../../engines/table-engines/special/distributed.md) table engine tries to send multiple files of inserted data in one operation instead of sending them separately. Batch sending improves cluster performance by better-utilizing server and network resources. Possible values: - 1 — Enabled. - 0 — Disabled.
   */
  distributed_background_insert_batch?: boolean;

  /**
   * Maximum interval for the [Distributed](../../engines/table-engines/special/distributed.md) table engine to send data. Limits exponential growth of the interval set in the [distributed_background_insert_sleep_time_ms](#distributed_background_insert_sleep_time_ms) setting. Possible values: - A positive integer number of milliseconds.
   */
  distributed_background_insert_max_sleep_time_ms?: number;

  /**
   * Base interval for the [Distributed](../../engines/table-engines/special/distributed.md) table engine to send data. The actual interval grows exponentially in the event of errors. Possible values: - A positive integer number of milliseconds.
   */
  distributed_background_insert_sleep_time_ms?: number;

  /**
   * Enables/disables splitting batches on failures. Sometimes sending particular batch to the remote shard may fail, because of some complex pipeline after (i.e. `MATERIALIZED VIEW` with `GROUP BY`) due to `Memory limit exceeded` or similar errors. In this case, retrying will not help (and this will stuck distributed sends for the table) but sending files from that batch one by one may succeed INSERT. So installing this setting to `1` will disable batching for such batches (i.e. temporary disables `distributed_background_insert_batch` for failed batches). Possible values: - 1 — Enabled. - 0 — Disabled. :::note This setting also affects broken batches (that may appears because of abnormal server (machine) termination and no `fsync_after_insert`/`fsync_directories` for [Distributed](../../engines/table-engines/special/distributed.md) table engine). ::: :::note You should not rely on automatic batch splitting, since this may hurt performance. :::
   */
  distributed_background_insert_split_batch_on_failure?: boolean;

  /**
   * Timeout for insert query into distributed. Setting is used only with insert_distributed_sync enabled. Zero value means no timeout.
   */
  distributed_background_insert_timeout?: number;

  /**
   * Only has an effect in ClickHouse Cloud. A setting for testing purposes, do not change it
   * @since 25.8
   */
  distributed_cache_alignment?: number;

  /**
   * Only has an effect in ClickHouse Cloud. Allow to bypass distributed cache connection pool
   */
  distributed_cache_bypass_connection_pool?: boolean;

  /**
   * Only has an effect in ClickHouse Cloud. Maximum backoff milliseconds for distributed cache connection creation.
   * @since 25.9
   */
  distributed_cache_connect_backoff_max_ms?: number;

  /**
   * Only has an effect in ClickHouse Cloud. Minimum backoff milliseconds for distributed cache connection creation.
   * @since 25.9
   */
  distributed_cache_connect_backoff_min_ms?: number;

  /**
   * Only has an effect in ClickHouse Cloud. Number of tries to connect to distributed cache if unsuccessful
   */
  distributed_cache_connect_max_tries?: number;

  /**
   * Only has an effect in ClickHouse Cloud. Connection timeout when connecting to distributed cache server.
   * @since 25.11
   */
  distributed_cache_connect_timeout_ms?: number;

  /**
   * Only has an effect in ClickHouse Cloud. A period of credentials refresh.
   * @since 25.8
   */
  distributed_cache_credentials_refresh_period_seconds?: number;

  /**
   * Only has an effect in ClickHouse Cloud. A window for sending ACK for DataPacket sequence in a single distributed cache read request
   */
  distributed_cache_data_packet_ack_window?: number;

  /**
   * Only has an effect in ClickHouse Cloud. Discard connection if some data is unread.
   * @since 24.12
   */
  distributed_cache_discard_connection_if_unread_data?: boolean;

  /**
   * Only has an effect in ClickHouse Cloud. Fetch metrics only from current availability zone in system.distributed_cache_metrics, system.distributed_cache_events
   */
  distributed_cache_fetch_metrics_only_from_current_az?: boolean;

  /**
   * Only has an effect in ClickHouse Cloud. Mode for writing to system.distributed_cache_log
   */
  distributed_cache_log_mode?: 'nothing' | 'on_error' | 'all';

  /**
   * Only has an effect in ClickHouse Cloud. A maximum number of unacknowledged in-flight packets in a single distributed cache read request
   */
  distributed_cache_max_unacked_inflight_packets?: number;

  /**
   * Only has an effect in ClickHouse Cloud. Minimum number of bytes to do seek in distributed cache.
   * @since 25.2
   */
  distributed_cache_min_bytes_for_seek?: number;

  /**
   * Only has an effect in ClickHouse Cloud. Identifies behaviour of distributed cache connection on pool limit reached
   */
  distributed_cache_pool_behaviour_on_limit?: 'wait' | 'allocate_bypassing_pool';

  /**
   * Only has an effect in ClickHouse Cloud. Same as filesystem_cache_prefer_bigger_buffer_size, but for distributed cache.
   * @since 25.11
   */
  distributed_cache_prefer_bigger_buffer_size?: boolean;

  /**
   * Only has an effect in ClickHouse Cloud. Allow to read only from current availability zone. If disabled, will read from all cache servers in all availability zones.
   * @since 25.6
   */
  distributed_cache_read_only_from_current_az?: boolean;

  /**
   * Only has an effect in ClickHouse Cloud. Number of tries to do distributed cache request if unsuccessful
   * @since 25.5
   */
  distributed_cache_read_request_max_tries?: number;

  /**
   * Only has an effect in ClickHouse Cloud. Wait time in milliseconds to receive data for request from distributed cache
   */
  distributed_cache_receive_response_wait_milliseconds?: number;

  /**
   * Only has an effect in ClickHouse Cloud. Wait time in milliseconds to receive any kind of response from distributed cache
   */
  distributed_cache_receive_timeout_milliseconds?: number;

  /**
   * Only has an effect in ClickHouse Cloud. Timeout for receiving data from distributed cache server, in milliseconds. If no bytes were received in this interval, the exception is thrown.
   * @since 25.11
   */
  distributed_cache_receive_timeout_ms?: number;

  /**
   * Only has an effect in ClickHouse Cloud. Timeout for sending data to istributed cache server, in milliseconds. If a client needs to send some data but is not able to send any bytes in this interval, the exception is thrown.
   * @since 25.11
   */
  distributed_cache_send_timeout_ms?: number;

  /**
   * Only has an effect in ClickHouse Cloud. The time in milliseconds the connection to distributed cache server needs to remain idle before TCP starts sending keepalive probes.
   * @since 25.11
   */
  distributed_cache_tcp_keep_alive_timeout_ms?: number;

  /**
   * Only has an effect in ClickHouse Cloud. Rethrow exception happened during communication with distributed cache or exception received from distributed cache. Otherwise fallback to skipping distributed cache on error
   */
  distributed_cache_throw_on_error?: boolean;

  /**
   * Only has an effect in ClickHouse Cloud. Use clients cache for read requests.
   * @since 26.1
   */
  distributed_cache_use_clients_cache_for_read?: boolean;

  /**
   * Only has an effect in ClickHouse Cloud. Use clients cache for write requests.
   * @since 26.1
   */
  distributed_cache_use_clients_cache_for_write?: boolean;

  /**
   * Only has an effect in ClickHouse Cloud. Wait time in milliseconds to receive connection from connection pool if distributed_cache_pool_behaviour_on_limit is wait
   */
  distributed_cache_wait_connection_from_pool_milliseconds?: number;

  /**
   * The maximum number of simultaneous connections with remote servers for distributed processing of all queries to a single Distributed table. We recommend setting a value no less than the number of servers in the cluster.
   */
  distributed_connections_pool_size?: number;

  /**
   * Compatibility version of distributed DDL (ON CLUSTER) queries
   */
  distributed_ddl_entry_format_version?: number;

  /**
   * Sets format of distributed DDL query result. Possible values: - `throw` — Returns result set with query execution status for all hosts where query is finished. If query has failed on some hosts, then it will rethrow the first exception. If query is not finished yet on some hosts and [distributed_ddl_task_timeout](#distributed_ddl_task_timeout) exceeded, then it throws `TIMEOUT_EXCEEDED` exception. - `none` — Is similar to throw, but distributed DDL query returns no result set. - `null_status_on_timeout` — Returns `NULL` as execution status in some rows of result set instead of throwing `TIMEOUT_EXCEEDED` if query is not finished on the corresponding hosts. - `never_throw` — Do not throw `TIMEOUT_EXCEEDED` and do not rethrow exceptions if query has failed on some hosts. - `none_only_active` - similar to `none`, but doesn't wait for inactive replicas of the `Replicated` database. Note: with this mode it's impossible to figure out that the query was not executed on some replica and will be executed in background. - `null_status_on_timeout_only_active` — similar to `null_status_on_timeout`, but doesn't wait for inactive replicas of the `Replicated` database - `throw_only_active` — similar to `throw`, but doesn't wait for inactive replicas of the `Replicated` database Cloud default value: `throw`.
   */
  distributed_ddl_output_mode?: 'none' | 'throw' | 'null_status_on_timeout' | 'throw_only_active' | 'null_status_on_timeout_only_active' | 'none_only_active' | 'never_throw';

  /**
   * Sets timeout for DDL query responses from all hosts in cluster. If a DDL request has not been performed on all hosts, a response will contain a timeout error and a request will be executed in an async mode. Negative value means infinite. Possible values: - Positive integer. - 0 — Async mode. - Negative integer — infinite timeout.
   */
  distributed_ddl_task_timeout?: number;

  /**
   * Enables or disables synchronous data insertion into a [Distributed](/engines/table-engines/special/distributed) table. By default, when inserting data into a `Distributed` table, the ClickHouse server sends data to cluster nodes in background mode. When `distributed_foreground_insert=1`, the data is processed synchronously, and the `INSERT` operation succeeds only after all the data is saved on all shards (at least one replica for each shard if `internal_replication` is true). Possible values: - `0` — Data is inserted in background mode. - `1` — Data is inserted in synchronous mode. Cloud default value: `0`. **See Also** - [Distributed Table Engine](/engines/table-engines/special/distributed) - [Managing Distributed Tables](/sql-reference/statements/system#managing-distributed-tables)
   */
  distributed_foreground_insert?: boolean;

  /**
   * Do not merge aggregation states from different servers for distributed query processing, you can use this in case it is for certain that there are different keys on different shards Possible values: - `0` — Disabled (final query processing is done on the initiator node). - `1` - Do not merge aggregation states from different servers for distributed query processing (query completely processed on the shard, initiator only proxy the data), can be used in case it is for certain that there are different keys on different shards. - `2` - Same as `1` but applies `ORDER BY` and `LIMIT` (it is not possible when the query processed completely on the remote node, like for `distributed_group_by_no_merge=1`) on the initiator (can be used for queries with `ORDER BY` and/or `LIMIT`). **Example** ```sql SELECT * FROM remote('127.0.0.{2,3}', system.one) GROUP BY dummy LIMIT 1 SETTINGS distributed_group_by_no_merge = 1 FORMAT PrettyCompactMonoBlock ┌─dummy─┐ │ 0 │ │ 0 │ └───────┘ ``` ```sql SELECT * FROM remote('127.0.0.{2,3}', system.one) GROUP BY dummy LIMIT 1 SETTINGS distributed_group_by_no_merge = 2 FORMAT PrettyCompactMonoBlock ┌─dummy─┐ │ 0 │ └───────┘ ```
   */
  distributed_group_by_no_merge?: number;

  /**
   * Enables skipping read-only replicas for INSERT queries into Distributed. Possible values: - 0 — INSERT was as usual, if it will go to read-only replica it will fail - 1 — Initiator will skip read-only replicas before sending data to shards.
   */
  distributed_insert_skip_read_only_replicas?: boolean;

  /**
   * Default number of tasks for parallel reading in distributed query. Tasks are spread across between replicas.
   * @since 25.7
   */
  distributed_plan_default_reader_bucket_count?: number;

  /**
   * Default number of buckets for distributed shuffle-hash-join.
   * @since 25.7
   */
  distributed_plan_default_shuffle_join_bucket_count?: number;

  /**
   * Run all tasks of a distributed query plan locally. Useful for testing and debugging.
   * @since 25.7
   */
  distributed_plan_execute_locally?: boolean;

  /**
   * Force specified kind of Exchange operators between distributed query stages. Possible values: - '' - do not force any kind of Exchange operators, let the optimizer choose, - 'Persisted' - use temporary files in object storage, - 'Streaming' - stream exchange data over network.
   * @since 25.7
   */
  distributed_plan_force_exchange_kind?: string;

  /**
   * Use Shuffle aggregation strategy instead of PartialAggregation + Merge in distributed query plan.
   * @since 25.8
   */
  distributed_plan_force_shuffle_aggregation?: boolean;

  /**
   * Maximum rows to use broadcast join instead of shuffle join in distributed query plan.
   * @since 25.8
   */
  distributed_plan_max_rows_to_broadcast?: number;

  /**
   * Removes unnecessary exchanges in distributed query plan. Disable it for debugging.
   * @since 25.7
   */
  distributed_plan_optimize_exchanges?: boolean;

  /**
   * Changes the behaviour of [distributed subqueries](../../sql-reference/operators/in.md). ClickHouse applies this setting when the query contains the product of distributed tables, i.e. when the query for a distributed table contains a non-GLOBAL subquery for the distributed table. Restrictions: - Only applied for IN and JOIN subqueries. - Only if the FROM section uses a distributed table containing more than one shard. - If the subquery concerns a distributed table containing more than one shard. - Not used for a table-valued [remote](../../sql-reference/table-functions/remote.md) function. Possible values: - `deny` — Default value. Prohibits using these types of subqueries (returns the "Double-distributed in/JOIN subqueries is denied" exception). - `local` — Replaces the database and table in the subquery with local ones for the destination server (shard), leaving the normal `IN`/`JOIN.` - `global` — Replaces the `IN`/`JOIN` query with `GLOBAL IN`/`GLOBAL JOIN.` - `allow` — Allows the use of these types of subqueries.
   */
  distributed_product_mode?: 'deny' | 'local' | 'global' | 'allow';

  /**
   * Enables or disables [LIMIT](#limit) applying on each shard separately. This will allow to avoid: - Sending extra rows over network; - Processing rows behind the limit on the initiator. Starting from 21.9 version you cannot get inaccurate results anymore, since `distributed_push_down_limit` changes query execution only if at least one of the conditions met: - [distributed_group_by_no_merge](#distributed_group_by_no_merge) > 0. - Query **does not have** `GROUP BY`/`DISTINCT`/`LIMIT BY`, but it has `ORDER BY`/`LIMIT`. - Query **has** `GROUP BY`/`DISTINCT`/`LIMIT BY` with `ORDER BY`/`LIMIT` and: - [optimize_skip_unused_shards](#optimize_skip_unused_shards) is enabled. - [optimize_distributed_group_by_sharding_key](#optimize_distributed_group_by_sharding_key) is enabled. Possible values: - 0 — Disabled. - 1 — Enabled. See also: - [distributed_group_by_no_merge](#distributed_group_by_no_merge) - [optimize_skip_unused_shards](#optimize_skip_unused_shards) - [optimize_distributed_group_by_sharding_key](#optimize_distributed_group_by_sharding_key)
   */
  distributed_push_down_limit?: number;

  /**
   * - Type: unsigned int - Default value: 1000 The error count of each replica is capped at this value, preventing a single replica from accumulating too many errors. See also: - [load_balancing](#load_balancing-round_robin) - [Table engine Distributed](../../engines/table-engines/special/distributed.md) - [distributed_replica_error_half_life](#distributed_replica_error_half_life) - [distributed_replica_max_ignored_errors](#distributed_replica_max_ignored_errors)
   */
  distributed_replica_error_cap?: number;

  /**
   * - Type: seconds - Default value: 60 seconds Controls how fast errors in distributed tables are zeroed. If a replica is unavailable for some time, accumulates 5 errors, and distributed_replica_error_half_life is set to 1 second, then the replica is considered normal 3 seconds after the last error. See also: - [load_balancing](#load_balancing-round_robin) - [Table engine Distributed](../../engines/table-engines/special/distributed.md) - [distributed_replica_error_cap](#distributed_replica_error_cap) - [distributed_replica_max_ignored_errors](#distributed_replica_max_ignored_errors)
   */
  distributed_replica_error_half_life?: number;

  /**
   * - Type: unsigned int - Default value: 0 The number of errors that will be ignored while choosing replicas (according to `load_balancing` algorithm). See also: - [load_balancing](#load_balancing-round_robin) - [Table engine Distributed](../../engines/table-engines/special/distributed.md) - [distributed_replica_error_cap](#distributed_replica_error_cap) - [distributed_replica_error_half_life](#distributed_replica_error_half_life)
   */
  distributed_replica_max_ignored_errors?: number;

  /**
   * Merge parts only in one partition in select final
   */
  do_not_merge_across_partitions_select_final?: boolean;

  /**
   * Return empty result when aggregating by constant keys on empty set.
   */
  empty_result_for_aggregation_by_constant_keys_on_empty_set?: boolean;

  /**
   * Return empty result when aggregating without keys on empty set.
   */
  empty_result_for_aggregation_by_empty_set?: boolean;

  /**
   * Trigger processor to spill data into external storage adpatively. grace join is supported at present.
   * @since 25.3
   */
  enable_adaptive_memory_spill_scheduler?: boolean;

  /**
   * Enable `DISTINCT` in `IN` subqueries. This is a trade-off setting: enabling it can greatly reduce the size of temporary tables transferred for distributed IN subqueries and significantly speed up data transfer between shards, by ensuring only unique values are sent. However, enabling this setting adds extra merging effort on each node, as deduplication (DISTINCT) must be performed. Use this setting when network transfer is a bottleneck and the additional merging cost is acceptable.
   * @since 25.9
   */
  enable_add_distinct_to_in_subqueries?: boolean;

  /**
   * Write information about blob storage operations to system.blob_storage_log table
   */
  enable_blob_storage_log?: boolean;

  /**
   * If turned on, the DEFLATE_QPL codec may be used to compress columns.
   * @since 25.3
   */
  enable_deflate_qpl_codec?: boolean;

  /**
   * Enable query optimization where we analyze function and subqueries results and rewrite query if there are constants there
   */
  enable_early_constant_folding?: boolean;

  /**
   * Enables or disables returning results of type `Date32` with extended range (compared to type `Date`) or `DateTime64` with extended range (compared to type `DateTime`). Possible values: - `0` — Functions return `Date` or `DateTime` for all types of arguments. - `1` — Functions return `Date32` or `DateTime64` for `Date32` or `DateTime64` arguments and `Date` or `DateTime` otherwise. The table below shows the behavior of this setting for various date-time functions. | Function | `enable_extended_results_for_datetime_functions = 0` | `enable_extended_results_for_datetime_functions = 1` | |----------|---------------------------------------------------|---------------------------------------------------| | `toStartOfYear` | Returns `Date` or `DateTime` | Returns `Date`/`DateTime` for `Date`/`DateTime` input<br/>Returns `Date32`/`DateTime64` for `Date32`/`DateTime64` input | | `toStartOfISOYear` | Returns `Date` or `DateTime` | Returns `Date`/`DateTime` for `Date`/`DateTime` input<br/>Returns `Date32`/`DateTime64` for `Date32`/`DateTime64` input | | `toStartOfQuarter` | Returns `Date` or `DateTime` | Returns `Date`/`DateTime` for `Date`/`DateTime` input<br/>Returns `Date32`/`DateTime64` for `Date32`/`DateTime64` input | | `toStartOfMonth` | Returns `Date` or `DateTime` | Returns `Date`/`DateTime` for `Date`/`DateTime` input<br/>Returns `Date32`/`DateTime64` for `Date32`/`DateTime64` input | | `toStartOfWeek` | Returns `Date` or `DateTime` | Returns `Date`/`DateTime` for `Date`/`DateTime` input<br/>Returns `Date32`/`DateTime64` for `Date32`/`DateTime64` input | | `toLastDayOfWeek` | Returns `Date` or `DateTime` | Returns `Date`/`DateTime` for `Date`/`DateTime` input<br/>Returns `Date32`/`DateTime64` for `Date32`/`DateTime64` input | | `toLastDayOfMonth` | Returns `Date` or `DateTime` | Returns `Date`/`DateTime` for `Date`/`DateTime` input<br/>Returns `Date32`/`DateTime64` for `Date32`/`DateTime64` input | | `toMonday` | Returns `Date` or `DateTime` | Returns `Date`/`DateTime` for `Date`/`DateTime` input<br/>Returns `Date32`/`DateTime64` for `Date32`/`DateTime64` input | | `toStartOfDay` | Returns `DateTime`<br/>*Note: Wrong results for values outside 1970-2149 range* | Returns `DateTime` for `Date`/`DateTime` input<br/>Returns `DateTime64` for `Date32`/`DateTime64` input | | `toStartOfHour` | Returns `DateTime`<br/>*Note: Wrong results for values outside 1970-2149 range* | Returns `DateTime` for `Date`/`DateTime` input<br/>Returns `DateTime64` for `Date32`/`DateTime64` input | | `toStartOfFifteenMinutes` | Returns `DateTime`<br/>*Note: Wrong results for values outside 1970-2149 range* | Returns `DateTime` for `Date`/`DateTime` input<br/>Returns `DateTime64` for `Date32`/`DateTime64` input | | `toStartOfTenMinutes` | Returns `DateTime`<br/>*Note: Wrong results for values outside 1970-2149 range* | Returns `DateTime` for `Date`/`DateTime` input<br/>Returns `DateTime64` for `Date32`/`DateTime64` input | | `toStartOfFiveMinutes` | Returns `DateTime`<br/>*Note: Wrong results for values outside 1970-2149 range* | Returns `DateTime` for `Date`/`DateTime` input<br/>Returns `DateTime64` for `Date32`/`DateTime64` input | | `toStartOfMinute` | Returns `DateTime`<br/>*Note: Wrong results for values outside 1970-2149 range* | Returns `DateTime` for `Date`/`DateTime` input<br/>Returns `DateTime64` for `Date32`/`DateTime64` input | | `timeSlot` | Returns `DateTime`<br/>*Note: Wrong results for values outside 1970-2149 range* | Returns `DateTime` for `Date`/`DateTime` input<br/>Returns `DateTime64` for `Date32`/`DateTime64` input |
   */
  enable_extended_results_for_datetime_functions?: boolean;

  /**
   * Use cache for remote filesystem. This setting does not turn on/off cache for disks (must be done via disk config), but allows to bypass cache for some queries if intended
   */
  enable_filesystem_cache?: boolean;

  /**
   * Allows to record the filesystem caching log for each query
   */
  enable_filesystem_cache_log?: boolean;

  /**
   * Enables or disables `write-through` cache. If set to `false`, the `write-through` cache is disabled for write operations. If set to `true`, `write-through` cache is enabled as long as `cache_on_write_operations` is turned on in the server config's cache disk configuration section. See ["Using local cache"](/operations/storing-data#using-local-cache) for more details.
   */
  enable_filesystem_cache_on_write_operations?: boolean;

  /**
   * Log to system.filesystem prefetch_log during query. Should be used only for testing or debugging, not recommended to be turned on by default
   */
  enable_filesystem_read_prefetches_log?: boolean;

  /**
   * If set to true, allow using the text index.
   * @since 26.1
   */
  enable_full_text_index?: boolean;

  /**
   * Propagate WITH statements to UNION queries and all subqueries
   */
  enable_global_with_statement?: boolean;

  /**
   * Enable or disables pread for HDFS files. By default, `hdfsPread` is used. If disabled, `hdfsRead` and `hdfsSeek` will be used to read hdfs files.
   * @since 25.5
   */
  enable_hdfs_pread?: boolean;

  /**
   * Enables or disables data compression in the response to an HTTP request. For more information, read the [HTTP interface description](/interfaces/http). Possible values: - 0 — Disabled. - 1 — Enabled.
   */
  enable_http_compression?: boolean;

  /**
   * Output stack trace of a job creator when job results in exception. Disabled by default to avoid performance overhead.
   */
  enable_job_stack_trace?: boolean;

  /**
   * Filter left side by set of JOIN keys collected from the right side at runtime.
   * @since 25.11
   */
  enable_join_runtime_filters?: boolean;

  /**
   * Enables lazy columns replication in JOIN and ARRAY JOIN, it allows to avoid unnecessary copy of the same rows multiple times in memory.
   * @since 25.12
   */
  enable_lazy_columns_replication?: boolean;

  /**
   * Enable lightweight DELETE mutations for mergetree tables.
   */
  enable_lightweight_delete?: boolean;

  /**
   * Allow to use lightweight updates.
   * @since 25.9
   */
  enable_lightweight_update?: boolean;

  /**
   * Enable memory bound merging strategy for aggregation.
   */
  enable_memory_bound_merging_of_aggregation_results?: boolean;

  /**
   * Move more conditions from WHERE to PREWHERE and do reads from disk and filtering in multiple steps if there are multiple conditions combined with AND
   */
  enable_multiple_prewhere_read_steps?: boolean;

  /**
   * Generate named tuples in function tuple() when all names are unique and can be treated as unquoted identifiers.
   */
  enable_named_columns_in_function_tuple?: boolean;

  /**
   * Turns on predicate pushdown in `SELECT` queries. Predicate pushdown may significantly reduce network traffic for distributed queries. Possible values: - 0 — Disabled. - 1 — Enabled. Usage Consider the following queries: 1. `SELECT count() FROM test_table WHERE date = '2018-10-10'` 2. `SELECT count() FROM (SELECT * FROM test_table) WHERE date = '2018-10-10'` If `enable_optimize_predicate_expression = 1`, then the execution time of these queries is equal because ClickHouse applies `WHERE` to the subquery when processing it. If `enable_optimize_predicate_expression = 0`, then the execution time of the second query is much longer because the `WHERE` clause applies to all the data after the subquery finishes.
   */
  enable_optimize_predicate_expression?: boolean;

  /**
   * Allow push predicate to final subquery.
   */
  enable_optimize_predicate_expression_to_final_subquery?: boolean;

  /**
   * Enables or disables sorting with `ORDER BY ALL` syntax, see [ORDER BY](../../sql-reference/statements/select/order-by.md). Possible values: - 0 — Disable ORDER BY ALL. - 1 — Enable ORDER BY ALL. **Example** Query: ```sql CREATE TABLE TAB(C1 Int, C2 Int, ALL Int) ENGINE=Memory(); INSERT INTO TAB VALUES (10, 20, 30), (20, 20, 10), (30, 10, 20); SELECT * FROM TAB ORDER BY ALL; -- returns an error that ALL is ambiguous SELECT * FROM TAB ORDER BY ALL SETTINGS enable_order_by_all = 0; ``` Result: ```text ┌─C1─┬─C2─┬─ALL─┐ │ 20 │ 20 │ 10 │ │ 30 │ 10 │ 20 │ │ 10 │ 20 │ 30 │ └────┴────┴─────┘ ```
   */
  enable_order_by_all?: boolean;

  /**
   * If true then data can be parsed directly to columns with custom serialization (e.g. Sparse) according to hints for serialization got from the table.
   */
  enable_parsing_to_custom_serialization?: boolean;

  /**
   * Enables or disables supporting positional arguments for [GROUP BY](/sql-reference/statements/select/group-by), [LIMIT BY](../../sql-reference/statements/select/limit-by.md), [ORDER BY](../../sql-reference/statements/select/order-by.md) statements. Possible values: - 0 — Positional arguments aren't supported. - 1 — Positional arguments are supported: column numbers can use instead of column names. **Example** Query: ```sql CREATE TABLE positional_arguments(one Int, two Int, three Int) ENGINE=Memory(); INSERT INTO positional_arguments VALUES (10, 20, 30), (20, 20, 10), (30, 10, 20); SELECT * FROM positional_arguments ORDER BY 2,3; ``` Result: ```text ┌─one─┬─two─┬─three─┐ │ 30 │ 10 │ 20 │ │ 20 │ 20 │ 10 │ │ 10 │ 20 │ 30 │ └─────┴─────┴───────┘ ```
   */
  enable_positional_arguments?: boolean;

  /**
   * Enables or disables supporting positional arguments in PROJECTION definitions. See also [enable_positional_arguments](#enable_positional_arguments) setting. :::note This is an expert-level setting, and you shouldn't change it if you're just getting started with ClickHouse. ::: Possible values: - 0 — Positional arguments aren't supported. - 1 — Positional arguments are supported: column numbers can use instead of column names.
   * @since 26.1
   */
  enable_positional_arguments_for_projections?: boolean;

  /**
   * Allow memory-efficient aggregation (see `distributed_aggregation_memory_efficient`) to produce buckets out of order. It may improve performance when aggregation bucket sizes are skewed by letting a replica to send buckets with higher id-s to the initiator while it is still processing some heavy buckets with lower id-s. The downside is potentially higher memory usage.
   * @since 25.10
   */
  enable_producing_buckets_out_of_order_in_aggregation?: boolean;

  /**
   * If turned on, results of `SELECT` queries are retrieved from the [query cache](../query-cache.md). Possible values: - 0 - Disabled - 1 - Enabled
   */
  enable_reads_from_query_cache?: boolean;

  /**
   * Enable very explicit logging of S3 requests. Makes sense for debug only.
   */
  enable_s3_requests_logging?: boolean;

  /**
   * If it is set to true, prevent scalar subqueries from (de)serializing large scalar values and possibly avoid running the same subquery more than once.
   */
  enable_scalar_subquery_optimization?: boolean;

  /**
   * If disabled, declarations in parent WITH cluases will behave the same scope as they declared in the current scope. Note that this is a compatibility setting for new analyzer to allow running some invalid queries that old analyzer could execute.
   * @since 25.8
   */
  enable_scopes_for_with_statement?: boolean;

  /**
   * If enabled, all subqueries within a single query will share the same StorageSnapshot for each table. This ensures a consistent view of the data across the entire query, even if the same table is accessed multiple times. This is required for queries where internal consistency of data parts is important. Example: ```sql SELECT count() FROM events WHERE (_part, _part_offset) IN ( SELECT _part, _part_offset FROM events WHERE user_id = 42 ) ``` Without this setting, the outer and inner queries may operate on different data snapshots, leading to incorrect results. :::note Enabling this setting disables the optimization which removes unnecessary data parts from snapshots once the planning stage is complete. As a result, long-running queries may hold onto obsolete parts for their entire duration, delaying part cleanup and increasing storage pressure. This setting currently applies only to tables from the MergeTree family. ::: Possible values: - 0 - Disabled - 1 - Enabled
   * @since 25.7
   */
  enable_shared_storage_snapshot_in_query?: boolean;

  /**
   * Allow sharing set objects build for IN subqueries between different tasks of the same mutation. This reduces memory usage and CPU consumption
   */
  enable_sharing_sets_for_mutations?: boolean;

  /**
   * Enable use of software prefetch in aggregation
   */
  enable_software_prefetch_in_aggregation?: boolean;

  /**
   * Allows creation of [Time](../../sql-reference/data-types/time.md) and [Time64](../../sql-reference/data-types/time64.md) data types.
   * @since 26.1
   */
  enable_time_time64_type?: boolean;

  /**
   * Allow ARRAY JOIN with multiple arrays that have different sizes. When this settings is enabled, arrays will be resized to the longest one.
   */
  enable_unaligned_array_join?: boolean;

  /**
   * Allows to enable/disable decoding/encoding path in uri in [URL](../../engines/table-engines/special/url.md) engine tables. Disabled by default.
   */
  enable_url_encoding?: boolean;

  /**
   * If enable, remove duplicated rows during FINAL by marking rows as deleted and filtering them later instead of merging rows
   */
  enable_vertical_final?: boolean;

  /**
   * If turned on, results of `SELECT` queries are stored in the [query cache](../query-cache.md). Possible values: - 0 - Disabled - 1 - Enabled
   */
  enable_writes_to_query_cache?: boolean;

  /**
   * If turned on, the ZSTD_QAT codec may be used to compress columns.
   */
  enable_zstd_qat_codec?: boolean;

  /**
   * If enabled, only allow identifiers containing alphanumeric characters and underscores.
   */
  enforce_strict_identifier_format?: boolean;

  /**
   * Enables or disables creating a new file on each insert in file engine tables if the format has the suffix (`JSON`, `ORC`, `Parquet`, etc.). If enabled, on each insert a new file will be created with a name following this pattern: `data.Parquet` -> `data.1.Parquet` -> `data.2.Parquet`, etc. Possible values: - 0 — `INSERT` query appends new data to the end of the file. - 1 — `INSERT` query creates a new file.
   */
  engine_file_allow_create_multiple_files?: boolean;

  /**
   * Allows to select data from a file engine table without file. Possible values: - 0 — `SELECT` throws exception. - 1 — `SELECT` returns empty result.
   */
  engine_file_empty_if_not_exists?: boolean;

  /**
   * Enables or disables skipping empty files in [File](../../engines/table-engines/special/file.md) engine tables. Possible values: - 0 — `SELECT` throws an exception if empty file is not compatible with requested format. - 1 — `SELECT` returns empty result for empty file.
   */
  engine_file_skip_empty_files?: boolean;

  /**
   * Enables or disables truncate before insert in [File](../../engines/table-engines/special/file.md) engine tables. Possible values: - 0 — `INSERT` query appends new data to the end of the file. - 1 — `INSERT` query replaces existing content of the file with the new data.
   */
  engine_file_truncate_on_insert?: boolean;

  /**
   * Enables or disables skipping empty files in [URL](../../engines/table-engines/special/url.md) engine tables. Possible values: - 0 — `SELECT` throws an exception if empty file is not compatible with requested format. - 1 — `SELECT` returns empty result for empty file.
   */
  engine_url_skip_empty_files?: boolean;

  /**
   * Set default mode in EXCEPT query. Possible values: empty string, 'ALL', 'DISTINCT'. If empty, query without mode will throw exception.
   */
  except_default_mode?: 'ALL' | 'DISTINCT';

  /**
   * Excludes specified skip indexes from being built and stored during INSERTs. The excluded skip indexes will still be built and stored [during merges](merge-tree-settings.md/#materialize_skip_indexes_on_merge) or by an explicit [MATERIALIZE INDEX](/sql-reference/statements/alter/skipping-index.md/#materialize-index) query. Has no effect if [materialize_skip_indexes_on_insert](#materialize_skip_indexes_on_insert) is false. Example: ```sql CREATE TABLE tab ( a UInt64, b UInt64, INDEX idx_a a TYPE minmax, INDEX idx_b b TYPE set(3) ) ENGINE = MergeTree ORDER BY tuple(); SET exclude_materialize_skip_indexes_on_insert='idx_a'; -- idx_a will be not be updated upon insert --SET exclude_materialize_skip_indexes_on_insert='idx_a, idx_b'; -- neither index would be updated on insert INSERT INTO tab SELECT number, number / 50 FROM numbers(100); -- only idx_b is updated -- since it is a session setting it can be set on a per-query level INSERT INTO tab SELECT number, number / 50 FROM numbers(100, 100) SETTINGS exclude_materialize_skip_indexes_on_insert='idx_b'; ALTER TABLE tab MATERIALIZE INDEX idx_a; -- this query can be used to explicitly materialize the index SET exclude_materialize_skip_indexes_on_insert = DEFAULT; -- reset setting to default ```
   * @since 25.11
   */
  exclude_materialize_skip_indexes_on_insert?: string;

  /**
   * Execute non-correlated EXISTS subqueries as scalar subqueries. As for scalar subqueries, the cache is used, and the constant folding applies to the result.
   * @since 25.9
   */
  execute_exists_as_scalar_subquery?: boolean;

  /**
   * Connect timeout in seconds. Now supported only for MySQL
   */
  external_storage_connect_timeout_sec?: number;

  /**
   * Limit maximum number of bytes when table with external engine should flush history data. Now supported only for MySQL table engine, database engine, and dictionary. If equal to 0, this setting is disabled
   */
  external_storage_max_read_bytes?: number;

  /**
   * Limit maximum number of rows when table with external engine should flush history data. Now supported only for MySQL table engine, database engine, and dictionary. If equal to 0, this setting is disabled
   */
  external_storage_max_read_rows?: number;

  /**
   * Read/write timeout in seconds. Now supported only for MySQL
   */
  external_storage_rw_timeout_sec?: number;

  /**
   * Defines how [mysql](../../sql-reference/table-functions/mysql.md), [postgresql](../../sql-reference/table-functions/postgresql.md) and [odbc](../../sql-reference/table-functions/odbc.md) table functions use Nullable columns. Possible values: - 0 — The table function explicitly uses Nullable columns. - 1 — The table function implicitly uses Nullable columns. **Usage** If the setting is set to `0`, the table function does not make Nullable columns and inserts default values instead of NULL. This is also applicable for NULL values inside arrays.
   */
  external_table_functions_use_nulls?: boolean;

  /**
   * If it is set to true, transforming expression to local filter is forbidden for queries to external tables.
   */
  external_table_strict_query?: boolean;

  /**
   * Max number of pairs that can be produced by the `extractKeyValuePairs` function. Used as a safeguard against consuming too much memory.
   */
  extract_key_value_pairs_max_pairs_per_row?: number;

  /**
   * Whether to count extreme values (the minimums and maximums in columns of a query result). Accepts 0 or 1. By default, 0 (disabled). For more information, see the section "Extreme values".
   */
  extremes?: boolean;

  /**
   * Forces a query to an out-of-date replica if updated data is not available. See [Replication](../../engines/table-engines/mergetree-family/replication.md). ClickHouse selects the most relevant from the outdated replicas of the table. Used when performing `SELECT` from a distributed table that points to replicated tables. By default, 1 (enabled).
   */
  fallback_to_stale_replicas_for_distributed_queries?: boolean;

  /**
   * Allow filesystem cache to enqueue background downloads for data read from remote storage. Disable to keep downloads in the foreground for the current query/session.
   * @since 25.12
   */
  filesystem_cache_allow_background_download?: boolean;

  /**
   * Filesystem cache boundary alignment. This setting is applied only for non-disk read (e.g. for cache of remote table engines / table functions, but not for storage configuration of MergeTree tables). Value 0 means no alignment.
   * @since 24.12
   */
  filesystem_cache_boundary_alignment?: number;

  /**
   * Only has an effect in ClickHouse Cloud. Wait time to lock cache for space reservation in filesystem cache
   * @since 24.12
   */
  filesystem_cache_enable_background_download_during_fetch?: boolean;

  /**
   * Only has an effect in ClickHouse Cloud. Wait time to lock cache for space reservation in filesystem cache
   * @since 24.12
   */
  filesystem_cache_enable_background_download_for_metadata_files_in_packed_storage?: boolean;

  /**
   * Max remote filesystem cache size that can be downloaded by a single query
   */
  filesystem_cache_max_download_size?: number;

  /**
   * Filesystem cache name to use for stateless table engines or data lakes
   */
  filesystem_cache_name?: string;

  /**
   * Prefer bigger buffer size if filesystem cache is enabled to avoid writing small file segments which deteriorate cache performance. On the other hand, enabling this setting might increase memory usage.
   * @since 24.12
   */
  filesystem_cache_prefer_bigger_buffer_size?: boolean;

  /**
   * Wait time to lock cache for space reservation in filesystem cache
   */
  filesystem_cache_reserve_space_wait_lock_timeout_milliseconds?: number;

  /**
   * Limit on size of a single batch of file segments that a read buffer can request from cache. Too low value will lead to excessive requests to cache, too large may slow down eviction from cache
   */
  filesystem_cache_segments_batch_size?: number;

  /**
   * Skip download from remote filesystem if exceeds query cache size
   * @since 24.12
   */
  filesystem_cache_skip_download_if_exceeds_per_query_cache_write_limit?: boolean;

  /**
   * Maximum memory usage for prefetches.
   */
  filesystem_prefetch_max_memory_usage?: number;

  /**
   * Prefetch step in bytes. Zero means `auto` - approximately the best prefetch step will be auto deduced, but might not be 100% the best. The actual value might be different because of setting filesystem_prefetch_min_bytes_for_single_read_task
   */
  filesystem_prefetch_step_bytes?: number;

  /**
   * Prefetch step in marks. Zero means `auto` - approximately the best prefetch step will be auto deduced, but might not be 100% the best. The actual value might be different because of setting filesystem_prefetch_min_bytes_for_single_read_task
   */
  filesystem_prefetch_step_marks?: number;

  /**
   * Maximum number of prefetches. Zero means unlimited. A setting `filesystem_prefetches_max_memory_usage` is more recommended if you want to limit the number of prefetches
   */
  filesystem_prefetches_limit?: number;

  /**
   * Automatically applies [FINAL](../../sql-reference/statements/select/from.md/#final-modifier) modifier to all tables in a query, to tables where [FINAL](../../sql-reference/statements/select/from.md/#final-modifier) is applicable, including joined tables and tables in sub-queries, and distributed tables. Possible values: - 0 - disabled - 1 - enabled Example: ```sql CREATE TABLE test ( key Int64, some String ) ENGINE = ReplacingMergeTree ORDER BY key; INSERT INTO test FORMAT Values (1, 'first'); INSERT INTO test FORMAT Values (1, 'second'); SELECT * FROM test; ┌─key─┬─some───┐ │ 1 │ second │ └─────┴────────┘ ┌─key─┬─some──┐ │ 1 │ first │ └─────┴───────┘ SELECT * FROM test SETTINGS final = 1; ┌─key─┬─some───┐ │ 1 │ second │ └─────┴────────┘ SET final = 1; SELECT * FROM test; ┌─key─┬─some───┐ │ 1 │ second │ └─────┴────────┘ ```
   */
  final?: boolean;

  /**
   * Sets the data format of a [nested](../../sql-reference/data-types/nested-data-structures/index.md) columns. Possible values: - 1 — Nested column is flattened to separate arrays. - 0 — Nested column stays a single array of tuples. **Usage** If the setting is set to `0`, it is possible to use an arbitrary level of nesting. **Examples** Query: ```sql SET flatten_nested = 1; CREATE TABLE t_nest (`n` Nested(a UInt32, b UInt32)) ENGINE = MergeTree ORDER BY tuple(); SHOW CREATE TABLE t_nest; ``` Result: ```text ┌─statement───────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────┐ │ CREATE TABLE default.t_nest ( `n.a` Array(UInt32), `n.b` Array(UInt32) ) ENGINE = MergeTree ORDER BY tuple() SETTINGS index_granularity = 8192 │ └─────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────┘ ``` Query: ```sql SET flatten_nested = 0; CREATE TABLE t_nest (`n` Nested(a UInt32, b UInt32)) ENGINE = MergeTree ORDER BY tuple(); SHOW CREATE TABLE t_nest; ``` Result: ```text ┌─statement──────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────┐ │ CREATE TABLE default.t_nest ( `n` Nested(a UInt32, b UInt32) ) ENGINE = MergeTree ORDER BY tuple() SETTINGS index_granularity = 8192 │ └────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────┘ ```
   */
  flatten_nested?: boolean;

  /**
   * Force the use of optimization when it is applicable, but heuristics decided not to use it
   */
  force_aggregate_partitions_independently?: boolean;

  /**
   * The setting is used by the server itself to support distributed queries. Do not change it manually, because it will break normal operations. (Forces use of aggregation in order on remote nodes during distributed aggregation).
   */
  force_aggregation_in_order?: boolean;

  /**
   * Disables query execution if passed data skipping indices wasn't used. Consider the following example: ```sql CREATE TABLE data ( key Int, d1 Int, d1_null Nullable(Int), INDEX d1_idx d1 TYPE minmax GRANULARITY 1, INDEX d1_null_idx assumeNotNull(d1_null) TYPE minmax GRANULARITY 1 ) Engine=MergeTree() ORDER BY key; SELECT * FROM data_01515; SELECT * FROM data_01515 SETTINGS force_data_skipping_indices=''; -- query will produce CANNOT_PARSE_TEXT error. SELECT * FROM data_01515 SETTINGS force_data_skipping_indices='d1_idx'; -- query will produce INDEX_NOT_USED error. SELECT * FROM data_01515 WHERE d1 = 0 SETTINGS force_data_skipping_indices='d1_idx'; -- Ok. SELECT * FROM data_01515 WHERE d1 = 0 SETTINGS force_data_skipping_indices='`d1_idx`'; -- Ok (example of full featured parser). SELECT * FROM data_01515 WHERE d1 = 0 SETTINGS force_data_skipping_indices='`d1_idx`, d1_null_idx'; -- query will produce INDEX_NOT_USED error, since d1_null_idx is not used. SELECT * FROM data_01515 WHERE d1 = 0 AND assumeNotNull(d1_null) = 0 SETTINGS force_data_skipping_indices='`d1_idx`, d1_null_idx'; -- Ok. ```
   */
  force_data_skipping_indices?: string;

  /**
   * Make GROUPING function to return 1 when argument is not used as an aggregation key
   */
  force_grouping_standard_compatibility?: boolean;

  /**
   * Disables query execution if the index can't be used by date. Works with tables in the MergeTree family. If `force_index_by_date=1`, ClickHouse checks whether the query has a date key condition that can be used for restricting data ranges. If there is no suitable condition, it throws an exception. However, it does not check whether the condition reduces the amount of data to read. For example, the condition `Date != ' 2000-01-01 '` is acceptable even when it matches all the data in the table (i.e., running the query requires a full scan). For more information about ranges of data in MergeTree tables, see [MergeTree](../../engines/table-engines/mergetree-family/mergetree.md).
   */
  force_index_by_date?: boolean;

  /**
   * Enables or disables the obligatory use of [projections](../../engines/table-engines/mergetree-family/mergetree.md/#projections) in `SELECT` queries, when projection optimization is enabled (see [optimize_use_projections](#optimize_use_projections) setting). Possible values: - 0 — Projection optimization is not obligatory. - 1 — Projection optimization is obligatory.
   */
  force_optimize_projection?: boolean;

  /**
   * If it is set to a non-empty string, check that this projection is used in the query at least once. Possible values: - string: name of projection that used in a query
   */
  force_optimize_projection_name?: string;

  /**
   * Enables or disables query execution if [optimize_skip_unused_shards](#optimize_skip_unused_shards) is enabled and skipping of unused shards is not possible. If the skipping is not possible and the setting is enabled, an exception will be thrown. Possible values: - 0 — Disabled. ClickHouse does not throw an exception. - 1 — Enabled. Query execution is disabled only if the table has a sharding key. - 2 — Enabled. Query execution is disabled regardless of whether a sharding key is defined for the table.
   */
  force_optimize_skip_unused_shards?: number;

  /**
   * Controls [`force_optimize_skip_unused_shards`](#force_optimize_skip_unused_shards) (hence still requires [`force_optimize_skip_unused_shards`](#force_optimize_skip_unused_shards)) depends on the nesting level of the distributed query (case when you have `Distributed` table that look into another `Distributed` table). Possible values: - 0 - Disabled, `force_optimize_skip_unused_shards` works always. - 1 — Enables `force_optimize_skip_unused_shards` only for the first level. - 2 — Enables `force_optimize_skip_unused_shards` up to the second level.
   */
  force_optimize_skip_unused_shards_nesting?: number;

  /**
   * Disables query execution if indexing by the primary key is not possible. Works with tables in the MergeTree family. If `force_primary_key=1`, ClickHouse checks to see if the query has a primary key condition that can be used for restricting data ranges. If there is no suitable condition, it throws an exception. However, it does not check whether the condition reduces the amount of data to read. For more information about data ranges in MergeTree tables, see [MergeTree](../../engines/table-engines/mergetree-family/mergetree.md).
   */
  force_primary_key?: boolean;

  /**
   * Recursively remove data on DROP query. Avoids 'Directory not empty' error, but may silently remove detached data
   */
  force_remove_data_recursively_on_drop?: boolean;

  /**
   * Formatter '%e' in function 'formatDateTime' prints single-digit days with a leading space, e.g. ' 2' instead of '2'.
   * @since 25.6
   */
  formatdatetime_e_with_space_padding?: boolean;

  /**
   * Formatter '%f' in function 'formatDateTime' prints only the scale amount of digits for a DateTime64 instead of fixed 6 digits.
   * @since 25.2
   */
  formatdatetime_f_prints_scale_number_of_digits?: boolean;

  /**
   * Formatter '%f' in function 'formatDateTime' prints a single zero instead of six zeros if the formatted value has no fractional seconds.
   */
  formatdatetime_f_prints_single_zero?: boolean;

  /**
   * Formatters '%c', '%l' and '%k' in function 'formatDateTime' print months and hours without leading zeros.
   */
  formatdatetime_format_without_leading_zeros?: boolean;

  /**
   * Formatter '%M' in functions 'formatDateTime' and 'parseDateTime' print/parse the month name instead of minutes.
   */
  formatdatetime_parsedatetime_m_is_month_name?: boolean;

  /**
   * Enables or disables [fsync](http://pubs.opengroup.org/onlinepubs/9699919799/functions/fsync.html) when writing `.sql` files. Enabled by default. It makes sense to disable it if the server has millions of tiny tables that are constantly being created and destroyed.
   */
  fsync_metadata?: boolean;

  /**
   * Allows to change the behaviour of the result type of `dateTrunc` function. Possible values: - 0 - When the second argument is `DateTime64/Date32` the return type will be `DateTime64/Date32` regardless of the time unit in the first argument. - 1 - For `Date32` the result is always `Date`. For `DateTime64` the result is `DateTime` for time units `second` and higher.
   * @since 25.8
   */
  function_date_trunc_return_type_behavior?: number;

  /**
   * Choose function implementation for specific target or variant (experimental). If empty enable all of them.
   */
  function_implementation?: string;

  /**
   * Control whether allow to return complex type (such as: struct, array, map) for json_value function. ```sql SELECT JSON_VALUE('{"hello":{"world":"!"}}', '$.hello') settings function_json_value_return_type_allow_complex=true ┌─JSON_VALUE('{"hello":{"world":"!"}}', '$.hello')─┐ │ {"world":"!"} │ └──────────────────────────────────────────────────┘ 1 row in set. Elapsed: 0.001 sec. ``` Possible values: - true — Allow. - false — Disallow.
   */
  function_json_value_return_type_allow_complex?: boolean;

  /**
   * Control whether allow to return `NULL` when value is not exist for JSON_VALUE function. ```sql SELECT JSON_VALUE('{"hello":"world"}', '$.b') settings function_json_value_return_type_allow_nullable=true; ┌─JSON_VALUE('{"hello":"world"}', '$.b')─┐ │ ᴺᵁᴸᴸ │ └────────────────────────────────────────┘ 1 row in set. Elapsed: 0.001 sec. ``` Possible values: - true — Allow. - false — Disallow.
   */
  function_json_value_return_type_allow_nullable?: boolean;

  /**
   * Controls the order of arguments in function [locate](../../sql-reference/functions/string-search-functions.md/#locate). Possible values: - 0 — Function `locate` accepts arguments `(haystack, needle[, start_pos])`. - 1 — Function `locate` accepts arguments `(needle, haystack, [, start_pos])` (MySQL-compatible behavior)
   */
  function_locate_has_mysql_compatible_argument_order?: boolean;

  /**
   * Sets the safety threshold for data volume generated by function [range](/sql-reference/functions/array-functions#range). Defines the maximum number of values generated by function per block of data (sum of array sizes for every row in a block). Possible values: - Positive integer. **See Also** - [`max_block_size`](#max_block_size) - [`min_insert_block_size_rows`](#min_insert_block_size_rows)
   */
  function_range_max_elements_in_block?: number;

  /**
   * Maximum number of microseconds the function `sleep` is allowed to sleep for each block. If a user called it with a larger value, it throws an exception. It is a safety threshold.
   */
  function_sleep_max_microseconds_per_block?: number;

  /**
   * The version of `visibleWidth` behavior. 0 - only count the number of code points; 1 - correctly count zero-width and combining characters, count full-width characters as two, estimate the tab width, count delete characters.
   */
  function_visible_width_behavior?: number;

  /**
   * If all four arguments to `geoDistance`, `greatCircleDistance`, `greatCircleAngle` functions are Float64, return Float64 and use double precision for internal calculations. In previous ClickHouse versions, the functions always returned Float32.
   */
  geo_distance_returns_float64_on_float64_arguments?: boolean;

  /**
   * Function 'geoToH3' accepts (lon, lat) if set to 'lon_lat' and (lat, lon) if set to 'lat_lon'.
   * @since 25.7
   */
  geotoh3_argument_order?: 'lat_lon' | 'lon_lat';

  /**
   * Maximum number of allowed addresses (For external storages, table functions, etc).
   */
  glob_expansion_max_elements?: number;

  /**
   * Initial number of grace hash join buckets
   */
  grace_hash_join_initial_buckets?: number;

  /**
   * Limit on the number of grace hash join buckets
   */
  grace_hash_join_max_buckets?: number;

  /**
   * Sets what happens when the number of unique keys for aggregation exceeds the limit: - `throw`: throw an exception - `break`: stop executing the query and return the partial result - `any`: continue aggregation for the keys that got into the set, but do not add new keys to the set. Using the 'any' value lets you run an approximation of GROUP BY. The quality of this approximation depends on the statistical nature of the data.
   */
  group_by_overflow_mode?: 'throw' | 'break' | 'any';

  /**
   * From what number of keys, a two-level aggregation starts. 0 - the threshold is not set.
   */
  group_by_two_level_threshold?: number;

  /**
   * From what size of the aggregation state in bytes, a two-level aggregation begins to be used. 0 - the threshold is not set. Two-level aggregation is used when at least one of the thresholds is triggered.
   */
  group_by_two_level_threshold_bytes?: number;

  /**
   * Changes the way the [GROUP BY clause](/sql-reference/statements/select/group-by) treats the types of aggregation keys. When the `ROLLUP`, `CUBE`, or `GROUPING SETS` specifiers are used, some aggregation keys may not be used to produce some result rows. Columns for these keys are filled with either default value or `NULL` in corresponding rows depending on this setting. Possible values: - 0 — The default value for the aggregation key type is used to produce missing values. - 1 — ClickHouse executes `GROUP BY` the same way as the SQL standard says. The types of aggregation keys are converted to [Nullable](/sql-reference/data-types/nullable). Columns for corresponding aggregation keys are filled with [NULL](/sql-reference/syntax#null) for rows that didn't use it. See also: - [GROUP BY clause](/sql-reference/statements/select/group-by)
   */
  group_by_use_nulls?: boolean;

  /**
   * Function 'h3ToGeo' returns (lon, lat) if true, otherwise (lat, lon).
   * @since 25.2
   */
  h3togeo_lon_lat_result_order?: boolean;

  /**
   * Timeout in milliseconds for receiving Hello packet from replicas during handshake.
   */
  handshake_timeout_ms?: number;

  /**
   * Enables or disables creating a new file on each insert in HDFS engine tables. If enabled, on each insert a new HDFS file will be created with the name, similar to this pattern: initial: `data.Parquet.gz` -> `data.1.Parquet.gz` -> `data.2.Parquet.gz`, etc. Possible values: - 0 — `INSERT` query appends new data to the end of the file. - 1 — `INSERT` query creates a new file.
   */
  hdfs_create_new_file_on_insert?: boolean;

  /**
   * Ignore absence of file if it does not exist when reading certain keys. Possible values: - 1 — `SELECT` returns empty result. - 0 — `SELECT` throws an exception.
   */
  hdfs_ignore_file_doesnt_exist?: boolean;

  /**
   * The actual number of replications can be specified when the hdfs file is created.
   */
  hdfs_replication?: number;

  /**
   * Enables or disables skipping empty files in [HDFS](../../engines/table-engines/integrations/hdfs.md) engine tables. Possible values: - 0 — `SELECT` throws an exception if empty file is not compatible with requested format. - 1 — `SELECT` returns empty result for empty file.
   */
  hdfs_skip_empty_files?: boolean;

  /**
   * Throw an error if matched zero files according to glob expansion rules. Possible values: - 1 — `SELECT` throws an exception. - 0 — `SELECT` returns empty result.
   */
  hdfs_throw_on_zero_files_match?: boolean;

  /**
   * Enables or disables truncation before an insert in hdfs engine tables. If disabled, an exception will be thrown on an attempt to insert if a file in HDFS already exists. Possible values: - 0 — `INSERT` query appends new data to the end of the file. - 1 — `INSERT` query replaces existing content of the file with the new data.
   */
  hdfs_truncate_on_insert?: boolean;

  /**
   * Connection timeout for establishing connection with replica for Hedged requests
   */
  hedged_connection_timeout_ms?: number;

  /**
   * The size of the dynamic candidate list when searching the vector similarity index, also known as 'ef_search'.
   */
  hnsw_candidate_list_size_for_search?: number;

  /**
   * Expired time for HSTS. 0 means disable HSTS.
   */
  hsts_max_age?: number;

  /**
   * HTTP connection timeout (in seconds). Possible values: - Any positive integer. - 0 - Disabled (infinite timeout).
   */
  http_connection_timeout?: number;

  /**
   * Do not send HTTP headers X-ClickHouse-Progress more frequently than at each specified interval.
   */
  http_headers_progress_interval_ms?: number;

  /**
   * The `http_make_head_request` setting allows the execution of a `HEAD` request while reading data from HTTP to retrieve information about the file to be read, such as its size. Since it's enabled by default, it may be desirable to disable this setting in cases where the server does not support `HEAD` requests.
   */
  http_make_head_request?: boolean;

  /**
   * Maximum length of field name in HTTP header
   */
  http_max_field_name_size?: number;

  /**
   * Maximum length of field value in HTTP header
   */
  http_max_field_value_size?: number;

  /**
   * Maximum number of fields in HTTP header
   */
  http_max_fields?: number;

  /**
   * Limit on size of multipart/form-data content. This setting cannot be parsed from URL parameters and should be set in a user profile. Note that content is parsed and external tables are created in memory before the start of query execution. And this is the only limit that has an effect on that stage (limits on max memory usage and max execution time have no effect while reading HTTP form data).
   */
  http_max_multipart_form_data_size?: number;

  /**
   * Limit on size of request data used as a query parameter in predefined HTTP requests.
   */
  http_max_request_param_data_size?: number;

  /**
   * Max attempts to read via http.
   */
  http_max_tries?: number;

  /**
   * Sets the maximum URI length of an HTTP request. Possible values: - Positive integer.
   */
  http_max_uri_size?: number;

  /**
   * Enables or disables checksum verification when decompressing the HTTP POST data from the client. Used only for ClickHouse native compression format (not used with `gzip` or `deflate`). For more information, read the [HTTP interface description](/interfaces/http). Possible values: - 0 — Disabled. - 1 — Enabled.
   */
  http_native_compression_disable_checksumming_on_decompress?: boolean;

  /**
   * HTTP receive timeout (in seconds). Possible values: - Any positive integer. - 0 - Disabled (infinite timeout).
   */
  http_receive_timeout?: number;

  /**
   * The number of bytes to buffer in the server memory before sending a HTTP response to the client or flushing to disk (when http_wait_end_of_query is enabled).
   */
  http_response_buffer_size?: number;

  /**
   * Allows to add or override HTTP headers which the server will return in the response with a successful query result. This only affects the HTTP interface. If the header is already set by default, the provided value will override it. If the header was not set by default, it will be added to the list of headers. Headers that are set by the server by default and not overridden by this setting, will remain. The setting allows you to set a header to a constant value. Currently there is no way to set a header to a dynamically calculated value. Neither names or values can contain ASCII control characters. If you implement a UI application which allows users to modify settings but at the same time makes decisions based on the returned headers, it is recommended to restrict this setting to readonly. Example: `SET http_response_headers = '{"Content-Type": "image/png"}'`
   * @since 25.1
   */
  http_response_headers?: Record<string, string>;

  /**
   * Min milliseconds for backoff, when retrying read via http
   */
  http_retry_initial_backoff_ms?: number;

  /**
   * Max milliseconds for backoff, when retrying read via http
   */
  http_retry_max_backoff_ms?: number;

  /**
   * HTTP send timeout (in seconds). Possible values: - Any positive integer. - 0 - Disabled (infinite timeout). :::note It's applicable only to the default profile. A server reboot is required for the changes to take effect. :::
   */
  http_send_timeout?: number;

  /**
   * Skip URLs for globs with HTTP_NOT_FOUND error
   */
  http_skip_not_found_url_for_globs?: boolean;

  /**
   * Enable HTTP response buffering on the server-side.
   */
  http_wait_end_of_query?: boolean;

  /**
   * Write exception in output format to produce valid output. Works with JSON and XML formats.
   */
  http_write_exception_in_output_format?: boolean;

  /**
   * Sets the level of data compression in the response to an HTTP request if [enable_http_compression = 1](#enable_http_compression). Possible values: Numbers from 1 to 9.
   */
  http_zlib_compression_level?: number;

  /**
   * Whether to delete all iceberg files on drop or not.
   * @since 25.10
   */
  iceberg_delete_data_on_drop?: boolean;

  /**
   * Max bytes of iceberg parquet data file on insert operation.
   * @since 25.11
   */
  iceberg_insert_max_bytes_in_data_file?: number;

  /**
   * Max allowed partitions count per one insert operation for Iceberg table engine.
   * @since 26.1
   */
  iceberg_insert_max_partitions?: number;

  /**
   * Max rows of iceberg parquet data file on insert operation.
   * @since 25.11
   */
  iceberg_insert_max_rows_in_data_file?: number;

  /**
   * Method to compress `.metadata.json` file.
   * @since 25.9
   */
  iceberg_metadata_compression_method?: string;

  /**
   * Controls the level of metadata logging for Iceberg tables to system.iceberg_metadata_log. Usually this setting can be modified for debugging purposes. Possible values: - none - No metadata log. - metadata - Root metadata.json file. - manifest_list_metadata - Everything above + metadata from avro manifest list which corresponds to a snapshot. - manifest_list_entry - Everything above + avro manifest list entries. - manifest_file_metadata - Everything above + metadata from traversed avro manifest files. - manifest_file_entry - Everything above + traversed avro manifest files entries.
   * @since 25.10
   */
  iceberg_metadata_log_level?: 'none' | 'metadata' | 'manifest_list_metadata' | 'manifest_list_entry' | 'manifest_file_metadata' | 'manifest_file_entry';

  /**
   * Query Iceberg table using the specific snapshot id.
   * @since 25.5
   */
  iceberg_snapshot_id?: number;

  /**
   * Query Iceberg table using the snapshot that was current at a specific timestamp.
   * @since 25.5
   */
  iceberg_timestamp_ms?: number;

  /**
   * Timeout to close idle TCP connections after specified number of seconds. Possible values: - Positive integer (0 - close immediately, after 0 seconds).
   */
  idle_connection_timeout?: number;

  /**
   * Only has an effect in ClickHouse Cloud. Exclude new data parts from SELECT queries until they're either pre-warmed (see [cache_populated_by_fetch](merge-tree-settings.md/#cache_populated_by_fetch)) or this many seconds old. Only for Replicated-/SharedMergeTree.
   */
  ignore_cold_parts_seconds?: number;

  /**
   * Ignores the skipping indexes specified if used by the query. Consider the following example: ```sql CREATE TABLE data ( key Int, x Int, y Int, INDEX x_idx x TYPE minmax GRANULARITY 1, INDEX y_idx y TYPE minmax GRANULARITY 1, INDEX xy_idx (x,y) TYPE minmax GRANULARITY 1 ) Engine=MergeTree() ORDER BY key; INSERT INTO data VALUES (1, 2, 3); SELECT * FROM data; SELECT * FROM data SETTINGS ignore_data_skipping_indices=''; -- query will produce CANNOT_PARSE_TEXT error. SELECT * FROM data SETTINGS ignore_data_skipping_indices='x_idx'; -- Ok. SELECT * FROM data SETTINGS ignore_data_skipping_indices='na_idx'; -- Ok. SELECT * FROM data WHERE x = 1 AND y = 1 SETTINGS ignore_data_skipping_indices='xy_idx',force_data_skipping_indices='xy_idx' ; -- query will produce INDEX_NOT_USED error, since xy_idx is explicitly ignored. SELECT * FROM data WHERE x = 1 AND y = 2 SETTINGS ignore_data_skipping_indices='xy_idx'; ``` The query without ignoring any indexes: ```sql EXPLAIN indexes = 1 SELECT * FROM data WHERE x = 1 AND y = 2; Expression ((Projection + Before ORDER BY)) Filter (WHERE) ReadFromMergeTree (default.data) Indexes: PrimaryKey Condition: true Parts: 1/1 Granules: 1/1 Skip Name: x_idx Description: minmax GRANULARITY 1 Parts: 0/1 Granules: 0/1 Skip Name: y_idx Description: minmax GRANULARITY 1 Parts: 0/0 Granules: 0/0 Skip Name: xy_idx Description: minmax GRANULARITY 1 Parts: 0/0 Granules: 0/0 ``` Ignoring the `xy_idx` index: ```sql EXPLAIN indexes = 1 SELECT * FROM data WHERE x = 1 AND y = 2 SETTINGS ignore_data_skipping_indices='xy_idx'; Expression ((Projection + Before ORDER BY)) Filter (WHERE) ReadFromMergeTree (default.data) Indexes: PrimaryKey Condition: true Parts: 1/1 Granules: 1/1 Skip Name: x_idx Description: minmax GRANULARITY 1 Parts: 0/1 Granules: 0/1 Skip Name: y_idx Description: minmax GRANULARITY 1 Parts: 0/0 Granules: 0/0 ``` Works with tables in the MergeTree family.
   */
  ignore_data_skipping_indices?: string;

  /**
   * If enabled, server will ignore all DROP table queries with specified probability (for Memory and JOIN engines it will replace DROP to TRUNCATE). Used for testing purposes
   */
  ignore_drop_queries_probability?: number;

  /**
   * Ignore MVs with dropped target table during pushing to views
   */
  ignore_materialized_views_with_dropped_target_table?: boolean;

  /**
   * Ignore ON CLUSTER clause for replicated access entities management queries.
   */
  ignore_on_cluster_for_replicated_access_entities_queries?: boolean;

  /**
   * Ignore ON CLUSTER clause for replicated named collections management queries.
   */
  ignore_on_cluster_for_replicated_named_collections_queries?: boolean;

  /**
   * Ignore ON CLUSTER clause for replicated UDF management queries.
   */
  ignore_on_cluster_for_replicated_udf_queries?: boolean;

  /**
   * Allow writing simple SELECT queries without the leading SELECT keyword, which makes it simple for calculator-style usage, e.g. `1 + 2` becomes a valid query. In `clickhouse-local` it is enabled by default and can be explicitly disabled.
   */
  implicit_select?: boolean;

  /**
   * If not empty, queries without FROM at the top level will read from this table instead of system.one. This is used in clickhouse-local for input data processing. The setting could be set explicitly by a user but is not intended for this type of usage. Subqueries are not affected by this setting (neither scalar, FROM, or IN subqueries). SELECTs at the top level of UNION, INTERSECT, EXCEPT chains are treated uniformly and affected by this setting, regardless of their grouping in parentheses. It is unspecified how this setting affects views and distributed queries. The setting accepts a table name (then the table is resolved from the current database) or a qualified name in the form of 'database.table'. Both database and table names have to be unquoted - only simple identifiers are allowed.
   * @since 25.6
   */
  implicit_table_at_top_level?: string;

  /**
   * If enabled and not already inside a transaction, wraps the query inside a full transaction (begin + commit or rollback)
   */
  implicit_transaction?: boolean;

  /**
   * If enabled, injects 'ORDER BY rand()' into SELECT queries without ORDER BY clause. Applied only for subquery depth = 0. Subqueries and INSERT INTO ... SELECT are not affected. If the top-level construct is UNION, 'ORDER BY rand()' is injected into all children independently. Only useful for testing and development (missing ORDER BY is a source of non-deterministic query results).
   * @since 25.11
   */
  inject_random_order_for_select_without_order_by?: boolean;

  /**
   * Enables or disables order-preserving parallel parsing of data formats. Supported only for [TabSeparated (TSV)](/interfaces/formats/TabSeparated), [TSKV](/interfaces/formats/TSKV), [CSV](/interfaces/formats/CSV) and [JSONEachRow](/interfaces/formats/JSONEachRow) formats. Possible values: - 1 — Enabled. - 0 — Disabled.
   */
  input_format_parallel_parsing?: boolean;

  /**
   * If setting is enabled, Allow materialized columns in INSERT.
   */
  insert_allow_materialized_columns?: boolean;

  /**
   * Enables or disables block deduplication of `INSERT` (for Replicated* tables). Possible values: - 0 — Disabled. - 1 — Enabled. By default, blocks inserted into replicated tables by the `INSERT` statement are deduplicated (see [Data Replication](../../engines/table-engines/mergetree-family/replication.md)). For the replicated tables by default the only 100 of the most recent blocks for each partition are deduplicated (see [replicated_deduplication_window](merge-tree-settings.md/#replicated_deduplication_window), [replicated_deduplication_window_seconds](merge-tree-settings.md/#replicated_deduplication_window_seconds)). For not replicated tables see [non_replicated_deduplication_window](merge-tree-settings.md/#non_replicated_deduplication_window).
   */
  insert_deduplicate?: boolean;

  /**
   * The setting allows a user to provide own deduplication semantic in MergeTree/ReplicatedMergeTree For example, by providing a unique value for the setting in each INSERT statement, user can avoid the same inserted data being deduplicated. Possible values: - Any string `insert_deduplication_token` is used for deduplication _only_ when not empty. For the replicated tables by default the only 100 of the most recent inserts for each partition are deduplicated (see [replicated_deduplication_window](merge-tree-settings.md/#replicated_deduplication_window), [replicated_deduplication_window_seconds](merge-tree-settings.md/#replicated_deduplication_window_seconds)). For not replicated tables see [non_replicated_deduplication_window](merge-tree-settings.md/#non_replicated_deduplication_window). :::note `insert_deduplication_token` works on a partition level (the same as `insert_deduplication` checksum). Multiple partitions can have the same `insert_deduplication_token`. ::: Example: ```sql CREATE TABLE test_table ( A Int64 ) ENGINE = MergeTree ORDER BY A SETTINGS non_replicated_deduplication_window = 100; INSERT INTO test_table SETTINGS insert_deduplication_token = 'test' VALUES (1); -- the next insert won't be deduplicated because insert_deduplication_token is different INSERT INTO test_table SETTINGS insert_deduplication_token = 'test1' VALUES (1); -- the next insert will be deduplicated because insert_deduplication_token -- is the same as one of the previous INSERT INTO test_table SETTINGS insert_deduplication_token = 'test' VALUES (2); SELECT * FROM test_table ┌─A─┐ │ 1 │ └───┘ ┌─A─┐ │ 1 │ └───┘ ```
   */
  insert_deduplication_token?: string;

  /**
   * Approximate probability of failure for a keeper request during insert. Valid value is in interval [0.0f, 1.0f]
   */
  insert_keeper_fault_injection_probability?: number;

  /**
   * 0 - random seed, otherwise the setting value
   */
  insert_keeper_fault_injection_seed?: number;

  /**
   * The setting sets the maximum number of retries for ClickHouse Keeper (or ZooKeeper) requests during insert into replicated MergeTree. Only Keeper requests which failed due to network error, Keeper session timeout, or request timeout are considered for retries. Possible values: - Positive integer. - 0 — Retries are disabled Cloud default value: `20`. Keeper request retries are done after some timeout. The timeout is controlled by the following settings: `insert_keeper_retry_initial_backoff_ms`, `insert_keeper_retry_max_backoff_ms`. The first retry is done after `insert_keeper_retry_initial_backoff_ms` timeout. The consequent timeouts will be calculated as follows: ``` timeout = min(insert_keeper_retry_max_backoff_ms, latest_timeout * 2) ``` For example, if `insert_keeper_retry_initial_backoff_ms=100`, `insert_keeper_retry_max_backoff_ms=10000` and `insert_keeper_max_retries=8` then timeouts will be `100, 200, 400, 800, 1600, 3200, 6400, 10000`. Apart from fault tolerance, the retries aim to provide a better user experience - they allow to avoid returning an error during INSERT execution if Keeper is restarted, for example, due to an upgrade.
   */
  insert_keeper_max_retries?: number;

  /**
   * Initial timeout(in milliseconds) to retry a failed Keeper request during INSERT query execution Possible values: - Positive integer. - 0 — No timeout
   */
  insert_keeper_retry_initial_backoff_ms?: number;

  /**
   * Maximum timeout (in milliseconds) to retry a failed Keeper request during INSERT query execution Possible values: - Positive integer. - 0 — Maximum timeout is not limited
   */
  insert_keeper_retry_max_backoff_ms?: number;

  /**
   * Enables or disables the insertion of [default values](/sql-reference/statements/create/table#default_values) instead of [NULL](/sql-reference/syntax#null) into columns with not [nullable](/sql-reference/data-types/nullable) data type. If column type is not nullable and this setting is disabled, then inserting `NULL` causes an exception. If column type is nullable, then `NULL` values are inserted as is, regardless of this setting. This setting is applicable to [INSERT ... SELECT](../../sql-reference/statements/insert-into.md/#inserting-the-results-of-select) queries. Note that `SELECT` subqueries may be concatenated with `UNION ALL` clause. Possible values: - 0 — Inserting `NULL` into a not nullable column causes an exception. - 1 — Default column value is inserted instead of `NULL`.
   */
  insert_null_as_default?: boolean;

  /**
   * :::note This setting is not applicable to SharedMergeTree, see [SharedMergeTree consistency](/cloud/reference/shared-merge-tree#consistency) for more information. ::: Enables the quorum writes. - If `insert_quorum < 2`, the quorum writes are disabled. - If `insert_quorum >= 2`, the quorum writes are enabled. - If `insert_quorum = 'auto'`, use majority number (`number_of_replicas / 2 + 1`) as quorum number. Quorum writes `INSERT` succeeds only when ClickHouse manages to correctly write data to the `insert_quorum` of replicas during the `insert_quorum_timeout`. If for any reason the number of replicas with successful writes does not reach the `insert_quorum`, the write is considered failed and ClickHouse will delete the inserted block from all the replicas where data has already been written. When `insert_quorum_parallel` is disabled, all replicas in the quorum are consistent, i.e. they contain data from all previous `INSERT` queries (the `INSERT` sequence is linearized). When reading data written using `insert_quorum` and `insert_quorum_parallel` is disabled, you can turn on sequential consistency for `SELECT` queries using [select_sequential_consistency](#select_sequential_consistency). ClickHouse generates an exception: - If the number of available replicas at the time of the query is less than the `insert_quorum`. - When `insert_quorum_parallel` is disabled and an attempt to write data is made when the previous block has not yet been inserted in `insert_quorum` of replicas. This situation may occur if the user tries to perform another `INSERT` query to the same table before the previous one with `insert_quorum` is completed. See also: - [insert_quorum_timeout](#insert_quorum_timeout) - [insert_quorum_parallel](#insert_quorum_parallel) - [select_sequential_consistency](#select_sequential_consistency)
   */
  insert_quorum?: string;

  /**
   * :::note This setting is not applicable to SharedMergeTree, see [SharedMergeTree consistency](/cloud/reference/shared-merge-tree#consistency) for more information. ::: Enables or disables parallelism for quorum `INSERT` queries. If enabled, additional `INSERT` queries can be sent while previous queries have not yet finished. If disabled, additional writes to the same table will be rejected. Possible values: - 0 — Disabled. - 1 — Enabled. See also: - [insert_quorum](#insert_quorum) - [insert_quorum_timeout](#insert_quorum_timeout) - [select_sequential_consistency](#select_sequential_consistency)
   */
  insert_quorum_parallel?: boolean;

  /**
   * Write to a quorum timeout in milliseconds. If the timeout has passed and no write has taken place yet, ClickHouse will generate an exception and the client must repeat the query to write the same block to the same or any other replica. See also: - [insert_quorum](#insert_quorum) - [insert_quorum_parallel](#insert_quorum_parallel) - [select_sequential_consistency](#select_sequential_consistency)
   */
  insert_quorum_timeout?: number;

  /**
   * Enables or disables block deduplication of `INSERT SELECT` (for Replicated* tables). The setting overrids `insert_deduplicate` for `INSERT SELECT` queries. That setting has three possible values: - 0 — Deduplication is disabled for `INSERT SELECT` query. - 1 — Deduplication is enabled for `INSERT SELECT` query. If select result is not stable, exception is thrown. - auto — Deduplication is enabled if `insert_deduplicate` is enable and select result is stable, otherwise disabled.
   * @since 26.1
   */
  insert_select_deduplicate?: string;

  /**
   * If not `0`, specifies the shard of [Distributed](/engines/table-engines/special/distributed) table into which the data will be inserted synchronously. If `insert_shard_id` value is incorrect, the server will throw an exception. To get the number of shards on `requested_cluster`, you can check server config or use this query: ```sql SELECT uniq(shard_num) FROM system.clusters WHERE cluster = 'requested_cluster'; ``` Possible values: - 0 — Disabled. - Any number from `1` to `shards_num` of corresponding [Distributed](/engines/table-engines/special/distributed) table. **Example** Query: ```sql CREATE TABLE x AS system.numbers ENGINE = MergeTree ORDER BY number; CREATE TABLE x_dist AS x ENGINE = Distributed('test_cluster_two_shards_localhost', currentDatabase(), x); INSERT INTO x_dist SELECT * FROM numbers(5) SETTINGS insert_shard_id = 1; SELECT * FROM x_dist ORDER BY number ASC; ``` Result: ```text ┌─number─┐ │ 0 │ │ 0 │ │ 1 │ │ 1 │ │ 2 │ │ 2 │ │ 3 │ │ 3 │ │ 4 │ │ 4 │ └────────┘ ```
   */
  insert_shard_id?: number;

  /**
   * The interval in microseconds for checking whether request execution has been canceled and sending the progress.
   */
  interactive_delay?: number;

  /**
   * Set default mode in INTERSECT query. Possible values: empty string, 'ALL', 'DISTINCT'. If empty, query without mode will throw exception.
   */
  intersect_default_mode?: 'ALL' | 'DISTINCT';

  /**
   * Collect jemalloc allocation and deallocation samples in trace log.
   * @since 25.10
   */
  jemalloc_collect_profile_samples_in_trace_log?: boolean;

  /**
   * Enable jemalloc profiler for the query. Jemalloc will sample allocations and all deallocations for sampled allocations. Profiles can be flushed using SYSTEM JEMALLOC FLUSH PROFILE which can be used for allocation analysis. Samples can also be stored in system.trace_log using config jemalloc_collect_global_profile_samples_in_trace_log or with query setting jemalloc_collect_profile_samples_in_trace_log. See [Allocation Profiling](/operations/allocation-profiling)
   * @since 25.10
   */
  jemalloc_enable_profiler?: boolean;

  /**
   * Changes the behaviour of join operations with `ANY` strictness. :::note This setting applies only for `JOIN` operations with [Join](../../engines/table-engines/special/join.md) engine tables. ::: Possible values: - 0 — If the right table has more than one matching row, only the first one found is joined. - 1 — If the right table has more than one matching row, only the last one found is joined. See also: - [JOIN clause](/sql-reference/statements/select/join) - [Join table engine](../../engines/table-engines/special/join.md) - [join_default_strictness](#join_default_strictness)
   */
  join_any_take_last_row?: boolean;

  /**
   * Sets default strictness for [JOIN clauses](/sql-reference/statements/select/join). Possible values: - `ALL` — If the right table has several matching rows, ClickHouse creates a [Cartesian product](https://en.wikipedia.org/wiki/Cartesian_product) from matching rows. This is the normal `JOIN` behaviour from standard SQL. - `ANY` — If the right table has several matching rows, only the first one found is joined. If the right table has only one matching row, the results of `ANY` and `ALL` are the same. - `ASOF` — For joining sequences with an uncertain match. - `Empty string` — If `ALL` or `ANY` is not specified in the query, ClickHouse throws an exception.
   */
  join_default_strictness?: 'ALL' | 'ANY';

  /**
   * Limits the number of files allowed for parallel sorting in MergeJoin operations when they are executed on disk. The bigger the value of the setting, the more RAM is used and the less disk I/O is needed. Possible values: - Any positive integer, starting from 2.
   */
  join_on_disk_max_files_to_merge?: number;

  /**
   * The lower limit of per-key average rows in the right table to determine whether to output by row list in hash join.
   */
  join_output_by_rowlist_perkey_rows_threshold?: number;

  /**
   * Defines what action ClickHouse performs when any of the following join limits is reached: - [max_bytes_in_join](/operations/settings/settings#max_bytes_in_join) - [max_rows_in_join](/operations/settings/settings#max_rows_in_join) Possible values: - `THROW` — ClickHouse throws an exception and breaks operation. - `BREAK` — ClickHouse breaks operation and does not throw an exception. Default value: `THROW`. **See Also** - [JOIN clause](/sql-reference/statements/select/join) - [Join table engine](/engines/table-engines/special/join)
   */
  join_overflow_mode?: 'throw' | 'break';

  /**
   * Size in bytes of a bloom filter used as JOIN runtime filter (see enable_join_runtime_filters setting).
   * @since 25.11
   */
  join_runtime_bloom_filter_bytes?: number;

  /**
   * Number of hash functions in a bloom filter used as JOIN runtime filter (see enable_join_runtime_filters setting).
   * @since 25.11
   */
  join_runtime_bloom_filter_hash_functions?: number;

  /**
   * Maximum number of elements in runtime filter that are stored as is in a set, when this threshold is exceeded if switches to bloom filter.
   * @since 25.11
   */
  join_runtime_filter_exact_values_limit?: number;

  /**
   * The maximum number of rows in the right table to determine whether to rerange the right table by key in left or inner join.
   */
  join_to_sort_maximum_table_rows?: number;

  /**
   * The lower limit of per-key average rows in the right table to determine whether to rerange the right table by key in left or inner join. This setting ensures that the optimization is not applied for sparse table keys
   */
  join_to_sort_minimum_perkey_rows?: number;

  /**
   * Sets the type of [JOIN](../../sql-reference/statements/select/join.md) behaviour. When merging tables, empty cells may appear. ClickHouse fills them differently based on this setting. Possible values: - 0 — The empty cells are filled with the default value of the corresponding field type. - 1 — `JOIN` behaves the same way as in standard SQL. The type of the corresponding field is converted to [Nullable](/sql-reference/data-types/nullable), and empty cells are filled with [NULL](/sql-reference/syntax).
   */
  join_use_nulls?: boolean;

  /**
   * Allow to chunk hash join result by rows corresponding to single row from left table. This may reduce memory usage in case of row with many matches in right table, but may increase CPU usage. Note that `max_joined_block_size_rows != 0` is mandatory for this setting to have effect. The `max_joined_block_size_bytes` combined with this setting is helpful to avoid excessive memory usage in case of skewed data with some large rows having many matches in right table.
   * @since 25.11
   */
  joined_block_split_single_row?: boolean;

  /**
   * Force joined subqueries and table functions to have aliases for correct name qualification.
   */
  joined_subquery_requires_alias?: boolean;

  /**
   * Disable limit on kafka_num_consumers that depends on the number of available CPU cores.
   */
  kafka_disable_num_consumers_limit?: boolean;

  /**
   * The wait time in milliseconds for reading messages from [Kafka](/engines/table-engines/integrations/kafka) before retry. Possible values: - Positive integer. - 0 — Infinite timeout. See also: - [Apache Kafka](https://kafka.apache.org/)
   */
  kafka_max_wait_ms?: number;

  /**
   * Enforce additional checks during operations on KeeperMap. E.g. throw an exception on an insert for already existing key
   */
  keeper_map_strict_mode?: boolean;

  /**
   * Max retries for general keeper operations
   */
  keeper_max_retries?: number;

  /**
   * Initial backoff timeout for general keeper operations
   */
  keeper_retry_initial_backoff_ms?: number;

  /**
   * Max backoff timeout for general keeper operations
   */
  keeper_retry_max_backoff_ms?: number;

  /**
   * If enabled, functions 'least' and 'greatest' return NULL if one of their arguments is NULL.
   * @since 25.2
   */
  least_greatest_legacy_null_behavior?: boolean;

  /**
   * List all names of element of large tuple literals in their column names instead of hash. This settings exists only for compatibility reasons. It makes sense to set to 'true', while doing rolling update of cluster from version lower than 21.7 to higher.
   */
  legacy_column_name_of_tuple_literal?: boolean;

  /**
   * A mode of internal update query that is executed as a part of lightweight delete. Possible values: - `alter_update` - run `ALTER UPDATE` query that creates a heavyweight mutation. - `lightweight_update` - run lightweight update if possible, run `ALTER UPDATE` otherwise. - `lightweight_update_force` - run lightweight update if possible, throw otherwise.
   * @since 25.6
   */
  lightweight_delete_mode?: 'alter_update' | 'lightweight_update' | 'lightweight_update_force';

  /**
   * The same as [`mutations_sync`](#mutations_sync), but controls only execution of lightweight deletes. Possible values: | Value | Description | |-------|-------------------------------------------------------------------------------------------------------------------------------------------------------| | `0` | Mutations execute asynchronously. | | `1` | The query waits for the lightweight deletes to complete on the current server. | | `2` | The query waits for the lightweight deletes to complete on all replicas (if they exist). | | `3` | The query waits only for active replicas. Supported only for `SharedMergeTree`. For `ReplicatedMergeTree` it behaves the same as `mutations_sync = 2`.| **See Also** - [Synchronicity of ALTER Queries](../../sql-reference/statements/alter/index.md/#synchronicity-of-alter-queries) - [Mutations](../../sql-reference/statements/alter/index.md/#mutations)
   */
  lightweight_deletes_sync?: number;

  /**
   * Sets the maximum number of rows to get from the query result. It adjusts the value set by the [LIMIT](/sql-reference/statements/select/limit) clause, so that the limit, specified in the query, cannot exceed the limit, set by this setting. Possible values: - 0 — The number of rows is not limited. - Positive integer.
   */
  limit?: number;

  /**
   * Specifies the algorithm of replicas selection that is used for distributed query processing. ClickHouse supports the following algorithms of choosing replicas: - [Random](#load_balancing-random) (by default) - [Nearest hostname](#load_balancing-nearest_hostname) - [Hostname levenshtein distance](#load_balancing-hostname_levenshtein_distance) - [In order](#load_balancing-in_order) - [First or random](#load_balancing-first_or_random) - [Round robin](#load_balancing-round_robin) See also: - [distributed_replica_max_ignored_errors](#distributed_replica_max_ignored_errors) ### Random (by Default) {#load_balancing-random} ```sql load_balancing = random ``` The number of errors is counted for each replica. The query is sent to the replica with the fewest errors, and if there are several of these, to anyone of them. Disadvantages: Server proximity is not accounted for; if the replicas have different data, you will also get different data. ### Nearest Hostname {#load_balancing-nearest_hostname} ```sql load_balancing = nearest_hostname ``` The number of errors is counted for each replica. Every 5 minutes, the number of errors is integrally divided by 2. Thus, the number of errors is calculated for a recent time with exponential smoothing. If there is one replica with a minimal number of errors (i.e. errors occurred recently on the other replicas), the query is sent to it. If there are multiple replicas with the same minimal number of errors, the query is sent to the replica with a hostname that is most similar to the server's hostname in the config file (for the number of different characters in identical positions, up to the minimum length of both hostnames). For instance, example01-01-1 and example01-01-2 are different in one position, while example01-01-1 and example01-02-2 differ in two places. This method might seem primitive, but it does not require external data about network topology, and it does not compare IP addresses, which would be complicated for our IPv6 addresses. Thus, if there are equivalent replicas, the closest one by name is preferred. We can also assume that when sending a query to the same server, in the absence of failures, a distributed query will also go to the same servers. So even if different data is placed on the replicas, the query will return mostly the same results. ### Hostname levenshtein distance {#load_balancing-hostname_levenshtein_distance} ```sql load_balancing = hostname_levenshtein_distance ``` Just like `nearest_hostname`, but it compares hostname in a [levenshtein distance](https://en.wikipedia.org/wiki/Levenshtein_distance) manner. For example: ```text example-clickhouse-0-0 ample-clickhouse-0-0 1 example-clickhouse-0-0 example-clickhouse-1-10 2 example-clickhouse-0-0 example-clickhouse-12-0 3 ``` ### In Order {#load_balancing-in_order} ```sql load_balancing = in_order ``` Replicas with the same number of errors are accessed in the same order as they are specified in the configuration. This method is appropriate when you know exactly which replica is preferable. ### First or Random {#load_balancing-first_or_random} ```sql load_balancing = first_or_random ``` This algorithm chooses the first replica in the set or a random replica if the first is unavailable. It's effective in cross-replication topology setups, but useless in other configurations. The `first_or_random` algorithm solves the problem of the `in_order` algorithm. With `in_order`, if one replica goes down, the next one gets a double load while the remaining replicas handle the usual amount of traffic. When using the `first_or_random` algorithm, the load is evenly distributed among replicas that are still available. It's possible to explicitly define what the first replica is by using the setting `load_balancing_first_offset`. This gives more control to rebalance query workloads among replicas. ### Round Robin {#load_balancing-round_robin} ```sql load_balancing = round_robin ``` This algorithm uses a round-robin policy across replicas with the same number of errors (only the queries with `round_robin` policy is accounted).
   */
  load_balancing?: 'random' | 'nearest_hostname' | 'hostname_levenshtein_distance' | 'in_order' | 'first_or_random' | 'round_robin';

  /**
   * Which replica to preferably send a query when FIRST_OR_RANDOM load balancing strategy is used.
   */
  load_balancing_first_offset?: number;

  /**
   * Load MergeTree marks asynchronously
   */
  load_marks_asynchronously?: boolean;

  /**
   * Method of reading data from local filesystem, one of: read, pread, mmap, io_uring, pread_threadpool. The 'io_uring' method is experimental and does not work for Log, TinyLog, StripeLog, File, Set and Join, and other tables with append-able files in presence of concurrent reads and writes. If you read various articles about 'io_uring' on the Internet, don't be blinded by them. It is not a better method of reading files, unless the case of a large amount of small IO requests, which is not the case in ClickHouse. There are no reasons to enable 'io_uring'.
   */
  local_filesystem_read_method?: string;

  /**
   * Should use prefetching when reading data from local filesystem.
   */
  local_filesystem_read_prefetch?: boolean;

  /**
   * Defines how many seconds a locking request waits before failing. Locking timeout is used to protect from deadlocks while executing read/write operations with tables. When the timeout expires and the locking request fails, the ClickHouse server throws an exception "Locking attempt timed out! Possible deadlock avoided. Client should retry." with error code `DEADLOCK_AVOIDED`. Possible values: - Positive integer (in seconds). - 0 — No locking timeout.
   */
  lock_acquire_timeout?: number;

  /**
   * Specifies the value for the `log_comment` field of the [system.query_log](../system-tables/query_log.md) table and comment text for the server log. It can be used to improve the readability of server logs. Additionally, it helps to select queries related to the test from the `system.query_log` after running [clickhouse-test](../../development/tests.md). Possible values: - Any string no longer than [max_query_size](#max_query_size). If the max_query_size is exceeded, the server throws an exception. **Example** Query: ```sql SET log_comment = 'log_comment test', log_queries = 1; SELECT 1; SYSTEM FLUSH LOGS; SELECT type, query FROM system.query_log WHERE log_comment = 'log_comment test' AND event_date >= yesterday() ORDER BY event_time DESC LIMIT 2; ``` Result: ```text ┌─type────────┬─query─────┐ │ QueryStart │ SELECT 1; │ │ QueryFinish │ SELECT 1; │ └─────────────┴───────────┘ ```
   */
  log_comment?: string;

  /**
   * Allows to log formatted queries to the [system.query_log](../../operations/system-tables/query_log.md) system table (populates `formatted_query` column in the [system.query_log](../../operations/system-tables/query_log.md)). Possible values: - 0 — Formatted queries are not logged in the system table. - 1 — Formatted queries are logged in the system table.
   */
  log_formatted_queries?: boolean;

  /**
   * Write time that processor spent during execution/waiting for data to `system.processors_profile_log` table. See also: - [`system.processors_profile_log`](../../operations/system-tables/processors_profile_log.md) - [`EXPLAIN PIPELINE`](../../sql-reference/statements/explain.md/#explain-pipeline)
   */
  log_processors_profiles?: boolean;

  /**
   * Log query performance statistics into the query_log, query_thread_log and query_views_log.
   */
  log_profile_events?: boolean;

  /**
   * Setting up query logging. Queries sent to ClickHouse with this setup are logged according to the rules in the [query_log](../../operations/server-configuration-parameters/settings.md/#query_log) server configuration parameter. Example: ```text log_queries=1 ```
   */
  log_queries?: boolean;

  /**
   * If query length is greater than a specified threshold (in bytes), then cut query when writing to query log. Also limit the length of printed query in ordinary text log.
   */
  log_queries_cut_to_length?: number;

  /**
   * If enabled (non-zero), queries faster than the value of this setting will not be logged (you can think about this as a `long_query_time` for [MySQL Slow Query Log](https://dev.mysql.com/doc/refman/5.7/slow-query-log.html)), and this basically means that you will not find them in the following tables: - `system.query_log` - `system.query_thread_log` Only the queries with the following type will get to the log: - `QUERY_FINISH` - `EXCEPTION_WHILE_PROCESSING` - Type: milliseconds - Default value: 0 (any query)
   */
  log_queries_min_query_duration_ms?: number;

  /**
   * `query_log` minimal type to log. Possible values: - `QUERY_START` (`=1`) - `QUERY_FINISH` (`=2`) - `EXCEPTION_BEFORE_START` (`=3`) - `EXCEPTION_WHILE_PROCESSING` (`=4`) Can be used to limit which entities will go to `query_log`, say you are interested only in errors, then you can use `EXCEPTION_WHILE_PROCESSING`: ```text log_queries_min_type='EXCEPTION_WHILE_PROCESSING' ```
   */
  log_queries_min_type?: string;

  /**
   * Allows a user to write to [query_log](../../operations/system-tables/query_log.md), [query_thread_log](../../operations/system-tables/query_thread_log.md), and [query_views_log](../../operations/system-tables/query_views_log.md) system tables only a sample of queries selected randomly with the specified probability. It helps to reduce the load with a large volume of queries in a second. Possible values: - 0 — Queries are not logged in the system tables. - Positive floating-point number in the range [0..1]. For example, if the setting value is `0.5`, about half of the queries are logged in the system tables. - 1 — All queries are logged in the system tables.
   */
  log_queries_probability?: number;

  /**
   * Log query settings into the query_log and OpenTelemetry span log.
   */
  log_query_settings?: boolean;

  /**
   * Setting up query threads logging. Query threads log into the [system.query_thread_log](../../operations/system-tables/query_thread_log.md) table. This setting has effect only when [log_queries](#log_queries) is true. Queries' threads run by ClickHouse with this setup are logged according to the rules in the [query_thread_log](/operations/server-configuration-parameters/settings#query_thread_log) server configuration parameter. Possible values: - 0 — Disabled. - 1 — Enabled. **Example** ```text log_query_threads=1 ```
   */
  log_query_threads?: boolean;

  /**
   * Setting up query views logging. When a query run by ClickHouse with this setting enabled has associated views (materialized or live views), they are logged in the [query_views_log](/operations/server-configuration-parameters/settings#query_views_log) server configuration parameter. Example: ```text log_query_views=1 ```
   */
  log_query_views?: boolean;

  /**
   * Allows or restricts using the [LowCardinality](../../sql-reference/data-types/lowcardinality.md) data type with the [Native](/interfaces/formats/Native) format. If usage of `LowCardinality` is restricted, ClickHouse server converts `LowCardinality`-columns to ordinary ones for `SELECT` queries, and convert ordinary columns to `LowCardinality`-columns for `INSERT` queries. This setting is required mainly for third-party clients which do not support `LowCardinality` data type. Possible values: - 1 — Usage of `LowCardinality` is not restricted. - 0 — Usage of `LowCardinality` is restricted.
   */
  low_cardinality_allow_in_native_format?: boolean;

  /**
   * Sets a maximum size in rows of a shared global dictionary for the [LowCardinality](../../sql-reference/data-types/lowcardinality.md) data type that can be written to a storage file system. This setting prevents issues with RAM in case of unlimited dictionary growth. All the data that can't be encoded due to maximum dictionary size limitation ClickHouse writes in an ordinary method. Possible values: - Any positive integer.
   */
  low_cardinality_max_dictionary_size?: number;

  /**
   * Turns on or turns off using of single dictionary for the data part. By default, the ClickHouse server monitors the size of dictionaries and if a dictionary overflows then the server starts to write the next one. To prohibit creating several dictionaries set `low_cardinality_use_single_dictionary_for_part = 1`. Possible values: - 1 — Creating several dictionaries for the data part is prohibited. - 0 — Creating several dictionaries for the data part is not prohibited.
   */
  low_cardinality_use_single_dictionary_for_part?: boolean;

  /**
   * When the query prioritization mechanism is employed (see setting `priority`), low-priority queries wait for higher-priority queries to finish. This setting specifies the duration of waiting.
   * @since 25.5
   */
  low_priority_query_wait_time_ms?: number;

  /**
   * Make distributed query plan.
   * @since 25.6
   */
  make_distributed_plan?: boolean;

  /**
   * If INSERTs build and store skip indexes. If disabled, skip indexes will only be built and stored [during merges](merge-tree-settings.md/#materialize_skip_indexes_on_merge) or by explicit [MATERIALIZE INDEX](/sql-reference/statements/alter/skipping-index.md/#materialize-index). See also [exclude_materialize_skip_indexes_on_insert](#exclude_materialize_skip_indexes_on_insert).
   */
  materialize_skip_indexes_on_insert?: boolean;

  /**
   * If INSERTs build and insert statistics. If disabled, statistics will be build and stored during merges or by explicit MATERIALIZE STATISTICS
   */
  materialize_statistics_on_insert?: boolean;

  /**
   * Apply TTL for old data, after ALTER MODIFY TTL query
   */
  materialize_ttl_after_modify?: boolean;

  /**
   * Allows to ignore errors for MATERIALIZED VIEW, and deliver original block to the table regardless of MVs
   */
  materialized_views_ignore_errors?: boolean;

  /**
   * Squash inserts to materialized views destination table of a single INSERT query from parallel inserts to reduce amount of generated parts. If set to false and `parallel_view_processing` is enabled, INSERT query will generate part in the destination table for each `max_insert_thread`.
   * @since 25.11
   */
  materialized_views_squash_parallel_inserts?: boolean;

  /**
   * Maximum number of analyses performed by interpreter.
   */
  max_analyze_depth?: number;

  /**
   * The maximum nesting depth of a query syntactic tree. If exceeded, an exception is thrown. :::note At this time, it isn't checked during parsing, but only after parsing the query. This means that a syntactic tree that is too deep can be created during parsing, but the query will fail. :::
   */
  max_ast_depth?: number;

  /**
   * The maximum number of elements in a query syntactic tree. If exceeded, an exception is thrown. :::note At this time, it isn't checked during parsing, but only after parsing the query. This means that a syntactic tree that is too deep can be created during parsing, but the query will fail. :::
   */
  max_ast_elements?: number;

  /**
   * The limit on the number of series created by the `generateSerialID` function. As each series represents a node in Keeper, it is recommended to have no more than a couple of millions of them.
   * @since 25.2
   */
  max_autoincrement_series?: number;

  /**
   * The maximum read speed in bytes per second for particular backup on server. Zero means unlimited.
   */
  max_backup_bandwidth?: number;

  /**
   * In ClickHouse, data is processed by blocks, which are sets of column parts. The internal processing cycles for a single block are efficient but there are noticeable costs when processing each block. The `max_block_size` setting indicates the recommended maximum number of rows to include in a single block when loading data from tables. Blocks the size of `max_block_size` are not always loaded from the table: if ClickHouse determines that less data needs to be retrieved, a smaller block is processed. The block size should not be too small to avoid noticeable costs when processing each block. It should also not be too large to ensure that queries with a LIMIT clause execute quickly after processing the first block. When setting `max_block_size`, the goal should be to avoid consuming too much memory when extracting a large number of columns in multiple threads and to preserve at least some cache locality.
   */
  max_block_size?: number;

  /**
   * Cloud default value: half the memory amount per replica. Enables or disables execution of `GROUP BY` clauses in external memory. (See [GROUP BY in external memory](/sql-reference/statements/select/group-by#group-by-in-external-memory)) Possible values: - Maximum volume of RAM (in bytes) that can be used by the single [GROUP BY](/sql-reference/statements/select/group-by) operation. - `0` — `GROUP BY` in external memory disabled. :::note If memory usage during GROUP BY operations is exceeding this threshold in bytes, activate the 'external aggregation' mode (spill data to disk). The recommended value is half of the available system memory. :::
   */
  max_bytes_before_external_group_by?: number;

  /**
   * Cloud default value: half the memory amount per replica. Enables or disables execution of `ORDER BY` clauses in external memory. See [ORDER BY Implementation Details](../../sql-reference/statements/select/order-by.md#implementation-details) If memory usage during ORDER BY operation exceeds this threshold in bytes, the 'external sorting' mode (spill data to disk) is activated. Possible values: - Maximum volume of RAM (in bytes) that can be used by the single [ORDER BY](../../sql-reference/statements/select/order-by.md) operation. The recommended value is half of available system memory - `0` — `ORDER BY` in external memory disabled.
   */
  max_bytes_before_external_sort?: number;

  /**
   * In case of ORDER BY with LIMIT, when memory usage is higher than specified threshold, perform additional steps of merging blocks before final merge to keep just top LIMIT rows.
   */
  max_bytes_before_remerge_sort?: number;

  /**
   * The maximum number of bytes of the state (in uncompressed bytes) in memory, which is used by a hash table when using DISTINCT.
   */
  max_bytes_in_distinct?: number;

  /**
   * The maximum size in number of bytes of the hash table used when joining tables. This setting applies to [SELECT ... JOIN](/sql-reference/statements/select/join) operations and the [Join table engine](/engines/table-engines/special/join). If the query contains joins, ClickHouse checks this setting for every intermediate result. ClickHouse can proceed with different actions when the limit is reached. Use the [join_overflow_mode](/operations/settings/settings#join_overflow_mode) settings to choose the action. Possible values: - Positive integer. - 0 — Memory control is disabled.
   */
  max_bytes_in_join?: number;

  /**
   * The maximum number of bytes (of uncompressed data) used by a set in the IN clause created from a subquery.
   */
  max_bytes_in_set?: number;

  /**
   * The ratio of available memory that is allowed for `GROUP BY`. Once reached, external memory is used for aggregation. For example, if set to `0.6`, `GROUP BY` will allow using 60% of the available memory (to server/user/merges) at the beginning of the execution, after that, it will start using external aggregation.
   * @since 25.1
   */
  max_bytes_ratio_before_external_group_by?: string;

  /**
   * The ratio of available memory that is allowed for `ORDER BY`. Once reached, external sort is used. For example, if set to `0.6`, `ORDER BY` will allow using `60%` of available memory (to server/user/merges) at the beginning of the execution, after that, it will start using external sort. Note, that `max_bytes_before_external_sort` is still respected, spilling to disk will be done only if the sorting block is bigger then `max_bytes_before_external_sort`.
   * @since 25.1
   */
  max_bytes_ratio_before_external_sort?: string;

  /**
   * The maximum number of bytes (of uncompressed data) that can be read from a table when running a query. The restriction is checked for each processed chunk of data, applied only to the deepest table expression and when reading from a remote server, checked only on the remote server.
   */
  max_bytes_to_read?: number;

  /**
   * The maximum number of bytes (of uncompressed data) that can be read from a local table on a leaf node when running a distributed query. While distributed queries can issue a multiple sub-queries to each shard (leaf) - this limit will be checked only on the read stage on the leaf nodes and will be ignored on the merging of results stage on the root node. For example, a cluster consists of 2 shards and each shard contains a table with 100 bytes of data. A distributed query which is supposed to read all the data from both tables with setting `max_bytes_to_read=150` will fail as in total it will be 200 bytes. A query with `max_bytes_to_read_leaf=150` will succeed since leaf nodes will read 100 bytes at max. The restriction is checked for each processed chunk of data. :::note This setting is unstable with `prefer_localhost_replica=1`. :::
   */
  max_bytes_to_read_leaf?: number;

  /**
   * The maximum number of bytes before sorting. If more than the specified amount of uncompressed bytes have to be processed for ORDER BY operation, the behavior will be determined by the `sort_overflow_mode` which by default is set to `throw`.
   */
  max_bytes_to_sort?: number;

  /**
   * The maximum number of bytes (uncompressed data) that can be passed to a remote server or saved in a temporary table when the GLOBAL IN/JOIN section is executed.
   */
  max_bytes_to_transfer?: number;

  /**
   * The maximum number of columns that can be read from a table in a single query. If a query requires reading more than the specified number of columns, an exception is thrown. :::tip This setting is useful for preventing overly complex queries. ::: `0` value means unlimited.
   */
  max_columns_to_read?: number;

  /**
   * The maximum size of blocks of uncompressed data before compressing for writing to a table. By default, 1,048,576 (1 MiB). Specifying a smaller block size generally leads to slightly reduced compression ratio, the compression and decompression speed increases slightly due to cache locality, and memory consumption is reduced. :::note This is an expert-level setting, and you shouldn't change it if you're just getting started with ClickHouse. ::: Don't confuse blocks for compression (a chunk of memory consisting of bytes) with blocks for query processing (a set of rows from a table).
   */
  max_compress_block_size?: number;

  /**
   * Throw exception if the value of this setting is less or equal than the current number of simultaneously processed queries. Example: `max_concurrent_queries_for_all_users` can be set to 99 for all users and database administrator can set it to 100 for itself to run queries for investigation even when the server is overloaded. Modifying the setting for one query or user does not affect other queries. Possible values: - Positive integer. - 0 — No limit. **Example** ```xml <max_concurrent_queries_for_all_users>99</max_concurrent_queries_for_all_users> ``` **See Also** - [max_concurrent_queries](/operations/server-configuration-parameters/settings#max_concurrent_queries)
   */
  max_concurrent_queries_for_all_users?: number;

  /**
   * The maximum number of simultaneously processed queries per user. Possible values: - Positive integer. - 0 — No limit. **Example** ```xml <max_concurrent_queries_for_user>5</max_concurrent_queries_for_user> ```
   */
  max_concurrent_queries_for_user?: number;

  /**
   * The maximum number of simultaneous connections with remote servers for distributed processing of a single query to a single Distributed table. We recommend setting a value no less than the number of servers in the cluster. The following parameters are only used when creating Distributed tables (and when launching a server), so there is no reason to change them at runtime.
   */
  max_distributed_connections?: number;

  /**
   * Limits the maximum depth of recursive queries for [Distributed](../../engines/table-engines/special/distributed.md) tables. If the value is exceeded, the server throws an exception. Possible values: - Positive integer. - 0 — Unlimited depth.
   */
  max_distributed_depth?: number;

  /**
   * The maximal size of buffer for parallel downloading (e.g. for URL engine) per each thread.
   */
  max_download_buffer_size?: number;

  /**
   * The maximum number of threads to download data (e.g. for URL engine).
   */
  max_download_threads?: number;

  /**
   * Maximum query estimate execution time in seconds. Checked on every data block when [`timeout_before_checking_execution_speed`](/operations/settings/settings#timeout_before_checking_execution_speed) expires.
   */
  max_estimated_execution_time?: number;

  /**
   * The maximum number of execution rows per second. Checked on every data block when [`timeout_before_checking_execution_speed`](/operations/settings/settings#timeout_before_checking_execution_speed) expires. If the execution speed is high, the execution speed will be reduced.
   */
  max_execution_speed?: number;

  /**
   * The maximum number of execution bytes per second. Checked on every data block when [`timeout_before_checking_execution_speed`](/operations/settings/settings#timeout_before_checking_execution_speed) expires. If the execution speed is high, the execution speed will be reduced.
   */
  max_execution_speed_bytes?: number;

  /**
   * The maximum query execution time in seconds. The `max_execution_time` parameter can be a bit tricky to understand. It operates based on interpolation relative to the current query execution speed (this behaviour is controlled by [`timeout_before_checking_execution_speed`](/operations/settings/settings#timeout_before_checking_execution_speed)). ClickHouse will interrupt a query if the projected execution time exceeds the specified `max_execution_time`. By default, the `timeout_before_checking_execution_speed` is set to 10 seconds. This means that after 10 seconds of query execution, ClickHouse will begin estimating the total execution time. If, for example, `max_execution_time` is set to 3600 seconds (1 hour), ClickHouse will terminate the query if the estimated time exceeds this 3600-second limit. If you set `timeout_before_checking_execution_speed` to 0, ClickHouse will use the clock time as the basis for `max_execution_time`. If query runtime exceeds the specified number of seconds, the behavior will be determined by the 'timeout_overflow_mode', which by default is set to `throw`. :::note The timeout is checked and the query can stop only in designated places during data processing. It currently cannot stop during merging of aggregation states or during query analysis, and the actual run time will be higher than the value of this setting. :::
   */
  max_execution_time?: number;

  /**
   * Similar semantically to [`max_execution_time`](#max_execution_time) but only applied on leaf nodes for distributed or remote queries. For example, if we want to limit the execution time on a leaf node to `10s` but have no limit on the initial node, instead of having `max_execution_time` in the nested subquery settings: ```sql SELECT count() FROM cluster(cluster, view(SELECT * FROM t SETTINGS max_execution_time = 10)); ``` We can use `max_execution_time_leaf` as the query settings: ```sql SELECT count() FROM cluster(cluster, view(SELECT * FROM t)) SETTINGS max_execution_time_leaf = 10; ```
   */
  max_execution_time_leaf?: number;

  /**
   * Maximum size of query syntax tree in number of nodes after expansion of aliases and the asterisk.
   */
  max_expanded_ast_elements?: number;

  /**
   * Amount of retries while fetching partition from another host.
   */
  max_fetch_partition_retries_count?: number;

  /**
   * Sets the maximum number of parallel threads for the `SELECT` query data read phase with the [FINAL](/sql-reference/statements/select/from#final-modifier) modifier. Possible values: - Positive integer. - 0 or 1 — Disabled. `SELECT` queries are executed in a single thread.
   */
  max_final_threads?: number;

  /**
   * Max number of HTTP GET redirects hops allowed. Ensures additional security measures are in place to prevent a malicious server from redirecting your requests to unexpected services.nnIt is the case when an external server redirects to another address, but that address appears to be internal to the company's infrastructure, and by sending an HTTP request to an internal server, you could request an internal API from the internal network, bypassing the auth, or even query other services, such as Redis or Memcached. When you don't have an internal infrastructure (including something running on your localhost), or you trust the server, it is safe to allow redirects. Although keep in mind, that if the URL uses HTTP instead of HTTPS, and you will have to trust not only the remote server but also your ISP and every network in the middle.
   */
  max_http_get_redirects?: number;

  /**
   * Defines the maximum length for each regular expression in the [hyperscan multi-match functions](/sql-reference/functions/string-search-functions#multiMatchAny). Possible values: - Positive integer. - 0 - The length is not limited. **Example** Query: ```sql SELECT multiMatchAny('abcd', ['ab','bcd','c','d']) SETTINGS max_hyperscan_regexp_length = 3; ``` Result: ```text ┌─multiMatchAny('abcd', ['ab', 'bcd', 'c', 'd'])─┐ │ 1 │ └────────────────────────────────────────────────┘ ``` Query: ```sql SELECT multiMatchAny('abcd', ['ab','bcd','c','d']) SETTINGS max_hyperscan_regexp_length = 2; ``` Result: ```text Exception: Regexp length too large. ``` **See Also** - [max_hyperscan_regexp_total_length](#max_hyperscan_regexp_total_length)
   */
  max_hyperscan_regexp_length?: number;

  /**
   * Sets the maximum length total of all regular expressions in each [hyperscan multi-match function](/sql-reference/functions/string-search-functions#multiMatchAny). Possible values: - Positive integer. - 0 - The length is not limited. **Example** Query: ```sql SELECT multiMatchAny('abcd', ['a','b','c','d']) SETTINGS max_hyperscan_regexp_total_length = 5; ``` Result: ```text ┌─multiMatchAny('abcd', ['a', 'b', 'c', 'd'])─┐ │ 1 │ └─────────────────────────────────────────────┘ ``` Query: ```sql SELECT multiMatchAny('abcd', ['ab','bc','c','d']) SETTINGS max_hyperscan_regexp_total_length = 5; ``` Result: ```text Exception: Total regexp lengths too large. ``` **See Also** - [max_hyperscan_regexp_length](#max_hyperscan_regexp_length)
   */
  max_hyperscan_regexp_total_length?: number;

  /**
   * The size of blocks (in a count of rows) to form for insertion into a table. This setting only applies in cases when the server forms the blocks. For example, for an INSERT via the HTTP interface, the server parses the data format and forms blocks of the specified size. But when using clickhouse-client, the client parses the data itself, and the 'max_insert_block_size' setting on the server does not affect the size of the inserted blocks. The setting also does not have a purpose when using INSERT SELECT, since data is inserted using the same blocks that are formed after SELECT. The default is slightly more than `max_block_size`. The reason for this is that certain table engines (`*MergeTree`) form a data part on the disk for each inserted block, which is a fairly large entity. Similarly, `*MergeTree` tables sort data during insertion, and a large enough block size allow sorting more data in RAM.
   */
  max_insert_block_size?: number;

  /**
   * The maximum number of streams (columns) to delay final part flush. Default - auto (100 in case of underlying storage supports parallel write, for example S3 and disabled otherwise)
   */
  max_insert_delayed_streams_for_parallel_write?: number;

  /**
   * The maximum number of threads to execute the `INSERT SELECT` query. Possible values: - 0 (or 1) — `INSERT SELECT` no parallel execution. - Positive integer. Bigger than 1. Cloud default value: - `1` for nodes with 8 GiB memory - `2` for nodes with 16 GiB memory - `4` for larger nodes Parallel `INSERT SELECT` has effect only if the `SELECT` part is executed in parallel, see [`max_threads`](#max_threads) setting. Higher values will lead to higher memory usage.
   */
  max_insert_threads?: number;

  /**
   * Maximum block size in bytes for JOIN result (if join algorithm supports it). 0 means unlimited.
   * @since 25.9
   */
  max_joined_block_size_bytes?: number;

  /**
   * Maximum block size for JOIN result (if join algorithm supports it). 0 means unlimited.
   */
  max_joined_block_size_rows?: number;

  /**
   * SELECT queries with LIMIT bigger than this setting cannot use vector similarity indices. Helps to prevent memory overflows in vector similarity indices.
   * @since 25.6
   */
  max_limit_for_vector_search_queries?: number;

  /**
   * The maximum speed of local reads in bytes per second.
   */
  max_local_read_bandwidth?: number;

  /**
   * The maximum speed of local writes in bytes per second.
   */
  max_local_write_bandwidth?: number;

  /**
   * Cloud default value: depends on the amount of RAM on the replica. The maximum amount of RAM to use for running a query on a single server. A value of `0` means unlimited. This setting does not consider the volume of available memory or the total volume of memory on the machine. The restriction applies to a single query within a single server. You can use `SHOW PROCESSLIST` to see the current memory consumption for each query. Peak memory consumption is tracked for each query and written to the log. Memory usage is not fully tracked for states of the following aggregate functions from `String` and `Array` arguments: - `min` - `max` - `any` - `anyLast` - `argMin` - `argMax` Memory consumption is also restricted by the parameters [`max_memory_usage_for_user`](/operations/settings/settings#max_memory_usage_for_user) and [`max_server_memory_usage`](/operations/server-configuration-parameters/settings#max_server_memory_usage).
   */
  max_memory_usage?: number;

  /**
   * The maximum amount of RAM to use for running a user's queries on a single server. Zero means unlimited. By default, the amount is not restricted (`max_memory_usage_for_user = 0`). Also see the description of [`max_memory_usage`](/operations/settings/settings#max_memory_usage). For example if you want to set `max_memory_usage_for_user` to 1000 bytes for a user named `clickhouse_read`, you can use the statement ```sql ALTER USER clickhouse_read SETTINGS max_memory_usage_for_user = 1000; ``` You can verify it worked by logging out of your client, logging back in, then use the `getSetting` function: ```sql SELECT getSetting('max_memory_usage_for_user'); ```
   */
  max_memory_usage_for_user?: number;

  /**
   * Limits the speed of the data exchange over the network in bytes per second. This setting applies to every query. Possible values: - Positive integer. - 0 — Bandwidth control is disabled.
   */
  max_network_bandwidth?: number;

  /**
   * Limits the speed that data is exchanged at over the network in bytes per second. This setting applies to all concurrently running queries on the server. Possible values: - Positive integer. - 0 — Control of the data speed is disabled.
   */
  max_network_bandwidth_for_all_users?: number;

  /**
   * Limits the speed of the data exchange over the network in bytes per second. This setting applies to all concurrently running queries performed by a single user. Possible values: - Positive integer. - 0 — Control of the data speed is disabled.
   */
  max_network_bandwidth_for_user?: number;

  /**
   * Limits the data volume (in bytes) that is received or transmitted over the network when executing a query. This setting applies to every individual query. Possible values: - Positive integer. - 0 — Data volume control is disabled.
   */
  max_network_bytes?: number;

  /**
   * Maximal number of partitions in table to apply optimization
   */
  max_number_of_partitions_for_independent_aggregation?: number;

  /**
   * The maximum number of replicas for each shard when executing a query. Possible values: - Positive integer. **Additional Info** This options will produce different results depending on the settings used. :::note This setting will produce incorrect results when joins or subqueries are involved, and all tables don't meet certain requirements. See [Distributed Subqueries and max_parallel_replicas](/operations/settings/settings#max_parallel_replicas) for more details. ::: ### Parallel processing using `SAMPLE` key A query may be processed faster if it is executed on several servers in parallel. But the query performance may degrade in the following cases: - The position of the sampling key in the partitioning key does not allow efficient range scans. - Adding a sampling key to the table makes filtering by other columns less efficient. - The sampling key is an expression that is expensive to calculate. - The cluster latency distribution has a long tail, so that querying more servers increases the query overall latency. ### Parallel processing using [parallel_replicas_custom_key](#parallel_replicas_custom_key) This setting is useful for any replicated table.
   */
  max_parallel_replicas?: number;

  /**
   * Maximum parser backtracking (how many times it tries different alternatives in the recursive descend parsing process).
   */
  max_parser_backtracks?: number;

  /**
   * Limits maximum recursion depth in the recursive descent parser. Allows controlling the stack size. Possible values: - Positive integer. - 0 — Recursion depth is unlimited.
   */
  max_parser_depth?: number;

  /**
   * The maximum number of threads to parse data in input formats that support parallel parsing. By default, it is determined automatically.
   */
  max_parsing_threads?: number;

  /**
   * Restriction on dropping partitions in query time. The value `0` means that you can drop partitions without any restrictions. Cloud default value: 1 TB. :::note This query setting overwrites its server setting equivalent, see [max_partition_size_to_drop](/operations/server-configuration-parameters/settings#max_partition_size_to_drop) :::
   */
  max_partition_size_to_drop?: number;

  /**
   * Limits the maximum number of partitions in a single inserted block and an exception is thrown if the block contains too many partitions. - Positive integer. - `0` — Unlimited number of partitions. **Details** When inserting data, ClickHouse calculates the number of partitions in the inserted block. If the number of partitions is more than `max_partitions_per_insert_block`, ClickHouse either logs a warning or throws an exception based on `throw_on_max_partitions_per_insert_block`. Exceptions have the following text: > "Too many partitions for a single INSERT block (`partitions_count` partitions, limit is " + toString(max_partitions) + "). The limit is controlled by the 'max_partitions_per_insert_block' setting. A large number of partitions is a common misconception. It will lead to severe negative performance impact, including slow server startup, slow INSERT queries and slow SELECT queries. Recommended total number of partitions for a table is under 1000..10000. Please note, that partitioning is not intended to speed up SELECT queries (ORDER BY key is sufficient to make range queries fast). Partitions are intended for data manipulation (DROP PARTITION, etc)." :::note This setting is a safety threshold because using a large number of partitions is a common misconception. :::
   */
  max_partitions_per_insert_block?: number;

  /**
   * Limits the maximum number of partitions that can be accessed in a single query. The setting value specified when the table is created can be overridden via query-level setting. Possible values: - Positive integer - `-1` - unlimited (default) :::note You can also specify the MergeTree setting [`max_partitions_to_read`](/operations/settings/settings#max_partitions_to_read) in tables' setting. :::
   */
  max_partitions_to_read?: number;

  /**
   * If the number of rows to read from the projection index is less than or equal to this threshold, ClickHouse will try to apply the projection index during query execution.
   * @since 25.12
   */
  max_projection_rows_to_use_projection_index?: number;

  /**
   * The maximum number of bytes of a query string parsed by the SQL parser. Data in the VALUES clause of INSERT queries is processed by a separate stream parser (that consumes O(1) RAM) and not affected by this restriction. :::note `max_query_size` cannot be set within an SQL query (e.g., `SELECT now() SETTINGS max_query_size=10000`) because ClickHouse needs to allocate a buffer to parse the query, and this buffer size is determined by the `max_query_size` setting, which must be configured before the query is executed. :::
   */
  max_query_size?: number;

  /**
   * The maximum size of the buffer to read from the filesystem.
   */
  max_read_buffer_size?: number;

  /**
   * The maximum size of the buffer to read from local filesystem. If set to 0 then max_read_buffer_size will be used.
   */
  max_read_buffer_size_local_fs?: number;

  /**
   * The maximum size of the buffer to read from remote filesystem. If set to 0 then max_read_buffer_size will be used.
   */
  max_read_buffer_size_remote_fs?: number;

  /**
   * Maximum limit on recursive CTE evaluation depth
   */
  max_recursive_cte_evaluation_depth?: number;

  /**
   * The maximum speed of data exchange over the network in bytes per second for read.
   */
  max_remote_read_network_bandwidth?: number;

  /**
   * The maximum speed of data exchange over the network in bytes per second for write.
   */
  max_remote_write_network_bandwidth?: number;

  /**
   * Disables lagging replicas for distributed queries. See [Replication](../../engines/table-engines/mergetree-family/replication.md). Sets the time in seconds. If a replica's lag is greater than or equal to the set value, this replica is not used. Possible values: - Positive integer. - 0 — Replica lags are not checked. To prevent the use of any replica with a non-zero lag, set this parameter to 1. Used when performing `SELECT` from a distributed table that points to replicated tables.
   */
  max_replica_delay_for_distributed_queries?: number;

  /**
   * Limits the result size in bytes (uncompressed). The query will stop after processing a block of data if the threshold is met, but it will not cut the last block of the result, therefore the result size can be larger than the threshold. **Caveats** The result size in memory is taken into account for this threshold. Even if the result size is small, it can reference larger data structures in memory, representing dictionaries of LowCardinality columns, and Arenas of AggregateFunction columns, so the threshold can be exceeded despite the small result size. :::warning The setting is fairly low level and should be used with caution :::
   */
  max_result_bytes?: number;

  /**
   * Cloud default value: `0`. Limits the number of rows in the result. Also checked for subqueries, and on remote servers when running parts of a distributed query. No limit is applied when the value is `0`. The query will stop after processing a block of data if the threshold is met, but it will not cut the last block of the result, therefore the result size can be larger than the threshold.
   */
  max_result_rows?: number;

  /**
   * Maximum size in bytes of the per-query reverse dictionary lookup cache used by the function `dictGetKeys`. The cache stores serialized key tuples per attribute value to avoid re-scanning the dictionary within the same query. When the limit is reached, entries are evicted using LRU. Set to 0 to disable caching.
   * @since 26.1
   */
  max_reverse_dictionary_lookup_cache_size_bytes?: number;

  /**
   * The maximum number of different rows when using DISTINCT.
   */
  max_rows_in_distinct?: number;

  /**
   * Limits the number of rows in the hash table that is used when joining tables. This settings applies to [SELECT ... JOIN](/sql-reference/statements/select/join) operations and the [Join](/engines/table-engines/special/join) table engine. If a query contains multiple joins, ClickHouse checks this setting for every intermediate result. ClickHouse can proceed with different actions when the limit is reached. Use the [`join_overflow_mode`](/operations/settings/settings#join_overflow_mode) setting to choose the action. Possible values: - Positive integer. - `0` — Unlimited number of rows.
   */
  max_rows_in_join?: number;

  /**
   * The maximum number of rows for a data set in the IN clause created from a subquery.
   */
  max_rows_in_set?: number;

  /**
   * Maximal size of the set to filter joined tables by each other's row sets before joining. Possible values: - 0 — Disable. - Any positive integer.
   */
  max_rows_in_set_to_optimize_join?: number;

  /**
   * The maximum number of unique keys received from aggregation. This setting lets you limit memory consumption when aggregating. If aggregation during GROUP BY is generating more than the specified number of rows (unique GROUP BY keys), the behavior will be determined by the 'group_by_overflow_mode' which by default is `throw`, but can be also switched to an approximate GROUP BY mode.
   */
  max_rows_to_group_by?: number;

  /**
   * The maximum number of rows that can be read from a table when running a query. The restriction is checked for each processed chunk of data, applied only to the deepest table expression and when reading from a remote server, checked only on the remote server.
   */
  max_rows_to_read?: number;

  /**
   * The maximum number of rows that can be read from a local table on a leaf node when running a distributed query. While distributed queries can issue multiple sub-queries to each shard (leaf) - this limit will be checked only on the read stage on the leaf nodes and ignored on the merging of results stage on the root node. For example, a cluster consists of 2 shards and each shard contains a table with 100 rows. The distributed query which is supposed to read all the data from both tables with setting `max_rows_to_read=150` will fail, as in total there will be 200 rows. A query with `max_rows_to_read_leaf=150` will succeed, since leaf nodes will read at max 100 rows. The restriction is checked for each processed chunk of data. :::note This setting is unstable with `prefer_localhost_replica=1`. :::
   */
  max_rows_to_read_leaf?: number;

  /**
   * The maximum number of rows before sorting. This allows you to limit memory consumption when sorting. If more than the specified amount of records have to be processed for the ORDER BY operation, the behavior will be determined by the `sort_overflow_mode` which by default is set to `throw`.
   */
  max_rows_to_sort?: number;

  /**
   * Maximum size (in rows) that can be passed to a remote server or saved in a temporary table when the GLOBAL IN/JOIN section is executed.
   */
  max_rows_to_transfer?: number;

  /**
   * Maximum number of simultaneous sessions per authenticated user to the ClickHouse server. Example: ```xml <profiles> <single_session_profile> <max_sessions_for_user>1</max_sessions_for_user> </single_session_profile> <two_sessions_profile> <max_sessions_for_user>2</max_sessions_for_user> </two_sessions_profile> <unlimited_sessions_profile> <max_sessions_for_user>0</max_sessions_for_user> </unlimited_sessions_profile> </profiles> <users> <!-- User Alice can connect to a ClickHouse server no more than once at a time. --> <Alice> <profile>single_session_user</profile> </Alice> <!-- User Bob can use 2 simultaneous sessions. --> <Bob> <profile>two_sessions_profile</profile> </Bob> <!-- User Charles can use arbitrarily many of simultaneous sessions. --> <Charles> <profile>unlimited_sessions_profile</profile> </Charles> </users> ``` Possible values: - Positive integer - `0` - infinite count of simultaneous sessions (default)
   */
  max_sessions_for_user?: number;

  /**
   * For how many elements it is allowed to preallocate space in all hash tables in total before aggregation
   */
  max_size_to_preallocate_for_aggregation?: number;

  /**
   * For how many elements it is allowed to preallocate space in all hash tables in total before join
   */
  max_size_to_preallocate_for_joins?: number;

  /**
   * If is not zero, limit the number of threads reading data from files in *Cluster table functions.
   * @since 26.1
   */
  max_streams_for_files_processing_in_cluster_functions?: number;

  /**
   * If is not zero, limit the number of reading streams for MergeTree table.
   */
  max_streams_for_merge_tree_reading?: number;

  /**
   * Ask more streams when reading from Merge table. Streams will be spread across tables that Merge table will use. This allows more even distribution of work across threads and is especially helpful when merged tables differ in size.
   */
  max_streams_multiplier_for_merge_tables?: number;

  /**
   * Allows you to use more sources than the number of threads - to more evenly distribute work across threads. It is assumed that this is a temporary solution since it will be possible in the future to make the number of sources equal to the number of threads, but for each source to dynamically select available work for itself.
   */
  max_streams_to_max_threads_ratio?: number;

  /**
   * If a query has more than the specified number of nested subqueries, throws an exception. :::tip This allows you to have a sanity check to protect against the users of your cluster from writing overly complex queries. :::
   */
  max_subquery_depth?: number;

  /**
   * Restriction on deleting tables in query time. The value `0` means that you can delete all tables without any restrictions. Cloud default value: 1 TB. :::note This query setting overwrites its server setting equivalent, see [max_table_size_to_drop](/operations/server-configuration-parameters/settings#max_table_size_to_drop) :::
   */
  max_table_size_to_drop?: number;

  /**
   * The maximum number of temporary columns that must be kept in RAM simultaneously when running a query, including constant columns. If a query generates more than the specified number of temporary columns in memory as a result of intermediate calculation, then an exception is thrown. :::tip This setting is useful for preventing overly complex queries. ::: `0` value means unlimited.
   */
  max_temporary_columns?: number;

  /**
   * The maximum amount of data consumed by temporary files on disk in bytes for all concurrently running queries. Possible values: - Positive integer. - `0` — unlimited (default)
   */
  max_temporary_data_on_disk_size_for_query?: number;

  /**
   * The maximum amount of data consumed by temporary files on disk in bytes for all concurrently running user queries. Possible values: - Positive integer. - `0` — unlimited (default)
   */
  max_temporary_data_on_disk_size_for_user?: number;

  /**
   * Like `max_temporary_columns`, the maximum number of temporary columns that must be kept in RAM simultaneously when running a query, but without counting constant columns. :::note Constant columns are formed fairly often when running a query, but they require approximately zero computing resources. :::
   */
  max_temporary_non_const_columns?: number;

  /**
   * The maximum number of query processing threads, excluding threads for retrieving data from remote servers (see the ['max_distributed_connections'](/operations/settings/settings#max_distributed_connections) parameter). This parameter applies to threads that perform the same stages of the query processing pipeline in parallel. For example, when reading from a table, if it is possible to evaluate expressions with functions, filter with `WHERE` and pre-aggregate for `GROUP BY` in parallel using at least 'max_threads' number of threads, then 'max_threads' are used. For queries that are completed quickly because of a LIMIT, you can set a lower 'max_threads'. For example, if the necessary number of entries are located in every block and max_threads = 8, then 8 blocks are retrieved, although it would have been enough to read just one. The smaller the `max_threads` value, the less memory is consumed. The `max_threads` setting by default matches the number of hardware threads available to ClickHouse. Without SMT (e.g. Intel HyperThreading), this corresponds to the number of CPU cores. For ClickHouse Cloud users, the default value will display as `auto(N)` where N matches the vCPU size of your service e.g. 2vCPU/8GiB, 4vCPU/16GiB etc. See the settings tab in the Cloud console for a list of all service sizes.
   */
  max_threads?: number;

  /**
   * The maximum number of threads process indices.
   */
  max_threads_for_indexes?: number;

  /**
   * Small allocations and deallocations are grouped in thread local variable and tracked or profiled only when an amount (in absolute value) becomes larger than the specified value. If the value is higher than 'memory_profiler_step' it will be effectively lowered to 'memory_profiler_step'.
   */
  max_untracked_memory?: number;

  /**
   * It represents the soft memory limit when the hard limit is reached on the global level. This value is used to compute the overcommit ratio for the query. Zero means skip the query. Read more about [memory overcommit](memory-overcommit.md).
   */
  memory_overcommit_ratio_denominator?: number;

  /**
   * It represents the soft memory limit when the hard limit is reached on the user level. This value is used to compute the overcommit ratio for the query. Zero means skip the query. Read more about [memory overcommit](memory-overcommit.md).
   */
  memory_overcommit_ratio_denominator_for_user?: number;

  /**
   * Collect random allocations of size less or equal than the specified value with probability equal to `memory_profiler_sample_probability`. 0 means disabled. You may want to set 'max_untracked_memory' to 0 to make this threshold work as expected.
   */
  memory_profiler_sample_max_allocation_size?: number;

  /**
   * Collect random allocations of size greater or equal than the specified value with probability equal to `memory_profiler_sample_probability`. 0 means disabled. You may want to set 'max_untracked_memory' to 0 to make this threshold work as expected.
   */
  memory_profiler_sample_min_allocation_size?: number;

  /**
   * Collect random allocations and deallocations and write them into system.trace_log with 'MemorySample' trace_type. The probability is for every alloc/free regardless of the size of the allocation (can be changed with `memory_profiler_sample_min_allocation_size` and `memory_profiler_sample_max_allocation_size`). Note that sampling happens only when the amount of untracked memory exceeds 'max_untracked_memory'. You may want to set 'max_untracked_memory' to 0 for extra fine-grained sampling.
   */
  memory_profiler_sample_probability?: number;

  /**
   * Sets the step of memory profiler. Whenever query memory usage becomes larger than every next step in number of bytes the memory profiler will collect the allocating stacktrace and will write it into [trace_log](/operations/system-tables/trace_log). Possible values: - A positive integer number of bytes. - 0 for turning off the memory profiler.
   */
  memory_profiler_step?: number;

  /**
   * For testing of `exception safety` - throw an exception every time you allocate memory with the specified probability.
   */
  memory_tracker_fault_probability?: number;

  /**
   * Maximum time thread will wait for memory to be freed in the case of memory overcommit on a user level. If the timeout is reached and memory is not freed, an exception is thrown. Read more about [memory overcommit](memory-overcommit.md).
   */
  memory_usage_overcommit_max_wait_microseconds?: number;

  /**
   * When creating a `Merge` table without an explicit schema or when using the `merge` table function, infer schema as a union of not more than the specified number of matching tables. If there is a larger number of tables, the schema will be inferred from the first specified number of tables.
   * @since 25.2
   */
  merge_table_max_tables_to_look_for_schema_inference?: number;

  /**
   * When searching for data, ClickHouse checks the data marks in the index file. If ClickHouse finds that required keys are in some range, it divides this range into `merge_tree_coarse_index_granularity` subranges and searches the required keys there recursively. Possible values: - Any positive even integer.
   */
  merge_tree_coarse_index_granularity?: number;

  /**
   * Only has an effect in ClickHouse Cloud. Number of granules in stripe of compact part of MergeTree tables to use multibuffer reader, which supports parallel reading and prefetch. In case of reading from remote fs using of multibuffer reader increases number of read request.
   */
  merge_tree_compact_parts_min_granules_to_multibuffer_read?: number;

  /**
   * Whether to use only prewhere columns size to determine reading task size.
   */
  merge_tree_determine_task_size_by_prewhere_columns?: boolean;

  /**
   * If ClickHouse should read more than `merge_tree_max_bytes_to_use_cache` bytes in one query, it does not use the cache of uncompressed blocks. The cache of uncompressed blocks stores data extracted for queries. ClickHouse uses this cache to speed up responses to repeated small queries. This setting protects the cache from trashing by queries that read a large amount of data. The [uncompressed_cache_size](/operations/server-configuration-parameters/settings#uncompressed_cache_size) server setting defines the size of the cache of uncompressed blocks. Possible values: - Any positive integer.
   */
  merge_tree_max_bytes_to_use_cache?: number;

  /**
   * If ClickHouse should read more than `merge_tree_max_rows_to_use_cache` rows in one query, it does not use the cache of uncompressed blocks. The cache of uncompressed blocks stores data extracted for queries. ClickHouse uses this cache to speed up responses to repeated small queries. This setting protects the cache from trashing by queries that read a large amount of data. The [uncompressed_cache_size](/operations/server-configuration-parameters/settings#uncompressed_cache_size) server setting defines the size of the cache of uncompressed blocks. Possible values: - Any positive integer.
   */
  merge_tree_max_rows_to_use_cache?: number;

  /**
   * If the number of bytes to read from one file of a [MergeTree](../../engines/table-engines/mergetree-family/mergetree.md)-engine table exceeds `merge_tree_min_bytes_for_concurrent_read`, then ClickHouse tries to concurrently read from this file in several threads. Possible value: - Positive integer.
   */
  merge_tree_min_bytes_for_concurrent_read?: number;

  /**
   * The minimum number of bytes to read from one file before [MergeTree](../../engines/table-engines/mergetree-family/mergetree.md) engine can parallelize reading, when reading from remote filesystem. We do not recommend using this setting. Possible values: - Positive integer.
   */
  merge_tree_min_bytes_for_concurrent_read_for_remote_filesystem?: number;

  /**
   * If the distance between two data blocks to be read in one file is less than `merge_tree_min_bytes_for_seek` bytes, then ClickHouse sequentially reads a range of file that contains both blocks, thus avoiding extra seek. Possible values: - Any positive integer.
   */
  merge_tree_min_bytes_for_seek?: number;

  /**
   * Min bytes to read per task.
   */
  merge_tree_min_bytes_per_task_for_remote_reading?: number;

  /**
   * Hard lower limit on the task size (even when the number of granules is low and the number of available threads is high we won't allocate smaller tasks
   */
  merge_tree_min_read_task_size?: number;

  /**
   * If the number of rows to be read from a file of a [MergeTree](../../engines/table-engines/mergetree-family/mergetree.md) table exceeds `merge_tree_min_rows_for_concurrent_read` then ClickHouse tries to perform a concurrent reading from this file on several threads. Possible values: - Positive integer.
   */
  merge_tree_min_rows_for_concurrent_read?: number;

  /**
   * The minimum number of lines to read from one file before the [MergeTree](../../engines/table-engines/mergetree-family/mergetree.md) engine can parallelize reading, when reading from remote filesystem. We do not recommend using this setting. Possible values: - Positive integer.
   */
  merge_tree_min_rows_for_concurrent_read_for_remote_filesystem?: number;

  /**
   * If the distance between two data blocks to be read in one file is less than `merge_tree_min_rows_for_seek` rows, then ClickHouse does not seek through the file but reads the data sequentially. Possible values: - Any positive integer.
   */
  merge_tree_min_rows_for_seek?: number;

  /**
   * For testing of `PartsSplitter` - split read ranges into intersecting and non intersecting every time you read from MergeTree with the specified probability.
   */
  merge_tree_read_split_ranges_into_intersecting_and_non_intersecting_injection_probability?: number;

  /**
   * Inject artificial delay (in milliseconds) when creating a storage snapshot for MergeTree tables. Used for testing and debugging purposes only. Possible values: - 0 - No delay (default) - N - Delay in milliseconds
   * @since 25.7
   */
  merge_tree_storage_snapshot_sleep_ms?: number;

  /**
   * Whether to use constant size tasks for reading from a remote table.
   */
  merge_tree_use_const_size_tasks_for_remote_reading?: boolean;

  /**
   * Enables caching of columns metadata from the file prefixes during reading from remote disks in MergeTree.
   * @since 25.3
   */
  merge_tree_use_deserialization_prefixes_cache?: boolean;

  /**
   * Enables usage of the thread pool for parallel prefixes reading in Wide parts in MergeTree. Size of that thread pool is controlled by server setting `max_prefixes_deserialization_thread_pool_size`.
   * @since 25.3
   */
  merge_tree_use_prefixes_deserialization_thread_pool?: boolean;

  /**
   * When enabled, V1 serialization version of JSON and Dynamic types will be used in MergeTree instead of V2. Changing this setting takes affect only after server restart.
   * @since 25.1
   */
  merge_tree_use_v1_object_and_dynamic_serialization?: boolean;

  /**
   * If enabled, some of the perf events will be measured throughout queries' execution.
   */
  metrics_perf_events_enabled?: boolean;

  /**
   * Comma separated list of perf metrics that will be measured throughout queries' execution. Empty means all events. See PerfEventInfo in sources for the available events.
   */
  metrics_perf_events_list?: string;

  /**
   * The minimum data volume required for using direct I/O access to the storage disk. ClickHouse uses this setting when reading data from tables. If the total storage volume of all the data to be read exceeds `min_bytes_to_use_direct_io` bytes, then ClickHouse reads the data from the storage disk with the `O_DIRECT` option. Possible values: - 0 — Direct I/O is disabled. - Positive integer.
   */
  min_bytes_to_use_direct_io?: number;

  /**
   * This is an experimental setting. Sets the minimum amount of memory for reading large files without copying data from the kernel to userspace. Recommended threshold is about 64 MB, because [mmap/munmap](https://en.wikipedia.org/wiki/Mmap) is slow. It makes sense only for large files and helps only if data reside in the page cache. Possible values: - Positive integer. - 0 — Big files read with only copying data from kernel to userspace.
   */
  min_bytes_to_use_mmap_io?: number;

  /**
   * - Type: unsigned int - Default value: 1 MiB The minimum chunk size in bytes, which each thread will parse in parallel.
   */
  min_chunk_bytes_for_parallel_parsing?: number;

  /**
   * For [MergeTree](../../engines/table-engines/mergetree-family/mergetree.md) tables. In order to reduce latency when processing queries, a block is compressed when writing the next mark if its size is at least `min_compress_block_size`. By default, 65,536. The actual size of the block, if the uncompressed data is less than `max_compress_block_size`, is no less than this value and no less than the volume of data for one mark. Let's look at an example. Assume that `index_granularity` was set to 8192 during table creation. We are writing a UInt32-type column (4 bytes per value). When writing 8192 rows, the total will be 32 KB of data. Since min_compress_block_size = 65,536, a compressed block will be formed for every two marks. We are writing a URL column with the String type (average size of 60 bytes per value). When writing 8192 rows, the average will be slightly less than 500 KB of data. Since this is more than 65,536, a compressed block will be formed for each mark. In this case, when reading data from the disk in the range of a single mark, extra data won't be decompressed. :::note This is an expert-level setting, and you shouldn't change it if you're just getting started with ClickHouse. :::
   */
  min_compress_block_size?: number;

  /**
   * The minimum number of identical aggregate expressions to start JIT-compilation. Works only if the [compile_aggregate_expressions](#compile_aggregate_expressions) setting is enabled. Possible values: - Positive integer. - 0 — Identical aggregate expressions are always JIT-compiled.
   */
  min_count_to_compile_aggregate_expression?: number;

  /**
   * Minimum count of executing same expression before it is get compiled.
   */
  min_count_to_compile_expression?: number;

  /**
   * The number of identical sort descriptions before they are JIT-compiled
   */
  min_count_to_compile_sort_description?: number;

  /**
   * Minimal execution speed in rows per second. Checked on every data block when [`timeout_before_checking_execution_speed`](/operations/settings/settings#timeout_before_checking_execution_speed) expires. If the execution speed is lower, an exception is thrown.
   */
  min_execution_speed?: number;

  /**
   * The minimum number of execution bytes per second. Checked on every data block when [`timeout_before_checking_execution_speed`](/operations/settings/settings#timeout_before_checking_execution_speed) expires. If the execution speed is lower, an exception is thrown.
   */
  min_execution_speed_bytes?: number;

  /**
   * Squash blocks passed to the external table to a specified size in bytes, if blocks are not big enough.
   */
  min_external_table_block_size_bytes?: number;

  /**
   * Squash blocks passed to external table to specified size in rows, if blocks are not big enough.
   */
  min_external_table_block_size_rows?: number;

  /**
   * Minimum free disk space bytes to perform an insert.
   */
  min_free_disk_bytes_to_perform_insert?: number;

  /**
   * Minimum free disk space ratio to perform an insert.
   */
  min_free_disk_ratio_to_perform_insert?: number;

  /**
   * The minimum disk space to keep while writing temporary data used in external sorting and aggregation.
   */
  min_free_disk_space_for_temporary_data?: number;

  /**
   * Minimal hit rate of a cache which is used for consecutive keys optimization in aggregation to keep it enabled
   */
  min_hit_rate_to_use_consecutive_keys_optimization?: number;

  /**
   * Sets the minimum number of bytes in the block which can be inserted into a table by an `INSERT` query. Smaller-sized blocks are squashed into bigger ones. Possible values: - Positive integer. - 0 — Squashing disabled.
   */
  min_insert_block_size_bytes?: number;

  /**
   * Sets the minimum number of bytes in the block which can be inserted into a table by an `INSERT` query. Smaller-sized blocks are squashed into bigger ones. This setting is applied only for blocks inserted into [materialized view](../../sql-reference/statements/create/view.md). By adjusting this setting, you control blocks squashing while pushing to materialized view and avoid excessive memory usage. Possible values: - Any positive integer. - 0 — Squashing disabled. **See also** - [min_insert_block_size_bytes](#min_insert_block_size_bytes)
   */
  min_insert_block_size_bytes_for_materialized_views?: number;

  /**
   * Sets the minimum number of rows in the block that can be inserted into a table by an `INSERT` query. Smaller-sized blocks are squashed into bigger ones. Possible values: - Positive integer. - 0 — Squashing disabled.
   */
  min_insert_block_size_rows?: number;

  /**
   * Sets the minimum number of rows in the block which can be inserted into a table by an `INSERT` query. Smaller-sized blocks are squashed into bigger ones. This setting is applied only for blocks inserted into [materialized view](../../sql-reference/statements/create/view.md). By adjusting this setting, you control blocks squashing while pushing to materialized view and avoid excessive memory usage. Possible values: - Any positive integer. - 0 — Squashing disabled. **See Also** - [min_insert_block_size_rows](#min_insert_block_size_rows)
   */
  min_insert_block_size_rows_for_materialized_views?: number;

  /**
   * Minimum block size in bytes for JOIN input and output blocks (if join algorithm supports it). Small blocks will be squashed. 0 means unlimited.
   * @since 24.12
   */
  min_joined_block_size_bytes?: number;

  /**
   * Minimum block size in rows for JOIN input and output blocks (if join algorithm supports it). Small blocks will be squashed. 0 means unlimited.
   * @since 25.8
   */
  min_joined_block_size_rows?: number;

  /**
   * Specifies the minimum number of output streams of a `Resize` or `StrictResize` processor after the split is performed during pipeline generation. If the resulting number of streams is less than this value, the split operation will not occur. ### What is a Resize Node A `Resize` node is a processor in the query pipeline that adjusts the number of data streams flowing through the pipeline. It can either increase or decrease the number of streams to balance the workload across multiple threads or processors. For example, if a query requires more parallelism, the `Resize` node can split a single stream into multiple streams. Conversely, it can merge multiple streams into fewer streams to consolidate data processing. The `Resize` node ensures that data is evenly distributed across streams, maintaining the structure of the data blocks. This helps optimize resource utilization and improve query performance. ### Why the Resize Node Needs to Be Split During pipeline execution, ExecutingGraph::Node::status_mutex of the centrally-hubbed `Resize` node is heavily contended especially in high-core-count environments, and this contention leads to: 1. Increased latency for ExecutingGraph::updateNode, directly impacting query performance. 2. Excessive CPU cycles are wasted in spin-lock contention (native_queued_spin_lock_slowpath), degrading efficiency. 3. Reduced CPU utilization, limiting parallelism and throughput. ### How the Resize Node Gets Split 1. The number of output streams is checked to ensure the split could be performed: the output streams of each split processor meet or exceed the `min_outstreams_per_resize_after_split` threshold. 2. The `Resize` node is divided into smaller `Resize` nodes with equal count of ports, each handling a subset of input and output streams. 3. Each group is processed independently, reducing the lock contention. ### Splitting Resize Node with Arbitrary Inputs/Outputs In some cases, where the inputs/outputs are indivisible by the number of split `Resize` nodes, some inputs are connected to `NullSource`s and some outputs are connected to `NullSink`s. This allows the split to occur without affecting the overall data flow. ### Purpose of the Setting The `min_outstreams_per_resize_after_split` setting ensures that the splitting of `Resize` nodes is meaningful and avoids creating too few streams, which could lead to inefficient parallel processing. By enforcing a minimum number of output streams, this setting helps maintain a balance between parallelism and overhead, optimizing query execution in scenarios involving stream splitting and merging. ### Disabling the Setting To disable the split of `Resize` nodes, set this setting to 0. This will prevent the splitting of `Resize` nodes during pipeline generation, allowing them to retain their original structure without division into smaller nodes.
   * @since 25.7
   */
  min_outstreams_per_resize_after_split?: number;

  /**
   * If the estimated number of rows to read from the table is greater than or equal to this threshold, ClickHouse will try to use the projection index during query execution.
   * @since 25.12
   */
  min_table_rows_to_use_projection_index?: number;

  /**
   * If enabled, MongoDB tables will return an error when a MongoDB query cannot be built. Otherwise, ClickHouse reads the full table and processes it locally. This option does not apply when 'allow_experimental_analyzer=0'.
   */
  mongodb_throw_on_unsupported_query?: boolean;

  /**
   * Move all viable conditions from WHERE to PREWHERE
   */
  move_all_conditions_to_prewhere?: boolean;

  /**
   * Move PREWHERE conditions containing primary key columns to the end of AND chain. It is likely that these conditions are taken into account during primary key analysis and thus will not contribute a lot to PREWHERE filtering.
   */
  move_primary_key_columns_to_end_of_prewhere?: boolean;

  /**
   * Do not add aliases to top level expression list on multiple joins rewrite
   */
  multiple_joins_try_to_keep_original_names?: boolean;

  /**
   * If true constant nondeterministic functions (e.g. function `now()`) are executed on initiator and replaced to literals in `UPDATE` and `DELETE` queries. It helps to keep data in sync on replicas while executing mutations with constant nondeterministic functions. Default value: `false`.
   */
  mutations_execute_nondeterministic_on_initiator?: boolean;

  /**
   * If true scalar subqueries are executed on initiator and replaced to literals in `UPDATE` and `DELETE` queries. Default value: `false`.
   */
  mutations_execute_subqueries_on_initiator?: boolean;

  /**
   * The maximum size of serialized literal in bytes to replace in `UPDATE` and `DELETE` queries. Takes effect only if at least one the two settings above is enabled. Default value: 16384 (16 KiB).
   */
  mutations_max_literal_size_to_replace?: number;

  /**
   * Allows to execute `ALTER TABLE ... UPDATE|DELETE|MATERIALIZE INDEX|MATERIALIZE PROJECTION|MATERIALIZE COLUMN|MATERIALIZE STATISTICS` queries ([mutations](../../sql-reference/statements/alter/index.md/#mutations)) synchronously. Possible values: | Value | Description | |-------|-------------------------------------------------------------------------------------------------------------------------------------------------------| | `0` | Mutations execute asynchronously. | | `1` | The query waits for all mutations to complete on the current server. | | `2` | The query waits for all mutations to complete on all replicas (if they exist). | | `3` | The query waits only for active replicas. Supported only for `SharedMergeTree`. For `ReplicatedMergeTree` it behaves the same as `mutations_sync = 2`.|
   */
  mutations_sync?: number;

  /**
   * Defines how MySQL types are converted to corresponding ClickHouse types. A comma separated list in any combination of `decimal`, `datetime64`, `date2Date32` or `date2String`. - `decimal`: convert `NUMERIC` and `DECIMAL` types to `Decimal` when precision allows it. - `datetime64`: convert `DATETIME` and `TIMESTAMP` types to `DateTime64` instead of `DateTime` when precision is not `0`. - `date2Date32`: convert `DATE` to `Date32` instead of `Date`. Takes precedence over `date2String`. - `date2String`: convert `DATE` to `String` instead of `Date`. Overridden by `datetime64`.
   */
  mysql_datatypes_support_level?: 'decimal' | 'datetime64' | 'date2Date32' | 'date2String';

  /**
   * When enabled, [FixedString](../../sql-reference/data-types/fixedstring.md) ClickHouse data type will be displayed as `TEXT` in [SHOW COLUMNS](../../sql-reference/statements/show.md/#show_columns). Has an effect only when the connection is made through the MySQL wire protocol. - 0 - Use `BLOB`. - 1 - Use `TEXT`.
   */
  mysql_map_fixed_string_to_text_in_show_columns?: boolean;

  /**
   * When enabled, [String](../../sql-reference/data-types/string.md) ClickHouse data type will be displayed as `TEXT` in [SHOW COLUMNS](../../sql-reference/statements/show.md/#show_columns). Has an effect only when the connection is made through the MySQL wire protocol. - 0 - Use `BLOB`. - 1 - Use `TEXT`.
   */
  mysql_map_string_to_text_in_show_columns?: boolean;

  /**
   * The maximum number of rows in MySQL batch insertion of the MySQL storage engine
   */
  mysql_max_rows_to_insert?: number;

  /**
   * The codec for compressing the client/server and server/server communication. Possible values: - `NONE` — no compression. - `LZ4` — use the LZ4 codec. - `LZ4HC` — use the LZ4HC codec. - `ZSTD` — use the ZSTD codec. **See Also** - [network_zstd_compression_level](#network_zstd_compression_level)
   */
  network_compression_method?: string;

  /**
   * Adjusts the level of ZSTD compression. Used only when [network_compression_method](#network_compression_method) is set to `ZSTD`. Possible values: - Positive integer from 1 to 15.
   */
  network_zstd_compression_level?: number;

  /**
   * Normalize function names to their canonical names
   */
  normalize_function_names?: boolean;

  /**
   * If the mutated table contains at least that many unfinished mutations, artificially slow down mutations of table. 0 - disabled
   */
  number_of_mutations_to_delay?: number;

  /**
   * If the mutated table contains at least that many unfinished mutations, throw 'Too many mutations ...' exception. 0 - disabled
   */
  number_of_mutations_to_throw?: number;

  /**
   * Connection pool size for each connection settings string in ODBC bridge.
   */
  odbc_bridge_connection_pool_size?: number;

  /**
   * Use connection pooling in ODBC bridge. If set to false, a new connection is created every time.
   */
  odbc_bridge_use_connection_pooling?: boolean;

  /**
   * Sets the number of rows to skip before starting to return rows from the query. It adjusts the offset set by the [OFFSET](/sql-reference/statements/select/offset) clause, so that these two values are summarized. Possible values: - 0 — No rows are skipped . - Positive integer. **Example** Input table: ```sql CREATE TABLE test (i UInt64) ENGINE = MergeTree() ORDER BY i; INSERT INTO test SELECT number FROM numbers(500); ``` Query: ```sql SET limit = 5; SET offset = 7; SELECT * FROM test LIMIT 10 OFFSET 100; ``` Result: ```text ┌───i─┐ │ 107 │ │ 108 │ │ 109 │ └─────┘ ```
   */
  offset?: number;

  /**
   * Sets the probability that the ClickHouse can start a trace for executed queries (if no parent [trace context](https://www.w3.org/TR/trace-context/) is supplied). Possible values: - 0 — The trace for all executed queries is disabled (if no parent trace context is supplied). - Positive floating-point number in the range [0..1]. For example, if the setting value is `0,5`, ClickHouse can start a trace on average for half of the queries. - 1 — The trace for all executed queries is enabled.
   */
  opentelemetry_start_trace_probability?: number;

  /**
   * Collect OpenTelemetry spans for workload preemptive CPU scheduling.
   * @since 25.9
   */
  opentelemetry_trace_cpu_scheduling?: boolean;

  /**
   * Collect OpenTelemetry spans for processors.
   */
  opentelemetry_trace_processors?: boolean;

  /**
   * Enables [GROUP BY](/sql-reference/statements/select/group-by) optimization in [SELECT](../../sql-reference/statements/select/index.md) queries for aggregating data in corresponding order in [MergeTree](../../engines/table-engines/mergetree-family/mergetree.md) tables. Possible values: - 0 — `GROUP BY` optimization is disabled. - 1 — `GROUP BY` optimization is enabled. **See Also** - [GROUP BY optimization](/sql-reference/statements/select/group-by#group-by-optimization-depending-on-table-sorting-key)
   */
  optimize_aggregation_in_order?: boolean;

  /**
   * Eliminates min/max/any/anyLast aggregators of GROUP BY keys in SELECT section
   */
  optimize_aggregators_of_group_by_keys?: boolean;

  /**
   * Populate constant comparison in AND chains to enhance filtering ability. Support operators `<`, `<=`, `>`, `>=`, `=` and mix of them. For example, `(a < b) AND (b < c) AND (c < 5)` would be `(a < b) AND (b < c) AND (c < 5) AND (b < 5) AND (a < 5)`.
   * @since 25.3
   */
  optimize_and_compare_chain?: boolean;

  /**
   * Use [constraints](../../sql-reference/statements/create/table.md/#constraints) in order to append index condition. The default is `false`. Possible values: - true, false
   */
  optimize_append_index?: boolean;

  /**
   * Move arithmetic operations out of aggregation functions
   */
  optimize_arithmetic_operations_in_aggregate_functions?: boolean;

  /**
   * Replace with scalar and use hash as a name for large constants (size is estimated by the name length). Possible values: - positive integer - max length of the name, - 0 — always, - negative integer - never.
   * @since 25.12
   */
  optimize_const_name_size?: number;

  /**
   * Enables or disables the optimization of counting number of rows from files in different input formats. It applies to table functions/engines `file`/`s3`/`url`/`hdfs`/`azureBlobStorage`. Possible values: - 0 — Optimization disabled. - 1 — Optimization enabled.
   */
  optimize_count_from_files?: boolean;

  /**
   * Enable DISTINCT optimization if some columns in DISTINCT form a prefix of sorting. For example, prefix of sorting key in merge tree or ORDER BY statement
   */
  optimize_distinct_in_order?: boolean;

  /**
   * Optimize `GROUP BY sharding_key` queries, by avoiding costly aggregation on the initiator server (which will reduce memory usage for the query on the initiator server). The following types of queries are supported (and all combinations of them): - `SELECT DISTINCT [..., ]sharding_key[, ...] FROM dist` - `SELECT ... FROM dist GROUP BY sharding_key[, ...]` - `SELECT ... FROM dist GROUP BY sharding_key[, ...] ORDER BY x` - `SELECT ... FROM dist GROUP BY sharding_key[, ...] LIMIT 1` - `SELECT ... FROM dist GROUP BY sharding_key[, ...] LIMIT 1 BY x` The following types of queries are not supported (support for some of them may be added later): - `SELECT ... GROUP BY sharding_key[, ...] WITH TOTALS` - `SELECT ... GROUP BY sharding_key[, ...] WITH ROLLUP` - `SELECT ... GROUP BY sharding_key[, ...] WITH CUBE` - `SELECT ... GROUP BY sharding_key[, ...] SETTINGS extremes=1` Possible values: - 0 — Disabled. - 1 — Enabled. See also: - [distributed_group_by_no_merge](#distributed_group_by_no_merge) - [distributed_push_down_limit](#distributed_push_down_limit) - [optimize_skip_unused_shards](#optimize_skip_unused_shards) :::note Right now it requires `optimize_skip_unused_shards` (the reason behind this is that one day it may be enabled by default, and it will work correctly only if data was inserted via Distributed table, i.e. data is distributed according to sharding_key). :::
   */
  optimize_distributed_group_by_sharding_key?: boolean;

  /**
   * Convert expressions like col = '' or '' = col into empty(col), and col != '' or '' != col into notEmpty(col), only when col is of String or FixedString type.
   * @since 25.11
   */
  optimize_empty_string_comparisons?: boolean;

  /**
   * Allow extracting common expressions from disjunctions in WHERE, PREWHERE, ON, HAVING and QUALIFY expressions. A logical expression like `(A AND B) OR (A AND C)` can be rewritten to `A AND (B OR C)`, which might help to utilize: - indices in simple filtering expressions - cross to inner join optimization
   * @since 25.1
   */
  optimize_extract_common_expressions?: boolean;

  /**
   * Enables or disables optimization by transforming some functions to reading subcolumns. This reduces the amount of data to read. These functions can be transformed: - [length](/sql-reference/functions/array-functions#length) to read the [size0](../../sql-reference/data-types/array.md/#array-size) subcolumn. - [empty](/sql-reference/functions/array-functions#empty) to read the [size0](../../sql-reference/data-types/array.md/#array-size) subcolumn. - [notEmpty](/sql-reference/functions/array-functions#notEmpty) to read the [size0](../../sql-reference/data-types/array.md/#array-size) subcolumn. - [isNull](/sql-reference/functions/functions-for-nulls#isNull) to read the [null](../../sql-reference/data-types/nullable.md/#finding-null) subcolumn. - [isNotNull](/sql-reference/functions/functions-for-nulls#isNotNull) to read the [null](../../sql-reference/data-types/nullable.md/#finding-null) subcolumn. - [count](/sql-reference/aggregate-functions/reference/count) to read the [null](../../sql-reference/data-types/nullable.md/#finding-null) subcolumn. - [mapKeys](/sql-reference/functions/tuple-map-functions#mapkeys) to read the [keys](/sql-reference/data-types/map#reading-subcolumns-of-map) subcolumn. - [mapValues](/sql-reference/functions/tuple-map-functions#mapvalues) to read the [values](/sql-reference/data-types/map#reading-subcolumns-of-map) subcolumn. Possible values: - 0 — Optimization disabled. - 1 — Optimization enabled.
   */
  optimize_functions_to_subcolumns?: boolean;

  /**
   * Optimize GROUP BY when all keys in block are constant
   */
  optimize_group_by_constant_keys?: boolean;

  /**
   * Eliminates functions of other keys in GROUP BY section
   */
  optimize_group_by_function_keys?: boolean;

  /**
   * Replace if(cond1, then1, if(cond2, ...)) chains to multiIf. Currently it's not beneficial for numeric types.
   */
  optimize_if_chain_to_multiif?: boolean;

  /**
   * Replaces string-type arguments in If and Transform to enum. Disabled by default cause it could make inconsistent change in distributed query that would lead to its fail.
   */
  optimize_if_transform_strings_to_enum?: boolean;

  /**
   * Replaces injective functions by it's arguments in GROUP BY section
   */
  optimize_injective_functions_in_group_by?: boolean;

  /**
   * Delete injective functions of one argument inside uniq*() functions.
   */
  optimize_injective_functions_inside_uniq?: boolean;

  /**
   * Avoid repeated inverse dictionary lookup by doing faster lookups into a precomputed set of possible key values.
   * @since 26.1
   */
  optimize_inverse_dictionary_lookup?: boolean;

  /**
   * The minimum length of the expression `expr = x1 OR ... expr = xN` for optimization
   */
  optimize_min_equality_disjunction_chain_length?: number;

  /**
   * The minimum length of the expression `expr <> x1 AND ... expr <> xN` for optimization
   */
  optimize_min_inequality_conjunction_chain_length?: number;

  /**
   * Enables or disables automatic [PREWHERE](../../sql-reference/statements/select/prewhere.md) optimization in [SELECT](../../sql-reference/statements/select/index.md) queries. Works only for [*MergeTree](../../engines/table-engines/mergetree-family/index.md) tables. Possible values: - 0 — Automatic `PREWHERE` optimization is disabled. - 1 — Automatic `PREWHERE` optimization is enabled.
   */
  optimize_move_to_prewhere?: boolean;

  /**
   * Enables or disables automatic [PREWHERE](../../sql-reference/statements/select/prewhere.md) optimization in [SELECT](../../sql-reference/statements/select/index.md) queries with [FINAL](/sql-reference/statements/select/from#final-modifier) modifier. Works only for [*MergeTree](../../engines/table-engines/mergetree-family/index.md) tables. Possible values: - 0 — Automatic `PREWHERE` optimization in `SELECT` queries with `FINAL` modifier is disabled. - 1 — Automatic `PREWHERE` optimization in `SELECT` queries with `FINAL` modifier is enabled. **See Also** - [optimize_move_to_prewhere](#optimize_move_to_prewhere) setting
   */
  optimize_move_to_prewhere_if_final?: boolean;

  /**
   * Replace 'multiIf' with only one condition to 'if'.
   */
  optimize_multiif_to_if?: boolean;

  /**
   * Rewrite aggregate functions that semantically equals to count() as count().
   */
  optimize_normalize_count_variants?: boolean;

  /**
   * Enables or disables data transformation before the insertion, as if merge was done on this block (according to table engine). Possible values: - 0 — Disabled. - 1 — Enabled. **Example** The difference between enabled and disabled: Query: ```sql SET optimize_on_insert = 1; CREATE TABLE test1 (`FirstTable` UInt32) ENGINE = ReplacingMergeTree ORDER BY FirstTable; INSERT INTO test1 SELECT number % 2 FROM numbers(5); SELECT * FROM test1; SET optimize_on_insert = 0; CREATE TABLE test2 (`SecondTable` UInt32) ENGINE = ReplacingMergeTree ORDER BY SecondTable; INSERT INTO test2 SELECT number % 2 FROM numbers(5); SELECT * FROM test2; ``` Result: ```text ┌─FirstTable─┐ │ 0 │ │ 1 │ └────────────┘ ┌─SecondTable─┐ │ 0 │ │ 0 │ │ 0 │ │ 1 │ │ 1 │ └─────────────┘ ``` Note that this setting influences [Materialized view](/sql-reference/statements/create/view#materialized-view) behaviour.
   */
  optimize_on_insert?: boolean;

  /**
   * Optimize multiple OR LIKE into multiMatchAny. This optimization should not be enabled by default, because it defies index analysis in some cases.
   */
  optimize_or_like_chain?: boolean;

  /**
   * Replace distance functions on `QBit` data type with equivalent ones that only read the columns necessary for the calculation from the storage.
   * @since 25.11
   */
  optimize_qbit_distance_function_reads?: boolean;

  /**
   * Enables [ORDER BY](/sql-reference/statements/select/order-by#optimization-of-data-reading) optimization in [SELECT](../../sql-reference/statements/select/index.md) queries for reading data from [MergeTree](../../engines/table-engines/mergetree-family/mergetree.md) tables. Possible values: - 0 — `ORDER BY` optimization is disabled. - 1 — `ORDER BY` optimization is enabled. **See Also** - [ORDER BY Clause](/sql-reference/statements/select/order-by#optimization-of-data-reading)
   */
  optimize_read_in_order?: boolean;

  /**
   * Enable ORDER BY optimization in window clause for reading data in corresponding order in MergeTree tables.
   */
  optimize_read_in_window_order?: boolean;

  /**
   * Remove functions from ORDER BY if its argument is also in ORDER BY
   */
  optimize_redundant_functions_in_order_by?: boolean;

  /**
   * If it is set to true, it will respect aliases in WHERE/GROUP BY/ORDER BY, that will help with partition pruning/secondary indexes/optimize_aggregation_in_order/optimize_read_in_order/optimize_trivial_count
   */
  optimize_respect_aliases?: boolean;

  /**
   * Rewrite aggregate functions with if expression as argument when logically equivalent. For example, `avg(if(cond, col, null))` can be rewritten to `avgOrNullIf(cond, col)`. It may improve performance. :::note Supported only with the analyzer (`enable_analyzer = 1`). :::
   */
  optimize_rewrite_aggregate_function_with_if?: boolean;

  /**
   * Rewrite arrayExists() functions to has() when logically equivalent. For example, arrayExists(x -> x = 1, arr) can be rewritten to has(arr, 1)
   */
  optimize_rewrite_array_exists_to_has?: boolean;

  /**
   * Rewrite LIKE expressions with perfect prefix or suffix (e.g. `col LIKE 'ClickHouse%'`) to startsWith or endsWith functions (e.g. `startsWith(col, 'ClickHouse')`).
   * @since 25.11
   */
  optimize_rewrite_like_perfect_affix?: boolean;

  /**
   * Rewrite regular expression related functions into simpler and more efficient forms
   * @since 25.9
   */
  optimize_rewrite_regexp_functions?: boolean;

  /**
   * Rewrite sumIf() and sum(if()) function countIf() function when logically equivalent
   */
  optimize_rewrite_sum_if_to_count_if?: boolean;

  /**
   * Enables or disables optimization for [OPTIMIZE TABLE ... FINAL](../../sql-reference/statements/optimize.md) query if there is only one part with level > 0 and it doesn't have expired TTL. - `OPTIMIZE TABLE ... FINAL SETTINGS optimize_skip_merged_partitions=1` By default, `OPTIMIZE TABLE ... FINAL` query rewrites the one part even if there is only a single part. Possible values: - 1 - Enable optimization. - 0 - Disable optimization.
   */
  optimize_skip_merged_partitions?: boolean;

  /**
   * Enables or disables skipping of unused shards for [SELECT](../../sql-reference/statements/select/index.md) queries that have sharding key condition in `WHERE/PREWHERE` (assuming that the data is distributed by sharding key, otherwise a query yields incorrect result). Possible values: - 0 — Disabled. - 1 — Enabled.
   */
  optimize_skip_unused_shards?: boolean;

  /**
   * Limit for number of sharding key values, turns off `optimize_skip_unused_shards` if the limit is reached. Too many values may require significant amount for processing, while the benefit is doubtful, since if you have huge number of values in `IN (...)`, then most likely the query will be sent to all shards anyway.
   */
  optimize_skip_unused_shards_limit?: number;

  /**
   * Controls [`optimize_skip_unused_shards`](#optimize_skip_unused_shards) (hence still requires [`optimize_skip_unused_shards`](#optimize_skip_unused_shards)) depends on the nesting level of the distributed query (case when you have `Distributed` table that look into another `Distributed` table). Possible values: - 0 — Disabled, `optimize_skip_unused_shards` works always. - 1 — Enables `optimize_skip_unused_shards` only for the first level. - 2 — Enables `optimize_skip_unused_shards` up to the second level.
   */
  optimize_skip_unused_shards_nesting?: number;

  /**
   * Rewrite IN in query for remote shards to exclude values that does not belong to the shard (requires optimize_skip_unused_shards). Possible values: - 0 — Disabled. - 1 — Enabled.
   */
  optimize_skip_unused_shards_rewrite_in?: boolean;

  /**
   * Optimize sorting by sorting properties of input stream
   */
  optimize_sorting_by_input_stream_properties?: boolean;

  /**
   * Use [constraints](../../sql-reference/statements/create/table.md/#constraints) for column substitution. The default is `false`. Possible values: - true, false
   */
  optimize_substitute_columns?: boolean;

  /**
   * Enables to fuse aggregate functions with identical argument. It rewrites query contains at least two aggregate functions from [sum](/sql-reference/aggregate-functions/reference/sum), [count](/sql-reference/aggregate-functions/reference/count) or [avg](/sql-reference/aggregate-functions/reference/avg) with identical argument to [sumCount](/sql-reference/aggregate-functions/reference/sumcount). Possible values: - 0 — Functions with identical argument are not fused. - 1 — Functions with identical argument are fused. **Example** Query: ```sql CREATE TABLE fuse_tbl(a Int8, b Int8) Engine = Log; SET optimize_syntax_fuse_functions = 1; EXPLAIN SYNTAX SELECT sum(a), sum(b), count(b), avg(b) from fuse_tbl FORMAT TSV; ``` Result: ```text SELECT sum(a), sumCount(b).1, sumCount(b).2, (sumCount(b).1) / (sumCount(b).2) FROM fuse_tbl ```
   */
  optimize_syntax_fuse_functions?: boolean;

  /**
   * Enables or disables throwing an exception if an [OPTIMIZE](../../sql-reference/statements/optimize.md) query didn't perform a merge. By default, `OPTIMIZE` returns successfully even if it didn't do anything. This setting lets you differentiate these situations and get the reason in an exception message. Possible values: - 1 — Throwing an exception is enabled. - 0 — Throwing an exception is disabled.
   */
  optimize_throw_if_noop?: boolean;

  /**
   * Optimize Date and DateTime predicates by converting functions into equivalent comparisons without conversions (e.g. `toYear(col) = 2023 -> col >= '2023-01-01' AND col <= '2023-12-31'`)
   */
  optimize_time_filter_with_preimage?: boolean;

  /**
   * Use an approximate value for trivial count optimization of storages that support such estimation, for example, EmbeddedRocksDB. Possible values: - 0 — Optimization disabled. - 1 — Optimization enabled.
   */
  optimize_trivial_approximate_count_query?: boolean;

  /**
   * Enables or disables the optimization to trivial query `SELECT count() FROM table` using metadata from MergeTree. If you need to use row-level security, disable this setting. Possible values: - 0 — Optimization disabled. - 1 — Optimization enabled. See also: - [optimize_functions_to_subcolumns](#optimize_functions_to_subcolumns)
   */
  optimize_trivial_count_query?: boolean;

  /**
   * Optimize trivial 'INSERT INTO table SELECT ... FROM TABLES' query
   */
  optimize_trivial_insert_select?: boolean;

  /**
   * Rewrite uniq and its variants(except uniqUpTo) to count if subquery has distinct or group by clause.
   */
  optimize_uniq_to_count?: boolean;

  /**
   * Automatically choose implicit projections to perform SELECT query
   */
  optimize_use_implicit_projections?: boolean;

  /**
   * Enables using projections to filter part ranges even when projections are not selected to perform SELECT query.
   * @since 25.7
   */
  optimize_use_projection_filtering?: boolean;

  /**
   * Enables or disables [projection](../../engines/table-engines/mergetree-family/mergetree.md/#projections) optimization when processing `SELECT` queries. Possible values: - 0 — Projection optimization disabled. - 1 — Projection optimization enabled.
   */
  optimize_use_projections?: boolean;

  /**
   * Use [constraints](../../sql-reference/statements/create/table.md/#constraints) for query optimization. The default is `false`. Possible values: - true, false
   */
  optimize_using_constraints?: boolean;

  /**
   * Linux nice value for materialized view threads. Lower values mean higher CPU priority. Requires CAP_SYS_NICE capability, otherwise no-op. Possible values: -20 to 19.
   * @since 25.10
   */
  os_threads_nice_value_materialized_view?: string;

  /**
   * Linux nice value for query processing threads. Lower values mean higher CPU priority. Requires CAP_SYS_NICE capability, otherwise no-op. Possible values: -20 to 19.
   * @since 25.10
   */
  os_threads_nice_value_query?: string;

  /**
   * Default compression level if query output is compressed. The setting is applied when `SELECT` query has `INTO OUTFILE` or when writing to table functions `file`, `url`, `hdfs`, `s3`, or `azureBlobStorage`. Possible values: from `1` to `22`
   */
  output_format_compression_level?: number;

  /**
   * Can be used when the output compression method is `zstd`. If greater than `0`, this setting explicitly sets compression window size (power of `2`) and enables a long-range mode for zstd compression. This can help to achieve a better compression ratio. Possible values: non-negative numbers. Note that if the value is too small or too big, `zstdlib` will throw an exception. Typical values are from `20` (window size = `1MB`) to `30` (window size = `1GB`).
   */
  output_format_compression_zstd_window_log?: number;

  /**
   * Enables or disables parallel formatting of data formats. Supported only for [TSV](/interfaces/formats/TabSeparated), [TSKV](/interfaces/formats/TSKV), [CSV](/interfaces/formats/CSV) and [JSONEachRow](/interfaces/formats/JSONEachRow) formats. Possible values: - 1 — Enabled. - 0 — Disabled.
   */
  output_format_parallel_formatting?: boolean;

  /**
   * Size of file chunks to store in the userspace page cache, in bytes. All reads that go through the cache will be rounded up to a multiple of this size. This setting can be adjusted on a per-query level basis, but cache entries with different block sizes cannot be reused. Changing this setting effectively invalidates existing entries in the cache. A higher value, like 1 MiB is good for high-throughput queries, and a lower value, like 64 KiB is good for low-latency point queries.
   * @since 25.6
   */
  page_cache_block_size?: number;

  /**
   * Userspace page cache will sometimes invalidate some pages at random. Intended for testing.
   */
  page_cache_inject_eviction?: boolean;

  /**
   * On userspace page cache miss, read up to this many consecutive blocks at once from the underlying storage, if they're also not in the cache. Each block is page_cache_block_size bytes. A higher value is good for high-throughput queries, while low-latency point queries will work better without readahead.
   * @since 25.6
   */
  page_cache_lookahead_blocks?: number;

  /**
   * Enables parallel distributed `INSERT ... SELECT` query. If we execute `INSERT INTO distributed_table_a SELECT ... FROM distributed_table_b` queries and both tables use the same cluster, and both tables are either [replicated](../../engines/table-engines/mergetree-family/replication.md) or non-replicated, then this query is processed locally on every shard. Possible values: - `0` — Disabled. - `1` — `SELECT` will be executed on each shard from the underlying table of the distributed engine. - `2` — `SELECT` and `INSERT` will be executed on each shard from/to the underlying table of the distributed engine. Setting `enable_parallel_replicas = 1` is needed when using this setting.
   */
  parallel_distributed_insert_select?: number;

  /**
   * When hash-based join algorithm is applied, this threshold helps to decide between using `hash` and `parallel_hash` (only if estimation of the right table size is available). The former is used when we know that the right table size is below the threshold.
   * @since 25.6
   */
  parallel_hash_join_threshold?: number;

  /**
   * This is internal setting that should not be used directly and represents an implementation detail of the 'parallel replicas' mode. This setting will be automatically set up by the initiator server for distributed queries to the index of the replica participating in query processing among parallel replicas.
   */
  parallel_replica_offset?: number;

  /**
   * If true, subquery for IN will be executed on every follower replica.
   */
  parallel_replicas_allow_in_with_subquery?: boolean;

  /**
   * Allow usage of materialized views with parallel replicas
   * @since 26.1
   */
  parallel_replicas_allow_materialized_views?: boolean;

  /**
   * The timeout in milliseconds for connecting to a remote replica during query execution with parallel replicas. If the timeout is expired, the corresponding replicas is not used for query execution
   * @since 25.7
   */
  parallel_replicas_connect_timeout_ms?: number;

  /**
   * This is internal setting that should not be used directly and represents an implementation detail of the 'parallel replicas' mode. This setting will be automatically set up by the initiator server for distributed queries to the number of parallel replicas participating in query processing.
   */
  parallel_replicas_count?: number;

  /**
   * An arbitrary integer expression that can be used to split work between replicas for a specific table. The value can be any integer expression. Simple expressions using primary keys are preferred. If the setting is used on a cluster that consists of a single shard with multiple replicas, those replicas will be converted into virtual shards. Otherwise, it will behave same as for `SAMPLE` key, it will use multiple replicas of each shard.
   */
  parallel_replicas_custom_key?: string;

  /**
   * Allows the filter type `range` to split the work evenly between replicas based on the custom range `[parallel_replicas_custom_key_range_lower, INT_MAX]`. When used in conjunction with [parallel_replicas_custom_key_range_upper](#parallel_replicas_custom_key_range_upper), it lets the filter evenly split the work over replicas for the range `[parallel_replicas_custom_key_range_lower, parallel_replicas_custom_key_range_upper]`. Note: This setting will not cause any additional data to be filtered during query processing, rather it changes the points at which the range filter breaks up the range `[0, INT_MAX]` for parallel processing.
   */
  parallel_replicas_custom_key_range_lower?: number;

  /**
   * Allows the filter type `range` to split the work evenly between replicas based on the custom range `[0, parallel_replicas_custom_key_range_upper]`. A value of 0 disables the upper bound, setting it the max value of the custom key expression. When used in conjunction with [parallel_replicas_custom_key_range_lower](#parallel_replicas_custom_key_range_lower), it lets the filter evenly split the work over replicas for the range `[parallel_replicas_custom_key_range_lower, parallel_replicas_custom_key_range_upper]`. Note: This setting will not cause any additional data to be filtered during query processing, rather it changes the points at which the range filter breaks up the range `[0, INT_MAX]` for parallel processing
   */
  parallel_replicas_custom_key_range_upper?: number;

  /**
   * Replace table function engines with their -Cluster alternatives
   * @since 25.4
   */
  parallel_replicas_for_cluster_engines?: boolean;

  /**
   * If true, ClickHouse will use parallel replicas algorithm also for non-replicated MergeTree tables
   */
  parallel_replicas_for_non_replicated_merge_tree?: boolean;

  /**
   * Index analysis done only on replica-coordinator and skipped on other replicas. Effective only with enabled parallel_replicas_local_plan
   * @since 25.1
   */
  parallel_replicas_index_analysis_only_on_coordinator?: boolean;

  /**
   * Use local pipeline during distributed INSERT SELECT with parallel replicas
   * @since 25.6
   */
  parallel_replicas_insert_select_local_pipeline?: boolean;

  /**
   * Build local plan for local replica
   */
  parallel_replicas_local_plan?: boolean;

  /**
   * Parts virtually divided into segments to be distributed between replicas for parallel reading. This setting controls the size of these segments. Not recommended to change until you're absolutely sure in what you're doing. Value should be in range [128; 16384]
   */
  parallel_replicas_mark_segment_size?: number;

  /**
   * Limit the number of replicas used in a query to (estimated rows to read / min_number_of_rows_per_replica). The max is still limited by 'max_parallel_replicas'
   */
  parallel_replicas_min_number_of_rows_per_replica?: number;

  /**
   * Type of filter to use with custom key for parallel replicas. default - use modulo operation on the custom key, range - use range filter on custom key using all possible values for the value type of custom key.
   */
  parallel_replicas_mode?: 'auto' | 'read_tasks' | 'custom_key_sampling' | 'custom_key_range' | 'sampling_key';

  /**
   * The analyzer should be enabled to use parallel replicas. With disabled analyzer query execution fallbacks to local execution, even if parallel reading from replicas is enabled. Using parallel replicas without the analyzer enabled is not supported
   * @since 25.4
   */
  parallel_replicas_only_with_analyzer?: boolean;

  /**
   * If true, and JOIN can be executed with parallel replicas algorithm, and all storages of right JOIN part are *MergeTree, local JOIN will be used instead of GLOBAL JOIN.
   */
  parallel_replicas_prefer_local_join?: boolean;

  /**
   * Optimization of projections can be applied in parallel replicas. Effective only with enabled parallel_replicas_local_plan and aggregation_in_order is inactive.
   * @since 25.9
   */
  parallel_replicas_support_projection?: boolean;

  /**
   * Enables pushing to attached views concurrently instead of sequentially.
   */
  parallel_view_processing?: boolean;

  /**
   * Parallelize output for reading step from storage. It allows parallelization of query processing right after reading from storage if possible
   */
  parallelize_output_from_storages?: boolean;

  /**
   * Formatter '%e' in function 'parseDateTime' expects that single-digit days are space-padded, e.g., ' 2' is accepted but '2' raises an error.
   * @since 25.6
   */
  parsedatetime_e_requires_space_padding?: boolean;

  /**
   * Formatters '%c', '%l' and '%k' in function 'parseDateTime' parse months and hours without leading zeros.
   */
  parsedatetime_parse_without_leading_zeros?: boolean;

  /**
   * If not 0 group left table blocks in bigger ones for left-side table in partial merge join. It uses up to 2x of specified memory per joining thread.
   */
  partial_merge_join_left_table_buffer_bytes?: number;

  /**
   * Limits sizes of right-hand join data blocks in partial merge join algorithm for [JOIN](../../sql-reference/statements/select/join.md) queries. ClickHouse server: 1. Splits right-hand join data into blocks with up to the specified number of rows. 2. Indexes each block with its minimum and maximum values. 3. Unloads prepared blocks to disk if it is possible. Possible values: - Any positive integer. Recommended range of values: [1000, 100000].
   */
  partial_merge_join_rows_in_right_blocks?: number;

  /**
   * Allows query to return a partial result after cancel.
   */
  partial_result_on_first_cancel?: boolean;

  /**
   * If the destination table contains at least that many active parts in a single partition, artificially slow down insert into table.
   */
  parts_to_delay_insert?: number;

  /**
   * If more than this number active parts in a single partition of the destination table, throw 'Too many parts ...' exception.
   */
  parts_to_throw_insert?: number;

  /**
   * Logs index statistics per part
   * @since 25.9
   */
  per_part_index_stats?: boolean;

  /**
   * Block at the query wait loop on the server for the specified number of seconds.
   */
  poll_interval?: number;

  /**
   * Connection timeout in seconds of a single attempt to connect PostgreSQL end-point. The value is passed as a `connect_timeout` parameter of the connection URL.
   */
  postgresql_connection_attempt_timeout?: number;

  /**
   * Close connection before returning connection to the pool.
   */
  postgresql_connection_pool_auto_close_connection?: boolean;

  /**
   * Connection pool push/pop retries number for PostgreSQL table engine and database engine.
   */
  postgresql_connection_pool_retries?: number;

  /**
   * Connection pool size for PostgreSQL table engine and database engine.
   */
  postgresql_connection_pool_size?: number;

  /**
   * Connection pool push/pop timeout on empty pool for PostgreSQL table engine and database engine. By default it will block on empty pool.
   */
  postgresql_connection_pool_wait_timeout?: number;

  /**
   * Approximate probability of failing internal (for replication) PostgreSQL queries. Valid value is in interval [0.0f, 1.0f]
   * @since 25.3
   */
  postgresql_fault_injection_probability?: number;

  /**
   * Enables or disables using the original column names instead of aliases in query expressions and clauses. It especially matters when alias is the same as the column name, see [Expression Aliases](/sql-reference/syntax#notes-on-usage). Enable this setting to make aliases syntax rules in ClickHouse more compatible with most other database engines. Possible values: - 0 — The column name is substituted with the alias. - 1 — The column name is not substituted with the alias. **Example** The difference between enabled and disabled: Query: ```sql SET prefer_column_name_to_alias = 0; SELECT avg(number) AS number, max(number) FROM numbers(10); ``` Result: ```text Received exception from server (version 21.5.1): Code: 184. DB::Exception: Received from localhost:9000. DB::Exception: Aggregate function avg(number) is found inside another aggregate function in query: While processing avg(number) AS number. ``` Query: ```sql SET prefer_column_name_to_alias = 1; SELECT avg(number) AS number, max(number) FROM numbers(10); ``` Result: ```text ┌─number─┬─max(number)─┐ │ 4.5 │ 9 │ └────────┴─────────────┘ ```
   */
  prefer_column_name_to_alias?: boolean;

  /**
   * Prefer maximum block bytes for external sort, reduce the memory usage during merging.
   */
  prefer_external_sort_block_bytes?: number;

  /**
   * Enables the replacement of `IN`/`JOIN` operators with `GLOBAL IN`/`GLOBAL JOIN`. Possible values: - 0 — Disabled. `IN`/`JOIN` operators are not replaced with `GLOBAL IN`/`GLOBAL JOIN`. - 1 — Enabled. `IN`/`JOIN` operators are replaced with `GLOBAL IN`/`GLOBAL JOIN`. **Usage** Although `SET distributed_product_mode=global` can change the queries behavior for the distributed tables, it's not suitable for local tables or tables from external resources. Here is when the `prefer_global_in_and_join` setting comes into play. For example, we have query serving nodes that contain local tables, which are not suitable for distribution. We need to scatter their data on the fly during distributed processing with the `GLOBAL` keyword — `GLOBAL IN`/`GLOBAL JOIN`. Another use case of `prefer_global_in_and_join` is accessing tables created by external engines. This setting helps to reduce the number of calls to external sources while joining such tables: only one call per query. **See also:** - [Distributed subqueries](/sql-reference/operators/in#distributed-subqueries) for more information on how to use `GLOBAL IN`/`GLOBAL JOIN`
   */
  prefer_global_in_and_join?: boolean;

  /**
   * Enables/disables preferable using the localhost replica when processing distributed queries. Possible values: - 1 — ClickHouse always sends a query to the localhost replica if it exists. - 0 — ClickHouse uses the balancing strategy specified by the [load_balancing](#load_balancing) setting. :::note Disable this setting if you use [max_parallel_replicas](#max_parallel_replicas) without [parallel_replicas_custom_key](#parallel_replicas_custom_key). If [parallel_replicas_custom_key](#parallel_replicas_custom_key) is set, disable this setting only if it's used on a cluster with multiple shards containing multiple replicas. If it's used on a cluster with a single shard and multiple replicas, disabling this setting will have negative effects. :::
   */
  prefer_localhost_replica?: boolean;

  /**
   * Only has an effect in ClickHouse Cloud. If a merged part is less than this many seconds old and is not pre-warmed (see [cache_populated_by_fetch](merge-tree-settings.md/#cache_populated_by_fetch)), but all its source parts are available and pre-warmed, SELECT queries will read from those parts instead. Only for Replicated-/SharedMergeTree. Note that this only checks whether CacheWarmer processed the part; if the part was fetched into cache by something else, it'll still be considered cold until CacheWarmer gets to it; if it was warmed, then evicted from cache, it'll still be considered warm.
   */
  prefer_warmed_unmerged_parts_seconds?: number;

  /**
   * This setting adjusts the data block size for query processing and represents additional fine-tuning to the more rough 'max_block_size' setting. If the columns are large and with 'max_block_size' rows the block size is likely to be larger than the specified amount of bytes, its size will be lowered for better CPU cache locality.
   */
  preferred_block_size_bytes?: number;

  /**
   * Limit on max column size in block while reading. Helps to decrease cache misses count. Should be close to L2 cache size.
   */
  preferred_max_column_in_block_size_bytes?: number;

  /**
   * If it is set to a non-empty string, ClickHouse will try to apply specified projection in query. Possible values: - string: name of preferred projection
   */
  preferred_optimize_projection_name?: string;

  /**
   * The maximum size of the prefetch buffer to read from the filesystem.
   */
  prefetch_buffer_size?: number;

  /**
   * Allows to print deep-nested type names in a pretty way with indents in `DESCRIBE` query and in `toTypeName()` function. Example: ```sql CREATE TABLE test (a Tuple(b String, c Tuple(d Nullable(UInt64), e Array(UInt32), f Array(Tuple(g String, h Map(String, Array(Tuple(i String, j UInt64))))), k Date), l Nullable(String))) ENGINE=Memory; DESCRIBE TABLE test FORMAT TSVRaw SETTINGS print_pretty_type_names=1; ``` ``` a Tuple( b String, c Tuple( d Nullable(UInt64), e Array(UInt32), f Array(Tuple( g String, h Map( String, Array(Tuple( i String, j UInt64 )) ) )), k Date ), l Nullable(String) ) ```
   */
  print_pretty_type_names?: boolean;

  /**
   * Priority of the query. 1 - the highest, higher value - lower priority; 0 - do not use priorities.
   */
  priority?: number;

  /**
   * Specifies the database name used by the 'promql' dialect. Empty string means the current database.
   * @since 25.9
   */
  promql_database?: string;

  /**
   * Sets the evaluation time to be used with promql dialect. 'auto' means the current time.
   * @since 25.10
   */
  promql_evaluation_time?: string;

  /**
   * Specifies the name of a TimeSeries table used by the 'promql' dialect.
   * @since 25.9
   */
  promql_table?: string;

  /**
   * Enable pushing user roles from originator to other nodes while performing a query.
   * @since 24.12
   */
  push_external_roles_in_interserver_queries?: boolean;

  /**
   * Compress entries in the [query cache](../query-cache.md). Lessens the memory consumption of the query cache at the cost of slower inserts into / reads from it. Possible values: - 0 - Disabled - 1 - Enabled
   */
  query_cache_compress_entries?: boolean;

  /**
   * The maximum number of query results the current user may store in the [query cache](../query-cache.md). 0 means unlimited. Possible values: - Positive integer >= 0.
   */
  query_cache_max_entries?: number;

  /**
   * The maximum amount of memory (in bytes) the current user may allocate in the [query cache](../query-cache.md). 0 means unlimited. Possible values: - Positive integer >= 0.
   */
  query_cache_max_size_in_bytes?: number;

  /**
   * Minimum duration in milliseconds a query needs to run for its result to be stored in the [query cache](../query-cache.md). Possible values: - Positive integer >= 0.
   */
  query_cache_min_query_duration?: number;

  /**
   * Minimum number of times a `SELECT` query must run before its result is stored in the [query cache](../query-cache.md). Possible values: - Positive integer >= 0.
   */
  query_cache_min_query_runs?: number;

  /**
   * Controls how the [query cache](../query-cache.md) handles `SELECT` queries with non-deterministic functions like `rand()` or `now()`. Possible values: - `'throw'` - Throw an exception and don't cache the query result. - `'save'` - Cache the query result. - `'ignore'` - Don't cache the query result and don't throw an exception.
   */
  query_cache_nondeterministic_function_handling?: 'throw' | 'save' | 'ignore';

  /**
   * If turned on, the result of `SELECT` queries cached in the [query cache](../query-cache.md) can be read by other users. It is not recommended to enable this setting due to security reasons. Possible values: - 0 - Disabled - 1 - Enabled
   */
  query_cache_share_between_users?: boolean;

  /**
   * Squash partial result blocks to blocks of size [max_block_size](#max_block_size). Reduces performance of inserts into the [query cache](../query-cache.md) but improves the compressability of cache entries (see [query_cache_compress-entries](#query_cache_compress_entries)). Possible values: - 0 - Disabled - 1 - Enabled
   */
  query_cache_squash_partial_results?: boolean;

  /**
   * Controls how the [query cache](../query-cache.md) handles `SELECT` queries against system tables, i.e. tables in databases `system.*` and `information_schema.*`. Possible values: - `'throw'` - Throw an exception and don't cache the query result. - `'save'` - Cache the query result. - `'ignore'` - Don't cache the query result and don't throw an exception.
   */
  query_cache_system_table_handling?: 'throw' | 'save' | 'ignore';

  /**
   * A string which acts as a label for [query cache](../query-cache.md) entries. The same queries with different tags are considered different by the query cache. Possible values: - Any string
   */
  query_cache_tag?: string;

  /**
   * After this time in seconds entries in the [query cache](../query-cache.md) become stale. Possible values: - Positive integer >= 0.
   */
  query_cache_ttl?: number;

  /**
   * Stores the filter condition for the [query condition cache](/operations/query-condition-cache) in plaintext. If enabled, system.query_condition_cache shows the verbatim filter condition which makes it easier to debug issues with the cache. Disabled by default because plaintext filter conditions may expose sensitive information. Possible values: - 0 - Disabled - 1 - Enabled
   * @since 25.6
   */
  query_condition_cache_store_conditions_as_plaintext?: boolean;

  /**
   * The interval in milliseconds at which the [query_metric_log](../../operations/system-tables/query_metric_log.md) for individual queries is collected. If set to any negative value, it will take the value `collect_interval_milliseconds` from the [query_metric_log setting](/operations/server-configuration-parameters/settings#query_metric_log) or default to 1000 if not present. To disable the collection of a single query, set `query_metric_log_interval` to 0. Default value: -1
   */
  query_metric_log_interval?: number;

  /**
   * Toggles the aggregation in-order query-plan-level optimization. Only takes effect if setting [`query_plan_enable_optimizations`](#query_plan_enable_optimizations) is 1. :::note This is an expert-level setting which should only be used for debugging by developers. The setting may change in future in backward-incompatible ways or be removed. ::: Possible values: - 0 - Disable - 1 - Enable
   */
  query_plan_aggregation_in_order?: boolean;

  /**
   * Allow to convert ANY JOIN to SEMI or ANTI JOIN if filter after JOIN always evaluates to false for not-matched or matched rows
   * @since 25.10
   */
  query_plan_convert_any_join_to_semi_or_anti_join?: boolean;

  /**
   * Allow to convert `JOIN` to subquery with `IN` if output columns tied to only left table. May cause wrong results with non-ANY JOINs (e.g. ALL JOINs which is the default).
   * @since 25.5
   */
  query_plan_convert_join_to_in?: boolean;

  /**
   * Allow to convert `OUTER JOIN` to `INNER JOIN` if filter after `JOIN` always filters default values
   */
  query_plan_convert_outer_join_to_inner_join?: boolean;

  /**
   * Allow to perform full text search filtering using only the inverted text index in query plan.
   * @since 25.10
   */
  query_plan_direct_read_from_text_index?: boolean;

  /**
   * Show internal aliases (such as __table1) in EXPLAIN PLAN instead of those specified in the original query.
   * @since 25.10
   */
  query_plan_display_internal_aliases?: boolean;

  /**
   * Enable multithreading after evaluating window functions to allow parallel stream processing
   */
  query_plan_enable_multithreading_after_window_functions?: boolean;

  /**
   * Toggles query optimization at the query plan level. :::note This is an expert-level setting which should only be used for debugging by developers. The setting may change in future in backward-incompatible ways or be removed. ::: Possible values: - 0 - Disable all optimizations at the query plan level - 1 - Enable optimizations at the query plan level (but individual optimizations may still be disabled via their individual settings)
   */
  query_plan_enable_optimizations?: boolean;

  /**
   * Toggles a query-plan-level optimization which moves expressions after sorting steps. Only takes effect if setting [`query_plan_enable_optimizations`](#query_plan_enable_optimizations) is 1. :::note This is an expert-level setting which should only be used for debugging by developers. The setting may change in future in backward-incompatible ways or be removed. ::: Possible values: - 0 - Disable - 1 - Enable
   */
  query_plan_execute_functions_after_sorting?: boolean;

  /**
   * Toggles a query-plan-level optimization which moves filters down in the execution plan. Only takes effect if setting [query_plan_enable_optimizations](#query_plan_enable_optimizations) is 1. :::note This is an expert-level setting which should only be used for debugging by developers. The setting may change in future in backward-incompatible ways or be removed. ::: Possible values: - 0 - Disable - 1 - Enable
   */
  query_plan_filter_push_down?: boolean;

  /**
   * Apply sharding for JOIN if join keys contain a prefix of PRIMARY KEY for both tables. Supported for hash, parallel_hash and full_sorting_merge algorithms. Usually does not speed up queries but may lower memory consumption.
   * @since 25.5
   */
  query_plan_join_shard_by_pk_ranges?: boolean;

  /**
   * Determine which side of the join should be the build table (also called inner, the one inserted into the hash table for a hash join) in the query plan. This setting is supported only for `ALL` join strictness with the `JOIN ON` clause. Possible values are: - 'auto': Let the planner decide which table to use as the build table. - 'false': Never swap tables (the right table is the build table). - 'true': Always swap tables (the left table is the build table).
   * @since 25.1
   */
  query_plan_join_swap_table?: string;

  /**
   * Toggles a query-plan-level optimization which moves ARRAY JOINs up in the execution plan. Only takes effect if setting [query_plan_enable_optimizations](#query_plan_enable_optimizations) is 1. :::note This is an expert-level setting which should only be used for debugging by developers. The setting may change in future in backward-incompatible ways or be removed. ::: Possible values: - 0 - Disable - 1 - Enable
   */
  query_plan_lift_up_array_join?: boolean;

  /**
   * Toggles a query-plan-level optimization which moves larger subtrees of the query plan into union to enable further optimizations. Only takes effect if setting [`query_plan_enable_optimizations`](#query_plan_enable_optimizations) is 1. :::note This is an expert-level setting which should only be used for debugging by developers. The setting may change in future in backward-incompatible ways or be removed. ::: Possible values: - 0 - Disable - 1 - Enable
   */
  query_plan_lift_up_union?: boolean;

  /**
   * Control maximum limit value that allows to use query plan for lazy materialization optimization. If zero, there is no limit.
   * @since 25.5
   */
  query_plan_max_limit_for_lazy_materialization?: number;

  /**
   * Control maximum limit value that allows to evaluate query plan for TopK optimization by using minmax skip index and dynamic threshold filtering. If zero, there is no limit.
   * @since 26.1
   */
  query_plan_max_limit_for_top_k_optimization?: number;

  /**
   * Limits the total number of optimizations applied to query plan, see setting [query_plan_enable_optimizations](#query_plan_enable_optimizations). Useful to avoid long optimization times for complex queries. In the EXPLAIN PLAN query, stop applying optimizations after this limit is reached and return the plan as is. For regular query execution if the actual number of optimizations exceeds this setting, an exception is thrown. :::note This is an expert-level setting which should only be used for debugging by developers. The setting may change in future in backward-incompatible ways or be removed. :::
   */
  query_plan_max_optimizations_to_apply?: number;

  /**
   * Maximum length of step description in EXPLAIN PLAN.
   * @since 25.10
   */
  query_plan_max_step_description_length?: number;

  /**
   * Toggles a query-plan-level optimization which merges consecutive filters. Only takes effect if setting [query_plan_enable_optimizations](#query_plan_enable_optimizations) is 1. :::note This is an expert-level setting which should only be used for debugging by developers. The setting may change in future in backward-incompatible ways or be removed. ::: Possible values: - 0 - Disable - 1 - Enable
   */
  query_plan_merge_expressions?: boolean;

  /**
   * Allow to merge filter into `JOIN` condition and convert `CROSS JOIN` to `INNER`.
   * @since 25.6
   */
  query_plan_merge_filter_into_join_condition?: boolean;

  /**
   * Allow to merge filters in the query plan.
   */
  query_plan_merge_filters?: boolean;

  /**
   * Specifies which JOIN order algorithms to attempt during query plan optimization. The following algorithms are available: - 'greedy' - basic greedy algorithm - works fast but might not produce the best join order - 'dpsize' - implements DPsize algorithm currently only for Inner joins - considers all possible join orders and finds the most optimal one but might be slow for queries with many tables and join predicates. Multiple algorithms can be specified, e.g. 'dpsize,greedy'.
   * @since 26.1
   */
  query_plan_optimize_join_order_algorithm?: 'greedy' | 'dpsize';

  /**
   * Optimize the order of joins within the same subquery. Currently only supported for very limited cases. Value is the maximum number of tables to optimize.
   * @since 25.10
   */
  query_plan_optimize_join_order_limit?: number;

  /**
   * Use query plan for lazy materialization optimization.
   * @since 25.5
   */
  query_plan_optimize_lazy_materialization?: boolean;

  /**
   * Allow to push down filter to PREWHERE expression for supported storages
   */
  query_plan_optimize_prewhere?: boolean;

  /**
   * Toggles a query-plan-level optimization which moves LIMITs down in the execution plan. Only takes effect if setting [query_plan_enable_optimizations](#query_plan_enable_optimizations) is 1. :::note This is an expert-level setting which should only be used for debugging by developers. The setting may change in future in backward-incompatible ways or be removed. ::: Possible values: - 0 - Disable - 1 - Enable
   */
  query_plan_push_down_limit?: boolean;

  /**
   * Toggles the read in-order optimization query-plan-level optimization. Only takes effect if setting [`query_plan_enable_optimizations`](#query_plan_enable_optimizations) is 1. :::note This is an expert-level setting which should only be used for debugging by developers. The setting may change in future in backward-incompatible ways or be removed. ::: Possible values: - 0 - Disable - 1 - Enable
   */
  query_plan_read_in_order?: boolean;

  /**
   * Toggles a query-plan-level optimization which removes redundant DISTINCT steps. Only takes effect if setting [`query_plan_enable_optimizations`](#query_plan_enable_optimizations) is 1. :::note This is an expert-level setting which should only be used for debugging by developers. The setting may change in future in backward-incompatible ways or be removed. ::: Possible values: - 0 - Disable - 1 - Enable
   */
  query_plan_remove_redundant_distinct?: boolean;

  /**
   * Toggles a query-plan-level optimization which removes redundant sorting steps, e.g. in subqueries. Only takes effect if setting [`query_plan_enable_optimizations`](#query_plan_enable_optimizations) is 1. :::note This is an expert-level setting which should only be used for debugging by developers. The setting may change in future in backward-incompatible ways or be removed. ::: Possible values: - 0 - Disable - 1 - Enable
   */
  query_plan_remove_redundant_sorting?: boolean;

  /**
   * Toggles a query-plan-level optimization which tries to remove unused columns (both input and output columns) from query plan steps. Only takes effect if setting [query_plan_enable_optimizations](#query_plan_enable_optimizations) is 1. :::note This is an expert-level setting which should only be used for debugging by developers. The setting may change in future in backward-incompatible ways or be removed. ::: Possible values: - 0 - Disable - 1 - Enable
   * @since 26.1
   */
  query_plan_remove_unused_columns?: boolean;

  /**
   * Toggles a query-plan-level optimization which uses storage sorting when sorting for window functions. Only takes effect if setting [`query_plan_enable_optimizations`](#query_plan_enable_optimizations) is 1. :::note This is an expert-level setting which should only be used for debugging by developers. The setting may change in future in backward-incompatible ways or be removed. ::: Possible values: - 0 - Disable - 1 - Enable
   */
  query_plan_reuse_storage_ordering_for_window_functions?: boolean;

  /**
   * :::note This is an expert-level setting which should only be used for debugging by developers. The setting may change in future in backward-incompatible ways or be removed. ::: Toggles a query-plan-level optimization which splits filters into expressions. Only takes effect if setting [query_plan_enable_optimizations](#query_plan_enable_optimizations) is 1. Possible values: - 0 - Disable - 1 - Enable
   */
  query_plan_split_filter?: boolean;

  /**
   * Allow to add hint (additional predicate) for filtering built from the inverted text index in query plan.
   * @since 26.1
   */
  query_plan_text_index_add_hint?: boolean;

  /**
   * Toggles a query-plan-level optimization which tries to use the vector similarity index. Only takes effect if setting [`query_plan_enable_optimizations`](#query_plan_enable_optimizations) is 1. :::note This is an expert-level setting which should only be used for debugging by developers. The setting may change in future in backward-incompatible ways or be removed. ::: Possible values: - 0 - Disable - 1 - Enable
   * @since 25.2
   */
  query_plan_try_use_vector_search?: boolean;

  /**
   * Use logical join step in query plan. Note: setting `query_plan_use_new_logical_join_step` is deprecated, use `query_plan_use_logical_join_step` instead.
   * @since 25.2
   */
  query_plan_use_new_logical_join_step?: boolean;

  /**
   * Sets the period for a CPU clock timer of the [query profiler](../../operations/optimizing-performance/sampling-query-profiler.md). This timer counts only CPU time. Possible values: - A positive integer number of nanoseconds. Recommended values: - 10000000 (100 times a second) nanoseconds and more for single queries. - 1000000000 (once a second) for cluster-wide profiling. - 0 for turning off the timer. See also: - System table [trace_log](/operations/system-tables/trace_log)
   */
  query_profiler_cpu_time_period_ns?: number;

  /**
   * Sets the period for a real clock timer of the [query profiler](../../operations/optimizing-performance/sampling-query-profiler.md). Real clock timer counts wall-clock time. Possible values: - Positive integer number, in nanoseconds. Recommended values: - 10000000 (100 times a second) nanoseconds and less for single queries. - 1000000000 (once a second) for cluster-wide profiling. - 0 for turning off the timer. See also: - System table [trace_log](/operations/system-tables/trace_log)
   */
  query_profiler_real_time_period_ns?: number;

  /**
   * The wait time in the request queue, if the number of concurrent requests exceeds the maximum.
   */
  queue_max_wait_ms?: number;

  /**
   * The wait time for reading from RabbitMQ before retry.
   */
  rabbitmq_max_wait_ms?: number;

  /**
   * Settings to reduce the number of threads in case of slow reads. Count events when the read bandwidth is less than that many bytes per second.
   */
  read_backoff_max_throughput?: number;

  /**
   * Settings to try keeping the minimal number of threads in case of slow reads.
   */
  read_backoff_min_concurrency?: number;

  /**
   * Settings to reduce the number of threads in case of slow reads. The number of events after which the number of threads will be reduced.
   */
  read_backoff_min_events?: number;

  /**
   * Settings to reduce the number of threads in case of slow reads. Do not pay attention to the event, if the previous one has passed less than a certain amount of time.
   */
  read_backoff_min_interval_between_events_ms?: number;

  /**
   * Setting to reduce the number of threads in case of slow reads. Pay attention only to reads that took at least that much time.
   */
  read_backoff_min_latency_ms?: number;

  /**
   * Only has an effect in ClickHouse Cloud. Same as read_from_filesystem_cache_if_exists_otherwise_bypass_cache, but for distributed cache.
   * @since 25.11
   */
  read_from_distributed_cache_if_exists_otherwise_bypass_cache?: boolean;

  /**
   * Allow to use the filesystem cache in passive mode - benefit from the existing cache entries, but don't put more entries into the cache. If you set this setting for heavy ad-hoc queries and leave it disabled for short real-time queries, this will allows to avoid cache threshing by too heavy queries and to improve the overall system efficiency.
   */
  read_from_filesystem_cache_if_exists_otherwise_bypass_cache?: boolean;

  /**
   * Use userspace page cache in passive mode, similar to read_from_filesystem_cache_if_exists_otherwise_bypass_cache.
   */
  read_from_page_cache_if_exists_otherwise_bypass_cache?: boolean;

  /**
   * Minimal number of parts to read to run preliminary merge step during multithread reading in order of primary key.
   */
  read_in_order_two_level_merge_threshold?: number;

  /**
   * Use buffering before merging while reading in order of primary key. It increases the parallelism of query execution
   */
  read_in_order_use_buffering?: boolean;

  /**
   * Use virtual row while reading in order of primary key or its monotonic function fashion. It is useful when searching over multiple parts as only relevant ones are touched.
   * @since 24.12
   */
  read_in_order_use_virtual_row?: boolean;

  /**
   * What to do when the limit is exceeded.
   */
  read_overflow_mode?: 'throw' | 'break';

  /**
   * Sets what happens when the volume of data read exceeds one of the leaf limits. Possible options: - `throw`: throw an exception (default). - `break`: stop executing the query and return the partial result.
   */
  read_overflow_mode_leaf?: 'throw' | 'break';

  /**
   * Priority to read data from local filesystem or remote filesystem. Only supported for 'pread_threadpool' method for local filesystem and for `threadpool` method for remote filesystem.
   */
  read_priority?: number;

  /**
   * Only has an effect in ClickHouse Cloud. Allow reading from distributed cache
   */
  read_through_distributed_cache?: boolean;

  /**
   * 0 - no read-only restrictions. 1 - only read requests, as well as changing explicitly allowed settings. 2 - only read requests, as well as changing settings, except for the 'readonly' setting.
   */
  readonly?: number;

  /**
   * Connection timeout for receiving first packet of data or packet with positive progress from replica
   */
  receive_data_timeout_ms?: number;

  /**
   * Timeout for receiving data from the network, in seconds. If no bytes were received in this interval, the exception is thrown. If you set this setting on the client, the 'send_timeout' for the socket will also be set on the corresponding connection end on the server.
   */
  receive_timeout?: number;

  /**
   * Sets the maximum number of matches for a single regular expression per row. Use it to protect against memory overload when using greedy regular expression in the [extractAllGroupsHorizontal](/sql-reference/functions/string-search-functions#extractAllGroupsHorizontal) function. Possible values: - Positive integer.
   */
  regexp_max_matches_per_row?: number;

  /**
   * Reject patterns which will likely be expensive to evaluate with hyperscan (due to NFA state explosion)
   */
  reject_expensive_hyperscan_regexps?: boolean;

  /**
   * If memory usage after remerge does not reduced by this ratio, remerge will be disabled.
   */
  remerge_sort_lowered_memory_bytes_ratio?: number;

  /**
   * Method of reading data from remote filesystem, one of: read, threadpool.
   */
  remote_filesystem_read_method?: string;

  /**
   * Should use prefetching when reading data from remote filesystem.
   */
  remote_filesystem_read_prefetch?: boolean;

  /**
   * Max attempts to read with backoff
   */
  remote_fs_read_backoff_max_tries?: number;

  /**
   * Max wait time when trying to read data for remote disk
   */
  remote_fs_read_max_backoff_ms?: number;

  /**
   * Min bytes required for remote read (url, s3) to do seek, instead of read with ignore.
   */
  remote_read_min_bytes_for_seek?: number;

  /**
   * - **Type:** String - **Default value:** Empty string This setting allows to specify renaming pattern for files processed by `file` table function. When option is set, all files read by `file` table function will be renamed according to specified pattern with placeholders, only if files processing was successful. ### Placeholders - `%a` — Full original filename (e.g., "sample.csv"). - `%f` — Original filename without extension (e.g., "sample"). - `%e` — Original file extension with dot (e.g., ".csv"). - `%t` — Timestamp (in microseconds). - `%%` — Percentage sign ("%"). ### Example - Option: `--rename_files_after_processing="processed_%f_%t%e"` - Query: `SELECT * FROM file('sample.csv')` If reading `sample.csv` is successful, file will be renamed to `processed_sample_1683473210851438.csv`
   */
  rename_files_after_processing?: string;

  /**
   * When using the HTTP interface, the 'query_id' parameter can be passed. This is any string that serves as the query identifier. If a query from the same user with the same 'query_id' already exists at this time, the behaviour depends on the 'replace_running_query' parameter. `0` (default) – Throw an exception (do not allow the query to run if a query with the same 'query_id' is already running). `1` – Cancel the old query and start running the new one. Set this parameter to 1 for implementing suggestions for segmentation conditions. After entering the next character, if the old query hasn't finished yet, it should be cancelled.
   */
  replace_running_query?: boolean;

  /**
   * The wait time for running the query with the same `query_id` to finish, when the [replace_running_query](#replace_running_query) setting is active. Possible values: - Positive integer. - 0 — Throwing an exception that does not allow to run a new query if the server already executes a query with the same `query_id`.
   */
  replace_running_query_max_wait_ms?: number;

  /**
   * Specifies how long (in seconds) to wait for inactive replicas to execute [`ALTER`](../../sql-reference/statements/alter/index.md), [`OPTIMIZE`](../../sql-reference/statements/optimize.md) or [`TRUNCATE`](../../sql-reference/statements/truncate.md) queries. Possible values: - `0` — Do not wait. - Negative integer — Wait for unlimited time. - Positive integer — The number of seconds to wait.
   */
  replication_wait_for_inactive_replica_timeout?: number;

  /**
   * Replace external dictionary sources to Null on restore. Useful for testing purposes
   */
  restore_replace_external_dictionary_source_to_null?: boolean;

  /**
   * For testing purposes. Replaces all external engines to Null to not initiate external connections.
   */
  restore_replace_external_engines_to_null?: boolean;

  /**
   * For testing purposes. Replaces all external table functions to Null to not initiate external connections.
   */
  restore_replace_external_table_functions_to_null?: boolean;

  /**
   * Replace table engine from Replicated*MergeTree -> Shared*MergeTree during RESTORE.
   * @since 25.3
   */
  restore_replicated_merge_tree_to_shared_merge_tree?: boolean;

  /**
   * Cloud default value: `throw` Sets what to do if the volume of the result exceeds one of the limits. Possible values: - `throw`: throw an exception (default). - `break`: stop executing the query and return the partial result, as if the source data ran out. Using 'break' is similar to using LIMIT. `Break` interrupts execution only at the block level. This means that amount of returned rows is greater than [`max_result_rows`](/operations/settings/settings#max_result_rows), multiple of [`max_block_size`](/operations/settings/settings#max_block_size) and depends on [`max_threads`](/operations/settings/settings#max_threads). **Example** ```sql title="Query" SET max_threads = 3, max_block_size = 3333; SET max_result_rows = 3334, result_overflow_mode = 'break'; SELECT * FROM numbers_mt(100000) FORMAT Null; ``` ```text title="Result" 6666 rows in set. ... ```
   */
  result_overflow_mode?: 'throw' | 'break';

  /**
   * Allows you to rewrite `countDistcintIf` with [count_distinct_implementation](#count_distinct_implementation) setting. Possible values: - true — Allow. - false — Disallow.
   */
  rewrite_count_distinct_if_with_count_distinct_implementation?: boolean;

  /**
   * Rewrite expressions like 'x IN subquery' to JOIN. This might be useful for optimizing the whole query with join reordering.
   * @since 25.11
   */
  rewrite_in_to_join?: boolean;

  /**
   * Allow multipart copy in S3.
   * @since 25.4
   */
  s3_allow_multipart_copy?: boolean;

  /**
   * Use multiple threads for s3 multipart upload. It may lead to slightly higher memory usage
   */
  s3_allow_parallel_part_upload?: boolean;

  /**
   * Check each uploaded object to s3 with head request to be sure that upload was successful
   */
  s3_check_objects_after_upload?: boolean;

  /**
   * Connection timeout for host from s3 disks.
   */
  s3_connect_timeout_ms?: number;

  /**
   * Enables or disables creating a new file on each insert in s3 engine tables. If enabled, on each insert a new S3 object will be created with the key, similar to this pattern: initial: `data.Parquet.gz` -> `data.1.Parquet.gz` -> `data.2.Parquet.gz`, etc. Possible values: - 0 — `INSERT` query creates a new file or fail if file exists and s3_truncate_on_insert is not set. - 1 — `INSERT` query creates a new file on each insert using suffix (from the second one) if s3_truncate_on_insert is not set. See more details [here](/integrations/s3#inserting-data).
   */
  s3_create_new_file_on_insert?: boolean;

  /**
   * Do not calculate a checksum when sending a file to S3. This speeds up writes by avoiding excessive processing passes on a file. It is mostly safe as the data of MergeTree tables is checksummed by ClickHouse anyway, and when S3 is accessed with HTTPS, the TLS layer already provides integrity while transferring through the network. While additional checksums on S3 give defense in depth.
   */
  s3_disable_checksum?: boolean;

  /**
   * Ignore absence of file if it does not exist when reading certain keys. Possible values: - 1 — `SELECT` returns empty result. - 0 — `SELECT` throws an exception.
   */
  s3_ignore_file_doesnt_exist?: boolean;

  /**
   * Maximum number of files that could be returned in batch by ListObject request
   */
  s3_list_object_keys_size?: number;

  /**
   * The maximum number of connections per server.
   */
  s3_max_connections?: number;

  /**
   * Max number of requests that can be issued simultaneously before hitting request per second limit. By default (0) equals to `s3_max_get_rps`
   */
  s3_max_get_burst?: number;

  /**
   * Limit on S3 GET request per second rate before throttling. Zero means unlimited.
   */
  s3_max_get_rps?: number;

  /**
   * The maximum number of a concurrent loaded parts in multipart upload request. 0 means unlimited.
   */
  s3_max_inflight_parts_for_one_file?: number;

  /**
   * Maximum part number number for s3 upload part.
   */
  s3_max_part_number?: number;

  /**
   * Max number of requests that can be issued simultaneously before hitting request per second limit. By default (0) equals to `s3_max_put_rps`
   */
  s3_max_put_burst?: number;

  /**
   * Limit on S3 PUT request per second rate before throttling. Zero means unlimited.
   */
  s3_max_put_rps?: number;

  /**
   * Maximum size for single-operation copy in s3. This setting is used only if s3_allow_multipart_copy is true.
   */
  s3_max_single_operation_copy_size?: number;

  /**
   * The maximum size of object to upload using singlepart upload to S3.
   */
  s3_max_single_part_upload_size?: number;

  /**
   * The maximum number of retries during single S3 read.
   */
  s3_max_single_read_retries?: number;

  /**
   * The maximum number of retries in case of unexpected errors during S3 write.
   */
  s3_max_unexpected_write_error_retries?: number;

  /**
   * The maximum size of part to upload during multipart upload to S3.
   */
  s3_max_upload_part_size?: number;

  /**
   * The minimum size of part to upload during multipart upload to S3.
   */
  s3_min_upload_part_size?: number;

  /**
   * Maximum number of `_path` values that can be extracted from query filters to use for file iteration instead of glob listing. 0 means disabled.
   * @since 26.1
   */
  s3_path_filter_limit?: number;

  /**
   * Idleness timeout for sending and receiving data to/from S3. Fail if a single TCP read or write call blocks for this long.
   */
  s3_request_timeout_ms?: number;

  /**
   * Enables or disables skipping empty files in [S3](../../engines/table-engines/integrations/s3.md) engine tables. Possible values: - 0 — `SELECT` throws an exception if empty file is not compatible with requested format. - 1 — `SELECT` returns empty result for empty file.
   */
  s3_skip_empty_files?: boolean;

  /**
   * When set to `true`, all threads executing S3 requests to the same backup endpoint are slowed down after any single s3 request encounters a retryable network error, such as socket timeout. When set to `false`, each thread handles S3 request backoff independently of the others.
   * @since 25.6
   */
  s3_slow_all_threads_after_network_error?: boolean;

  /**
   * The exact size of part to upload during multipart upload to S3 (some implementations does not supports variable size parts).
   */
  s3_strict_upload_part_size?: number;

  /**
   * Throw an error, when ListObjects request cannot match any files
   */
  s3_throw_on_zero_files_match?: boolean;

  /**
   * Enables or disables truncate before inserts in s3 engine tables. If disabled, an exception will be thrown on insert attempts if an S3 object already exists. Possible values: - 0 — `INSERT` query creates a new file or fail if file exists and s3_create_new_file_on_insert is not set. - 1 — `INSERT` query replaces existing content of the file with the new data. See more details [here](/integrations/s3#inserting-data).
   */
  s3_truncate_on_insert?: boolean;

  /**
   * Multiply s3_min_upload_part_size by this factor each time s3_multiply_parts_count_threshold parts were uploaded from a single write to S3.
   */
  s3_upload_part_size_multiply_factor?: number;

  /**
   * Each time this number of parts was uploaded to S3, s3_min_upload_part_size is multiplied by s3_upload_part_size_multiply_factor.
   */
  s3_upload_part_size_multiply_parts_count_threshold?: number;

  /**
   * When set to `true` than for all s3 requests first two attempts are made with low send and receive timeouts. When set to `false` than all attempts are made with identical timeouts.
   */
  s3_use_adaptive_timeouts?: boolean;

  /**
   * Enables s3 request settings validation. Possible values: - 1 — validate settings. - 0 — do not validate settings.
   */
  s3_validate_request_settings?: boolean;

  /**
   * Default zookeeper path prefix for S3Queue engine
   */
  s3queue_default_zookeeper_path?: string;

  /**
   * Enable writing to system.s3queue_log. The value can be overwritten per table with table settings
   */
  s3queue_enable_logging_to_s3queue_log?: boolean;

  /**
   * Keeper fault injection probability for S3Queue.
   * @since 25.12
   */
  s3queue_keeper_fault_injection_probability?: number;

  /**
   * Migrate old metadata structure of S3Queue table to a new one
   * @since 25.2
   */
  s3queue_migrate_old_metadata_to_buckets?: boolean;

  /**
   * Use schema from cache for URL with last modification time validation (for URLs with Last-Modified header)
   */
  schema_inference_cache_require_modification_time_for_url?: boolean;

  /**
   * Use cache in schema inference while using azure table function
   */
  schema_inference_use_cache_for_azure?: boolean;

  /**
   * Use cache in schema inference while using file table function
   */
  schema_inference_use_cache_for_file?: boolean;

  /**
   * Use cache in schema inference while using hdfs table function
   */
  schema_inference_use_cache_for_hdfs?: boolean;

  /**
   * Use cache in schema inference while using s3 table function
   */
  schema_inference_use_cache_for_s3?: boolean;

  /**
   * Use cache in schema inference while using url table function
   */
  schema_inference_use_cache_for_url?: boolean;

  /**
   * Enable the bulk filtering algorithm for indices. It is expected to be always better, but we have this setting for compatibility and control.
   * @since 25.6
   */
  secondary_indices_enable_bulk_filtering?: boolean;

  /**
   * :::note This setting differ in behavior between SharedMergeTree and ReplicatedMergeTree, see [SharedMergeTree consistency](/cloud/reference/shared-merge-tree#consistency) for more information about the behavior of `select_sequential_consistency` in SharedMergeTree. ::: Enables or disables sequential consistency for `SELECT` queries. Requires `insert_quorum_parallel` to be disabled (enabled by default). Possible values: - 0 — Disabled. - 1 — Enabled. Usage When sequential consistency is enabled, ClickHouse allows the client to execute the `SELECT` query only for those replicas that contain data from all previous `INSERT` queries executed with `insert_quorum`. If the client refers to a partial replica, ClickHouse will generate an exception. The SELECT query will not include data that has not yet been written to the quorum of replicas. When `insert_quorum_parallel` is enabled (the default), then `select_sequential_consistency` does not work. This is because parallel `INSERT` queries can be written to different sets of quorum replicas so there is no guarantee a single replica will have received all writes. See also: - [insert_quorum](#insert_quorum) - [insert_quorum_timeout](#insert_quorum_timeout) - [insert_quorum_parallel](#insert_quorum_parallel)
   */
  select_sequential_consistency?: number;

  /**
   * Send server text logs with specified minimum level to client. Valid values: 'trace', 'debug', 'information', 'warning', 'error', 'fatal', 'none'
   */
  send_logs_level?: string;

  /**
   * Send server text logs with specified regexp to match log source name. Empty means all sources.
   */
  send_logs_source_regexp?: string;

  /**
   * Enables or disables sending of [ProfileEvents](/native-protocol/server.md#profile-events) packets to the client. This can be disabled to reduce network traffic for clients that do not require profile events. Possible values: - 0 — Disabled. - 1 — Enabled.
   * @since 25.12
   */
  send_profile_events?: boolean;

  /**
   * Enables or disables `X-ClickHouse-Progress` HTTP response headers in `clickhouse-server` responses. For more information, read the [HTTP interface description](/interfaces/http). Possible values: - 0 — Disabled. - 1 — Enabled.
   */
  send_progress_in_http_headers?: boolean;

  /**
   * Timeout for sending data to the network, in seconds. If a client needs to send some data but is not able to send any bytes in this interval, the exception is thrown. If you set this setting on the client, the 'receive_timeout' for the socket will also be set on the corresponding connection end on the server.
   */
  send_timeout?: number;

  /**
   * Serialize query plan for distributed processing
   * @since 25.5
   */
  serialize_query_plan?: boolean;

  /**
   * Serialize String values during aggregation with zero byte at the end. Enable to keep compatibility when querying cluster of incompatible versions.
   * @since 26.1
   */
  serialize_string_in_memory_with_zero_byte?: boolean;

  /**
   * Sets the implicit time zone of the current session or query. The implicit time zone is the time zone applied to values of type DateTime/DateTime64 which have no explicitly specified time zone. The setting takes precedence over the globally configured (server-level) implicit time zone. A value of '' (empty string) means that the implicit time zone of the current session or query is equal to the [server time zone](../server-configuration-parameters/settings.md/#timezone). You can use functions `timeZone()` and `serverTimeZone()` to get the session time zone and server time zone. Possible values: - Any time zone name from `system.time_zones`, e.g. `Europe/Berlin`, `UTC` or `Zulu` Examples: ```sql SELECT timeZone(), serverTimeZone() FORMAT CSV "Europe/Berlin","Europe/Berlin" ``` ```sql SELECT timeZone(), serverTimeZone() SETTINGS session_timezone = 'Asia/Novosibirsk' FORMAT CSV "Asia/Novosibirsk","Europe/Berlin" ``` Assign session time zone 'America/Denver' to the inner DateTime without explicitly specified time zone: ```sql SELECT toDateTime64(toDateTime64('1999-12-12 23:23:23.123', 3), 3, 'Europe/Zurich') SETTINGS session_timezone = 'America/Denver' FORMAT TSV 1999-12-13 07:23:23.123 ``` :::warning Not all functions that parse DateTime/DateTime64 respect `session_timezone`. This can lead to subtle errors. See the following example and explanation. ::: ```sql CREATE TABLE test_tz (`d` DateTime('UTC')) ENGINE = Memory AS SELECT toDateTime('2000-01-01 00:00:00', 'UTC'); SELECT *, timeZone() FROM test_tz WHERE d = toDateTime('2000-01-01 00:00:00') SETTINGS session_timezone = 'Asia/Novosibirsk' 0 rows in set. SELECT *, timeZone() FROM test_tz WHERE d = '2000-01-01 00:00:00' SETTINGS session_timezone = 'Asia/Novosibirsk' ┌───────────────────d─┬─timeZone()───────┐ │ 2000-01-01 00:00:00 │ Asia/Novosibirsk │ └─────────────────────┴──────────────────┘ ``` This happens due to different parsing pipelines: - `toDateTime()` without explicitly given time zone used in the first `SELECT` query honors setting `session_timezone` and the global time zone. - In the second query, a DateTime is parsed from a String, and inherits the type and time zone of the existing column`d`. Thus, setting `session_timezone` and the global time zone are not honored. **See also** - [timezone](../server-configuration-parameters/settings.md/#timezone)
   */
  session_timezone?: string;

  /**
   * Sets what happens when the amount of data exceeds one of the limits. Possible values: - `throw`: throw an exception (default). - `break`: stop executing the query and return the partial result, as if the source data ran out.
   */
  set_overflow_mode?: 'throw' | 'break';

  /**
   * Automatically synchronize set of data parts after MOVE|REPLACE|ATTACH partition operations in SMT tables. Cloud only
   * @since 25.2
   */
  shared_merge_tree_sync_parts_on_partition_operations?: boolean;

  /**
   * Allows calculating the [if](../../sql-reference/functions/conditional-functions.md/#if), [multiIf](../../sql-reference/functions/conditional-functions.md/#multiIf), [and](/sql-reference/functions/logical-functions#and), and [or](/sql-reference/functions/logical-functions#or) functions according to a [short scheme](https://en.wikipedia.org/wiki/Short-circuit_evaluation). This helps optimize the execution of complex expressions in these functions and prevent possible exceptions (such as division by zero when it is not expected). Possible values: - `enable` — Enables short-circuit function evaluation for functions that are suitable for it (can throw an exception or computationally heavy). - `force_enable` — Enables short-circuit function evaluation for all functions. - `disable` — Disables short-circuit function evaluation.
   */
  short_circuit_function_evaluation?: 'enable' | 'force_enable' | 'disable';

  /**
   * Optimizes evaluation of functions that return NULL when any argument is NULL. When the percentage of NULL values in the function's arguments exceeds the short_circuit_function_evaluation_for_nulls_threshold, the system skips evaluating the function row-by-row. Instead, it immediately returns NULL for all rows, avoiding unnecessary computation.
   * @since 24.12
   */
  short_circuit_function_evaluation_for_nulls?: boolean;

  /**
   * Ratio threshold of NULL values to execute functions with Nullable arguments only on rows with non-NULL values in all arguments. Applies when setting short_circuit_function_evaluation_for_nulls is enabled. When the ratio of rows containing NULL values to the total number of rows exceeds this threshold, these rows containing NULL values will not be evaluated.
   * @since 24.12
   */
  short_circuit_function_evaluation_for_nulls_threshold?: string;

  /**
   * Enables showing data lake catalogs in system tables.
   * @since 25.9
   */
  show_data_lake_catalogs_in_system_tables?: boolean;

  /**
   * Show internal auxiliary processes in the `SHOW PROCESSLIST` query output. Internal processes include dictionary reloads, refreshable materialized view reloads, auxiliary `SELECT`s executed in `SHOW ...` queries, auxiliary `CREATE DATABASE ...` queries executed internally to accommodate broken tables and more.
   * @since 25.12
   */
  show_processlist_include_internal?: boolean;

  /**
   * Sets the `SHOW TABLE` query display. Possible values: - 0 — The query will be displayed without table UUID. - 1 — The query will be displayed with table UUID.
   */
  show_table_uuid_in_table_create_query_if_not_nil?: boolean;

  /**
   * For single JOIN in case of identifier ambiguity prefer left table
   */
  single_join_prefer_left_table?: boolean;

  /**
   * Redundant aliases are not used (substituted) in user-defined functions in order to simplify it's usage. Possible values: - 1 — The aliases are skipped (substituted) in UDFs. - 0 — The aliases are not skipped (substituted) in UDFs. **Example** The difference between enabled and disabled: Query: ```sql SET skip_redundant_aliases_in_udf = 0; CREATE FUNCTION IF NOT EXISTS test_03274 AS ( x ) -> ((x + 1 as y, y + 2)); EXPLAIN SYNTAX SELECT test_03274(4 + 2); ``` Result: ```text SELECT ((4 + 2) + 1 AS y, y + 2) ``` Query: ```sql SET skip_redundant_aliases_in_udf = 1; CREATE FUNCTION IF NOT EXISTS test_03274 AS ( x ) -> ((x + 1 as y, y + 2)); EXPLAIN SYNTAX SELECT test_03274(4 + 2); ``` Result: ```text SELECT ((4 + 2) + 1, ((4 + 2) + 1) + 2) ```
   * @since 25.2
   */
  skip_redundant_aliases_in_udf?: boolean;

  /**
   * Enables or disables silently skipping of unavailable shards. Shard is considered unavailable if all its replicas are unavailable. A replica is unavailable in the following cases: - ClickHouse can't connect to replica for any reason. When connecting to a replica, ClickHouse performs several attempts. If all these attempts fail, the replica is considered unavailable. - Replica can't be resolved through DNS. If replica's hostname can't be resolved through DNS, it can indicate the following situations: - Replica's host has no DNS record. It can occur in systems with dynamic DNS, for example, [Kubernetes](https://kubernetes.io), where nodes can be unresolvable during downtime, and this is not an error. - Configuration error. ClickHouse configuration file contains a wrong hostname. Possible values: - 1 — skipping enabled. If a shard is unavailable, ClickHouse returns a result based on partial data and does not report node availability issues. - 0 — skipping disabled. If a shard is unavailable, ClickHouse throws an exception.
   */
  skip_unavailable_shards?: boolean;

  /**
   * Time to sleep after receiving query in TCPHandler
   */
  sleep_after_receiving_query_ms?: number;

  /**
   * Time to sleep in sending data in TCPHandler
   */
  sleep_in_send_data_ms?: number;

  /**
   * Time to sleep in sending tables status response in TCPHandler
   */
  sleep_in_send_tables_status_ms?: number;

  /**
   * Sets what happens if the number of rows received before sorting exceeds one of the limits. Possible values: - `throw`: throw an exception. - `break`: stop executing the query and return the partial result.
   */
  sort_overflow_mode?: 'throw' | 'break';

  /**
   * Split intersecting parts ranges into layers during FINAL optimization
   */
  split_intersecting_parts_ranges_into_layers_final?: boolean;

  /**
   * Split parts ranges into intersecting and non intersecting during FINAL optimization
   */
  split_parts_ranges_into_intersecting_and_non_intersecting_final?: boolean;

  /**
   * Controls whether function [splitBy*()](../../sql-reference/functions/splitting-merging-functions.md) with argument `max_substrings` > 0 will include the remaining string in the last element of the result array. Possible values: - `0` - The remaining string will not be included in the last element of the result array. - `1` - The remaining string will be included in the last element of the result array. This is the behavior of Spark's [`split()`](https://spark.apache.org/docs/3.1.2/api/python/reference/api/pyspark.sql.functions.split.html) function and Python's ['string.split()'](https://docs.python.org/3/library/stdtypes.html#str.split) method.
   */
  splitby_max_substrings_includes_remaining_string?: boolean;

  /**
   * On server startup, prevent scheduling of refreshable materialized views, as if with SYSTEM STOP VIEWS. You can manually start them with `SYSTEM START VIEWS` or `SYSTEM START VIEW <name>` afterwards. Also applies to newly created views. Has no effect on non-refreshable materialized views.
   */
  stop_refreshable_materialized_views_on_startup?: boolean;

  /**
   * Method of reading data from storage file, one of: `read`, `pread`, `mmap`. The mmap method does not apply to clickhouse-server (it's intended for clickhouse-local).
   */
  storage_file_read_method?: string;

  /**
   * Maximum time to read from a pipe for receiving information from the threads when querying the `system.stack_trace` table. This setting is used for testing purposes and not meant to be changed by users.
   */
  storage_system_stack_trace_pipe_read_timeout_ms?: number;

  /**
   * Works for tables with streaming in the case of a timeout, or when a thread generates [max_insert_block_size](#max_insert_block_size) rows. The default value is 7500. The smaller the value, the more often data is flushed into the table. Setting the value too low leads to poor performance.
   */
  stream_flush_interval_ms?: number;

  /**
   * Allow direct SELECT query for Kafka, RabbitMQ, FileLog, Redis Streams, S3Queue, AzureQueue and NATS engines. In case there are attached materialized views, SELECT query is not allowed even if this setting is enabled. If there are no attached materialized views, enabling this setting allows to read data. Be aware that usually the read data is removed from the queue. In order to avoid removing read data the related engine settings should be configured properly.
   */
  stream_like_engine_allow_direct_select?: boolean;

  /**
   * When stream-like engine reads from multiple queues, the user will need to select one queue to insert into when writing. Used by Redis Streams and NATS.
   */
  stream_like_engine_insert_queue?: string;

  /**
   * Timeout for polling data from/to streaming storages.
   */
  stream_poll_timeout_ms?: number;

  /**
   * Allows to select zero-valued events from [`system.events`](../../operations/system-tables/events.md). Some monitoring systems require passing all the metrics values to them for each checkpoint, even if the metric value is zero. Possible values: - 0 — Disabled. - 1 — Enabled. **Examples** Query ```sql SELECT * FROM system.events WHERE event='QueryMemoryLimitExceeded'; ``` Result ```text Ok. ``` Query ```sql SET system_events_show_zero_values = 1; SELECT * FROM system.events WHERE event='QueryMemoryLimitExceeded'; ``` Result ```text ┌─event────────────────────┬─value─┬─description───────────────────────────────────────────┐ │ QueryMemoryLimitExceeded │ 0 │ Number of times when memory limit exceeded for query. │ └──────────────────────────┴───────┴───────────────────────────────────────────────────────┘ ```
   */
  system_events_show_zero_values?: boolean;

  /**
   * Only has an effect in ClickHouse Cloud. Allow reading from distributed cache via table engines / table functions (s3, azure, etc)
   * @since 25.8
   */
  table_engine_read_through_distributed_cache?: boolean;

  /**
   * Sets the maximum number of addresses generated from patterns for the [remote](../../sql-reference/table-functions/remote.md) function. Possible values: - Positive integer.
   */
  table_function_remote_max_addresses?: number;

  /**
   * The time in seconds the connection needs to remain idle before TCP starts sending keepalive probes
   */
  tcp_keep_alive_timeout?: number;

  /**
   * Wait time to lock cache for space reservation for temporary data in filesystem cache
   */
  temporary_data_in_cache_reserve_space_wait_lock_timeout_milliseconds?: number;

  /**
   * Sets compression codec for temporary files used in sorting and joining operations on disk. Possible values: - LZ4 — [LZ4](https://en.wikipedia.org/wiki/LZ4_(compression_algorithm)) compression is applied. - NONE — No compression is applied.
   */
  temporary_files_codec?: string;

  /**
   * Maximal selectivity of the filter to use the hint built from the inverted text index.
   * @since 26.1
   */
  text_index_hint_max_selectivity?: number;

  /**
   * For testing purposes, enables or disables usage of bloom filter in text index.
   * @since 25.10
   */
  text_index_use_bloom_filter?: boolean;

  /**
   * Throw exception on INSERT query when the setting `deduplicate_blocks_in_dependent_materialized_views` is enabled along with `async_insert`. It guarantees correctness, because these features can't work together.
   */
  throw_if_deduplication_in_dependent_materialized_views_enabled_with_async_insert?: boolean;

  /**
   * Allows or forbids empty INSERTs, enabled by default (throws an error on an empty insert). Only applies to INSERTs using [`clickhouse-client`](/interfaces/cli) or using the [gRPC interface](/interfaces/grpc).
   */
  throw_if_no_data_to_insert?: boolean;

  /**
   * Ignore error from cache when caching on write operations (INSERT, merges)
   */
  throw_on_error_from_cache_on_write_operations?: boolean;

  /**
   * Allows you to control the behaviour when `max_partitions_per_insert_block` is reached. Possible values: - `true` - When an insert block reaches `max_partitions_per_insert_block`, an exception is raised. - `false` - Logs a warning when `max_partitions_per_insert_block` is reached. :::tip This can be useful if you're trying to understand the impact on users when changing [`max_partitions_per_insert_block`](/operations/settings/settings#max_partitions_per_insert_block). :::
   */
  throw_on_max_partitions_per_insert_block?: boolean;

  /**
   * Throw exception if unsupported query is used inside transaction
   */
  throw_on_unsupported_query_inside_transaction?: boolean;

  /**
   * Checks that execution speed is not too slow (no less than `min_execution_speed`), after the specified time in seconds has expired.
   */
  timeout_before_checking_execution_speed?: number;

  /**
   * Sets what to do if the query is run longer than the `max_execution_time` or the estimated running time is longer than `max_estimated_execution_time`. Possible values: - `throw`: throw an exception (default). - `break`: stop executing the query and return the partial result, as if the source data ran out.
   */
  timeout_overflow_mode?: 'throw' | 'break';

  /**
   * Sets what happens when the query in leaf node run longer than `max_execution_time_leaf`. Possible values: - `throw`: throw an exception (default). - `break`: stop executing the query and return the partial result, as if the source data ran out.
   */
  timeout_overflow_mode_leaf?: 'throw' | 'break';

  /**
   * The threshold for `totals_mode = 'auto'`. See the section "WITH TOTALS modifier".
   */
  totals_auto_threshold?: number;

  /**
   * How to calculate TOTALS when HAVING is present, as well as when max_rows_to_group_by and group_by_overflow_mode = 'any' are present. See the section "WITH TOTALS modifier".
   */
  totals_mode?: 'before_having' | 'after_having_exclusive' | 'after_having_inclusive' | 'after_having_auto';

  /**
   * Enables or disables collecting stacktraces on each update of profile events along with the name of profile event and the value of increment and sending them into [trace_log](/operations/system-tables/trace_log). Possible values: - 1 — Tracing of profile events enabled. - 0 — Tracing of profile events disabled.
   */
  trace_profile_events?: boolean;

  /**
   * Sets what happens when the amount of data exceeds one of the limits. Possible values: - `throw`: throw an exception (default). - `break`: stop executing the query and return the partial result, as if the source data ran out.
   */
  transfer_overflow_mode?: 'throw' | 'break';

  /**
   * Enables equality of [NULL](/sql-reference/syntax#null) values for [IN](../../sql-reference/operators/in.md) operator. By default, `NULL` values can't be compared because `NULL` means undefined value. Thus, comparison `expr = NULL` must always return `false`. With this setting `NULL = NULL` returns `true` for `IN` operator. Possible values: - 0 — Comparison of `NULL` values in `IN` operator returns `false`. - 1 — Comparison of `NULL` values in `IN` operator returns `true`. **Example** Consider the `null_in` table: ```text ┌──idx─┬─────i─┐ │ 1 │ 1 │ │ 2 │ NULL │ │ 3 │ 3 │ └──────┴───────┘ ``` Query: ```sql SELECT idx, i FROM null_in WHERE i IN (1, NULL) SETTINGS transform_null_in = 0; ``` Result: ```text ┌──idx─┬────i─┐ │ 1 │ 1 │ └──────┴──────┘ ``` Query: ```sql SELECT idx, i FROM null_in WHERE i IN (1, NULL) SETTINGS transform_null_in = 1; ``` Result: ```text ┌──idx─┬─────i─┐ │ 1 │ 1 │ │ 2 │ NULL │ └──────┴───────┘ ``` **See Also** - [NULL Processing in IN Operators](/sql-reference/operators/in#null-processing)
   */
  transform_null_in?: boolean;

  /**
   * Traverse frozen data (shadow directory) in addition to actual table data when query system.remote_data_paths
   */
  traverse_shadow_remote_data_paths?: boolean;

  /**
   * Sets a mode for combining `SELECT` query results. The setting is only used when shared with [UNION](../../sql-reference/statements/select/union.md) without explicitly specifying the `UNION ALL` or `UNION DISTINCT`. Possible values: - `'DISTINCT'` — ClickHouse outputs rows as a result of combining queries removing duplicate rows. - `'ALL'` — ClickHouse outputs all rows as a result of combining queries including duplicate rows. - `''` — ClickHouse generates an exception when used with `UNION`. See examples in [UNION](../../sql-reference/statements/select/union.md).
   */
  union_default_mode?: 'ALL' | 'DISTINCT';

  /**
   * Send unknown packet instead of data Nth data packet
   */
  unknown_packet_in_send_data?: number;

  /**
   * Determines the behavior of concurrent update queries. Possible values: - `sync` - run sequentially all `UPDATE` queries. - `auto` - run sequentially only `UPDATE` queries with dependencies between columns updated in one query and columns used in expressions of another query. - `async` - do not synchronize update queries.
   * @since 25.6
   */
  update_parallel_mode?: 'sync' | 'async' | 'auto';

  /**
   * If true set of parts is updated to the latest version before execution of update.
   * @since 25.6
   */
  update_sequential_consistency?: boolean;

  /**
   * Use async and potentially multithreaded execution of materialized view query, can speedup views processing during INSERT, but also consume more memory.
   * @since 25.1
   */
  use_async_executor_for_materialized_views?: boolean;

  /**
   * Enables caching of rows number during count from files in table functions `file`/`s3`/`url`/`hdfs`/`azureBlobStorage`. Enabled by default.
   */
  use_cache_for_count_from_files?: boolean;

  /**
   * Use client timezone for interpreting DateTime string values, instead of adopting server timezone.
   */
  use_client_time_zone?: boolean;

  /**
   * Uses compact format for storing blocks for background (`distributed_foreground_insert`) INSERT into tables with `Distributed` engine. Possible values: - 0 — Uses `user[:password]@host:port#default_database` directory format. - 1 — Uses `[shard{shard_index}[_replica{replica_index}]]` directory format. :::note - with `use_compact_format_in_distributed_parts_names=0` changes from cluster definition will not be applied for background INSERT. - with `use_compact_format_in_distributed_parts_names=1` changing the order of the nodes in the cluster definition, will change the `shard_index`/`replica_index` so be aware. :::
   */
  use_compact_format_in_distributed_parts_names?: boolean;

  /**
   * Respect the server's concurrency control (see the `concurrent_threads_soft_limit_num` and `concurrent_threads_soft_limit_ratio_to_cores` global server settings). If disabled, it allows using a larger number of threads even if the server is overloaded (not recommended for normal usage, and needed mostly for tests).
   */
  use_concurrency_control?: boolean;

  /**
   * Enables hedged requests logic for remote queries. It allows to establish many connections with different replicas for query. New connection is enabled in case existent connection(s) with replica(s) were not established within `hedged_connection_timeout` or no data was received within `receive_data_timeout`. Query uses the first connection which send non empty progress packet (or data packet, if `allow_changing_replica_until_first_data_packet`); other connections are cancelled. Queries with `max_parallel_replicas > 1` are supported. Enabled by default. Cloud default value: `1`
   */
  use_hedged_requests?: boolean;

  /**
   * When enabled, ClickHouse will detect Hive-style partitioning in path (`/name=value/`) in file-like table engines [File](/sql-reference/table-functions/file#hive-style-partitioning)/[S3](/sql-reference/table-functions/s3#hive-style-partitioning)/[URL](/sql-reference/table-functions/url#hive-style-partitioning)/[HDFS](/sql-reference/table-functions/hdfs#hive-style-partitioning)/[AzureBlobStorage](/sql-reference/table-functions/azureBlobStorage#hive-style-partitioning) and will allow to use partition columns as virtual columns in the query. These virtual columns will have the same names as in the partitioned path, but starting with `_`.
   */
  use_hive_partitioning?: boolean;

  /**
   * If turned on, iceberg table function and iceberg storage may utilize the iceberg metadata files cache. Possible values: - 0 - Disabled - 1 - Enabled
   * @since 25.5
   */
  use_iceberg_metadata_files_cache?: boolean;

  /**
   * Use Iceberg partition pruning for Iceberg tables
   * @since 25.2
   */
  use_iceberg_partition_pruning?: boolean;

  /**
   * Try using an index if there is a subquery or a table expression on the right side of the IN operator.
   */
  use_index_for_in_with_subqueries?: boolean;

  /**
   * The maximum size of the set in the right-hand side of the IN operator to use table index for filtering. It allows to avoid performance degradation and higher memory usage due to the preparation of additional data structures for large queries. Zero means no limit.
   */
  use_index_for_in_with_subqueries_max_values?: number;

  /**
   * Enable pushing OR-connected parts of JOIN conditions down to the corresponding input sides ("partial pushdown"). This allows storage engines to filter earlier, which can reduce data read. The optimization is semantics-preserving and is applied only when each top-level OR branch contributes at least one deterministic predicate for the target side.
   * @since 25.11
   */
  use_join_disjunctions_push_down?: boolean;

  /**
   * When enabled, allows to use legacy toTime function, which converts a date with time to a certain fixed date, while preserving the time. Otherwise, uses a new toTime function, that converts different type of data into the Time type. The old legacy function is also unconditionally accessible as toTimeWithFixedDate.
   * @since 25.6
   */
  use_legacy_to_time?: boolean;

  /**
   * Use userspace page cache for remote disks that don't have filesystem cache enabled.
   */
  use_page_cache_for_disks_without_file_cache?: boolean;

  /**
   * Use userspace page cache when distributed cache is used.
   * @since 25.4
   */
  use_page_cache_with_distributed_cache?: boolean;

  /**
   * Use Paimon partition pruning for Paimon table functions
   * @since 26.1
   */
  use_paimon_partition_pruning?: boolean;

  /**
   * If turned on, `SELECT` queries may utilize the [query cache](../query-cache.md). Parameters [enable_reads_from_query_cache](#enable_reads_from_query_cache) and [enable_writes_to_query_cache](#enable_writes_to_query_cache) control in more detail how the cache is used. Possible values: - 0 - Disabled - 1 - Enabled
   */
  use_query_cache?: boolean;

  /**
   * Enable the [query condition cache](/operations/query-condition-cache). The cache stores ranges of granules in data parts which do not satisfy the condition in the `WHERE` clause, and reuse this information as an ephemeral index for subsequent queries. Possible values: - 0 - Disabled - 1 - Enabled
   * @since 25.4
   */
  use_query_condition_cache?: boolean;

  /**
   * Use roaring bitmap for iceberg positional deletes.
   * @since 25.9
   */
  use_roaring_bitmap_iceberg_positional_deletes?: boolean;

  /**
   * Use data skipping indexes during query execution. Possible values: - 0 — Disabled. - 1 — Enabled.
   */
  use_skip_indexes?: boolean;

  /**
   * Evaluate WHERE filters with mixed AND and OR conditions using skip indexes. Example: WHERE A = 5 AND (B = 5 OR C = 5). If disabled, skip indexes are still used to evaluate WHERE conditions but they must only contain AND-ed clauses. Possible values: - 0 — Disabled. - 1 — Enabled.
   * @since 26.1
   */
  use_skip_indexes_for_disjunctions?: boolean;

  /**
   * Enable using data skipping indexes for TopK filtering. When enabled, if a minmax skip index exists on the column in `ORDER BY <column> LIMIT n` query, optimizer will attempt to use the minmax index to skip granules that are not relevant for the final result . This can reduce query latency. Possible values: - 0 — Disabled. - 1 — Enabled.
   * @since 26.1
   */
  use_skip_indexes_for_top_k?: boolean;

  /**
   * Controls whether skipping indexes are used when executing a query with the FINAL modifier. Skip indexes may exclude rows (granules) containing the latest data, which could lead to incorrect results from a query with the FINAL modifier. When this setting is enabled, skipping indexes are applied even with the FINAL modifier, potentially improving performance but with the risk of missing recent updates. This setting should be enabled in sync with the setting use_skip_indexes_if_final_exact_mode (default is enabled). Possible values: - 0 — Disabled. - 1 — Enabled.
   */
  use_skip_indexes_if_final?: boolean;

  /**
   * Controls whether granules returned by a skipping index are expanded in newer parts to return correct results when executing a query with the FINAL modifier. Using skip indexes may exclude rows (granules) containing the latest data which could lead to incorrect results. This setting can ensure that correct results are returned by scanning newer parts that have overlap with the ranges returned by the skip index. This setting should be disabled only if approximate results based on looking up the skip index are okay for an application. Possible values: - 0 — Disabled. - 1 — Enabled.
   * @since 25.6
   */
  use_skip_indexes_if_final_exact_mode?: boolean;

  /**
   * Enable using data skipping indexes during data reading. When enabled, skip indexes are evaluated dynamically at the time each data granule is being read, rather than being analyzed in advance before query execution begins. This can reduce query startup latency. Possible values: - 0 — Disabled. - 1 — Enabled.
   * @since 25.10
   */
  use_skip_indexes_on_data_read?: boolean;

  /**
   * Use statistics cache in a query to avoid the overhead of loading statistics of every parts
   * @since 25.12
   */
  use_statistics_cache?: boolean;

  /**
   * Use structure from insertion table instead of schema inference from data. Possible values: 0 - disabled, 1 - enabled, 2 - auto
   */
  use_structure_from_insertion_table_in_table_functions?: number;

  /**
   * Whether to use a cache of deserialized text index dictionary block. Using the text index dictionary block cache can significantly reduce latency and increase throughput when working with a large number of text index queries.
   * @since 25.12
   */
  use_text_index_dictionary_cache?: boolean;

  /**
   * Whether to use a cache of deserialized text index header. Using the text index header cache can significantly reduce latency and increase throughput when working with a large number of text index queries.
   * @since 25.12
   */
  use_text_index_header_cache?: boolean;

  /**
   * Whether to use a cache of deserialized text index posting lists. Using the text index postings cache can significantly reduce latency and increase throughput when working with a large number of text index queries.
   * @since 25.12
   */
  use_text_index_postings_cache?: boolean;

  /**
   * Enable dynamic filtering optimization when executing a `ORDER BY <column> LIMIT n` query. When enabled, the query executor will try to skip granules and rows that will not be part of the final `top N` rows in the resultset. This optimization is dynamic in nature and latency improvements depends on data distribution and presence of other predicates in the query. Possible values: - 0 — Disabled. - 1 — Enabled.
   * @since 26.1
   */
  use_top_k_dynamic_filtering?: boolean;

  /**
   * Whether to use a cache of uncompressed blocks. Accepts 0 or 1. By default, 0 (disabled). Using the uncompressed cache (only for tables in the MergeTree family) can significantly reduce latency and increase throughput when working with a large number of short queries. Enable this setting for users who send frequent short requests. Also pay attention to the [uncompressed_cache_size](/operations/server-configuration-parameters/settings#uncompressed_cache_size) configuration parameter (only set in the config file) – the size of uncompressed cache blocks. By default, it is 8 GiB. The uncompressed cache is filled in as needed and the least-used data is automatically deleted. For queries that read at least a somewhat large volume of data (one million rows or more), the uncompressed cache is disabled automatically to save space for truly small queries. This means that you can keep the 'use_uncompressed_cache' setting always set to 1.
   */
  use_uncompressed_cache?: boolean;

  /**
   * Allows to use `Variant` type as a result type for [if](../../sql-reference/functions/conditional-functions.md/#if)/[multiIf](../../sql-reference/functions/conditional-functions.md/#multiIf)/[array](../../sql-reference/functions/array-functions.md)/[map](../../sql-reference/functions/tuple-map-functions.md) functions when there is no common type for argument types. Example: ```sql SET use_variant_as_common_type = 1; SELECT toTypeName(if(number % 2, number, range(number))) as variant_type FROM numbers(1); SELECT if(number % 2, number, range(number)) as variant FROM numbers(5); ``` ```text ┌─variant_type───────────────────┐ │ Variant(Array(UInt64), UInt64) │ └────────────────────────────────┘ ┌─variant───┐ │ [] │ │ 1 │ │ [0,1] │ │ 3 │ │ [0,1,2,3] │ └───────────┘ ``` ```sql SET use_variant_as_common_type = 1; SELECT toTypeName(multiIf((number % 4) = 0, 42, (number % 4) = 1, [1, 2, 3], (number % 4) = 2, 'Hello, World!', NULL)) AS variant_type FROM numbers(1); SELECT multiIf((number % 4) = 0, 42, (number % 4) = 1, [1, 2, 3], (number % 4) = 2, 'Hello, World!', NULL) AS variant FROM numbers(4); ``` ```text ─variant_type─────────────────────────┐ │ Variant(Array(UInt8), String, UInt8) │ └──────────────────────────────────────┘ ┌─variant───────┐ │ 42 │ │ [1,2,3] │ │ Hello, World! │ │ ᴺᵁᴸᴸ │ └───────────────┘ ``` ```sql SET use_variant_as_common_type = 1; SELECT toTypeName(array(range(number), number, 'str_' || toString(number))) as array_of_variants_type from numbers(1); SELECT array(range(number), number, 'str_' || toString(number)) as array_of_variants FROM numbers(3); ``` ```text ┌─array_of_variants_type────────────────────────┐ │ Array(Variant(Array(UInt64), String, UInt64)) │ └───────────────────────────────────────────────┘ ┌─array_of_variants─┐ │ [[],0,'str_0'] │ │ [[0],1,'str_1'] │ │ [[0,1],2,'str_2'] │ └───────────────────┘ ``` ```sql SET use_variant_as_common_type = 1; SELECT toTypeName(map('a', range(number), 'b', number, 'c', 'str_' || toString(number))) as map_of_variants_type from numbers(1); SELECT map('a', range(number), 'b', number, 'c', 'str_' || toString(number)) as map_of_variants FROM numbers(3); ``` ```text ┌─map_of_variants_type────────────────────────────────┐ │ Map(String, Variant(Array(UInt64), String, UInt64)) │ └─────────────────────────────────────────────────────┘ ┌─map_of_variants───────────────┐ │ {'a':[],'b':0,'c':'str_0'} │ │ {'a':[0],'b':1,'c':'str_1'} │ │ {'a':[0,1],'b':2,'c':'str_2'} │ └───────────────────────────────┘ ```
   */
  use_variant_as_common_type?: boolean;

  /**
   * Columns preceding WITH FILL columns in ORDER BY clause form sorting prefix. Rows with different values in sorting prefix are filled independently
   */
  use_with_fill_by_sorting_prefix?: boolean;

  /**
   * If enabled, validate enum literals in operators like `IN`, `NOT IN`, `==`, `!=` against the enum type and throw an exception if the literal is not a valid enum value.
   * @since 25.2
   */
  validate_enum_literals_in_operators?: boolean;

  /**
   * Validate mutation queries before accepting them. Mutations are executed in the background, and running an invalid query will cause mutations to get stuck, requiring manual intervention. Only change this setting if you encounter a backward-incompatible bug.
   * @since 24.12
   */
  validate_mutation_query?: boolean;

  /**
   * Enables or disables throwing an exception in the [pointInPolygon](/sql-reference/functions/geo/coordinates#pointinpolygon) function, if the polygon is self-intersecting or self-tangent. Possible values: - 0 — Throwing an exception is disabled. `pointInPolygon` accepts invalid polygons and returns possibly incorrect results for them. - 1 — Throwing an exception is enabled.
   */
  validate_polygons?: boolean;

  /**
   * If a vector search query has a WHERE clause, this setting determines if it is evaluated first (pre-filtering) OR if the vector similarity index is checked first (post-filtering). Possible values: - 'auto' - Postfiltering (the exact semantics may change in future). - 'postfilter' - Use vector similarity index to identify the nearest neighbours, then apply other filters - 'prefilter' - Evaluate other filters first, then perform brute-force search to identify neighbours.
   * @since 25.6
   */
  vector_search_filter_strategy?: 'auto' | 'prefilter' | 'postfilter';

  /**
   * Multiply the number of fetched nearest neighbors from the vector similarity index by this number. Only applied for post-filtering with other predicates or if setting 'vector_search_with_rescoring = 1'.
   * @since 25.9
   */
  vector_search_index_fetch_multiplier?: number;

  /**
   * If ClickHouse performs rescoring for queries that use the vector similarity index. Without rescoring, the vector similarity index returns the rows containing the best matches directly. With rescoring, the rows are extrapolated to granule level and all rows in the granule are checked again. In most situations, rescoring helps only marginally with accuracy but it deteriorates performance of vector search queries significantly. Note: A query run without rescoring and with parallel replicas enabled may fall back to rescoring.
   * @since 25.9
   */
  vector_search_with_rescoring?: boolean;

  /**
   * Wait for committed changes to become actually visible in the latest snapshot
   */
  wait_changes_become_visible_after_commit_mode?: 'async' | 'wait' | 'wait_unknown';

  /**
   * If true wait for processing of asynchronous insertion
   */
  wait_for_async_insert?: boolean;

  /**
   * Timeout for waiting for processing asynchronous insertion
   */
  wait_for_async_insert_timeout?: number;

  /**
   * Timeout for waiting for window view fire signal in event time processing
   */
  wait_for_window_view_fire_signal_timeout?: number;

  /**
   * The clean interval of window view in seconds to free outdated data.
   */
  window_view_clean_interval?: number;

  /**
   * The heartbeat interval in seconds to indicate watch query is alive.
   */
  window_view_heartbeat_interval?: number;

  /**
   * Name of workload to be used to access resources
   */
  workload?: string;

  /**
   * Write full paths (including s3://) into iceberg metadata files.
   * @since 25.9
   */
  write_full_path_in_iceberg_metadata?: boolean;

  /**
   * Only has an effect in ClickHouse Cloud. Allow writing to distributed cache (writing to s3 will also be done by distributed cache)
   */
  write_through_distributed_cache?: boolean;

  /**
   * Only has an effect in ClickHouse Cloud. Set buffer size for write-through distributed cache. If 0, will use buffer size which would have been used if there was not distributed cache.
   * @since 25.8
   */
  write_through_distributed_cache_buffer_size?: number;

  /**
   * Allows you to select the max window log of ZSTD (it will not be used for MergeTree family)
   */
  zstd_window_log_max?: number;

  /** Index signature for unknown/custom settings */
  [key: string]: unknown;
}
