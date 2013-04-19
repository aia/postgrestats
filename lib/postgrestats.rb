require 'pg'
require 'logger'

class PostgreStats
  
  attr_accessor :config, :connection, :db, :log
  attr_accessor :table_gauges, :table_counters, :aggregate_gauges, :aggregate_counters
  
  def initialize(config, logger = nil)
    @config = config
    
    initialize_log unless logger
    @log = logger if logger
    @log.error("Logging started")
    
    initialize_metrics
  end
  
  def initialize_log
    @config['log'] = {
      'file' => STDOUT,
      'level' => 'INFO'
    }.merge(@config['log'] || {})
    
    log_initialize = [@config['log']['file']]
    log_initialize << @config['log']['shift_age'] if @config['log']['shift_age']
    log_initialize << @config['log']['shift_size'] if @config['log']['shift_size']
    
    begin
      @log = Logger.new(*log_initialize)
      @log.level = Logger.const_get(@config['log']['level'])
    rescue Exception => e
      @config['log'] = {
        'file' => STDOUT,
        'level' => 'INFO'
      }
      @log = Logger.new(@config['log']['file'])
      @log.level = Logger.const_get(@config['log']['level'])
      @log.error("Caught a problem with log settings")
      @log.error("#{e.message}")
      @log.error("Setting log settings to defaults")
    end
  end
  
  def initialize_metrics
    @table_gauges = {
      'table_size' => {
        'units' => 'bytes',
        'handler' => :get_table_sizes
      },
      'index_size' => {
        'units' => 'bytes',
        'handler' => :get_index_sizes
      },
      'estimated_rows' => {
        'units' => 'rows',
        'handler' => :get_tables_estimated_rows
      },
      'dead' => {
        'units' => 'rows',
        'handler' => :get_dead_per_table
      }, 
      'last_vacuum' => {
        'units' => 'seconds since',
        'handler' => :get_last_autovacuum_per_table
      }
    }
    
    @table_counters = {
      'inserts' => {
        'units' => 'inserts/s',
        'ganglia_double' => true,
        'handler' => :get_inserts_per_table
      }, 
      'updates' => {
        'units' => 'updates/s',
        'ganglia_double' => true,
        'handler' => :get_updates_per_table
      },
      'deletes' => {
        'units' => 'deletes/s',
        'ganglia_double' => true,
        'handler' => :get_deletes_per_table
      },
      'hot_updates' => {
        'units' => 'updates/s',
        'ganglia_double' => true,
        'handler' => :get_hot_updates_per_table
      },
      'seq_scan' => {
        'units' => 'scans/s',
        'ganglia_double' => true,
        'handler' => :get_sequential_scans_per_table
      },
      'idx_scan' => {
        'units' => 'scans/s',
        'ganglia_double' => true,
        'handler' => :get_index_scans_per_table
      },
      'seq_tup_read' => {
        'units' => 'rows/s',
        'ganglia_double' => true,
        'handler' => :get_sequential_rows_read_per_table
      },
      'idx_tup_fetch' => {
        'units' => 'rows/s',
        'ganglia_double' => true,
        'handler' => :get_index_rows_fetched_per_table
      },
      'heap_blks_read' => {
        'units' => 'blocks/s',
        'ganglia_double' => true,
        'handler' => :get_heap_blocks_read_per_table
      },
      'heap_blks_hit' => {
        'units' => 'blocks/s',
        'ganglia_double' => true,
        'handler' => :get_heap_blocks_hit_per_table
      },
      'idx_blks_read' => {
        'units' => 'blocks/s',
        'ganglia_double' => true,
        'handler' => :get_index_blocks_read_per_table
      },
      'idx_blks_hit' => {
        'units' => 'blocks/s',
        'ganglia_double' => true,
        'handler' => :get_index_blocks_hit_per_table
      }
    }
    
    @aggregate_gauges = {
      'locks' => {
        'units' => 'locks',
        'handler' => :get_number_of_locks_per_database
      },
      'connections' => {
        'units' => 'connections',
        'handler' => :get_connections_per_database
      }
    }
    
    @aggregate_counters = {
      'blocks_fetched' => {
        'units' => 'blocks/s',
        'ganglia_double' => true,
        'handler' => :get_blocks_fetched_per_database
      },
      'blocks_hit' => {
        'units' => 'blocks/s',
        'ganglia_double' => true,
        'handler' => :get_blocks_hit_per_database
      },
      'commits' => {
        'units' => 'commits/s',
        'ganglia_double' => true,
        'handler' => :get_commits_per_database
      },
      'rollbacks' => {
        'units' => 'rollbacks/s',
        'ganglia_double' => true,
        'handler' => :get_rollbacks_per_database
      },
      'inserts' => {
        'units' => 'inserts/s',
        'ganglia_double' => true,
        'handler' => :get_inserts_per_database
      },
      'updates' => {
        'units' => 'updates/s',
        'ganglia_double' => true,
        'handler' => :get_updates_per_database
      },
      'deletes' => {
        'units' => 'deletes/s',
        'ganglia_double' => true,
        'handler' => :get_deletes_per_database
      }
    }
  end
  
  def connect(db)
    return if connection and (@db == db)
    
    connection.close if @db == db
    
    tmp_config ={
      'host' => @config['host'],
      'user' => @config['user'],
      'password' => @config['password'],
      'dbname' => db
    }
    
    @connection = PG.connect(tmp_config)
    @db = db
  end
  
  def run_query(db, query)
    connect(db)
    
    result = @connection.exec(query)
    fields = result.fields
    ret = []
    
    result.each do |row|
      item = {}
      fields.each { |field| item[field] = row[field] }
      ret << item
    end
    
    return ret
  end
  
  def get_table_metrics()
    return [table_gauges.keys, table_counters.keys].flatten
  end
  
  def get_aggregate_metrics()
    return [aggregate_gauges.keys, aggregate_counters.keys].flatten
  end
  
  def get_table_ganglia_double()
    return [
      table_gauges.select{ |k, v| v.has_key?('ganglia_double') }.keys,
      table_counters.select{ |k, v| v.has_key?('ganglia_double') }.keys
    ].flatten
  end
  
  def get_aggregate_ganglia_double()
    return [
      aggregate_gauges.select{ |k, v| v.has_key?('ganglia_double') }.keys,
      aggregate_counters.select{ |k, v| v.has_key?('ganglia_double') }.keys
    ].flatten
  end
  
  def get_databases()
    query = <<-END_GET_DATABASES_QUERY
      SELECT pg_database.datname AS "Database", pg_user.usename AS "Owner"
      FROM pg_database, pg_user
      WHERE pg_database.datdba = pg_user.usesysid
      UNION
      SELECT pg_database.datname AS "Database", NULL AS "Owner"
      FROM pg_database
      WHERE pg_database.datdba NOT IN (SELECT usesysid FROM pg_user)
      ORDER BY "Database"
    END_GET_DATABASES_QUERY
    
    ret = run_query('postgres', query)
    
    ret.select!{ |v| not @config['exclude_dbs'].include?(v['Database']) }.map!{ |v| v['Database'] }
    
    return ret
  end
  
  def get_tables(db)
    query = <<-END_GET_TABLES_QUERY
      SELECT c.relname FROM pg_catalog.pg_class c
      LEFT JOIN pg_catalog.pg_namespace n ON n.oid = c.relnamespace
      WHERE c.relkind IN ('r','') AND n.nspname NOT IN ('pg_catalog', 'pg_toast')
      AND pg_catalog.pg_table_is_visible(c.oid)
    END_GET_TABLES_QUERY
    
    ret = run_query(db, query)
    
    ret.map!{ |v| v['relname'] }
    
    return ret
  end
  
   # * size per database/table
   def get_table_sizes(db)
     query = <<-END_GET_TABLE_SIZES_QUERY
       SELECT table_name,pg_relation_size(table_name) AS size
       FROM information_schema.tables
       WHERE table_schema NOT IN ('information_schema', 'pg_catalog') AND table_type = 'BASE TABLE'
     END_GET_TABLE_SIZES_QUERY
     
     ret = run_query(db, query)
     
     ret.map!{ |v| [v['table_name'], v['size'].to_i] }
     
     return Hash[*ret.flatten]
   end
   
   # * size of indexes
   def get_index_sizes(db)
     query = <<-END_GET_INDEX_SIZES_QUERY
       SELECT table_name,(pg_total_relation_size(table_name) - pg_relation_size(table_name)) AS size
       FROM information_schema.tables
       WHERE table_schema NOT IN ('information_schema', 'pg_catalog') AND table_type = 'BASE TABLE'
     END_GET_INDEX_SIZES_QUERY
     
     ret = run_query(db, query)
     
     ret.map!{ |v| [v['table_name'], v['size'].to_i] }
     
     return Hash[*ret.flatten]
   end
   
   # * size of individual indexes
   # SELECT c3.relname AS "Table",
   #        c2.relname AS "Index",
   #        pg_size_pretty(pg_relation_size(c3.relname::text)) AS "Data Size",
   #        pg_size_pretty(pg_relation_size(c2.relname::text)) AS "Index Size",
   #        pg_size_pretty(pg_total_relation_size(c3.relname::text)) AS "Total"
   #   FROM pg_class c2
   #   LEFT JOIN pg_index i  ON c2.oid = i.indexrelid
   #   LEFT JOIN pg_class c1 ON c1.oid = i.indrelid
   #  RIGHT OUTER JOIN pg_class c3 ON c3.oid = c1.oid
   #   LEFT JOIN pg_catalog.pg_namespace n ON n.oid = c3.relnamespace
   #  WHERE c3.relkind IN ('r','')
   #    AND n.nspname NOT IN ('pg_catalog', 'pg_toast')
   #    AND pg_catalog.pg_table_is_visible(c3.oid)
   #  ORDER BY c3.relpages DESC;
   
   # * estimated rows per database/table
   def get_table_estimated_rows(db, table)
     query = <<-END_GET_TABLE_ESTIMATED_ROWS_QUERY
       SELECT (
         CASE 
         WHEN reltuples > 0 THEN pg_relation_size('#{table}')/(8192*relpages::bigint/reltuples)
         ELSE 0
         END
       )::bigint AS estimated_row_count
       FROM pg_class
       WHERE oid = '#{table}'::regclass;
     END_GET_TABLE_ESTIMATED_ROWS_QUERY
     
     ret = run_query(db, query)
     
     return ret.first['estimated_row_count'].to_i
   end
   
   # * estimated rows for every table in the database
   def get_tables_estimated_rows(db)
     ret = {}
     
     get_tables(db).each do |table|
        ret[table] = get_table_estimated_rows(db, table)
     end
     
     return ret
   end
   
   # * number of connections
   def get_connections_per_database(db = nil)
     query = <<-END_GET_CONNECTIONS_PER_DATABASE_QUERY
       SELECT datname, count(*) FROM pg_stat_activity GROUP BY datname;
     END_GET_CONNECTIONS_PER_DATABASE_QUERY
     
     ret = run_query('postgres', query)
     
     ret.select!{ |v| not @config['exclude_dbs'].include?(v['datname']) }.map!{ |v| [v['datname'], v['count'].to_i] }
     
     if db
       res = Hash[*ret.flatten][db].to_i || 0
     else
       res = Hash[*ret.flatten]
     end
     
     return res
   end
   
   # * dead unused space taken up in a table or index
   # * number of seconds since the last checkpoint per database
   # * number of seconds since the last autovacuum per database/table
   
   def get_last_autovacuum_per_table(db)
     query = <<-END_GET_LAST_AUTOVACUUM_PER_DATABASE_QUERY
       SELECT current_database() AS datname, nspname AS sname, relname AS tname,
         CASE WHEN v IS NULL THEN -1 ELSE round(extract(epoch FROM now()-v)) END AS ltime
       FROM (
         SELECT nspname, relname, pg_stat_get_last_autovacuum_time(c.oid) AS v
         FROM pg_class c, pg_namespace n
         WHERE relkind = 'r'
         AND n.oid = c.relnamespace
         AND n.nspname <> 'information_schema'
         AND n.nspname = 'public'
         ORDER BY 3
        ) AS foo
     END_GET_LAST_AUTOVACUUM_PER_DATABASE_QUERY
     
     ret = run_query(db, query)
     
     ret.map!{ |v| [v['tname'], v['ltime']] }
     
     return Hash[*ret.flatten]
   end
   
   # * number of blocks fetched per database
   
   def get_blocks_fetched_per_database(db)
     query = <<-END_GET_BLOCKS_FETCHED_PER_DATABASE_QUERY
       SELECT pg_stat_get_db_blocks_fetched(oid)
       FROM pg_database
       WHERE datname = '#{db}'
     END_GET_BLOCKS_FETCHED_PER_DATABASE_QUERY
     
     ret = run_query(db, query)
     
     return ret.first['pg_stat_get_db_blocks_fetched'].to_i
   end
   
   # * number of blocks hit per database
   
   def get_blocks_hit_per_database(db)
     query = <<-END_GET_BLOCK_HITS_PER_DATABASE_QUERY
       SELECT pg_stat_get_db_blocks_hit(oid)
       FROM pg_database
       WHERE datname = '#{db}'
     END_GET_BLOCK_HITS_PER_DATABASE_QUERY
     
     ret = run_query(db, query)
     
     return ret.first['pg_stat_get_db_blocks_hit'].to_i
   end
   
   # * number of commits per database
   
   def get_commits_per_database(db)
     query = <<-END_GET_COMMITS_PER_DATABASE_QUERY
       SELECT pg_stat_get_db_xact_commit(oid)
       FROM pg_database
       WHERE datname = '#{db}'
     END_GET_COMMITS_PER_DATABASE_QUERY
     
     ret = run_query(db, query)
     
     return ret.first['pg_stat_get_db_xact_commit'].to_i
   end
   
   # * number of rollbacks per database
   
   def get_rollbacks_per_database(db)
     query = <<-END_GET_ROLLBACKS_PER_DATABASE_QUERY
       SELECT pg_stat_get_db_xact_rollback(oid)
       FROM pg_database
       WHERE datname = '#{db}'
     END_GET_ROLLBACKS_PER_DATABASE_QUERY
     
     ret = run_query(db, query)
     
     return ret.first['pg_stat_get_db_xact_rollback'].to_i
   end
   
   # * number of disk block reads per database
   # * number of buffer hits per database
   # * number of rows returned per database
   # * number of rows inserted per database
   
   def get_inserts_per_database(db)
     query = <<-END_GET_INSERTS_PER_DATABASE_QUERY
       SELECT pg_stat_get_db_tuples_inserted(oid)
       FROM pg_database
       WHERE datname = '#{db}'
     END_GET_INSERTS_PER_DATABASE_QUERY
     
     ret = run_query(db, query)
     
     return ret.first['pg_stat_get_db_tuples_inserted'].to_i
   end
   
   # * number of rows updates per database
   
   def get_updates_per_database(db)
     query = <<-END_GET_UPDATES_PER_DATABASE_QUERY
       SELECT pg_stat_get_db_tuples_updated(oid)
       FROM pg_database
       WHERE datname = '#{db}'
     END_GET_UPDATES_PER_DATABASE_QUERY
     
     ret = run_query(db, query)
     
     return ret.first['pg_stat_get_db_tuples_updated'].to_i
   end
   
   # * number of rows deleted per database
   
   def get_deletes_per_database(db)
     query = <<-END_GET_DELETES_PER_DATABASE_QUERY
       SELECT pg_stat_get_db_tuples_deleted(oid)
       FROM pg_database
       WHERE datname = '#{db}'
     END_GET_DELETES_PER_DATABASE_QUERY
     
     ret = run_query(db, query)
     
     return ret.first['pg_stat_get_db_tuples_deleted'].to_i
   end
   
   # * number of sequential scans per table
   # * number of index scans per table

   # * list of locks
   
   def get_locks_per_database(db)
     query = <<-END_GET_LOCKS_PER_DATABASE_QUERY
       SELECT pid, virtualxid, datname, relname, locktype, mode
       FROM pg_locks l 
       LEFT JOIN pg_database d ON (d.oid=l.database)
       LEFT JOIN pg_class c on (c.oid=l.relation)
       WHERE datname = '#{db}' AND NOT relname ~ 'pg_'
     END_GET_LOCKS_PER_DATABASE_QUERY
     
     ret = run_query(db, query)
     
     return ret
   end
   
   # * number of locks
   def get_number_of_locks_per_database(db)
     return get_locks_per_database(db).size
   end
   
   # * number of "idle in transaction" queries per database
   # * number of open transactions per database
   
   # * number of returned rows per table
   
   def get_returned_per_table(db)
      query = <<-END_GET_RETURNED_PER_TABLE_QUERY
        SELECT c.relname, pg_stat_get_tuples_returned(c.oid) FROM pg_catalog.pg_class c
        LEFT JOIN pg_catalog.pg_namespace n ON n.oid = c.relnamespace
        WHERE c.relkind IN ('r','') AND n.nspname NOT IN ('pg_catalog', 'pg_toast')
        AND pg_catalog.pg_table_is_visible(c.oid)
      END_GET_RETURNED_PER_TABLE_QUERY

      ret = run_query(db, query)

      ret.map!{ |v| [v['relname'], v['pg_stat_get_tuples_returned'].to_i] }

      return Hash[*ret.flatten]
   end
   
   # * number of fetched rows per table
   
   def get_fetched_per_table(db)
      query = <<-END_GET_FETCHED_PER_TABLE_QUERY
        SELECT c.relname, pg_stat_get_tuples_fetched(c.oid) FROM pg_catalog.pg_class c
        LEFT JOIN pg_catalog.pg_namespace n ON n.oid = c.relnamespace
        WHERE c.relkind IN ('r','') AND n.nspname NOT IN ('pg_catalog', 'pg_toast')
        AND pg_catalog.pg_table_is_visible(c.oid)
      END_GET_FETCHED_PER_TABLE_QUERY

      ret = run_query(db, query)

      ret.map!{ |v| [v['relname'], v['pg_stat_get_tuples_fetched'].to_i] }

      return Hash[*ret.flatten]
   end
   
   # * number of inserted rows per table
   
   def get_inserts_per_table(db)
      query = <<-END_GET_INSERTS_PER_TABLE_QUERY
        SELECT c.relname, pg_stat_get_tuples_inserted(c.oid) FROM pg_catalog.pg_class c
        LEFT JOIN pg_catalog.pg_namespace n ON n.oid = c.relnamespace
        WHERE c.relkind IN ('r','') AND n.nspname NOT IN ('pg_catalog', 'pg_toast')
        AND pg_catalog.pg_table_is_visible(c.oid)
      END_GET_INSERTS_PER_TABLE_QUERY

      ret = run_query(db, query)

      ret.map!{ |v| [v['relname'], v['pg_stat_get_tuples_inserted'].to_i] }

      return Hash[*ret.flatten]
   end
   
   # * number of updates rows per table
   
   def get_updates_per_table(db)
      query = <<-END_GET_UPDATES_PER_TABLE_QUERY
        SELECT c.relname, pg_stat_get_tuples_updated(c.oid) FROM pg_catalog.pg_class c
        LEFT JOIN pg_catalog.pg_namespace n ON n.oid = c.relnamespace
        WHERE c.relkind IN ('r','') AND n.nspname NOT IN ('pg_catalog', 'pg_toast')
        AND pg_catalog.pg_table_is_visible(c.oid)
      END_GET_UPDATES_PER_TABLE_QUERY

      ret = run_query(db, query)

      ret.map!{ |v| [v['relname'], v['pg_stat_get_tuples_updated'].to_i] }

      return Hash[*ret.flatten]
   end
   
   def get_hot_updates_per_table(db)
      query = <<-END_GET_HOT_UPDATES_PER_TABLE_QUERY
        SELECT c.relname, pg_stat_get_tuples_hot_updated(c.oid) FROM pg_catalog.pg_class c
        LEFT JOIN pg_catalog.pg_namespace n ON n.oid = c.relnamespace
        WHERE c.relkind IN ('r','') AND n.nspname NOT IN ('pg_catalog', 'pg_toast')
        AND pg_catalog.pg_table_is_visible(c.oid)
      END_GET_HOT_UPDATES_PER_TABLE_QUERY

      ret = run_query(db, query)

      ret.map!{ |v| [v['relname'], v['pg_stat_get_tuples_hot_updated'].to_i] }

      return Hash[*ret.flatten]
   end
   
   # * number of deleted rows per table
   
   def get_deletes_per_table(db)
      query = <<-END_GET_DELETES_PER_TABLE_QUERY
        SELECT c.relname, pg_stat_get_tuples_deleted(c.oid) FROM pg_catalog.pg_class c
        LEFT JOIN pg_catalog.pg_namespace n ON n.oid = c.relnamespace
        WHERE c.relkind IN ('r','') AND n.nspname NOT IN ('pg_catalog', 'pg_toast')
        AND pg_catalog.pg_table_is_visible(c.oid)
      END_GET_DELETES_PER_TABLE_QUERY

      ret = run_query(db, query)

      ret.map!{ |v| [v['relname'], v['pg_stat_get_tuples_deleted'].to_i] }

      return Hash[*ret.flatten]
   end
   
   # * number of dead rows per table
   
   def get_dead_per_table(db)
     query = <<-END_GET_DEAD_PER_TABLE_QUERY
       SELECT c.relname, pg_stat_get_dead_tuples(c.oid) FROM pg_catalog.pg_class c
       LEFT JOIN pg_catalog.pg_namespace n ON n.oid = c.relnamespace
       WHERE c.relkind IN ('r','') AND n.nspname NOT IN ('pg_catalog', 'pg_toast')
       AND pg_catalog.pg_table_is_visible(c.oid)
     END_GET_DEAD_PER_TABLE_QUERY

     ret = run_query(db, query)

     ret.map!{ |v| [v['relname'], v['pg_stat_get_dead_tuples'].to_i] }

     return Hash[*ret.flatten]
   end
   
   def get_sequential_scans_per_table(db)
     query = <<-END_GET_SEQ_SCANS_PER_TABLE_QUERY
       SELECT stat.relname AS relname, seq_scan
       FROM pg_stat_user_tables stat
       RIGHT JOIN pg_statio_user_tables statio ON stat.relid=statio.relid;
     END_GET_SEQ_SCANS_PER_TABLE_QUERY

     ret = run_query(db, query)

     ret.map!{ |v| [v['relname'], v['seq_scan'].to_i] }

     return Hash[*ret.flatten]
   end
   
   def get_sequential_rows_read_per_table(db)
     query = <<-END_GET_SEQ_ROWS_READ_PER_TABLE_QUERY
       SELECT stat.relname AS relname, seq_tup_read
       FROM pg_stat_user_tables stat
       RIGHT JOIN pg_statio_user_tables statio ON stat.relid=statio.relid;
     END_GET_SEQ_ROWS_READ_PER_TABLE_QUERY

     ret = run_query(db, query)

     ret.map!{ |v| [v['relname'], v['seq_tup_read'].to_i] }

     return Hash[*ret.flatten]
   end
   
   def get_index_scans_per_table(db)
     query = <<-END_GET_INDEX_SCANS_PER_TABLE_QUERY
       SELECT stat.relname AS relname, idx_scan
       FROM pg_stat_user_tables stat
       RIGHT JOIN pg_statio_user_tables statio ON stat.relid=statio.relid;
     END_GET_INDEX_SCANS_PER_TABLE_QUERY

     ret = run_query(db, query)

     ret.map!{ |v| [v['relname'], v['idx_scan'].to_i] }

     return Hash[*ret.flatten]
   end
   
   def get_index_rows_fetched_per_table(db)
     query = <<-END_GET_INDEX_ROWS_FETCHED_PER_TABLE_QUERY
       SELECT stat.relname AS relname, idx_tup_fetch
       FROM pg_stat_user_tables stat
       RIGHT JOIN pg_statio_user_tables statio ON stat.relid=statio.relid;
     END_GET_INDEX_ROWS_FETCHED_PER_TABLE_QUERY

     ret = run_query(db, query)

     ret.map!{ |v| [v['relname'], v['idx_tup_fetch'].to_i] }

     return Hash[*ret.flatten]
   end
   
   def get_heap_blocks_read_per_table(db)
     query = <<-END_GET_HEAP_BLOCKS_READ_PER_TABLE_QUERY
       SELECT stat.relname AS relname, heap_blks_read
       FROM pg_stat_user_tables stat
       RIGHT JOIN pg_statio_user_tables statio ON stat.relid=statio.relid;
     END_GET_HEAP_BLOCKS_READ_PER_TABLE_QUERY

     ret = run_query(db, query)

     ret.map!{ |v| [v['relname'], v['heap_blks_read'].to_i] }

     return Hash[*ret.flatten]
   end
   
   def get_heap_blocks_hit_per_table(db)
     query = <<-END_GET_HEAP_BLOCKS_HIT_PER_TABLE_QUERY
       SELECT stat.relname AS relname, heap_blks_hit
       FROM pg_stat_user_tables stat
       RIGHT JOIN pg_statio_user_tables statio ON stat.relid=statio.relid;
     END_GET_HEAP_BLOCKS_HIT_PER_TABLE_QUERY

     ret = run_query(db, query)

     ret.map!{ |v| [v['relname'], v['heap_blks_hit'].to_i] }

     return Hash[*ret.flatten]
   end
   
   def get_index_blocks_read_per_table(db)
     query = <<-END_GET_INDEX_BLOCKS_READ_PER_TABLE_QUERY
       SELECT stat.relname AS relname, idx_blks_read
       FROM pg_stat_user_tables stat
       RIGHT JOIN pg_statio_user_tables statio ON stat.relid=statio.relid;
     END_GET_INDEX_BLOCKS_READ_PER_TABLE_QUERY

     ret = run_query(db, query)

     ret.map!{ |v| [v['relname'], v['idx_blks_read'].to_i] }

     return Hash[*ret.flatten]
   end
   
   def get_index_blocks_hit_per_table(db)
     query = <<-END_GET_INDEX_BLOCKS_HIT_PER_TABLE_QUERY
       SELECT stat.relname AS relname, idx_blks_hit
       FROM pg_stat_user_tables stat
       RIGHT JOIN pg_statio_user_tables statio ON stat.relid=statio.relid;
     END_GET_INDEX_BLOCKS_HIT_PER_TABLE_QUERY

     ret = run_query(db, query)

     ret.map!{ |v| [v['relname'], v['idx_blks_hit'].to_i] }

     return Hash[*ret.flatten]
   end
end

