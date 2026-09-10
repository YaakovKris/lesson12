from pyspark.sql import SparkSession

spark = (
    SparkSession.builder.master('local[*]')
    .appName('probe')
    .config('spark.ui.enabled', 'false')
    .config('spark.sql.warehouse.dir', 'spark-warehouse')
    .config('spark.sql.catalogImplementation', 'hive')
    .config('spark.hadoop.javax.jdo.option.ConnectionURL', 'jdbc:derby:metastore_db;create=true')
    .config('spark.hadoop.javax.jdo.option.ConnectionDriverName', 'org.apache.derby.jdbc.EmbeddedDriver')
    .config('spark.hadoop.javax.jdo.option.ConnectionUserName', 'APP')
    .config('spark.hadoop.javax.jdo.option.ConnectionPassword', 'mine')
    .enableHiveSupport()
    .getOrCreate()
)

queries = [
    "select from_json('{\"a\":1}', 'struct<action:string,issue:struct<number:int,title:string>>') as x",
    "select from_json('{\"a\":1}', 'struct<action:string,issue:struct<number:int,title:string,state:string>>') as x",
    "select from_json('{\"a\":1}', 'struct<action:string,issue:struct<number:int,title:string,state:string,labels:array<struct<name:string>>>' ) as x",
    "select from_json('{\"a\":1}', 'struct<action:string,issue:struct<number:int,title:string,state:string,created_at:string,closed_at:string,comments:int,author_association:string,user:struct<login:string>,labels:array<struct<name:string>>>>') as x",
    "select from_json('{\"a\":1}', 'struct<action:string,number:int,pull_request:struct<state:string,title:string,draft:boolean,merged:boolean,created_at:string,closed_at:string,merged_at:string,additions:int,deletions:int,changed_files:int,commits:int,comments:int,review_comments:int,author_association:string,user:struct<login:string>,labels:array<struct<name:string>>>>') as x",
]

for q in queries:
    print('--- QUERY ---')
    print(q)
    try:
        spark.sql(q).show(truncate=False)
        print('OK')
    except Exception as e:
        print(type(e).__name__, e)

spark.stop()

