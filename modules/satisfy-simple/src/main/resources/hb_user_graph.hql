source oozie-setup.hql ;

SET hive.exec.compress.output=true;
SET io.seqfile.compression.type=BLOCK;

drop view if exists hb_user_graph_view;
create view hb_user_graph_view as
select
      hbase_key,
      hbase_value
from
  (select encode_user_graph_key(ks_uid, network_abbr, relation_type, friend_ks_uid) as hbase_key,
          "1" as hbase_value
  from user_graph
  where dt = ${dateString} and
        network_abbr = "fb" and
        relation_type = "FACEBOOK_FRIENDS") a;

create external table if not exists hb_user_graph(
    hbase_key string,
    hbase_value string
)
partitioned by (dt string)
location '${hbUserGraphImportDir}';

alter table hb_user_graph add if not exists partition (dt='${dateString}')
  location '${dateString}';

insert overwrite table hb_user_graph partition(dt='${dateString}')
  select * from hb_user_graph_view;


