
create external table if not exists tw_friends_json(
unused string,
service_uid_json string,
friends_json string
)
partitioned by(dt string)
row format delimited fields terminated by "\t"
LOCATION '${nameNode}/${twFriendsDestDir}';

create external table if not exists tw_followers_json(
unused string,
service_uid_json string,
friends_json string
)
partitioned by(dt string)
row format delimited fields terminated by "\t"
LOCATION '${nameNode}/${twFollowersDestDir}';

alter table tw_${tableAlias}_json add if not exists partition
(dt=${dateString}) location '${dateString}/output';

drop view if exists tw_${tableAlias}_view;
create view tw_${tableAlias}_view
as
select get_json_object( service_uid_json, "$.klout_id" ) as ks_uid,
       get_json_object( service_uid_json, "$.service_uid" ) as service_uid,
       get_json_object( service_uid_json, "$.service_id" ) as service_id,
       json_split( get_json_object( friends_json, "$.ids" )) as friends_arr
  from tw_${tableAlias}_json
  where dt = ${dateString};

drop view if exists tw_${tableAlias}_explode_view;
create view tw_${tableAlias}_explode_view
as
select ks_uid,
       service_id,
       friend_service_uid
from
  ( select ks_uid, service_id, friends_arr
    from tw_${tableAlias}_view
    where ks_uid is not null and
          friends_arr is not null and
          size(friends_arr) > 0 ) tw_view
lateral view
   explode(friends_arr) fj as friend_service_uid
group by ks_uid, service_id, friend_service_uid;

drop view if exists user_graph_${tableAlias}_view_tw;
create view user_graph_${tableAlias}_view_tw as
select cast(friend.ks_uid as bigint) as ks_uid,
       cast(ids.ks_uid as bigint) as friend_ks_uid
from
( select ks_uid,
         service_id,
         friend_service_uid
  from tw_${tableAlias}_explode_view) friend
JOIN
( select ks_uid, service_id, service_uid
  from ksuid_mapping
  where dt = ${dateString}
    and service_id = 1 ) ids
ON
( friend.friend_service_uid = ids.service_uid)
;

create external table if not exists user_graph(
  ks_uid bigint,
  friend_ks_uid bigint
)
partitioned by(dt string, network_abbr string, relation_type string)
row format delimited fields terminated by "\t"
LOCATION '${nameNode}/${userGraphDir}'
;

alter table user_graph
add if not exists partition (dt=${dateString}, network_abbr = "tw", relation_type = "${graphType}" )
location '${dateString}/tw/${graphType}';
alter table user_graph partition (dt=${dateString}, network_abbr = "tw", relation_type = "${graphType}" )
set fileformat sequencefile;


insert overwrite table user_graph partition(dt=${dateString}, network_abbr = "tw", relation_type = "${graphType}" )
select ks_uid, friend_ks_uid
from user_graph_${tableAlias}_view_tw;
