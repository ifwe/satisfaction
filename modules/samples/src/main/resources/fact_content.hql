source oozie-setup.hql ;
set hive.exec.parallel=true;

SET hive.exec.compress.output=true; 
SET io.seqfile.compression.type=BLOCK;

SET io.sort.mb=512;

create external table if not exists raw_content
partitioned by (dt string, network_abbr string)
row format serde 'com.inadco.ecoadapters.hive.ProtoSerDe'
with serdeproperties( "messageClass"="com.klout.platform.protos.Topics$FactContent")
stored as sequencefile 
location '${rawContentDir}' ;


alter table raw_content  
add if not exists partition( dt="${dateString}", network_abbr="${networkAbbr}")
 location '${dateString}/${networkAbbr}';

create external table if not exists actor_action (
    ks_uid bigint,
    service_uid string,
    service_id string,
    tstamp string,
    message_id string,
    action string,
    actor_service_uid string,
    tstamp_type string,
    original_message_id string,
    original_tstamp string,
    registered_flag tinyint,
    actor_ks_uid bigint
)
partitioned by (dt string,network_abbr string)
stored as sequencefile
location '${actorActionDir}';

alter table actor_action
add if not exists partition (dt="${dateString}", network_abbr="${networkAbbr}")
location "${dateString}/${networkAbbr}";
alter table actor_action partition(dt="${dateString}",network_abbr="${networkAbbr}") 
set fileformat sequencefile;

drop view if exists fact_actor_action_feature_type_view_${networkAbbr};
create view fact_actor_action_feature_type_view_${networkAbbr}
as
  select
    user.user_id  as service_uid,
    user.service_id            as service_id,
    action.message.message_id  as message_id,
    action.message.`timestamp` as tstamp,
    concat(action.action_type.action, "_",  feature.messagefeaturekey) as action,
    actor.actor.user_id        as actor_service_uid,
    rc.dt             as dt,
    rc.network_abbr   as network_abbr,
    action.message.timestamp_type as tstamp_type,
    message.message_id as original_message_id,
    message.`timestamp` as original_tstamp
  from ( select  user,actions, messages, dt, network_abbr  from  raw_content
    where network_abbr = '${networkAbbr}' ) rc
  lateral view
    explode( actions )        a as action
  lateral view
    explode( action.actors ) aa as actor
  lateral view
    explode(messages)        mm as message
  lateral view
    explode(message.messagefeatures) mf as feature
  where network_abbr in ("fb", "fp") and (feature.messagefeaturekey = 'VIDEO' or feature.messagefeaturekey = 'PICTURE');


drop view if exists fact_actor_action_view_${networkAbbr};
create view fact_actor_action_view_${networkAbbr}
as select * from (
  select
     user.user_id as service_uid,
     user.service_id as service_id,
     action.message.message_id as message_id,
     action.message.`timestamp` as tstamp,
     action.action_type.action,
     actor.actor.user_id as actor_service_uid,
     rc.dt as dt,
     rc.network_abbr as network_abbr,
     action.message.timestamp_type as tstamp_type,
     messages[0].message_id as original_message_id,
     messages[0].`timestamp` as original_tstamp
  from ( select  user, actions, messages, dt, network_abbr from raw_content 
    where network_abbr='${networkAbbr}' ) rc
  lateral view
     explode( actions ) a as action
  lateral view
     explode( action.actors ) aa as actor
union all
  select *
  from fact_actor_action_feature_type_view_${networkAbbr}
) au;




insert overwrite table actor_action partition(dt="${dateString}", network_abbr="${networkAbbr}")
select
  dus.ks_uid,
  aa.service_uid,
  aa.service_id,
  aa.tstamp,
  aa.message_id,
  aa.action,
  aa.actor_service_uid,
  aa.tstamp_type,
  aa.original_message_id,
  aa.original_tstamp,
  dus.registered_flag,
  cast(dus_actor.ks_uid as bigint) as actor_ks_uid
from
  ( select *
      from  fact_actor_action_view_${networkAbbr}
      where dt=${dateString}  ) aa
JOIN
  ( select ks_uid, service_uid, service_id, registered_flag
       from ksuid_mapping
       where dt = ${dateString}
         and service_id = ${featureGroup} 
         and coalesce(optout, cast(0 as tinyint)) = 0 ) dus
ON
  ( aa.service_uid = dus.service_uid  )
JOIN
  ( select ks_uid,  service_uid, service_id
       from ksuid_mapping
       where dt = ${dateString}
       and service_id = ${featureGroup} 
      and coalesce(optout, cast(0 as tinyint)) = 0 ) dus_actor
ON
  ( aa.actor_service_uid = dus_actor.service_uid )
WHERE
  action != 'TWITTER_MENTION'
  and 
  action != 'TWITTER_RETWEET'
  
;

select assert(count(*) > ${minCount}, concat("Missing data for actor_action for ${networkAbbr}"))
from actor_action
where dt='${dateString}' and network_abbr='${networkAbbr}';


