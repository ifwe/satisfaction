source oozie-setup.hql ;
set hive.exec.parallel=true;

create external table if not exists raw_content
partitioned by (dt string, network_abbr string)
row format serde 'com.inadco.ecoadapters.hive.ProtoSerDe'
with serdeproperties( "messageClass"="com.klout.platform.protos.Topics$FactContent")
stored as sequencefile 
location '${rawContentDir}' ;


alter table raw_content  
add if not exists partition( dt="${dateString}", network_abbr="kl" ) 
 location '${dateString}/kl';

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
    registered_flag tinyint
)
partitioned by (dt string,network_abbr string)
location '${actorActionDir}';

alter table actor_action
add if not exists partition (dt="${dateString}", network_abbr="kl")
location "${dateString}/kl";


drop view if exists fact_actor_action_view_kl;
create view fact_actor_action_view_kl
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
    where network_abbr='kl' ) rc
  lateral view
     explode( actions ) a as action
  lateral view
     explode( action.actors ) aa as actor
) au;


insert overwrite table actor_action partition(dt="${dateString}", network_abbr="kl")
select 
  aa.service_uid as ks_uid,
  aa.service_uid,
  aa.service_id,
  aa.tstamp,
  aa.message_id,
  aa.action,
  aa.actor_service_uid,
  aa.tstamp_type,
  aa.original_message_id,
  aa.original_tstamp,
  1 as registered_flag,
  aa.actor_service_uid as actor_ks_uid
from 
   fact_actor_action_view_kl aa
      where dt=${dateString} 
;


