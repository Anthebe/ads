# ads
ADS
----------------------------SEP 22
create TABLE anthebe.apr23cvm_contributionadsb AS
select a.*,
coalesce(c.package_cd,b.package_cd)package_cd,
case when b.msisdn_key is not null then 1 else 0 end rgs90_prevmth,
case when b.dola<=29 then 1 else 0 end rgs30_existingbase,
c.sub_sts,
case when c.msisdn_key is not null then 1 else 0 end rgs90_currmth,
 coalesce(b.active_data_user,0)ads_prevmth,
 coalesce(c.active_data_user,0)ads_currmth,
case when d.msisdn_key is not null then 1 else 0 end churned_subs,
case when e.msisdn_key is not null then 1 else 0 end dnd_subs,
case when f.msisdn_key is not null then 1 else 0 end camp_sts,
case when g.msisdn_nsk is not null then 1 else 0 end ucg_sts
from
(select coalesce(a.msisdn_key,b.msisdn_key)msisdn_key,
a.dola dola365_prevmth,
b.dola dola365_currmth
from
(select msisdn_key,dola from nigeria.segment5b5_mon where tbl_dt=20230331
and dola<=364 and aggr='monthly') a 
full outer join 
(select msisdn_key,dola from nigeria.segment5b5_mon where tbl_dt=20230430
and dola<=364 and aggr='monthly') b 
on a.msisdn_key=b.msisdn_key) a 
left join 
(select *
from nigeria.segment5b5_mon where tbl_dt=20230331
and dola<=89 and flex_rge_1_txt='NA' and aggr='monthly') b 
on a.msisdn_key=b.msisdn_key
left join 
(select *
from nigeria.segment5b5_mon where tbl_dt=20230430
and dola<=89 and flex_rge_1_txt='NA' and aggr='monthly') c
on a.msisdn_key=c.msisdn_key
left join 
(select msisdn_key from nigeria.segment5b5_mon where tbl_dt=20230331
and dola<=89 and flex_rge_1_txt='NA' and aggr='monthly'
and msisdn_key not in (select msisdn_key from nigeria.segment5b5_mon where tbl_dt=20230430
and dola<=89 and flex_rge_1_txt='NA' and aggr='monthly')) d
on a.msisdn_key=d.msisdn_key
left join 
(select distinct MSISDN_KEY from flare_8.mvas_dnd_msisdn_report where tbl_dt = 20230331) e 
on a.msisdn_key=e.msisdn_key
left join 
(select distinct msisdn_key from (
select MSISDN_KEY from cvm_db.mtn4me_base_20230402
union all
select MSISDN_KEY from cvm_db.mtn4me_base_20230409   
union all
select MSISDN_KEY from cvm_db.mtn4me_base_20230416
union all
select MSISDN_KEY from cvm_db.mtn4me_base_20230423
union all
select MSISDN_KEY from cvm_db.mtn4me_base_20230430
union all
(select msisdn_key from flare_8.flytxt_campaign_events_data where tbl_dt between 20230401 and 20230430
and sub_sts='TG'
and acknowledge_status='1'))) f
on a.msisdn_key=f.msisdn_key
left join 
(select a.*,cast(msisdn_key as bigint)msisdn_nsk from 
cvm_db.may2023_finalcampaignmetrics a where add_sts=1 and ucg_sts=1)g 
on a.msisdn_key=g.msisdn_nsk

----
select count(msisdn_key) from nigeria.segment5b5_mon where tbl_dt=20230430
and dola<=89 and flex_rge_1_txt='NA' and aggr='monthly' and active_data_user =1 and package_cd not in('327','329','317','318') -----41310665

-----Addressable Base
select count(msisdn_key) from anthebe.apr23cvm_contributionadsb where rgs90_prevmth=1---74149607

-----# of Customers in your Do Not Disturb (DND) Base
select count(msisdn_key),count(distinct msisdn_key) from anthebe.apr23cvm_contributionadsb where dnd_subs=1 and rgs90_prevmth=1--6545236


-----Number of existing ADS customers in Addressable Base
select count(msisdn_key),count(distinct msisdn_key) from anthebe.apr23cvm_contributionadsb where rgs30_existingbase=1 and ads_prevmth=1---36796812
and package_cd not in('327','329','317','318')

-------Number of ADS customers that churned despite being a Target Group in CVM campaign(s)(rgs 30 prev mt,were ads and now churned in curr mth)

select count(msisdn_key),count(distinct msisdn_key) from anthebe.apr23cvm_contributionadsb where rgs30_existingbase=1 and ads_prevmth=1
and package_cd not in('327','329','317','318')
and ads_currmth=0 and camp_sts=1 and dnd_subs=0 and ucg_sts=0 and churned_subs=1--16434

----Number of ADS customers that became Marginal / Non Data User despite being a Target Group in CVM campaign(s)(rgs 30 prev mth,were ads,now non ads and were campaigned in curr mth)
select count(msisdn_key),count(distinct msisdn_key) from anthebe.apr23cvm_contributionadsb  where rgs30_existingbase=1 and ads_prevmth=1
and package_cd not in('327','329','317','318')
and ads_currmth=0 and dola365_currmth<=29 and camp_sts=1 and dnd_subs=0 and ucg_sts=0 and churned_subs=0---3,536,008

--------Number of non ADS customers in Addressable Base(rgs 30 prev mth,non ads prev mth)
select count(msisdn_key),count(distinct msisdn_key) from anthebe.apr23cvm_contributionadsb where rgs30_existingbase=1 and ads_prevmth=0
and package_cd not in('327','329','317','318')---29849093

---------Number of non ADS customers that became an ADS customer (Total)(rgs 30 prev mth,were non ads,now ads)
select count(msisdn_key),count(distinct msisdn_key) from anthebe.apr23cvm_contributionadsb where rgs30_existingbase=1 and ads_prevmth=0
and ads_currmth=1 and package_cd not in('327','329','317','318')---3846678

----this will be affected
------Number of non ADS customers that became an ADS customer due to CVM campaigns
select count(msisdn_key),count(distinct msisdn_key) from anthebe.apr23cvm_contributionadsb where rgs30_existingbase=1 and ads_prevmth=0
and ads_currmth=1 and camp_sts=1 and package_cd not in('327','329','317','318')---3687696

-------Number of Acquisitions for the month that became ADS customers
select count(msisdn_key),count(distinct msisdn_key) from anthebe.apr23cvm_contributionadsb where sub_sts in ('GROSS CONNECTIONS')
and ads_currmth=1 and package_cd not in('327','329','317','318')---1,138,086

-----Number attributable to CVM
select count(msisdn_key),count(distinct msisdn_key) from anthebe.apr23cvm_contributionadsb where sub_sts in ('GROSS CONNECTIONS')
and ads_currmth=1 and camp_sts=1 and package_cd not in('327','329','317','318')--1045540

-----Number of Dormant ADS customers 
select count(msisdn_key),count(distinct msisdn_key) from anthebe.apr23cvm_contributionadsb where ads_prevmth=1 and dola365_currmth>=30
and package_cd not in('327','329','317','318')
and sub_sts <> 'GROSS CONNECTIONS'---1272648

--------Number of Revived ADS customers (Total)
select count(msisdn_key),count(distinct msisdn_key) from anthebe.apr23cvm_contributionadsb where dola365_prevmth>=30 and ads_currmth=1
and rgs90_currmth = 1
and package_cd not in('327','329','317','318')
and sub_sts <> 'GROSS CONNECTIONS'--555451

------Number Attributable to CVM from the Total Revived customers
select count(msisdn_key),count(distinct msisdn_key) from anthebe.apr23cvm_contributionadsb where dola365_prevmth>=30 and ads_currmth=1
and camp_sts=1 and rgs90_currmth = 1 and package_cd not in('327','329','317','318')
and sub_sts <> 'GROSS CONNECTIONS'--534874


---------------------also calculating the overall inflow and cvm contribution
create table anthebe.ads_sts20230430 as
select coalesce(a.msisdn_key,b.msisdn_key)msisdn_key,
case when b.msisdn_key is null then 'OUTFLOW'
when a.msisdn_key is null then 'INFLOW'
else 'RETAINED' end sts 
from 
(select msisdn_key from nigeria.segment5b5_sub where tbl_dt=20230331 and aggr='daily'
and dola<=89 and exclusion_status ='NA' and active_data_user_30day =1
and service_class_id not in('327','329','317','318')) a 
full outer join 
(select msisdn_key from nigeria.segment5b5_sub where tbl_dt=20230430 and aggr='daily'
and dola<=89 and exclusion_status ='NA' and active_data_user_30day =1
and service_class_id not in('327','329','317','318')) b
on a.msisdn_key=b.msisdn_key



create table anthebe.ads_sts20230430final as
select a.*,
case when b.msisdn_key is not null then 'CVM' else 'NOT CVM' end camp_sts
from anthebe.ads_sts20230430 a 
left join 
(select distinct msisdn_key from (
select MSISDN_KEY from cvm_db.mtn4me_base_20230402
union all
select MSISDN_KEY from cvm_db.mtn4me_base_20230409   
union all
select MSISDN_KEY from cvm_db.mtn4me_base_20230416
union all
select MSISDN_KEY from cvm_db.mtn4me_base_20230423
union all
select MSISDN_KEY from cvm_db.mtn4me_base_20230430
union all
(select msisdn_key from flare_8.flytxt_campaign_events_data where tbl_dt between 20230401 and 20230430
and sub_sts='TG'
and acknowledge_status='1'))) b 
on a.msisdn_key=b.msisdn_key


select sts,camp_sts,count(msisdn_key) from anthebe.ads_sts20230430final
group by sts,camp_sts
order by sts,camp_sts

--------------------------------------------------------

select sts,camp_sts,count(msisdn_key) from glolex.ads_sts20221231final
group by sts,camp_sts
order by sts,camp_sts
-------------------------------------------------------------------

select * from anthebe.ads_sts20221031
limit 10
----------------------------------------


----------------------------SEP 22
create TABLE anthebe.jan23vm_contributionadsb AS
select a.*,
coalesce(c.package_cd,b.package_cd)package_cd,
case when b.msisdn_key is not null then 1 else 0 end rgs90_prevmth,
case when b.dola<=29 then 1 else 0 end rgs30_existingbase,
c.sub_sts,
case when c.msisdn_key is not null then 1 else 0 end rgs90_currmth,
 coalesce(b.active_data_user,0)ads_prevmth,
 coalesce(c.active_data_user,0)ads_currmth,
case when d.msisdn_key is not null then 1 else 0 end churned_subs,
case when e.msisdn_key is not null then 1 else 0 end dnd_subs,
case when f.msisdn_key is not null then 1 else 0 end camp_sts,
case when g.msisdn_nsk is not null then 1 else 0 end ucg_sts
from
(select coalesce(a.msisdn_key,b.msisdn_key)msisdn_key,
a.dola dola365_prevmth,
b.dola dola365_currmth
from
(select msisdn_key,dola from nigeria.segment5b5_mon where tbl_dt=20221231
and dola<=364 and aggr='monthly') a 
full outer join 
(select msisdn_key,dola from nigeria.segment5b5_mon where tbl_dt=20230131
and dola<=364 and aggr='monthly') b 
on a.msisdn_key=b.msisdn_key) a 
left join 
(select *
from nigeria.segment5b5_mon where tbl_dt=20221231
and dola<=89 and flex_rge_1_txt='NA' and aggr='monthly') b 
on a.msisdn_key=b.msisdn_key
left join 
(select *
from nigeria.segment5b5_mon where tbl_dt=20230131
and dola<=89 and flex_rge_1_txt='NA' and aggr='monthly') c
on a.msisdn_key=c.msisdn_key
left join 
(select msisdn_key from nigeria.segment5b5_mon where tbl_dt=20221231
and dola<=89 and flex_rge_1_txt='NA' and aggr='monthly'
and msisdn_key not in (select msisdn_key from nigeria.segment5b5_mon where tbl_dt=20230131
and dola<=89 and flex_rge_1_txt='NA' and aggr='monthly')) d
on a.msisdn_key=d.msisdn_key
left join 
(select distinct MSISDN_KEY from flare_8.mvas_dnd_msisdn_report where tbl_dt = 20221231) e 
on a.msisdn_key=e.msisdn_key
left join 
(select distinct msisdn_key from (
select MSISDN_KEY from cvm_db.Inbound_JDBC_Base_less_RM_20230101 
union all
select MSISDN_KEY from cvm_db.Inbound_JDBC_Base_less_RM_20230108    
union all
select MSISDN_KEY from cvm_db.Inbound_JDBC_Base_less_RM_20230115
union all
select MSISDN_KEY from cvm_db.Inbound_JDBC_Base_less_RM_20230122
union all
select MSISDN_KEY from cvm_db.Inbound_JDBC_Base_less_RM_20230129
union all
(select msisdn_key from flare_8.flytxt_campaign_events_data where tbl_dt between 20230101 and 20230131
and sub_sts='TG'
and acknowledge_status='1'))) f
on a.msisdn_key=f.msisdn_key
left join 
(select a.*,cast(msisdn_key as bigint)msisdn_nsk from 
cvm_db.jan2023_finalcampaignmetrics a where add_sts=1 and ucg_sts=1)g 
on a.msisdn_key=g.msisdn_nsk

----
select count(msisdn_key) from nigeria.segment5b5_mon where tbl_dt=20230131
and dola<=89 and flex_rge_1_txt='NA' and aggr='monthly' and active_data_user =1 and package_cd not in('327','329','317','318')------ 40,470,145

-----Addressable Base
select count(msisdn_key) from anthebe.jan23cvm_contributionadsb where rgs90_prevmth=1---75627806

-----# of Customers in your Do Not Disturb (DND) Base
select count(msisdn_key),count(distinct msisdn_key) from anthebe.jan23cvm_contributionadsb where dnd_subs=1 and rgs90_prevmth=1--6545236


-----Number of existing ADS customers in Addressable Base
select count(msisdn_key),count(distinct msisdn_key) from anthebe.jan23cvm_contributionadsb where rgs30_existingbase=1 and ads_prevmth=1---36796812
and package_cd not in('327','329','317','318')

-------Number of ADS customers that churned despite being a Target Group in CVM campaign(s)(rgs 30 prev mt,were ads and now churned in curr mth)

select count(msisdn_key),count(distinct msisdn_key) from anthebe.jan23cvm_contributionadsb where rgs30_existingbase=1 and ads_prevmth=1
and package_cd not in('327','329','317','318')
and ads_currmth=0 and camp_sts=1 and dnd_subs=0 and ucg_sts=0 and churned_subs=1--16434

----Number of ADS customers that became Marginal / Non Data User despite being a Target Group in CVM campaign(s)(rgs 30 prev mth,were ads,now non ads and were campaigned in curr mth)
select count(msisdn_key),count(distinct msisdn_key) from anthebe.jan23cvm_contributionadsb  where rgs30_existingbase=1 and ads_prevmth=1
and package_cd not in('327','329','317','318')
and ads_currmth=0 and dola365_currmth<=29 and camp_sts=1 and dnd_subs=0 and ucg_sts=0 and churned_subs=0---3,536,008

--------Number of non ADS customers in Addressable Base(rgs 30 prev mth,non ads prev mth)
select count(msisdn_key),count(distinct msisdn_key) from anthebe.jan23cvm_contributionadsb where rgs30_existingbase=1 and ads_prevmth=0
and package_cd not in('327','329','317','318')---29849093

---------Number of non ADS customers that became an ADS customer (Total)(rgs 30 prev mth,were non ads,now ads)
select count(msisdn_key),count(distinct msisdn_key) from anthebe.jan23cvm_contributionadsb where rgs30_existingbase=1 and ads_prevmth=0
and ads_currmth=1 and package_cd not in('327','329','317','318')---3846678

----this will be affected
------Number of non ADS customers that became an ADS customer due to CVM campaigns
select count(msisdn_key),count(distinct msisdn_key) from anthebe.jan23cvm_contributionadsb where rgs30_existingbase=1 and ads_prevmth=0
and ads_currmth=1 and camp_sts=1 and package_cd not in('327','329','317','318')---3687696

-------Number of Acquisitions for the month that became ADS customers
select count(msisdn_key),count(distinct msisdn_key) from anthebe.jan23cvm_contributionadsb where sub_sts in ('GROSS CONNECTIONS')
and ads_currmth=1 and package_cd not in('327','329','317','318')---1,138,086

-----Number attributable to CVM
select count(msisdn_key),count(distinct msisdn_key) from anthebe.jan23cvm_contributionadsb where sub_sts in ('GROSS CONNECTIONS')
and ads_currmth=1 and camp_sts=1 and package_cd not in('327','329','317','318')--1045540

-----Number of Dormant ADS customers 
select count(msisdn_key),count(distinct msisdn_key) from anthebe.jan23cvm_contributionadsb where ads_prevmth=1 and dola365_currmth>=30
and package_cd not in('327','329','317','318')
and sub_sts <> 'GROSS CONNECTIONS'---1272648

--------Number of Revived ADS customers (Total)
select count(msisdn_key),count(distinct msisdn_key) from anthebe.jan23cvm_contributionadsb where dola365_prevmth>=30 and ads_currmth=1
and rgs90_currmth = 1
and package_cd not in('327','329','317','318')
and sub_sts <> 'GROSS CONNECTIONS'--555451

------Number Attributable to CVM from the Total Revived customers
select count(msisdn_key),count(distinct msisdn_key) from anthebe.jan23cvm_contributionadsb where dola365_prevmth>=30 and ads_currmth=1
and camp_sts=1 and rgs90_currmth = 1 and package_cd not in('327','329','317','318')
and sub_sts <> 'GROSS CONNECTIONS'--534874


---------------------also calculating the overall inflow and cvm contribution
create table anthebe.ads_sts20221231 as
select coalesce(a.msisdn_key,b.msisdn_key)msisdn_key,
case when b.msisdn_key is null then 'OUTFLOW'
when a.msisdn_key is null then 'INFLOW'
else 'RETAINED' end sts 
from 
(select msisdn_key from nigeria.segment5b5_sub where tbl_dt=20221130 and aggr='daily'
and dola<=89 and exclusion_status ='NA' and active_data_user_30day =1
and service_class_id not in('327','329','317','318')) a 
full outer join 
(select msisdn_key from nigeria.segment5b5_sub where tbl_dt=20221231 and aggr='daily'
and dola<=89 and exclusion_status ='NA' and active_data_user_30day =1
and service_class_id not in('327','329','317','318')) b
on a.msisdn_key=b.msisdn_key



create table anthebe.ads_sts20221231final as
select a.*,
case when b.msisdn_key is not null then 'CVM' else 'NOT CVM' end camp_sts
from anthebe.ads_sts20221231 a 
left join 
(select distinct msisdn_key from (
select MSISDN_KEY from cvm_db.mtn4me_base_20230402
union all
select MSISDN_KEY from cvm_db.mtn4me_base_20230409   
union all
select MSISDN_KEY from cvm_db.mtn4me_base_20230416
union all
select MSISDN_KEY from cvm_db.mtn4me_base_20230423
union all
select MSISDN_KEY from cvm_db.mtn4me_base_20230430
union all
(select msisdn_key from flare_8.flytxt_campaign_events_data where tbl_dt between 20230401 and 20230430
and sub_sts='TG'
and acknowledge_status='1'))) b 
on a.msisdn_key=b.msisdn_key


select sts,camp_sts,count(msisdn_key) from anthebe.ads_sts20221231final
group by sts,camp_sts
order by sts,camp_sts

--------------------------------------------------------

select sts,camp_sts,count(msisdn_key) from glolex.ads_sts20221231final
group by sts,camp_sts
order by sts,camp_sts
-------------------------------------------------------------------

select * from anthebe.ads_sts20221031
limit 10
----------------------------------------
