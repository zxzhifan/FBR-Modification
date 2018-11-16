/*
truncate zz_cx.temp_match_fbr_ctrl_5;
insert into zz_cx.temp_match_fbr_ctrl_5
(fc_all_id,doc_nbr_prime, doc_carr_nbr, trnsc_date, fc_cpn_nbr)
select distinct fc_all_id,doc_nbr_prime, doc_carr_nbr, trnsc_date, fc_cpn_nbr
from zz_cx.temp_tbl_fc_all fc
where 
fc.rbd_ind <> '1' 
#and day(fare_lockin_date) between 16 and 25
#and
#mod(doc_nbr_prime, 5) = 0
limit 40000
;
*/
################################################# FBR1
drop table if exists zz_cx.fm_fbr1_fc_4;
create table if not exists zz_cx.fm_fbr1_fc_4
select distinct ctrl.fc_all_id
from zz_cx.temp_match_fbr_ctrl_5 ctrl
straight_join zz_cx.temp_tbl_fc_all fc on (ctrl.fc_all_id = fc.fc_all_id)
straight_join tmp.temp_map_fbcx_fbr_r fbcm on (fbcm.doc_nbr_prime = fc.doc_nbr_prime and fbcm.fc_cpn_nbr = fc.fc_cpn_nbr)
straight_join zz_cx.temp_map_bcode_acc mbc on (mbc.bcode = fc.tkt_endorse_cd and mbc.tar_nbr = fbcm.frt_nbr and mbc.carr_cd = fbcm.carr_cd and mbc.rule_nbr = fbcm.rule_nbr);


drop table if exists zz_cx.fm_fbr1_r3_4;
create table if not exists zz_cx.fm_fbr1_r3_4
select distinct fc.fc_all_id, r8.rule_id as r8_id, r2.rule_id as r2_id, r3.cat_id as r3_id, r2s.dir_ind, r3.base_tbl_t989, g16.ff_nbr
from zz_cx.fm_fbr1_fc_4 ctrl
straight_join zz_cx.temp_tbl_fc_all fc on (ctrl.fc_all_id = fc.fc_all_id)
straight_join tmp.temp_map_fbcx_fbr_r fbcm on (fbcm.doc_nbr_prime = fc.doc_nbr_prime and fbcm.fc_cpn_nbr = fc.fc_cpn_nbr)
straight_join zz_cx.temp_map_bcode_acc mbc on (mbc.bcode = fc.tkt_endorse_cd and mbc.tar_nbr = fbcm.frt_nbr and mbc.carr_cd = fbcm.carr_cd and mbc.rule_nbr = fbcm.rule_nbr)

straight_join atpco_fare.atpco_r8_fbr r8 on (r8.tar_nbr = fbcm.frt_nbr and r8.carr_cd = fbcm.carr_cd and r8.rule_nbr = fbcm.rule_nbr and r8.proc_ind in ('N', 'R'))
straight_join atpco_fare.atpco_r8_fbr_state r8t on (r8t.rule_id = r8.rule_id)

straight_join atpco_fare.atpco_r2_cat25_ctrl r2 on (r2.carr_cd = r8.carr_cd and r2.tar_nbr = r8.tar_nbr and r2.rule_nbr = r8.rule_nbr and r2.proc_ind in ('N', 'R'))
straight_join atpco_fare.atpco_r2_cat25_ctrl_state r2t on (r2t.rule_id = r2.rule_id)
straight_join atpco_fare.atpco_r2_cat25_ctrl_sup r2s on (r2s.rule_id = r2.rule_id)

straight_join atpco_fare.atpco_cat25 r3 on (r3.cat_id = r2s.tbl_nbr) #----------------------- cat 25 match to record 8 -------------------------------------------
and r3.pax_type = r8.pax_type

left join atpco_fare.atpco_t994_date dor on (dor.tbl_nbr = r3.dt_ovrd_t994)

straight_join atpco_fare.atpco_t989_base_fare t989 on (t989.tbl_nbr = r3.base_tbl_t989 and t989.bf_appl <> 'N')

straight_join zz_cx.g16_temp g16 on (g16.carr_cd = fc.fc_carr_cd and (g16.frt_nbr = t989.bf_rule_tar or (t989.bf_rule_tar = '000' and g16.pri_ind <> 'X')))

where fc.tkt_endorse_cd <> ''
and fc.fare_lockin_date between (r8t.rec_add_date - interval 1 day) and if(r8t.rec_cnx_date = '9999-12-31', r8t.rec_cnx_date, r8t.rec_cnx_date + interval 1 day)
and fc.jrny_dep_date between r8t.tvl_eff_date and r8t.tvl_dis_date
and if(mbc.pax_acc_cd <> '' and r8.pax_acc_cd <> '', mbc.pax_acc_cd = r8.pax_acc_cd, true)

and fc.fare_lockin_date between (r2t.rec_add_date - interval 1 day) and if(r2t.rec_cnx_date = '9999-12-31', r2t.rec_cnx_date, r2t.rec_cnx_date + interval 1 day)
and fc.jrny_dep_date between r2t.tvl_eff_date and r2t.tvl_dis_date
#----------------------- table 994 date override -------------------------------------------
and if(dor.tbl_nbr is null, true, fc.trnsc_date between dor.tkt_fr_date and dor.tkt_to_date)
and if(dor.tbl_nbr is null, true, fc.fc_dep_date between dor.tvl_fr_date and dor.tvl_to_date);

alter table zz_cx.fm_fbr1_r3_4
add index idx_tmp_id(fc_all_id, base_tbl_t989);

#analyze table zz_cx.fm_fbr1_r3_4;

drop table if exists zz_cx.fm_fbr1_g16_4;
create table zz_cx.fm_fbr1_g16_4
select distinct fc_all_id, ff_nbr
from zz_cx.fm_fbr1_r3_4;

drop table if exists zz_cx.fm_fbr1_t989_4;
create table zz_cx.fm_fbr1_t989_4
select distinct fc_all_id, base_tbl_t989
from zz_cx.fm_fbr1_r3_4;

alter table zz_cx.fm_fbr1_t989_4
add index idx_fbr_t989_id(fc_all_id);

alter table zz_cx.fm_fbr1_g16_4
add index idx_fbr_g16_id(fc_all_id);


drop table if exists zz_cx.fm_fbr1_f_4;

create table zz_cx.fm_fbr1_f_4
select distinct ctrl.fc_all_id, f.fare_id, ctrl.ff_nbr
from zz_cx.fm_fbr1_g16_4 ctrl
straight_join zz_cx.temp_tbl_fc_all fc on (ctrl.fc_all_id = fc.fc_all_id)
/*
straight_join tmp.temp_map_fbcx_fbr_r fbcm on (fbcm.doc_nbr_prime = fc.doc_nbr_prime and fbcm.fc_cpn_nbr = fc.fc_cpn_nbr)
straight_join zz_cx.temp_map_bcode_acc mbc on (mbc.bcode = fc.tkt_endorse_cd and mbc.tar_nbr = fbcm.frt_nbr and mbc.carr_cd = fbcm.carr_cd and mbc.rule_nbr = fbcm.rule_nbr)
*/
straight_join atpco_fare.atpco_fare f on (fc.fc_orig = f.orig_city and fc.fc_dest = f.dest_city and fc.fc_carr_cd = f.carr_cd and f.tar_nbr = ctrl.ff_nbr) 
straight_join atpco_fare.atpco_fare_state ft on f.fare_id = ft.fare_id

where 
fc.tkt_endorse_cd <> ''
#----------------------- matching fc to the base fare -------------------------------------------
and fc.fare_lockin_date between (ft.rec_add_date - interval 1 day) and if(ft.rec_cnx_date = '9999-12-31', ft.rec_cnx_date, ft.rec_cnx_date + interval 1 day)
and fc.jrny_dep_date between ft.tvl_eff_date and ft.tvl_dis_date
# not sure the logic is entirely right: and if(fc.map_di_ind = 'F', f.ftnt <> 'T', f.ftnt <> 'F')		-- useful for domestic, for international, ftnt.di is always 'F', international footnote can never be 'F' or 'T'
;
####################
drop table if exists zz_cx.fm_fbr1_syn_4;
create table zz_cx.fm_fbr1_syn_4
select distinct
fc.fc_all_id, r2_id as r2_rule_id, f.fare_id as f_fare_id, r8_id as r8_rule_id, r3_id as r3_cat_id, cr.dir_ind, t989.tbl_nbr as t989_tbl_nbr, t989.bf_pax_type,  t989.bf_rule_tar as frt_nbr

from zz_cx.fm_fbr1_f_4 ctrl -- fare part

straight_join zz_cx.temp_tbl_fc_all fc on (ctrl.fc_all_id = fc.fc_all_id)

straight_join atpco_fare.atpco_fare f on (ctrl.fare_id = f.fare_id and ctrl.ff_nbr = f.tar_nbr)

join zz_cx.fm_fbr1_t989_4 ct on ctrl.fc_all_id = ct.fc_all_id  -- r8 to r2

straight_join atpco_fare.atpco_t989_base_fare t989 on (t989.tbl_nbr = ct.base_tbl_t989 and t989.bf_appl <> 'N')

straight_join zz_cx.g16_temp g16 on (g16.carr_cd = fc.fc_carr_cd and (g16.frt_nbr = t989.bf_rule_tar or (t989.bf_rule_tar = '000' and g16.pri_ind <> 'X')) and ctrl.ff_nbr = g16.ff_nbr)

straight_join zz_cx.fm_fbr1_r3_4 cr on ct.fc_all_id = cr.fc_all_id and ct.base_tbl_t989 = cr.base_tbl_t989

where  (f.rule_nbr = t989.bf_rule_nbr or t989.bf_rule_nbr = '')
and if(t989.bf_fc_len = 0, true, 
    if(t989.bf_fc_wld = 0, f.fare_cls = t989.bf_fc, 
    if(t989.bf_fc_wld = t989.bf_fc_len, left(f.fare_cls, t989.bf_fc_len-1) = left(t989.bf_fc, t989.bf_fc_len-1),
    if(t989.bf_fc_wld = 1, instr(f.fare_cls, right(t989.bf_fc, t989.bf_fc_len-1)),
    f.fare_cls regexp t989.bf_fc_regex
    ))))
and (f.rtg_nbr = t989.bf_rtg_nbr or t989.bf_rtg_nbr = '99999')
and (f.ow_rt_ind = t989.bf_ow_rt or t989.bf_ow_rt = '')
-- bf_type, not yet implemented
-- bf_ssn, not yet implemented
-- bf_dow, not yet implemented
-- ftnt, not yet implemented
-- and (fc.fc_pax_type = t989.c25_pax_type or t989.c25_pax_type in ('ADT', 'JCB', ''))	-- JCB and ADT are general
;


drop table if exists zz_cx.fm_fbr1_loc_4;
create table zz_cx.fm_fbr1_loc_4 
select distinct 
fc.doc_nbr_prime, fc.fc_cpn_nbr, ctrl.fc_all_id, ctrl.r2_rule_id as r2_25_rule_id, ctrl.r3_cat_id as r3_25_cat_id, f.fare_id, 'R' as map_type, 'B' as map_code
 from zz_cx.fm_fbr1_syn_4 ctrl
join zz_cx.temp_tbl_fc_all fc on (ctrl.fc_all_id = fc.fc_all_id)
join atpco_fare.atpco_fare f on ctrl.f_fare_id = f.fare_id


join atpco_fare.atpco_r1_fare_cls r1 on (r1.carr_cd = f.carr_cd and r1.tar_nbr = ctrl.frt_nbr and r1.rule_nbr = f.rule_nbr and r1.fare_cls = f.fare_cls and r1.proc_ind in ('N', 'R'))
join atpco_fare.atpco_r1_fare_cls_state r1t on (r1t.rule_id = r1.rule_id)
join atpco_fare.atpco_r1_fare_cls_sup r1s on (r1s.rule_id = r1.rule_id)

join zz_cx.temp_fbcx_fbr_r fbcx on (fbcx.r8_rule_id = ctrl.r8_rule_id and fbcx.r2_25_rule_id = ctrl.r2_rule_id and fbcx.r3_25_cat_id = ctrl.r3_cat_id)			# matching based on record 2 and record 3  cat 25 id
join zz_cx.temp_tbl_fc_loc fc_loc on (fc_loc.doc_nbr_prime = fc.doc_nbr_prime and fc_loc.fc_cpn_nbr = fc.fc_cpn_nbr and fc_loc.map_di_ind = fc.map_di_ind)		
# to allow base fare matching and r2 loaction testing using somewhat different orig/dest

# forward direction location test
left join zz_cx.loc_m fo on (fo.fc_loc = fc_loc.fc_orig and fo.loc_type = fbcx.loc1_type and fo.loc = fbcx.loc1 and fo.loc_t = fbcx.loc1_t978)
left join zz_cx.loc_m fd on (fd.fc_loc = fc_loc.fc_dest and fd.loc_type = fbcx.loc2_type and fd.loc = fbcx.loc2 and fd.loc_t = fbcx.loc2_t978)

# reverse direction location test
left join zz_cx.loc_m ro on (ro.fc_loc = fc_loc.fc_dest and ro.loc_type = fbcx.loc1_type and ro.loc = fbcx.loc1 and ro.loc_t = fbcx.loc1_t978)
left join zz_cx.loc_m rd on (rd.fc_loc = fc_loc.fc_orig and rd.loc_type = fbcx.loc2_type and rd.loc = fbcx.loc2 and rd.loc_t = fbcx.loc2_t978)
##############

where fc.doc_nbr_prime > 0
#----------------------- conditions for fare record to r1 -------------------------------------------
-- location testing is not implemented
-- minimum sequence is not implemented
and fc.fare_lockin_date between date_add(r1t.rec_add_date, interval -1 day) and if(r1t.rec_cnx_date = '9999-12-31', r1t.rec_cnx_date, date_add(r1t.rec_cnx_date, interval 1 day))	
and fc.fare_lockin_date between date_add(r1t.rec_add_date, interval -1 day) and r1t.tvl_dis_date
and fc.jrny_dep_date between r1t.tvl_eff_date and r1t.tvl_dis_date
and (f.ow_rt_ind = r1.ow_rt_ind or (f.ow_rt_ind = 3 and r1.ow_rt_ind = 1))
and (f.rtg_nbr = r1.rtg_nbr or r1.rtg_nbr = '99999')
and if(r1.ftnt = '', true, if(length(f.ftnt) = 2 and (left(f.ftnt,1) between 'A' and 'Z'), left(f.ftnt,1) = r1.ftnt or right(f.ftnt,1) = r1.ftnt, f.ftnt = r1.ftnt))
and (r1s.pax_type = ctrl.bf_pax_type or (ctrl.bf_pax_type = 'ADT' and r1s.pax_type = ''))

#----------------------- check all conditions that come with datawarehouse.temp_fbcx_fbr -------------------------------------------
and fc.fare_lockin_date between date_add(fbcx.rec_add_date, interval -1 day) and if(fbcx.rec_cnx_date = '9999-12-31', fbcx.rec_cnx_date, date_add(fbcx.rec_cnx_date, interval 1 day))	
and fc.fare_lockin_date between date_add(fbcx.rec_add_date, interval -1 day) and fbcx.tvl_dis_date
and fc.jrny_dep_date between fbcx.tvl_eff_date and fbcx.tvl_dis_date

and (fbcx.ow_rt_ind = '' or f.ow_rt_ind = fbcx.ow_rt_ind or (f.ow_rt_ind = 3 and fbcx.ow_rt_ind = 1))
and (r1.fare_type = fbcx.fare_type or fbcx.fare_type = '')		-- Match fare type
and (r1.ssn_type = fbcx.ssn_type or fbcx.ssn_type = '')			-- Match Season Type
and (r1.dow_type = fbcx.dow_type or fbcx.dow_type = '')			-- Match Day of Week Type
and (f.rtg_nbr = fbcx.rtg_nbr or (f.rtg_nbr <> '00000' and fbcx.rtg_nbr = '88888') or fbcx.rtg_nbr = '99999')

-- r1.tkt_dsg_mod <> '' is not implemented, no real cases
-- and fc.fc_tkt_dsg = if(fbcx.dsg_rule = '', r1s.tkt_dsg, fbcx.dsg_rule)
and if(fbcx.dsg_rule <> '', fc.fc_tkt_dsg = fbcx.dsg_rule, if(r1s.tkt_dsg <> '', fc.fc_tkt_dsg = r1s.tkt_dsg, true))

and fc.fc_fbc = if(fbcx.fbc_rule = '', if(r1s.tkt_cd = '', f.fare_cls, r1s.tkt_cd),
					(case
						when left(fbcx.fbc_rule,1) = '*' then concat(left(f.fare_cls, 1), right(fbcx.fbc_rule, length(fbcx.fbc_rule)-1))
						when left(fbcx.fbc_rule,1) = '-' then concat(f.fare_cls, right(fbcx.fbc_rule, length(fbcx.fbc_rule)-1))
						when right(fbcx.fbc_rule,1) = '-' then concat(left(fbcx.fbc_rule, 1), right(f.fare_cls, length(f.fare_cls)-1))
						else fbcx.fbc_rule
					end)
				)

-- The location check must be done last, otherwise it takes up a lot of time
-- !!! lesson: must use simple and effective logics to reduce workload?
and (case ctrl.dir_ind
		when '1' then true	# only 31 instance, not checking it for now, pass
        when '2' then true	# only 20 
		when '3' then fo.fc_loc is not null and fd.fc_loc is not null
		when '4' then ro.fc_loc is not null and rd.fc_loc is not null
		else # blank, no direction, then test either one is true
				(fo.fc_loc is not null and fd.fc_loc is not null)
                or
                (ro.fc_loc is not null and rd.fc_loc is not null)
	end)

and if(fbcx.r2_fare_cls = '', true, if(instr(fbcx.r2_fare_cls, '-') = 0, fbcx.r2_fare_cls = f.fare_cls, f.fare_cls regexp fbcx.r2_fare_cls_regex));


######################## FBR 2

/*
drop table if exists zz_dev.temp_match_fbr_tc1_4;
create table zz_dev.temp_match_fbr_tc1_4;
*/

drop table if exists zz_dev.fm_fbr2_f_4;
create table if not exists zz_dev.fm_fbr2_f_4
select distinct
ctrl.fc_all_id, f.fare_id, fbc_match
from zz_cx.temp_match_fbr_ctrl_5 ctrl

straight_join zz_cx.temp_tbl_fc_all fc on (ctrl.fc_all_id = fc.fc_all_id)
#straight_join zz_dev.fm_fbr2_r3_4 cr on (ctrl.fc_all_id = cr.fc_all_id)
straight_join tmp.temp_map_fbcx_fbr_r fbcm on (fbcm.doc_nbr_prime = fc.doc_nbr_prime and fc.fc_cpn_nbr = fbcm.fc_cpn_nbr and fbcm.fbcx_mode in ('', '-')  )
straight_join zz_cx.temp_tbl_fc_tar_spec ts on fc.fc_orig_n = ts.orig_cntry and fc.fc_dest_n = ts.dest_cntry and fc.fc_carr_cd = ts.carr_cd
straight_join atpco_fare.atpco_fare f on (f.orig_city = fc.fc_orig and f.dest_city = fc.fc_dest and f.carr_cd = fc.fc_carr_cd  and f.tar_nbr = ts.tar_nbr and f.fare_cls = fbcm.fbc_match)
straight_join atpco_fare.atpco_fare_state ft on f.fare_id = ft.fare_id

where ctrl.map_code <> '2'
#----------------------- matching fc to the base fare -------------------------------------------
and fc.fare_lockin_date between (ft.rec_add_date - interval 1 day) and if(ft.rec_cnx_date = '9999-12-31', ft.rec_cnx_date, ft.rec_cnx_date + interval 1 day)
and fc.jrny_dep_date between ft.tvl_eff_date and ft.tvl_dis_date
# not sure the logic is entirely right: and if(fc.map_di_ind = 'F', f.ftn t <> 'T', f.ftnt <> 'F')		-- useful for domestic, for international, ftnt.di is always 'F', international footnote can never be 'F' or 'T'
;

select * from tmp.temp_map_fbcx_fbr_r;
drop table if exists zz_dev.fm_fbr2_r3_4;
create table zz_dev.fm_fbr2_r3_4
select distinct
fc.fc_all_id, r8.rule_id as r8_id, r2.rule_id as r2_id, r3.cat_id as r3_id, r2s.dir_ind, r3.base_tbl_t989
from zz_dev.fm_fbr2_f_4 ctrl

straight_join zz_cx.temp_tbl_fc_all fc on (ctrl.fc_all_id = fc.fc_all_id)

straight_join tmp.temp_map_fbcx_fbr_r fbcm on fbcm.doc_nbr_prime = fc.doc_nbr_prime and fc.fc_cpn_nbr = fbcm.fc_cpn_nbr and fbcm.fbcx_mode in ('', '-') and ctrl.fbc_match = fbcm.fbc_match
straight_join atpco_fare.atpco_r8_fbr r8 on (r8.tar_nbr = fbcm.frt_nbr and r8.carr_cd = fbcm.carr_cd and r8.rule_nbr = fbcm.rule_nbr and r8.proc_ind in ('N', 'R'))
straight_join atpco_fare.atpco_r8_fbr_state r8t on (r8t.rule_id = r8.rule_id)

straight_join atpco_fare.atpco_r2_cat25_ctrl r2 on (r2.carr_cd = r8.carr_cd and r2.tar_nbr = r8.tar_nbr and r2.rule_nbr = r8.rule_nbr and r2.proc_ind in ('N', 'R'))
straight_join atpco_fare.atpco_r2_cat25_ctrl_state r2t on (r2t.rule_id = r2.rule_id)
straight_join atpco_fare.atpco_r2_cat25_ctrl_sup r2s on (r2s.rule_id = r2.rule_id)

straight_join atpco_fare.atpco_cat25 r3 on (r3.cat_id = r2s.tbl_nbr)
left join atpco_fare.atpco_t994_date dor on (dor.tbl_nbr = r3.dt_ovrd_t994)

where 1
#----------------------- record 8 to the fare component -------------------------------------------
-- loc testing is not implemented
and fc.fare_lockin_date between (r8t.rec_add_date - interval 1 day) and if(r8t.rec_cnx_date = '9999-12-31', r8t.rec_cnx_date, r8t.rec_cnx_date + interval 1 day)
and fc.jrny_dep_date between r8t.tvl_eff_date and r8t.tvl_dis_date

#----------------------- applying record 2 to fc -------------------------------------------
-- loc testing is not implemented
and fc.fare_lockin_date between (r2t.rec_add_date - interval 1 day) and if(r2t.rec_cnx_date = '9999-12-31', r2t.rec_cnx_date, r2t.rec_cnx_date + interval 1 day)
and fc.jrny_dep_date between r2t.tvl_eff_date and r2t.tvl_dis_date

#----------------------- cat 25 -------------------------------------------
and r3.pax_type = r8.pax_type
and if(r3.rslt_fare_tkt_cd <> '', r3.rslt_fare_tkt_cd = fbcm.fbc_rule, if(r3.rslt_fare_cls <> '', r3.rslt_fare_cls = fbcm.fbc_rule, true))

#----------------------- table 994 date override -------------------------------------------
and if(dor.tbl_nbr is null, true, fc.trnsc_date between dor.tkt_fr_date and dor.tkt_to_date)
and if(dor.tbl_nbr is null, true, fc.fc_dep_date between dor.tvl_fr_date and dor.tvl_to_date);

alter table zz_dev.fm_fbr2_r3_4
add index idx_tmp_id(fc_all_id, base_tbl_t989);

analyze table zz_dev.fm_fbr2_r3_4;

drop table if exists zz_dev.fm_fbr2_t989_4;
create table zz_dev.fm_fbr2_t989_4
select distinct fc_all_id, base_tbl_t989
from zz_dev.fm_fbr2_r3_4;

alter table zz_dev.fm_fbr2_t989_4
add index idx_fbr_t989_id(fc_all_id);


drop table if exists zz_dev.fm_fbr2_syn_4;
create table if not exists zz_dev.fm_fbr2_syn_4
select distinct fc.fc_all_id, r2_id as r2_rule_id, r3_id as r3_25_cat_id, 
f.fare_id as f_fare_id, r8_id as r8_rule_id, t989.tbl_nbr as t989_tbl_nbr, t989.bf_pax_type, cr.dir_ind, g16.frt_nbr
from zz_dev.fm_fbr2_f_4 ctrl
straight_join zz_cx.temp_tbl_fc_all fc on (ctrl.fc_all_id = fc.fc_all_id)

straight_join atpco_fare.atpco_fare f on f.fare_id = ctrl.fare_id

join zz_dev.fm_fbr2_t989_4 ct on ctrl.fc_all_id = ct.fc_all_id 

straight_join atpco_fare.atpco_t989_base_fare t989 on (t989.tbl_nbr = ct.base_tbl_t989 and t989.bf_appl <> 'N')

straight_join zz_dev.fm_fbr2_r3_4 cr on ct.fc_all_id = cr.fc_all_id and ct.base_tbl_t989 = cr.base_tbl_t989

straight_join zz_cx.g16_temp g16 on (g16.carr_cd = fc.fc_carr_cd and (g16.frt_nbr = t989.bf_rule_tar or (t989.bf_rule_tar = '000' and g16.pri_ind <> 'X'))) and g16.ff_nbr = f.tar_nbr

where
#----------------------- matching t989 to the base fare -------------------------------------------
 (f.rule_nbr = t989.bf_rule_nbr or t989.bf_rule_nbr = '')
and if(t989.bf_fc = '', true, f.fare_cls regexp t989.bf_fc_regex)
and (f.rtg_nbr = t989.bf_rtg_nbr or t989.bf_rtg_nbr = '99999')
and (f.ow_rt_ind = t989.bf_ow_rt or t989.bf_ow_rt = '')
;

drop table if exists zz_dev.fm_fbr2_loc_4;
create table if not exists zz_dev.fm_fbr2_loc_4
select distinct fc.doc_nbr_prime, fc.fc_cpn_nbr, ctrl.fc_all_id, ctrl.r2_rule_id as r2_25_rule_id, ctrl.r3_25_cat_id as r3_25_cat_id, f.fare_id, 'R' as map_type, 'B' as map_code
from zz_dev.fm_fbr2_syn_4 ctrl
 
join zz_cx.temp_tbl_fc_all fc on (ctrl.fc_all_id = fc.fc_all_id)
join atpco_fare.atpco_fare f on ctrl.f_fare_id = f.fare_id

join atpco_fare.atpco_r1_fare_cls r1 on (r1.carr_cd = f.carr_cd and r1.tar_nbr = ctrl.frt_nbr and r1.rule_nbr = f.rule_nbr and r1.fare_cls = f.fare_cls and r1.proc_ind in ('N', 'R'))
join atpco_fare.atpco_r1_fare_cls_state r1t on (r1t.rule_id = r1.rule_id)
join atpco_fare.atpco_r1_fare_cls_sup r1s on (r1s.rule_id = r1.rule_id)

join zz_cx.temp_fbcx_fbr_r fbcx on (fbcx.r8_rule_id = ctrl.r8_rule_id and fbcx.r2_25_rule_id = ctrl.r2_rule_id and fbcx.r3_25_cat_id = ctrl.r3_25_cat_id)
join zz_cx.temp_tbl_fc_loc fc_loc on (fc_loc.doc_nbr_prime = fc.doc_nbr_prime and fc_loc.fc_cpn_nbr = fc.fc_cpn_nbr and fc_loc.map_di_ind = fc.map_di_ind)		
# to allow base fare matching and r2 loaction testing using somewhat different orig/dest

# forward direction location test
left join zz_cx.loc_m fo on (fo.fc_loc = fc_loc.fc_orig and fo.loc_type = fbcx.loc1_type and fo.loc = fbcx.loc1 and fo.loc_t = fbcx.loc1_t978)
left join zz_cx.loc_m fd on (fd.fc_loc = fc_loc.fc_dest and fd.loc_type = fbcx.loc2_type and fd.loc = fbcx.loc2 and fd.loc_t = fbcx.loc2_t978)

# reverse direction location test
left join zz_cx.loc_m ro on (ro.fc_loc = fc_loc.fc_dest and ro.loc_type = fbcx.loc1_type and ro.loc = fbcx.loc1 and ro.loc_t = fbcx.loc1_t978)
left join zz_cx.loc_m rd on (rd.fc_loc = fc_loc.fc_orig and rd.loc_type = fbcx.loc2_type and rd.loc = fbcx.loc2 and rd.loc_t = fbcx.loc2_t978)

########################################################################

where

#----------------------- conditions for fare record to r1 -------------------------------------------
-- location testing is not implemented
-- minimum sequence is not implemented
 fc.fare_lockin_date between date_add(r1t.rec_add_date, interval -1 day) and if(r1t.rec_cnx_date = '9999-12-31', r1t.rec_cnx_date, date_add(r1t.rec_cnx_date, interval 1 day))	
and fc.fare_lockin_date between date_add(r1t.rec_add_date, interval -1 day) and r1t.tvl_dis_date
and fc.jrny_dep_date between r1t.tvl_eff_date and r1t.tvl_dis_date
and (f.ow_rt_ind = r1.ow_rt_ind or (f.ow_rt_ind = 3 and r1.ow_rt_ind = 1))
and (f.rtg_nbr = r1.rtg_nbr or r1.rtg_nbr = '99999')
and if(r1.ftnt = '', true, if(length(f.ftnt) = 2 and (left(f.ftnt,1) between 'A' and 'Z'), left(f.ftnt,1) = r1.ftnt or right(f.ftnt,1) = r1.ftnt, f.ftnt = r1.ftnt))
and (r1s.pax_type = ctrl.bf_pax_type or (ctrl.bf_pax_type = 'ADT' and r1s.pax_type = ''))

#----------------------- check all conditions that come with datawarehouse.temp_fbcx_fbr -------------------------------------------
and fc.fare_lockin_date between date_add(fbcx.rec_add_date, interval -1 day) and if(fbcx.rec_cnx_date = '9999-12-31', fbcx.rec_cnx_date, date_add(fbcx.rec_cnx_date, interval 1 day))	
and fc.fare_lockin_date between date_add(fbcx.rec_add_date, interval -1 day) and fbcx.tvl_dis_date
and fc.jrny_dep_date between fbcx.tvl_eff_date and fbcx.tvl_dis_date
and (fbcx.ow_rt_ind = '' or f.ow_rt_ind = fbcx.ow_rt_ind or (f.ow_rt_ind = 3 and fbcx.ow_rt_ind = 1))
and (r1.fare_type = fbcx.fare_type or fbcx.fare_type = '')		-- Match fare type
and (r1.ssn_type = fbcx.ssn_type or fbcx.ssn_type = '')			-- Match Season Type
and (r1.dow_type = fbcx.dow_type or fbcx.dow_type = '')			-- Match Day of Week Type
and (f.rtg_nbr = fbcx.rtg_nbr or (f.rtg_nbr <> '00000' and fbcx.rtg_nbr = '88888') or fbcx.rtg_nbr = '99999')

-- r1.tkt_dsg_mod <> '' is not implemented, no real cases
-- and fc.fc_tkt_dsg = if(fbcx.dsg_rule = '', r1s.tkt_dsg, fbcx.dsg_rule)
and if(fbcx.dsg_rule <> '', fc.fc_tkt_dsg = fbcx.dsg_rule, if(r1s.tkt_dsg <> '', fc.fc_tkt_dsg = r1s.tkt_dsg, true))

and fc.fc_fbc = if(fbcx.fbc_rule = '', if(r1s.tkt_cd = '', f.fare_cls, r1s.tkt_cd),
					(case
						when left(fbcx.fbc_rule,1) = '*' then concat(left(f.fare_cls, 1), right(fbcx.fbc_rule, length(fbcx.fbc_rule)-1))
						when left(fbcx.fbc_rule,1) = '-' then concat(f.fare_cls, right(fbcx.fbc_rule, length(fbcx.fbc_rule)-1))
						when right(fbcx.fbc_rule,1) = '-' then concat(left(fbcx.fbc_rule, 1), right(f.fare_cls, length(f.fare_cls)-1))
						else fbcx.fbc_rule
					end)
				)

-- The location check must be done last, otherwise it takes up a lot of time
-- !!! lesson: must use simple and effective logics to reduce workload?
and (case ctrl.dir_ind
		when '1' then true	# only 31 instance, not checking it for now, pass
        when '2' then true	# only 20 
		when '3' then fo.fc_loc is not null and fd.fc_loc is not null
		when '4' then ro.fc_loc is not null and rd.fc_loc is not null
		else # blank, no direction, then test either one is true
				(fo.fc_loc is not null and fd.fc_loc is not null)
                or
                (ro.fc_loc is not null and rd.fc_loc is not null)
	end)

and if(fbcx.r2_fare_cls = '', true, if(instr(fbcx.r2_fare_cls, '-') = 0, fbcx.r2_fare_cls = f.fare_cls, f.fare_cls regexp fbcx.r2_fare_cls_regex))
;

############################# FBR3
/*

create table tmp.temp_map_fbcx_fbr_r like tmp.temp_map_fbcx_fbr_r;

insert into tmp.temp_map_fbcx_fbr_r
select * from tmp.temp_map_fbcx_fbr_r;

ALTER TABLE `zz_cx`.`temp_map_fbcx_fbr` 
DROP INDEX `doc` ,
ADD INDEX `doc` (`doc_nbr_prime` ASC, `fc_cpn_nbr` ASC) VISIBLE;
;

create table tmp.temp_map_fbcx_fbr_m
select distinct doc_nbr_prime, fc_cpn_nbr, fbc_match, fbcx_mode, tkt_cd_ind from tmp.temp_map_fbcx_fbr_r;

alter table tmp.temp_map_fbcx_fbr_m
add primary key tmp_mapfbcxfbrm(doc_nbr_prime, fc_cpn_nbr, fbcx_mode, tkt_cd_ind, fbc_match);

*/

drop table if exists zz_dev.fm_fbr3_f_4;
create table if not exists zz_dev.fm_fbr3_f_4
select distinct
ctrl.fc_all_id, f.fare_id, fbc_match
from zz_cx.temp_match_fbr_ctrl_5 ctrl

straight_join zz_cx.temp_tbl_fc_all fc on (ctrl.fc_all_id = fc.fc_all_id)
straight_join tmp.temp_map_fbcx_fbr_m fbcm on (fbcm.doc_nbr_prime = fc.doc_nbr_prime and fbcm.fc_cpn_nbr = fc.fc_cpn_nbr and fbcm.fbcx_mode = '' and fbcm.tkt_cd_ind = 'Y')

#straight_join tmp.temp_map_fbcx_fbr_r fbcm on (fbcm.doc_nbr_prime = fc.doc_nbr_prime and fbcm.fc_cpn_nbr = fc.fc_cpn_nbr and fbcm.fbcx_mode = '' and fbcm.tkt_cd_ind = 'Y')
straight_join zz_cx.temp_tbl_fc_tar_spec ts on fc.fc_orig_n = ts.orig_cntry and fc.fc_dest_n = ts.dest_cntry and fc.fc_carr_cd = ts.carr_cd
straight_join zz_cx.temp_map_r1_tkt_cd tcd on (tcd.tkt_cd = fbcm.fbc_match and tcd.carr_cd = fc.fc_carr_cd)  # reverse look from ticketing code to fare class code

straight_join atpco_fare.atpco_fare f on (f.orig_city = fc.fc_orig and f.dest_city = fc.fc_dest and f.carr_cd = fc.fc_carr_cd and f.tar_nbr = ts.tar_nbr and f.fare_cls = tcd.fare_cls)
straight_join atpco_fare.atpco_fare_state ft on f.fare_id = ft.fare_id

where fc.doc_nbr_prime > 0

#----------------------- matching fc to the base fare -------------------------------------------
and fc.fare_lockin_date between (ft.rec_add_date - interval 1 day) and if(ft.rec_cnx_date = '9999-12-31', ft.rec_cnx_date, ft.rec_cnx_date + interval 1 day)
and fc.jrny_dep_date between ft.tvl_eff_date and ft.tvl_dis_date
# not sure the logic is entirely right: and if(fc.map_di_ind = 'F', f.ftnt <> 'T', f.ftnt <> 'F')		-- useful for domestic, for international, ftnt.di is always 'F', international footnote can never be 'F' or 'T'

and ctrl.map_code <> '2'
;

drop table if exists zz_dev.fm_fbr3_r2_4;
create table zz_dev.fm_fbr3_r2_4
select distinct
fc.fc_all_id, r8.rule_id as r8_id, r8.tar_nbr, r8.carr_cd, r8.rule_nbr, r8.pax_type,
fc.fc_fbc, 
fbcm.fbc_rule
from zz_dev.fm_fbr3_f_4 ctrl

straight_join zz_cx.temp_tbl_fc_all fc on (ctrl.fc_all_id = fc.fc_all_id)

straight_join tmp.temp_map_fbcx_fbr_r fbcm on (fbcm.doc_nbr_prime = fc.doc_nbr_prime and fbcm.fc_cpn_nbr = fc.fc_cpn_nbr and fbcm.fbcx_mode = '' and fbcm.tkt_cd_ind = 'Y') and ctrl.fbc_match = fbcm.fbc_match

straight_join atpco_fare.atpco_r8_fbr r8 on (r8.tar_nbr = fbcm.frt_nbr and r8.carr_cd = fbcm.carr_cd and r8.rule_nbr = fbcm.rule_nbr and r8.proc_ind in ('N', 'R'))
straight_join atpco_fare.atpco_r8_fbr_state r8t on (r8t.rule_id = r8.rule_id)

where 
#----------------------- record 8 to the fare component -------------------------------------------
-- loc testing is not implemented
 fc.fare_lockin_date between (r8t.rec_add_date - interval 1 day) and if(r8t.rec_cnx_date = '9999-12-31', r8t.rec_cnx_date, r8t.rec_cnx_date + interval 1 day)
and fc.jrny_dep_date between r8t.tvl_eff_date and r8t.tvl_dis_date

;

drop table if exists zz_dev.fm_fbr3_r3_4;
create table zz_dev.fm_fbr3_r3_4
select distinct
fc.fc_all_id, ctrl.r8_id as r8_id, r2.rule_id as r2_id, r3.cat_id as r3_id, r2s.dir_ind, r3.base_tbl_t989
from zz_dev.fm_fbr3_r2_4 ctrl

straight_join zz_cx.temp_tbl_fc_all fc on (ctrl.fc_all_id = fc.fc_all_id)

straight_join atpco_fare.atpco_r2_cat25_ctrl r2 on (r2.carr_cd = ctrl.carr_cd and r2.tar_nbr = ctrl.tar_nbr and r2.rule_nbr = ctrl.rule_nbr and r2.proc_ind in ('N', 'R'))
straight_join atpco_fare.atpco_r2_cat25_ctrl_state r2t on (r2t.rule_id = r2.rule_id)
straight_join atpco_fare.atpco_r2_cat25_ctrl_sup r2s on (r2s.rule_id = r2.rule_id)

straight_join atpco_fare.atpco_cat25 r3 on (r3.cat_id = r2s.tbl_nbr)
left join atpco_fare.atpco_t994_date dor on (dor.tbl_nbr = r3.dt_ovrd_t994)

where 


#----------------------- cat 25 -------------------------------------------
 r3.pax_type = ctrl.pax_type
and if(r3.rslt_fare_tkt_cd <> '', r3.rslt_fare_tkt_cd = ctrl.fbc_rule, if(r3.rslt_fare_cls <> '', r3.rslt_fare_cls = ctrl.fbc_rule, true))

#----------------------- table 994 date override -------------------------------------------
and if(dor.tbl_nbr is null, true, fc.trnsc_date between dor.tkt_fr_date and dor.tkt_to_date)
and if(dor.tbl_nbr is null, true, fc.fc_dep_date between dor.tvl_fr_date and dor.tvl_to_date)

#----------------------- applying record 2 to fc -------------------------------------------
-- loc testing is not implemented
and fc.fare_lockin_date between (r2t.rec_add_date - interval 1 day) and if(r2t.rec_cnx_date = '9999-12-31', r2t.rec_cnx_date, r2t.rec_cnx_date + interval 1 day)
and fc.jrny_dep_date between r2t.tvl_eff_date and r2t.tvl_dis_date

;


alter table zz_dev.fm_fbr3_r3_4
add index idx_tmp_id(fc_all_id, base_tbl_t989);

analyze table zz_dev.fm_fbr3_r3_4;

drop table if exists zz_dev.fm_fbr3_t989_4;
create table zz_dev.fm_fbr3_t989_4
select distinct fc_all_id, base_tbl_t989
from zz_dev.fm_fbr3_r3_4;

alter table zz_dev.fm_fbr3_t989_4
add index idx_fbr_t989_id(fc_all_id);

drop table if exists zz_dev.fm_fbr3_syn_4;
create table if not exists zz_dev.fm_fbr3_syn_4
select distinct fc.fc_all_id, r2_id as r2_rule_id, r3_id as r3_25_cat_id, 
f.fare_id as f_fare_id, r8_id as r8_rule_id, t989.tbl_nbr as t989_tbl_nbr, t989.bf_pax_type, cr.dir_ind, g16.frt_nbr, ctrl.fbc_match

from zz_dev.fm_fbr3_f_4 ctrl

straight_join zz_cx.temp_tbl_fc_all fc on (ctrl.fc_all_id = fc.fc_all_id)

straight_join atpco_fare.atpco_fare f on f.fare_id = ctrl.fare_id

join zz_dev.fm_fbr3_t989_4 ct on ctrl.fc_all_id = ct.fc_all_id 

straight_join atpco_fare.atpco_t989_base_fare t989 on (t989.tbl_nbr = ct.base_tbl_t989 and t989.bf_appl <> 'N')

straight_join zz_cx.g16_temp g16 on (g16.carr_cd = fc.fc_carr_cd and (g16.frt_nbr = t989.bf_rule_tar or (t989.bf_rule_tar = '000' and g16.pri_ind <> 'X')))

straight_join zz_dev.fm_fbr3_r3_4 cr on ct.fc_all_id = cr.fc_all_id and ct.base_tbl_t989 = cr.base_tbl_t989

where fc.doc_nbr_prime > 0

#----------------------- matching t989 to the base fare -------------------------------------------
and (f.rule_nbr = t989.bf_rule_nbr or t989.bf_rule_nbr = '')
and if(t989.bf_fc = '', true, f.fare_cls regexp t989.bf_fc_regex)
and (f.rtg_nbr = t989.bf_rtg_nbr or t989.bf_rtg_nbr = '99999')
and (f.ow_rt_ind = t989.bf_ow_rt or t989.bf_ow_rt = '')
-- bf_type, not yet implemented
-- bf_ssn, not yet implemented
-- bf_dow, not yet implemented
-- ftnt, not yet implemented
-- and (fc.fc_pax_type = t989.c25_pax_type or t989.c25_pax_type in ('ADT', 'JCB', ''))	-- JCB and ADT are general
;

drop table if exists zz_dev.fm_fbr3_loc_4;
create table if not exists zz_dev.fm_fbr3_loc_4
select distinct fc.doc_nbr_prime, fc.fc_cpn_nbr, ctrl.fc_all_id, ctrl.r2_rule_id as r2_25_rule_id, ctrl.r3_25_cat_id as r3_25_cat_id, f.fare_id, 'R' as map_type, 'B' as map_code
from zz_dev.fm_fbr3_syn_4 ctrl

straight_join zz_cx.temp_tbl_fc_all fc on (ctrl.fc_all_id = fc.fc_all_id)

join atpco_fare.atpco_fare f on ctrl.f_fare_id = f.fare_id

#straight_join tmp.temp_map_fbcx_fbr_r fbcm on (fbcm.doc_nbr_prime = fc.doc_nbr_prime and fbcm.fc_cpn_nbr = fc.fc_cpn_nbr and fbcm.fbcx_mode = '' and fbcm.tkt_cd_ind = 'Y')

straight_join atpco_fare.atpco_r1_fare_cls r1 on (r1.carr_cd = f.carr_cd and r1.tar_nbr = ctrl.frt_nbr and r1.rule_nbr = f.rule_nbr and r1.fare_cls = f.fare_cls and r1.proc_ind in ('N', 'R'))
straight_join atpco_fare.atpco_r1_fare_cls_state r1t on (r1t.rule_id = r1.rule_id)
straight_join atpco_fare.atpco_r1_fare_cls_sup r1s on (r1s.rule_id = r1.rule_id and r1s.tkt_cd = ctrl.fbc_match)

straight_join zz_cx.temp_fbcx_fbr fbcx on (fbcx.r8_rule_id = ctrl.r8_rule_id and fbcx.r2_25_rule_id = ctrl.r2_rule_id and fbcx.r3_25_cat_id = ctrl.r3_25_cat_id)
straight_join zz_cx.temp_tbl_fc_loc fc_loc on (fc_loc.doc_nbr_prime = fc.doc_nbr_prime and fc_loc.fc_cpn_nbr = fc.fc_cpn_nbr and fc_loc.map_di_ind = fc.map_di_ind)		
# to allow base fare matching and r2 loaction testing using somewhat different orig/dest

# forward direction location test
left join zz_cx.loc_m fo on (fo.fc_loc = fc_loc.fc_orig and fo.loc_type = fbcx.loc1_type and fo.loc = fbcx.loc1 and fo.loc_t = fbcx.loc1_t978)
left join zz_cx.loc_m fd on (fd.fc_loc = fc_loc.fc_dest and fd.loc_type = fbcx.loc2_type and fd.loc = fbcx.loc2 and fd.loc_t = fbcx.loc2_t978)

# reverse direction location test
left join zz_cx.loc_m ro on (ro.fc_loc = fc_loc.fc_dest and ro.loc_type = fbcx.loc1_type and ro.loc = fbcx.loc1 and ro.loc_t = fbcx.loc1_t978)
left join zz_cx.loc_m rd on (rd.fc_loc = fc_loc.fc_orig and rd.loc_type = fbcx.loc2_type and rd.loc = fbcx.loc2 and rd.loc_t = fbcx.loc2_t978)
where fc.doc_nbr_prime > 0

#----------------------- conditions for fare record to r1 -------------------------------------------
-- location testing is not implemented
-- minimum sequence is not implemented
and fc.fare_lockin_date between date_add(r1t.rec_add_date, interval -1 day) and if(r1t.rec_cnx_date = '9999-12-31', r1t.rec_cnx_date, date_add(r1t.rec_cnx_date, interval 1 day))	
and fc.fare_lockin_date between date_add(r1t.rec_add_date, interval -1 day) and r1t.tvl_dis_date
and fc.jrny_dep_date between r1t.tvl_eff_date and r1t.tvl_dis_date
and (f.ow_rt_ind = r1.ow_rt_ind or (f.ow_rt_ind = 3 and r1.ow_rt_ind = 1))
and (f.rtg_nbr = r1.rtg_nbr or r1.rtg_nbr = '99999')
and if(r1.ftnt = '', true, if(length(f.ftnt) = 2 and (left(f.ftnt,1) between 'A' and 'Z'), left(f.ftnt,1) = r1.ftnt or right(f.ftnt,1) = r1.ftnt, f.ftnt = r1.ftnt))
and (r1s.pax_type = ctrl.bf_pax_type or (ctrl.bf_pax_type = 'ADT' and r1s.pax_type = ''))

#----------------------- check all conditions that come with datawarehouse.temp_fbcx_fbr -------------------------------------------
and fc.fare_lockin_date between date_add(fbcx.rec_add_date, interval -1 day) and if(fbcx.rec_cnx_date = '9999-12-31', fbcx.rec_cnx_date, date_add(fbcx.rec_cnx_date, interval 1 day))	
and fc.fare_lockin_date between date_add(fbcx.rec_add_date, interval -1 day) and fbcx.tvl_dis_date
and fc.jrny_dep_date between fbcx.tvl_eff_date and fbcx.tvl_dis_date

and (fbcx.ow_rt_ind = '' or f.ow_rt_ind = fbcx.ow_rt_ind or (f.ow_rt_ind = 3 and fbcx.ow_rt_ind = 1))
and (r1.fare_type = fbcx.fare_type or fbcx.fare_type = '')		-- Match fare type
and (r1.ssn_type = fbcx.ssn_type or fbcx.ssn_type = '')			-- Match Season Type
and (r1.dow_type = fbcx.dow_type or fbcx.dow_type = '')			-- Match Day of Week Type
and (f.rtg_nbr = fbcx.rtg_nbr or (f.rtg_nbr <> '00000' and fbcx.rtg_nbr = '88888') or fbcx.rtg_nbr = '99999')

-- r1.tkt_dsg_mod <> '' is not implemented, no real cases
-- and fc.fc_tkt_dsg = if(fbcx.dsg_rule = '', r1s.tkt_dsg, fbcx.dsg_rule)
and if(fbcx.dsg_rule <> '', fc.fc_tkt_dsg = fbcx.dsg_rule, if(r1s.tkt_dsg <> '', fc.fc_tkt_dsg = r1s.tkt_dsg, true))

and fc.fc_fbc = if(fbcx.fbc_rule = '', if(r1s.tkt_cd = '', f.fare_cls, r1s.tkt_cd),
					(case
						when left(fbcx.fbc_rule,1) = '*' then concat(left(f.fare_cls, 1), right(fbcx.fbc_rule, length(fbcx.fbc_rule)-1))
						when left(fbcx.fbc_rule,1) = '-' then concat(f.fare_cls, right(fbcx.fbc_rule, length(fbcx.fbc_rule)-1))
						when right(fbcx.fbc_rule,1) = '-' then concat(left(fbcx.fbc_rule, 1), right(f.fare_cls, length(f.fare_cls)-1))
						else fbcx.fbc_rule
					end)
				)

-- The location check must be done last, otherwise it takes up a lot of time
-- !!! lesson: must use simple and effective logics to reduce workload?
and (case ctrl.dir_ind
		when '1' then true	# only 31 instance, not checking it for now, pass
        when '2' then true	# only 20 
		when '3' then fo.fc_loc is not null and fd.fc_loc is not null
		when '4' then ro.fc_loc is not null and rd.fc_loc is not null
		else # blank, no direction, then test either one is true
				(fo.fc_loc is not null and fd.fc_loc is not null)
                or
                (ro.fc_loc is not null and rd.fc_loc is not null)
	end)

and if(fbcx.r2_fare_cls = '', true, if(instr(fbcx.r2_fare_cls, '-') = 0, fbcx.r2_fare_cls = f.fare_cls, f.fare_cls regexp fbcx.r2_fare_cls_regex))
;

####################### FBR4

/*
ALTER TABLE `zz_cx`.`temp_fbcx_fbr` 
CHANGE COLUMN `rid` `rcid` INT(11) NOT NULL AUTO_INCREMENT ,
CHANGE COLUMN `fbc_rule` `fbc_rule` CHAR(20) NOT NULL DEFAULT '' ,
CHANGE COLUMN `dsg_rule` `dsg_rule` CHAR(20) NOT NULL DEFAULT '' ,
DROP INDEX `idx_r2` ,
ADD INDEX `idx_r2` (`r8_rule_id` ASC, `r2_25_rule_id` ASC, `r3_25_cat_id` ASC, `fbc_rule` ASC) VISIBLE;
;


*/
drop table if exists zz_dev.fm_fbr4_f_4;
create table if not exists zz_dev.fm_fbr4_f_4
select distinct
ctrl.fc_all_id, f.fare_id, fbc_match

from zz_cx.temp_match_fbr_ctrl_5 ctrl

straight_join zz_cx.temp_tbl_fc_all fc on (ctrl.fc_all_id = fc.fc_all_id)

straight_join tmp.temp_map_fbcx_fbr_m fbcm on (fbcm.doc_nbr_prime = fc.doc_nbr_prime and fbcm.fc_cpn_nbr = fc.fc_cpn_nbr and fbcm.fbcx_mode = 'X')
straight_join zz_cx.temp_tbl_fc_tar_spec ts on fc.fc_orig_n = ts.orig_cntry and fc.fc_dest_n = ts.dest_cntry and fc.fc_carr_cd = ts.carr_cd
straight_join atpco_fare.atpco_fare f on (f.orig_city = fc.fc_orig and f.dest_city = fc.fc_dest and f.carr_cd = fc.fc_carr_cd and f.tar_nbr = ts.tar_nbr)
straight_join atpco_fare.atpco_fare_state ft on f.fare_id = ft.fare_id

where fc.doc_nbr_prime > 0

#----------------------- matching fc to the base fare -------------------------------------------
and fc.fare_lockin_date between (ft.rec_add_date - interval 1 day) and if(ft.rec_cnx_date = '9999-12-31', ft.rec_cnx_date, ft.rec_cnx_date + interval 1 day)
and fc.jrny_dep_date between ft.tvl_eff_date and ft.tvl_dis_date
# not sure the logic is entirely right: and if(fc.map_di_ind = 'F', f.ftnt <> 'T', f.ftnt <> 'F')		-- useful for domestic, for international, ftnt.di is always 'F', international footnote can never be 'F' or 'T'

and ctrl.map_code <> '2'
;

/*
alter table zz_cx.temp_fbcx_fbr_r

add index idx_fbr_temp (fbc_rule, dsg_rule, r8_rule_id);

alter table zz_cx.temp_fbcx_fbr_r
drop index idx_fbr_temp2,
add index idx_fbr_temp2 (tar_nbr, carr_cd, rule_nbr, r2_25_rule_id, fbc_rule, dsg_rule);
*/
drop table if exists zz_dev.fm_fbr4_r2_4;

create table if not exists zz_dev.fm_fbr4_r2_4 
select distinct fc.fc_all_id, r8.tar_nbr, r8.carr_cd, r8.rule_nbr, r8.pax_type,
fc.fc_fbc, 
fbcm.fbc_rule from zz_cx.temp_match_fbr_ctrl_5 ctrl
straight_join zz_cx.temp_tbl_fc_all fc on (ctrl.fc_all_id = fc.fc_all_id)  
straight_join tmp.temp_map_fbcx_fbr_r fbcm on (fbcm.doc_nbr_prime = fc.doc_nbr_prime and fbcm.fc_cpn_nbr = fc.fc_cpn_nbr and fbcm.fbcx_mode = 'X')  
straight_join atpco_fare.atpco_r8_fbr r8 on (r8.tar_nbr = fbcm.frt_nbr and r8.carr_cd = fbcm.carr_cd and r8.rule_nbr = fbcm.rule_nbr and r8.proc_ind in ('N', 'R')) 
straight_join atpco_fare.atpco_r8_fbr_state r8t on (r8t.rule_id = r8.rule_id)  

left join zz_cx.temp_fbcx_fbr_r fbcx  on (fbcx.r8_rule_id = r8.rule_id  and fbcx.fbc_rule = fc.fc_fbc  and (fbcx.dsg_rule = fc.fc_tkt_dsg ) )  
left join zz_cx.temp_fbcx_fbr_r fbcx1  on fbcx1.r8_rule_id = r8.rule_id  and fbcx1.fbc_rule = fc.fc_fbc  and (fbcx1.dsg_rule = '')

straight_join atpco_fare.atpco_r2_cat25_ctrl r2 on (r2.carr_cd = r8.carr_cd and r2.tar_nbr = r8.tar_nbr and r2.rule_nbr = r8.rule_nbr and r2.proc_ind in ('N', 'R')) 
straight_join atpco_fare.atpco_r2_cat25_ctrl_state r2t on (r2t.rule_id = r2.rule_id) 
straight_join atpco_fare.atpco_r2_cat25_ctrl_sup r2s on (r2s.rule_id = r2.rule_id)  

where fc.doc_nbr_prime > 0 and (fbcx.rcid is not null or fbcx.rcid is not null) 
#----------------------- record 8 to the fare component ------------------------------------------- -- loc testing is not implemented 
and fc.fare_lockin_date between (r8t.rec_add_date - interval 1 day) 
and if(r8t.rec_cnx_date = '9999-12-31', r8t.rec_cnx_date, r8t.rec_cnx_date + interval 1 day) 
and fc.jrny_dep_date between r8t.tvl_eff_date and r8t.tvl_dis_date 

 #----------------------- check all conditions that come with datawarehouse.temp_fbcx_fbr ------------------------------------------- 

and fbcx.tvl_dis_date and fc.jrny_dep_date between fbcx.tvl_eff_date and fbcx.tvl_dis_date  and ctrl.map_code <> '2';


drop table if exists zz_dev.fm_fbr4_r3_4;

create table if not exists zz_dev.fm_fbr4_r3_4
select distinct fc.fc_all_id, r2.rule_id as r2_id, r3.cat_id as r3_id, r3.base_tbl_t989,
ctrl.tar_nbr, ctrl.carr_cd, ctrl.rule_nbr, ctrl.fc_fbc
, fbcx.rcid as fbcx_id
#, r8.rule_id as r8_id, r2s.dir_ind
from zz_dev.fm_fbr4_r2_4 ctrl

straight_join zz_cx.temp_tbl_fc_all fc on (ctrl.fc_all_id = fc.fc_all_id)

#straight_join tmp.temp_map_fbcx_fbr_r fbcm on (fbcm.doc_nbr_prime = fc.doc_nbr_prime and fbcm.fc_cpn_nbr = fc.fc_cpn_nbr and fbcm.fbcx_mode = 'X')

straight_join atpco_fare.atpco_r2_cat25_ctrl r2 on (r2.carr_cd = ctrl.carr_cd and r2.tar_nbr = ctrl.tar_nbr and r2.rule_nbr = ctrl.rule_nbr and r2.proc_ind in ('N', 'R'))
straight_join atpco_fare.atpco_r2_cat25_ctrl_state r2t on (r2t.rule_id = r2.rule_id)
straight_join atpco_fare.atpco_r2_cat25_ctrl_sup r2s on (r2s.rule_id = r2.rule_id)

straight_join atpco_fare.atpco_cat25 r3 on (r3.cat_id = r2s.tbl_nbr)
left join atpco_fare.atpco_t994_date dor on (dor.tbl_nbr = r3.dt_ovrd_t994)
straight_join zz_cx.temp_fbcx_fbr_r fbcx 
on (fbcx.tar_nbr = ctrl.tar_nbr and fbcx.carr_cd = ctrl.carr_cd and fbcx.rule_nbr = ctrl.rule_nbr and fbcx.r2_25_rule_id = r2.rule_id and fbcx.r3_25_cat_id = r3.cat_id 
and fbcx.fbc_rule = fc.fc_fbc 
and (fbcx.dsg_rule = fc.fc_tkt_dsg or fbcx.dsg_rule = ''))

straight_join zz_cx.temp_tbl_fc_loc fc_loc on (fc_loc.doc_nbr_prime = fc.doc_nbr_prime and fc_loc.fc_cpn_nbr = fc.fc_cpn_nbr and fc_loc.map_di_ind = fc.map_di_ind)		
# to allow base fare matching and r2 loaction testing using somewhat different orig/dest

# forward direction location test
left join zz_cx.loc_m fo on (fo.fc_loc = fc_loc.fc_orig and fo.loc_type = fbcx.loc1_type and fo.loc = fbcx.loc1 and fo.loc_t = fbcx.loc1_t978)
left join zz_cx.loc_m fd on (fd.fc_loc = fc_loc.fc_dest and fd.loc_type = fbcx.loc2_type and fd.loc = fbcx.loc2 and fd.loc_t = fbcx.loc2_t978)

# reverse direction location test
left join zz_cx.loc_m ro on (ro.fc_loc = fc_loc.fc_dest and ro.loc_type = fbcx.loc1_type and ro.loc = fbcx.loc1 and ro.loc_t = fbcx.loc1_t978)
left join zz_cx.loc_m rd on (rd.fc_loc = fc_loc.fc_orig and rd.loc_type = fbcx.loc2_type and rd.loc = fbcx.loc2 and rd.loc_t = fbcx.loc2_t978)

where fc.doc_nbr_prime > 0

#----------------------- applying record 2 to fc -------------------------------------------
-- loc testing is not implemented
and fc.fare_lockin_date between (r2t.rec_add_date - interval 1 day) and if(r2t.rec_cnx_date = '9999-12-31', r2t.rec_cnx_date, r2t.rec_cnx_date + interval 1 day)
and fc.jrny_dep_date between r2t.tvl_eff_date and r2t.tvl_dis_date
#----------------------- cat 25 -------------------------------------------
and r3.pax_type = ctrl.pax_type
and if(r3.rslt_fare_tkt_cd <> '', r3.rslt_fare_tkt_cd = ctrl.fbc_rule, if(r3.rslt_fare_cls <> '', r3.rslt_fare_cls = ctrl.fbc_rule, true))

#----------------------- table 994 date override -------------------------------------------
and if(dor.tbl_nbr is null, true, fc.trnsc_date between dor.tkt_fr_date and dor.tkt_to_date)
and if(dor.tbl_nbr is null, true, fc.fc_dep_date between dor.tvl_fr_date and dor.tvl_to_date)

-- The location check must be done last, otherwise it takes up a lot of time
-- !!! lesson: must use simple and effective logics to reduce workload?
and (case r2s.dir_ind
		when '1' then true	# only 31 instance, not checking it for now, pass
        when '2' then true	# only 20 
		when '3' then fo.fc_loc is not null and fd.fc_loc is not null
		when '4' then ro.fc_loc is not null and rd.fc_loc is not null
		else # blank, no direction, then test either one is true
				(fo.fc_loc is not null and fd.fc_loc is not null)
                or
                (ro.fc_loc is not null and rd.fc_loc is not null)
	end)
    
;


alter table zz_dev.fm_fbr4_r3_4
add index idx_tmp_id(fc_all_id, base_tbl_t989);

analyze table zz_dev.fm_fbr4_r3_4;

drop table if exists zz_dev.fm_fbr4_t989_4;
create table zz_dev.fm_fbr4_t989_4
select distinct fc_all_id, base_tbl_t989
from zz_dev.fm_fbr4_r3_4;

alter table zz_dev.fm_fbr4_t989_4
add index idx_fbr_t989_id(fc_all_id);


drop table if exists zz_dev.fm_fbr4_syn_4;
create table if not exists zz_dev.fm_fbr4_syn_4
select distinct fc.fc_all_id, r2_id as r2_rule_id, r3_id as r3_25_cat_id, 
f.fare_id as f_fare_id, 
#r8_id as r8_rule_id, 
#t989.tbl_nbr as t989_tbl_nbr, 
t989.bf_pax_type, 
#cr.dir_ind, 
g16.frt_nbr, 
#ctrl.fbc_match, 
cr.fbcx_id
from zz_dev.fm_fbr4_f_4 ctrl

straight_join zz_cx.temp_tbl_fc_all fc on (ctrl.fc_all_id = fc.fc_all_id)

straight_join atpco_fare.atpco_fare f on f.fare_id = ctrl.fare_id
join zz_dev.fm_fbr4_t989_4 ct on ctrl.fc_all_id = ct.fc_all_id 
straight_join atpco_fare.atpco_t989_base_fare t989 on (t989.tbl_nbr = ct.base_tbl_t989 and t989.bf_appl <> 'N')

straight_join zz_cx.g16_temp g16 on (g16.carr_cd = fc.fc_carr_cd and (g16.frt_nbr = t989.bf_rule_tar or (t989.bf_rule_tar = '000' and g16.pri_ind <> 'X')))
straight_join zz_dev.fm_fbr4_r3_4 cr on ct.fc_all_id = cr.fc_all_id and ct.base_tbl_t989 = cr.base_tbl_t989
where fc.doc_nbr_prime > 0

#----------------------- matching t989 to the base fare -------------------------------------------
and (f.rule_nbr = t989.bf_rule_nbr or t989.bf_rule_nbr = '')
and if(t989.bf_fc = '', true, f.fare_cls regexp t989.bf_fc_regex)
and (f.rtg_nbr = t989.bf_rtg_nbr or t989.bf_rtg_nbr = '99999')
and (f.ow_rt_ind = t989.bf_ow_rt or t989.bf_ow_rt = '')
-- bf_type, not yet implemented
-- bf_ssn, not yet implemented
-- bf_dow, not yet implemented
-- ftnt, not yet implemented
-- and (fc.fc_pax_type = t989.c25_pax_type or t989.c25_pax_type in ('ADT', 'JCB', ''))	-- JCB and ADT are general

;



drop table if exists zz_dev.fm_fbr4_loc_4;
create table if not exists zz_dev.fm_fbr4_loc_4
select distinct fc.doc_nbr_prime, fc.fc_cpn_nbr, ctrl.fc_all_id, ctrl.r2_rule_id as r2_25_rule_id, ctrl.r3_25_cat_id as r3_25_cat_id, f.fare_id, 'R' as map_type, 'B' as map_code
from zz_dev.fm_fbr4_syn_4 ctrl

straight_join zz_cx.temp_tbl_fc_all fc on (ctrl.fc_all_id = fc.fc_all_id)
join atpco_fare.atpco_fare f on ctrl.f_fare_id = f.fare_id
straight_join zz_cx.temp_fbcx_fbr_r fbcx on fbcx.rcid = ctrl.fbcx_id

straight_join atpco_fare.atpco_r1_fare_cls r1 on (r1.carr_cd = f.carr_cd and r1.tar_nbr = ctrl.frt_nbr and r1.rule_nbr = f.rule_nbr and r1.fare_cls = f.fare_cls and r1.proc_ind in ('N', 'R'))
straight_join atpco_fare.atpco_r1_fare_cls_state r1t on (r1t.rule_id = r1.rule_id)
straight_join atpco_fare.atpco_r1_fare_cls_sup r1s on (r1s.rule_id = r1.rule_id)
/*
straight_join zz_cx.temp_tbl_fc_loc fc_loc on (fc_loc.doc_nbr_prime = fc.doc_nbr_prime and fc_loc.fc_cpn_nbr = fc.fc_cpn_nbr and fc_loc.map_di_ind = fc.map_di_ind)		
# to allow base fare matching and r2 loaction testing using somewhat different orig/dest

# forward direction location test
left join zz_cx.loc_m fo on (fo.fc_loc = fc_loc.fc_orig and fo.loc_type = fbcx.loc1_type and fo.loc = fbcx.loc1 and fo.loc_t = fbcx.loc1_t978)
left join zz_cx.loc_m fd on (fd.fc_loc = fc_loc.fc_dest and fd.loc_type = fbcx.loc2_type and fd.loc = fbcx.loc2 and fd.loc_t = fbcx.loc2_t978)

# reverse direction location test
left join zz_cx.loc_m ro on (ro.fc_loc = fc_loc.fc_dest and ro.loc_type = fbcx.loc1_type and ro.loc = fbcx.loc1 and ro.loc_t = fbcx.loc1_t978)
left join zz_cx.loc_m rd on (rd.fc_loc = fc_loc.fc_orig and rd.loc_type = fbcx.loc2_type and rd.loc = fbcx.loc2 and rd.loc_t = fbcx.loc2_t978)
*/
where fc.doc_nbr_prime > 0

#----------------------- conditions for fare record to r1 -------------------------------------------
-- location testing is not implemented
-- minimum sequence is not implemented
and fc.fare_lockin_date between date_add(r1t.rec_add_date, interval -1 day) and if(r1t.rec_cnx_date = '9999-12-31', r1t.rec_cnx_date, date_add(r1t.rec_cnx_date, interval 1 day))	
and fc.fare_lockin_date between date_add(r1t.rec_add_date, interval -1 day) and r1t.tvl_dis_date
and fc.jrny_dep_date between r1t.tvl_eff_date and r1t.tvl_dis_date
and (f.ow_rt_ind = r1.ow_rt_ind or (f.ow_rt_ind = 3 and r1.ow_rt_ind = 1))
and (f.rtg_nbr = r1.rtg_nbr or r1.rtg_nbr = '99999')
and if(r1.ftnt = '', true, if(length(f.ftnt) = 2 and (left(f.ftnt,1) between 'A' and 'Z'), left(f.ftnt,1) = r1.ftnt or right(f.ftnt,1) = r1.ftnt, f.ftnt = r1.ftnt))
and (r1s.pax_type = ctrl.bf_pax_type or (ctrl.bf_pax_type = 'ADT' and r1s.pax_type = ''))

#----------------------- check all conditions that come with datawarehouse.temp_fbcx_fbr -------------------------------------------
and fc.fare_lockin_date between date_add(fbcx.rec_add_date, interval -1 day) and if(fbcx.rec_cnx_date = '9999-12-31', fbcx.rec_cnx_date, date_add(fbcx.rec_cnx_date, interval 1 day))	
and fc.fare_lockin_date between date_add(fbcx.rec_add_date, interval -1 day) and fbcx.tvl_dis_date
and fc.jrny_dep_date between fbcx.tvl_eff_date and fbcx.tvl_dis_date

and (fbcx.ow_rt_ind = '' or f.ow_rt_ind = fbcx.ow_rt_ind or (f.ow_rt_ind = 3 and fbcx.ow_rt_ind = 1))
and (r1.fare_type = fbcx.fare_type or fbcx.fare_type = '')		-- Match fare type
and (r1.ssn_type = fbcx.ssn_type or fbcx.ssn_type = '')			-- Match Season Type
and (r1.dow_type = fbcx.dow_type or fbcx.dow_type = '')			-- Match Day of Week Type
and (f.rtg_nbr = fbcx.rtg_nbr or (f.rtg_nbr <> '00000' and fbcx.rtg_nbr = '88888') or fbcx.rtg_nbr = '99999')

-- r1.tkt_dsg_mod <> '' is not implemented, no real cases
-- and fc.fc_tkt_dsg = if(fbcx.dsg_rule = '', r1s.tkt_dsg, fbcx.dsg_rule)

and if(fbcx.dsg_rule <> '', fc.fc_tkt_dsg = fbcx.dsg_rule, if(r1s.tkt_dsg <> '', fc.fc_tkt_dsg = r1s.tkt_dsg, true))

and fc.fc_fbc = if(fbcx.fbc_rule = '', if(r1s.tkt_cd = '', f.fare_cls, r1s.tkt_cd),
					(case
						when left(fbcx.fbc_rule,1) = '*' then concat(left(f.fare_cls, 1), right(fbcx.fbc_rule, length(fbcx.fbc_rule)-1))
						when left(fbcx.fbc_rule,1) = '-' then concat(f.fare_cls, right(fbcx.fbc_rule, length(fbcx.fbc_rule)-1))
						when right(fbcx.fbc_rule,1) = '-' then concat(left(fbcx.fbc_rule, 1), right(f.fare_cls, length(f.fare_cls)-1))
						else fbcx.fbc_rule
					end)
				)
                
and if(fbcx.r2_fare_cls = '', true, if(instr(fbcx.r2_fare_cls, '-') = 0, fbcx.r2_fare_cls = f.fare_cls, f.fare_cls regexp fbcx.r2_fare_cls_regex))
/*
-- The location check must be done last, otherwise it takes up a lot of time
-- !!! lesson: must use simple and effective logics to reduce workload?
and (case ctrl.dir_ind
		when '1' then true	# only 31 instance, not checking it for now, pass
        when '2' then true	# only 20 
		when '3' then fo.fc_loc is not null and fd.fc_loc is not null
		when '4' then ro.fc_loc is not null and rd.fc_loc is not null
		else # blank, no direction, then test either one is true
				(fo.fc_loc is not null and fd.fc_loc is not null)
                or
                (ro.fc_loc is not null and rd.fc_loc is not null)
	end)
*/

;

drop table if exists zz_cx.fm_fbr5_loc_4;

create table if not exists zz_cx.fm_fbr5_loc_4 engine = innoDB
select distinct
fc.fc_all_id, r2.rule_id as r2_25_rule_id, r3.cat_id as r3_25_cat_id, f.fare_id,'R' as map_type, 'T' as map_code

from zz_cx.temp_match_fbr_ctrl_5 ctrl

straight_join zz_cx.temp_tbl_fc_all fc on (ctrl.fc_all_id = fc.fc_all_id)
straight_join tmp.temp_map_fbcx_fbr_r fbcm on (fbcm.doc_nbr_prime = fc.doc_nbr_prime and fbcm.fc_cpn_nbr = fc.fc_cpn_nbr and fbcm.fbcx_mode = '*')

straight_join atpco_fare.atpco_r8_fbr r8 on (r8.tar_nbr = fbcm.frt_nbr and r8.carr_cd = fbcm.carr_cd and r8.rule_nbr = fbcm.rule_nbr and r8.proc_ind in ('N', 'R'))
straight_join atpco_fare.atpco_r8_fbr_state r8t on (r8t.rule_id = r8.rule_id)

straight_join atpco_fare.atpco_r2_cat25_ctrl r2 on (r2.carr_cd = r8.carr_cd and r2.tar_nbr = r8.tar_nbr and r2.rule_nbr = r8.rule_nbr and r2.proc_ind in ('N', 'R'))
straight_join atpco_fare.atpco_r2_cat25_ctrl_state r2t on (r2t.rule_id = r2.rule_id)
straight_join atpco_fare.atpco_r2_cat25_ctrl_sup r2s on (r2s.rule_id = r2.rule_id)

straight_join atpco_fare.atpco_cat25 r3 on (r3.cat_id = r2s.tbl_nbr)
left join atpco_fare.atpco_t994_date dor on (dor.tbl_nbr = r3.dt_ovrd_t994)
straight_join atpco_fare.atpco_t989_base_fare t989 on (t989.tbl_nbr = r3.base_tbl_t989 and t989.bf_appl <> 'N')

straight_join zz_cx.g16_temp g16 on (g16.carr_cd = fc.fc_carr_cd and (g16.frt_nbr = t989.bf_rule_tar or (t989.bf_rule_tar = '000' and g16.pri_ind <> 'X')))

straight_join atpco_fare.atpco_fare f on (f.orig_city = fc.fc_orig and f.dest_city = fc.fc_dest and f.carr_cd = fc.fc_carr_cd and f.tar_nbr = g16.ff_nbr)
straight_join atpco_fare.atpco_fare_state ft on f.fare_id = ft.fare_id

straight_join atpco_fare.atpco_r1_fare_cls r1 on (r1.carr_cd = f.carr_cd and r1.tar_nbr = g16.frt_nbr and r1.rule_nbr = f.rule_nbr and r1.fare_cls = f.fare_cls and r1.proc_ind in ('N', 'R'))
straight_join atpco_fare.atpco_r1_fare_cls_state r1t on (r1t.rule_id = r1.rule_id)
straight_join atpco_fare.atpco_r1_fare_cls_sup r1s on (r1s.rule_id = r1.rule_id)

straight_join zz_cx.temp_fbcx_fbr fbcx on (fbcx.r8_rule_id = r8.rule_id and fbcx.r2_25_rule_id = r2.rule_id and fbcx.r3_25_cat_id = r3.cat_id)
straight_join zz_cx.temp_tbl_fc_loc fc_loc on (fc_loc.doc_nbr_prime = fc.doc_nbr_prime and fc_loc.fc_cpn_nbr = fc.fc_cpn_nbr and fc_loc.map_di_ind = fc.map_di_ind)		
# to allow base fare matching and r2 loaction testing using somewhat different orig/dest

# forward direction location test
left join zz_cx.loc_m fo on (fo.fc_loc = fc_loc.fc_orig and fo.loc_type = fbcx.loc1_type and fo.loc = fbcx.loc1 and fo.loc_t = fbcx.loc1_t978)
left join zz_cx.loc_m fd on (fd.fc_loc = fc_loc.fc_dest and fd.loc_type = fbcx.loc2_type and fd.loc = fbcx.loc2 and fd.loc_t = fbcx.loc2_t978)

# reverse direction location test
left join zz_cx.loc_m ro on (ro.fc_loc = fc_loc.fc_dest and ro.loc_type = fbcx.loc1_type and ro.loc = fbcx.loc1 and ro.loc_t = fbcx.loc1_t978)
left join zz_cx.loc_m rd on (rd.fc_loc = fc_loc.fc_orig and rd.loc_type = fbcx.loc2_type and rd.loc = fbcx.loc2 and rd.loc_t = fbcx.loc2_t978)
where fc.doc_nbr_prime > 0
-- fc.doc_nbr_prime = 5918564205 and fc.fc_cpn_nbr = 1
-- and mod(fc.doc_nbr_prime, 1999) = 0
-- and fc.tkt_endorse_cd = ''

#----------------------- record 8 to the fare component -------------------------------------------
-- loc testing is not implemented
and fc.fare_lockin_date between (r8t.rec_add_date - interval 1 day) and if(r8t.rec_cnx_date = '9999-12-31', r8t.rec_cnx_date, r8t.rec_cnx_date + interval 1 day)
and fc.jrny_dep_date between r8t.tvl_eff_date and r8t.tvl_dis_date

#----------------------- cat 25 -------------------------------------------
and r3.pax_type = r8.pax_type
and if(r3.rslt_fare_tkt_cd <> '', r3.rslt_fare_tkt_cd = fbcm.fbc_rule, if(r3.rslt_fare_cls <> '', r3.rslt_fare_cls = fbcm.fbc_rule, true))

#----------------------- table 994 date override -------------------------------------------
and if(dor.tbl_nbr is null, true, fc.trnsc_date between dor.tkt_fr_date and dor.tkt_to_date)
and if(dor.tbl_nbr is null, true, fc.fc_dep_date between dor.tvl_fr_date and dor.tvl_to_date)

#----------------------- matching fc to the base fare -------------------------------------------
and fc.fare_lockin_date between (ft.rec_add_date - interval 1 day) and if(ft.rec_cnx_date = '9999-12-31', ft.rec_cnx_date, ft.rec_cnx_date + interval 1 day)
and fc.jrny_dep_date between ft.tvl_eff_date and ft.tvl_dis_date
# not sure the logic is entirely right: and if(fc.map_di_ind = 'F', f.ftnt <> 'T', f.ftnt <> 'F')		-- useful for domestic, for international, ftnt.di is always 'F', international footnote can never be 'F' or 'T'

#----------------------- applying record 2 to fc -------------------------------------------
-- loc testing is not implemented
and fc.fare_lockin_date between (r2t.rec_add_date - interval 1 day) and if(r2t.rec_cnx_date = '9999-12-31', r2t.rec_cnx_date, r2t.rec_cnx_date + interval 1 day)
and fc.jrny_dep_date between r2t.tvl_eff_date and r2t.tvl_dis_date

#----------------------- matching t989 to the base fare -------------------------------------------
and (f.rule_nbr = t989.bf_rule_nbr or t989.bf_rule_nbr = '')
and if(t989.bf_fc = '', true, f.fare_cls regexp t989.bf_fc_regex)
and (f.rtg_nbr = t989.bf_rtg_nbr or t989.bf_rtg_nbr = '99999')
and (f.ow_rt_ind = t989.bf_ow_rt or t989.bf_ow_rt = '')
-- bf_type, not yet implemented
-- bf_ssn, not yet implemented
-- bf_dow, not yet implemented
-- ftnt, not yet implemented
-- and (fc.fc_pax_type = t989.c25_pax_type or t989.c25_pax_type in ('ADT', 'JCB', ''))	-- JCB and ADT are general

#----------------------- conditions for fare record to r1 -------------------------------------------
-- location testing is not implemented
-- minimum sequence is not implemented
and fc.fare_lockin_date between date_add(r1t.rec_add_date, interval -1 day) and if(r1t.rec_cnx_date = '9999-12-31', r1t.rec_cnx_date, date_add(r1t.rec_cnx_date, interval 1 day))	
and fc.fare_lockin_date between date_add(r1t.rec_add_date, interval -1 day) and r1t.tvl_dis_date
and fc.jrny_dep_date between r1t.tvl_eff_date and r1t.tvl_dis_date
and (f.ow_rt_ind = r1.ow_rt_ind or (f.ow_rt_ind = 3 and r1.ow_rt_ind = 1))
and (f.rtg_nbr = r1.rtg_nbr or r1.rtg_nbr = '99999')
and if(r1.ftnt = '', true, if(length(f.ftnt) = 2 and (left(f.ftnt,1) between 'A' and 'Z'), left(f.ftnt,1) = r1.ftnt or right(f.ftnt,1) = r1.ftnt, f.ftnt = r1.ftnt))
and (r1s.pax_type = t989.bf_pax_type or (t989.bf_pax_type = 'ADT' and r1s.pax_type = ''))

#----------------------- check all conditions that come with datawarehouse.temp_fbcx_fbr -------------------------------------------
and fc.fare_lockin_date between date_add(fbcx.rec_add_date, interval -1 day) and if(fbcx.rec_cnx_date = '9999-12-31', fbcx.rec_cnx_date, date_add(fbcx.rec_cnx_date, interval 1 day))	
and fc.fare_lockin_date between date_add(fbcx.rec_add_date, interval -1 day) and fbcx.tvl_dis_date
and fc.jrny_dep_date between fbcx.tvl_eff_date and fbcx.tvl_dis_date

and (fbcx.ow_rt_ind = '' or f.ow_rt_ind = fbcx.ow_rt_ind or (f.ow_rt_ind = 3 and fbcx.ow_rt_ind = 1))
and (r1.fare_type = fbcx.fare_type or fbcx.fare_type = '')		-- Match fare type
and (r1.ssn_type = fbcx.ssn_type or fbcx.ssn_type = '')			-- Match Season Type
and (r1.dow_type = fbcx.dow_type or fbcx.dow_type = '')			-- Match Day of Week Type
and (f.rtg_nbr = fbcx.rtg_nbr or (f.rtg_nbr <> '00000' and fbcx.rtg_nbr = '88888') or fbcx.rtg_nbr = '99999')

-- r1.tkt_dsg_mod <> '' is not implemented, no real cases
-- and fc.fc_tkt_dsg = if(fbcx.dsg_rule = '', r1s.tkt_dsg, fbcx.dsg_rule)
and if(fbcx.dsg_rule <> '', fc.fc_tkt_dsg = fbcx.dsg_rule, if(r1s.tkt_dsg <> '', fc.fc_tkt_dsg = r1s.tkt_dsg, true))

and fc.fc_fbc = if(fbcx.fbc_rule = '', if(r1s.tkt_cd = '', f.fare_cls, r1s.tkt_cd),
					(case
						when left(fbcx.fbc_rule,1) = '*' then concat(left(f.fare_cls, 1), right(fbcx.fbc_rule, length(fbcx.fbc_rule)-1))
						when left(fbcx.fbc_rule,1) = '-' then concat(f.fare_cls, right(fbcx.fbc_rule, length(fbcx.fbc_rule)-1))
						when right(fbcx.fbc_rule,1) = '-' then concat(left(fbcx.fbc_rule, 1), right(f.fare_cls, length(f.fare_cls)-1))
						else fbcx.fbc_rule
					end)
				)

-- The location check must be done last, otherwise it takes up a lot of time
-- !!! lesson: must use simple and effective logics to reduce workload?
and (case r2s.dir_ind
		when '1' then true	# only 31 instance, not checking it for now, pass
        when '2' then true	# only 20 
		when '3' then fo.fc_loc is not null and fd.fc_loc is not null
		when '4' then ro.fc_loc is not null and rd.fc_loc is not null
		else # blank, no direction, then test either one is true
				(fo.fc_loc is not null and fd.fc_loc is not null)
                or
                (ro.fc_loc is not null and rd.fc_loc is not null)
	end)

and if(fbcx.r2_fare_cls = '', true, if(instr(fbcx.r2_fare_cls, '-') = 0, fbcx.r2_fare_cls = f.fare_cls, f.fare_cls regexp fbcx.r2_fare_cls_regex))
and ctrl.map_code <> '2'
;