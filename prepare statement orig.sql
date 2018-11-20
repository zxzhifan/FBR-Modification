CREATE DEFINER=`jaydenw`@`%` PROCEDURE `cx_fm_fbr`(in base long, in t_id long)
BEGIN

SET @s = CONCAT('drop table if exists zz_cx.temp_match_fbr_ctrl_', t_id);
PREPARE stmt FROM @s; EXECUTE stmt; DEALLOCATE PREPARE stmt;

SET @s = CONCAT('CREATE TABLE if not exists zz_cx.temp_match_fbr_ctrl_', t_id, '(
 fc_all_id int(20) NOT NULL,
  doc_nbr_prime bigint(20) NOT NULL,
  doc_carr_nbr char(3) NOT NULL,
  trnsc_date date NOT NULL,
  fc_cpn_nbr tinyint(3) unsigned NOT NULL,
  map_code char(1) DEFAULT '''',
  sys_proc_ts TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
  INDEX idx_doc (fc_all_id ASC)
) ENGINE=MyISAM AUTO_INCREMENT=1 DEFAULT CHARSET=latin1;');
PREPARE stmt FROM @s; EXECUTE stmt; DEALLOCATE PREPARE stmt;


SET @s = CONCAT('insert into zz_cx.temp_match_fbr_ctrl_', t_id, '
(fc_all_id,doc_nbr_prime, doc_carr_nbr, trnsc_date, fc_cpn_nbr)
select distinct fc_all_id,doc_nbr_prime, doc_carr_nbr, trnsc_date, fc_cpn_nbr
from zz_cx.temp_tbl_fc_all fc
where mod(fc.doc_nbr_prime, ', base, ') = ', t_id, ';');
PREPARE stmt FROM @s; EXECUTE stmt; DEALLOCATE PREPARE stmt;


set @s = CONCAT('drop table if exists  zz_cx.temp_match_fbr_tc_',t_id,';');
PREPARE stmt FROM @s; EXECUTE stmt; DEALLOCATE PREPARE stmt;

SET @s = CONCAT('create table zz_cx.temp_match_fbr_tc_', t_id,'(
fc_all_id int(11) NOT NULL,
r2_25_rule_id int(11) NOT NULL,
r3_25_cat_id int(11) NOT NULL,
fare_id int(11) NOT NULL,
map_type char(1) DEFAULT '''',
map_code char(1) DEFAULT '''',
sys_proc_ts TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP
 ) Engine = MyISAM DEFAULT CHARSET=latin1;');
PREPARE stmt FROM @s; EXECUTE stmt; DEALLOCATE PREPARE stmt;

# -------------------------------------------------------------------------------------------------------------------------------------------------------------------------------
# match with both tour code and account code (bcode for CX)

SET @s = CONCAT('drop table if exists zz_cx.temp_match_fbr_tc_ac_', t_id);
PREPARE stmt FROM @s; EXECUTE stmt; DEALLOCATE PREPARE stmt;

set @s = CONCAT('create table if not exists zz_cx.temp_match_fbr_tc_ac_' ,t_id, ' engine = MyISAM
select distinct
#''*fc*'', fc.*,
#''*t989*'', t989.tbl_nbr as t989_tbl_nbr,
#''*f*'', f.fare_id,
#''*r1*'', r1.rule_id as r1_rule_id,
#''*fbcx*'', fbcx.*, ''R'' as map_type, ''B'' as map_code

fc.fc_all_id, r2.rule_id as r2_25_rule_id, r3.cat_id as r3_25_cat_id, f.fare_id, ''R'' as map_type, ''B'' as map_code

from zz_cx.temp_match_fbr_ctrl_' ,t_id, ' ctrl

straight_join zz_cx.temp_tbl_fc_all fc on (ctrl.fc_all_id = fc.fc_all_id)

straight_join zz_cx.temp_map_fbcx_fbr fbcm on (fbcm.doc_nbr_prime = fc.doc_nbr_prime and fbcm.fc_cpn_nbr = fc.fc_cpn_nbr)
straight_join zz_cx.temp_map_bcode_acc mbc on (mbc.bcode = fc.tkt_endorse_cd and mbc.tar_nbr = fbcm.frt_nbr and mbc.carr_cd = fbcm.carr_cd and mbc.rule_nbr = fbcm.rule_nbr)

straight_join atpco_fare.atpco_r8_fbr r8 on (r8.tar_nbr = fbcm.frt_nbr and r8.carr_cd = fbcm.carr_cd and r8.rule_nbr = fbcm.rule_nbr and r8.proc_ind in (''N'', ''R''))
straight_join atpco_fare.atpco_r8_fbr_state r8t on (r8t.rule_id = r8.rule_id)

straight_join atpco_fare.atpco_r2_cat25_ctrl r2 on (r2.carr_cd = r8.carr_cd and r2.tar_nbr = r8.tar_nbr and r2.rule_nbr = r8.rule_nbr and r2.proc_ind in (''N'', ''R''))
straight_join atpco_fare.atpco_r2_cat25_ctrl_state r2t on (r2t.rule_id = r2.rule_id)
straight_join atpco_fare.atpco_r2_cat25_ctrl_sup r2s on (r2s.rule_id = r2.rule_id)

straight_join atpco_fare.atpco_cat25 r3 on (r3.cat_id = r2s.tbl_nbr)
left join atpco_fare.atpco_t994_date dor on (dor.tbl_nbr = r3.dt_ovrd_t994)
straight_join atpco_fare.atpco_t989_base_fare t989 on (t989.tbl_nbr = r3.base_tbl_t989 and t989.bf_appl <> ''N'')

straight_join zz_cx.g16_temp g16 on (g16.carr_cd = fc.fc_carr_cd and (g16.frt_nbr = t989.bf_rule_tar or (t989.bf_rule_tar = ''000'' and g16.pri_ind <> ''X'')))

straight_join atpco_fare.atpco_fare f on (fc.fc_orig = f.orig_city and fc.fc_dest = f.dest_city and fc.fc_carr_cd = f.carr_cd and f.tar_nbr = g16.ff_nbr)
straight_join atpco_fare.atpco_fare_state ft on f.fare_id = ft.fare_id

straight_join atpco_fare.atpco_r1_fare_cls r1 on (r1.carr_cd = f.carr_cd and r1.tar_nbr = g16.frt_nbr and r1.rule_nbr = f.rule_nbr and r1.fare_cls = f.fare_cls and r1.proc_ind in (''N'', ''R''))
straight_join atpco_fare.atpco_r1_fare_cls_state r1t on (r1t.rule_id = r1.rule_id)
straight_join atpco_fare.atpco_r1_fare_cls_sup r1s on (r1s.rule_id = r1.rule_id)

straight_join zz_cx.temp_fbcx_fbr fbcx on (fbcx.r8_rule_id = r8.rule_id and fbcx.r2_25_rule_id = r2.rule_id and fbcx.r3_25_cat_id = r3.cat_id)			# matching based on record 2 and record 3  cat 25 id
straight_join zz_cx.temp_tbl_fc_loc fc_loc on (fc_loc.doc_nbr_prime = fc.doc_nbr_prime and fc_loc.fc_cpn_nbr = fc.fc_cpn_nbr and fc_loc.map_di_ind = fc.map_di_ind)		
# to allow base fare matching and r2 loaction testing using somewhat different orig/dest

# forward direction location test
left join tmp.loc_m fo on (fo.fc_loc = fc_loc.fc_orig and fo.loc_type = fbcx.loc1_type and fo.loc = fbcx.loc1 and fo.loc_t = fbcx.loc1_t978)
left join tmp.loc_m fd on (fd.fc_loc = fc_loc.fc_dest and fd.loc_type = fbcx.loc2_type and fd.loc = fbcx.loc2 and fd.loc_t = fbcx.loc2_t978)

# reverse direction location test
left join tmp.loc_m ro on (ro.fc_loc = fc_loc.fc_dest and ro.loc_type = fbcx.loc1_type and ro.loc = fbcx.loc1 and ro.loc_t = fbcx.loc1_t978)
left join tmp.loc_m rd on (rd.fc_loc = fc_loc.fc_orig and rd.loc_type = fbcx.loc2_type and rd.loc = fbcx.loc2 and rd.loc_t = fbcx.loc2_t978)

where fc.tkt_endorse_cd <> ''''
-- fc.doc_nbr_prime = 5918564205 and fc.fc_cpn_nbr = 1
-- mod(fc.doc_nbr_prime, 13) = 0

#----------------------- record 8 to fc -------------------------------------------
-- loc testing is not implemented
and if(mbc.pax_acc_cd <> '''' and r8.pax_acc_cd <> '''', mbc.pax_acc_cd = r8.pax_acc_cd, true)
and fc.fare_lockin_date between (r8t.rec_add_date - interval 1 day) and if(r8t.rec_cnx_date = ''9999-12-31'', r8t.rec_cnx_date, r8t.rec_cnx_date + interval 1 day)
and fc.jrny_dep_date between r8t.tvl_eff_date and r8t.tvl_dis_date

#----------------------- cat 25 match to record 8 -------------------------------------------
and r3.pax_type = r8.pax_type

#----------------------- table 994 date override -------------------------------------------
and if(dor.tbl_nbr is null, true, fc.trnsc_date between dor.tkt_fr_date and dor.tkt_to_date)
and if(dor.tbl_nbr is null, true, fc.fc_dep_date between dor.tvl_fr_date and dor.tvl_to_date)

#----------------------- matching fc to the base fare -------------------------------------------
and fc.fare_lockin_date between (ft.rec_add_date - interval 1 day) and if(ft.rec_cnx_date = ''9999-12-31'', ft.rec_cnx_date, ft.rec_cnx_date + interval 1 day)
and fc.jrny_dep_date between ft.tvl_eff_date and ft.tvl_dis_date
# not sure the logic is entirely right: and if(fc.map_di_ind = ''F'', f.ftnt <> ''T'', f.ftnt <> ''F'')		-- useful for domestic, for international, ftnt.di is always ''F'', international footnote can never be ''F'' or ''T''

#----------------------- applying record 2 to fc -------------------------------------------
-- loc testing is not implemented
and fc.fare_lockin_date between (r2t.rec_add_date - interval 1 day) and if(r2t.rec_cnx_date = ''9999-12-31'', r2t.rec_cnx_date, r2t.rec_cnx_date + interval 1 day)
and fc.jrny_dep_date between r2t.tvl_eff_date and r2t.tvl_dis_date

#----------------------- matching t989 to the base fare -------------------------------------------
and (f.rule_nbr = t989.bf_rule_nbr or t989.bf_rule_nbr = '''')
and if(t989.bf_fc = '''', true, f.fare_cls regexp t989.bf_fc_regex)
and (f.rtg_nbr = t989.bf_rtg_nbr or t989.bf_rtg_nbr = ''99999'')
and (f.ow_rt_ind = t989.bf_ow_rt or t989.bf_ow_rt = '''')
-- bf_type, not yet implemented
-- bf_ssn, not yet implemented
-- bf_dow, not yet implemented
-- ftnt, not yet implemented
-- and (fc.fc_pax_type = t989.c25_pax_type or t989.c25_pax_type in (''ADT'', ''JCB'', ''''))	-- JCB and ADT are general

#----------------------- conditions for fare record to r1 -------------------------------------------
-- location testing is not implemented
-- minimum sequence is not implemented
and fc.fare_lockin_date between date_add(r1t.rec_add_date, interval -1 day) and if(r1t.rec_cnx_date = ''9999-12-31'', r1t.rec_cnx_date, date_add(r1t.rec_cnx_date, interval 1 day))	
and fc.fare_lockin_date between date_add(r1t.rec_add_date, interval -1 day) and r1t.tvl_dis_date
and fc.jrny_dep_date between r1t.tvl_eff_date and r1t.tvl_dis_date
and (f.ow_rt_ind = r1.ow_rt_ind or (f.ow_rt_ind = 3 and r1.ow_rt_ind = 1))
and (f.rtg_nbr = r1.rtg_nbr or r1.rtg_nbr = ''99999'')
and if(r1.ftnt = '''', true, if(length(f.ftnt) = 2 and (left(f.ftnt,1) between ''A'' and ''Z''), left(f.ftnt,1) = r1.ftnt or right(f.ftnt,1) = r1.ftnt, f.ftnt = r1.ftnt))
and (r1s.pax_type = t989.bf_pax_type or (t989.bf_pax_type = ''ADT'' and r1s.pax_type = ''''))

#----------------------- check all conditions that come with cx_dw.temp_fbcx_fbr -------------------------------------------
and fc.fare_lockin_date between date_add(fbcx.rec_add_date, interval -1 day) and if(fbcx.rec_cnx_date = ''9999-12-31'', fbcx.rec_cnx_date, date_add(fbcx.rec_cnx_date, interval 1 day))	
and fc.fare_lockin_date between date_add(fbcx.rec_add_date, interval -1 day) and fbcx.tvl_dis_date
and fc.jrny_dep_date between fbcx.tvl_eff_date and fbcx.tvl_dis_date

and (fbcx.ow_rt_ind = '''' or f.ow_rt_ind = fbcx.ow_rt_ind or (f.ow_rt_ind = 3 and fbcx.ow_rt_ind = 1))
and (r1.fare_type = fbcx.fare_type or fbcx.fare_type = '''')		-- Match fare type
and (r1.ssn_type = fbcx.ssn_type or fbcx.ssn_type = '''')			-- Match Season Type
and (r1.dow_type = fbcx.dow_type or fbcx.dow_type = '''')			-- Match Day of Week Type
and (f.rtg_nbr = fbcx.rtg_nbr or (f.rtg_nbr <> ''00000'' and fbcx.rtg_nbr = ''88888'') or fbcx.rtg_nbr = ''99999'')

-- r1.tkt_dsg_mod <> '''' is not implemented, no real cases
-- and fc.fc_tkt_dsg = if(fbcx.dsg_rule = '''', r1s.tkt_dsg, fbcx.dsg_rule)
and if(fbcx.dsg_rule <> '''', fc.fc_tkt_dsg = fbcx.dsg_rule, if(r1s.tkt_dsg <> '''', fc.fc_tkt_dsg = r1s.tkt_dsg, true))

and fc.fc_fbc = if(fbcx.fbc_rule = '''', if(r1s.tkt_cd = '''', f.fare_cls, r1s.tkt_cd),
					(case
						when left(fbcx.fbc_rule,1) = ''*'' then concat(left(f.fare_cls, 1), right(fbcx.fbc_rule, length(fbcx.fbc_rule)-1))
						when left(fbcx.fbc_rule,1) = ''-'' then concat(f.fare_cls, right(fbcx.fbc_rule, length(fbcx.fbc_rule)-1))
						when right(fbcx.fbc_rule,1) = ''-'' then concat(left(fbcx.fbc_rule, 1), right(f.fare_cls, length(f.fare_cls)-1))
						else fbcx.fbc_rule
					end)
				)

-- The location check must be done last, otherwise it takes up a lot of time
-- !!! lesson: must use simple and effective logics to reduce workload?
and (case r2s.dir_ind
		when ''1'' then true	# only 31 instance, not checking it for now, pass
        when ''2'' then true	# only 20 
		when ''3'' then fo.fc_loc is not null and fd.fc_loc is not null
		when ''4'' then ro.fc_loc is not null and fd.fc_loc is not null
		else # blank, no direction, then test either one is true
				(fo.fc_loc is not null and fd.fc_loc is not null)
                or
                (ro.fc_loc is not null and fd.fc_loc is not null)
	end)

and if(fbcx.r2_fare_cls = '''', true, if(instr(fbcx.r2_fare_cls, ''-'') = 0, fbcx.r2_fare_cls = f.fare_cls, f.fare_cls regexp fbcx.r2_fare_cls_regex))
;');
insert zz_law.performance_record( action ) select concat('start ', @s);
PREPARE stmt FROM @s; EXECUTE stmt; DEALLOCATE PREPARE stmt;


set @s = CONCAT('update zz_cx.temp_match_fbr_ctrl_',t_id, ' ctrl
join zz_cx.temp_match_fbr_tc_ac_',t_id, ' m on (ctrl.fc_all_id = m.fc_all_id)
#(ctrl.doc_nbr_prime = m.doc_nbr_prime and ctrl.fc_cpn_nbr = m.fc_cpn_nbr)
set ctrl.map_code = ''2'';		-- 2 for matched by both tour code and accound code (BCODE)');
PREPARE stmt FROM @s; EXECUTE stmt; DEALLOCATE PREPARE stmt;



# ---------------------------------------------------------------------------------------------------------------------------------------------------------------------------------
# match with account code (bcode) only
# the code is removed, Jun 6, due to the change made for WS, the new apporach relies on both tour code and ticketing designator




# ---------------------------------------------------------------------------------------------------------------------------------------------------------------------------------
# match with tour code only, part 1
# where FBCX is no change ('') or append ('-CH')
# matching base fare fare class
set @s = CONCAT('drop table if exists  zz_cx.temp_match_fbr_tc1_',t_id,';');
PREPARE stmt FROM @s; EXECUTE stmt; DEALLOCATE PREPARE stmt;

set @s = CONCAT('create table if not exists zz_cx.temp_match_fbr_tc1_',t_id,' engine = MyISAM
select distinct
fc.fc_all_id, r2.rule_id as r2_25_rule_id, r3.cat_id as r3_25_cat_id, f.fare_id,''R'' as map_type, ''T'' as map_code
from zz_cx.temp_match_fbr_ctrl_' ,t_id, ' ctrl

straight_join zz_cx.temp_tbl_fc_all fc on (ctrl.fc_all_id = fc.fc_all_id)

straight_join zz_cx.temp_map_fbcx_fbr fbcm on (fbcm.doc_nbr_prime = fc.doc_nbr_prime and fc.fc_cpn_nbr = fbcm.fc_cpn_nbr and fbcm.fbcx_mode in ('''', ''-''))

straight_join atpco_fare.atpco_r8_fbr r8 on (r8.tar_nbr = fbcm.frt_nbr and r8.carr_cd = fbcm.carr_cd and r8.rule_nbr = fbcm.rule_nbr and r8.proc_ind in (''N'', ''R''))
straight_join atpco_fare.atpco_r8_fbr_state r8t on (r8t.rule_id = r8.rule_id)

straight_join atpco_fare.atpco_r2_cat25_ctrl r2 on (r2.carr_cd = r8.carr_cd and r2.tar_nbr = r8.tar_nbr and r2.rule_nbr = r8.rule_nbr and r2.proc_ind in (''N'', ''R''))
straight_join atpco_fare.atpco_r2_cat25_ctrl_state r2t on (r2t.rule_id = r2.rule_id)
straight_join atpco_fare.atpco_r2_cat25_ctrl_sup r2s on (r2s.rule_id = r2.rule_id)

straight_join atpco_fare.atpco_cat25 r3 on (r3.cat_id = r2s.tbl_nbr)
left join atpco_fare.atpco_t994_date dor on (dor.tbl_nbr = r3.dt_ovrd_t994)
straight_join atpco_fare.atpco_t989_base_fare t989 on (t989.tbl_nbr = r3.base_tbl_t989 and t989.bf_appl <> ''N'')

straight_join zz_cx.g16_temp g16 on (g16.carr_cd = fc.fc_carr_cd and (g16.frt_nbr = t989.bf_rule_tar or (t989.bf_rule_tar = ''000'' and g16.pri_ind <> ''X'')))

straight_join atpco_fare.atpco_fare f on (f.orig_city = fc.fc_orig and f.dest_city = fc.fc_dest and f.carr_cd = fc.fc_carr_cd and f.tar_nbr = g16.ff_nbr and f.fare_cls = fbcm.fbc_match)
straight_join atpco_fare.atpco_fare_state ft on f.fare_id = ft.fare_id

straight_join atpco_fare.atpco_r1_fare_cls r1 on (r1.carr_cd = f.carr_cd and r1.tar_nbr = g16.frt_nbr and r1.rule_nbr = f.rule_nbr and r1.fare_cls = f.fare_cls and r1.proc_ind in (''N'', ''R''))
straight_join atpco_fare.atpco_r1_fare_cls_state r1t on (r1t.rule_id = r1.rule_id)
straight_join atpco_fare.atpco_r1_fare_cls_sup r1s on (r1s.rule_id = r1.rule_id)

straight_join zz_cx.temp_fbcx_fbr fbcx on (fbcx.r8_rule_id = r8.rule_id and fbcx.r2_25_rule_id = r2.rule_id and fbcx.r3_25_cat_id = r3.cat_id)
straight_join zz_cx.temp_tbl_fc_loc fc_loc on (fc_loc.doc_nbr_prime = fc.doc_nbr_prime and fc_loc.fc_cpn_nbr = fc.fc_cpn_nbr and fc_loc.map_di_ind = fc.map_di_ind)		
# to allow base fare matching and r2 loaction testing using somewhat different orig/dest

# forward direction location test
left join tmp.loc_m fo on (fo.fc_loc = fc_loc.fc_orig and fo.loc_type = fbcx.loc1_type and fo.loc = fbcx.loc1 and fo.loc_t = fbcx.loc1_t978)
left join tmp.loc_m fd on (fd.fc_loc = fc_loc.fc_dest and fd.loc_type = fbcx.loc2_type and fd.loc = fbcx.loc2 and fd.loc_t = fbcx.loc2_t978)

# reverse direction location test
left join tmp.loc_m ro on (ro.fc_loc = fc_loc.fc_dest and ro.loc_type = fbcx.loc1_type and ro.loc = fbcx.loc1 and ro.loc_t = fbcx.loc1_t978)
left join tmp.loc_m rd on (rd.fc_loc = fc_loc.fc_orig and rd.loc_type = fbcx.loc2_type and rd.loc = fbcx.loc2 and rd.loc_t = fbcx.loc2_t978)
where fc.doc_nbr_prime > 0
-- fc.doc_nbr_prime = 5918564205 and fc.fc_cpn_nbr = 1
-- and mod(fc.doc_nbr_prime, 1999) = 0
-- and fc.tkt_endorse_cd = ''''

#----------------------- record 8 to the fare component -------------------------------------------
-- loc testing is not implemented
and fc.fare_lockin_date between (r8t.rec_add_date - interval 1 day) and if(r8t.rec_cnx_date = ''9999-12-31'', r8t.rec_cnx_date, r8t.rec_cnx_date + interval 1 day)
and fc.jrny_dep_date between r8t.tvl_eff_date and r8t.tvl_dis_date

#----------------------- cat 25 -------------------------------------------
and r3.pax_type = r8.pax_type
and if(r3.rslt_fare_tkt_cd <> '''', r3.rslt_fare_tkt_cd = fbcm.fbc_rule, if(r3.rslt_fare_cls <> '''', r3.rslt_fare_cls = fbcm.fbc_rule, true))

#----------------------- table 994 date override -------------------------------------------
and if(dor.tbl_nbr is null, true, fc.trnsc_date between dor.tkt_fr_date and dor.tkt_to_date)
and if(dor.tbl_nbr is null, true, fc.fc_dep_date between dor.tvl_fr_date and dor.tvl_to_date)

#----------------------- matching fc to the base fare -------------------------------------------
and fc.fare_lockin_date between (ft.rec_add_date - interval 1 day) and if(ft.rec_cnx_date = ''9999-12-31'', ft.rec_cnx_date, ft.rec_cnx_date + interval 1 day)
and fc.jrny_dep_date between ft.tvl_eff_date and ft.tvl_dis_date
# not sure the logic is entirely right: and if(fc.map_di_ind = ''F'', f.ftnt <> ''T'', f.ftnt <> ''F'')		-- useful for domestic, for international, ftnt.di is always ''F'', international footnote can never be ''F'' or ''T''

#----------------------- applying record 2 to fc -------------------------------------------
-- loc testing is not implemented
and fc.fare_lockin_date between (r2t.rec_add_date - interval 1 day) and if(r2t.rec_cnx_date = ''9999-12-31'', r2t.rec_cnx_date, r2t.rec_cnx_date + interval 1 day)
and fc.jrny_dep_date between r2t.tvl_eff_date and r2t.tvl_dis_date

#----------------------- matching t989 to the base fare -------------------------------------------
and (f.rule_nbr = t989.bf_rule_nbr or t989.bf_rule_nbr = '''')
and if(t989.bf_fc = '''', true, f.fare_cls regexp t989.bf_fc_regex)
and (f.rtg_nbr = t989.bf_rtg_nbr or t989.bf_rtg_nbr = ''99999'')
and (f.ow_rt_ind = t989.bf_ow_rt or t989.bf_ow_rt = '''')
-- bf_type, not yet implemented
-- bf_ssn, not yet implemented
-- bf_dow, not yet implemented
-- ftnt, not yet implemented
-- and (fc.fc_pax_type = t989.c25_pax_type or t989.c25_pax_type in (''ADT'', ''JCB'', ''''))	-- JCB and ADT are general

#----------------------- conditions for fare record to r1 -------------------------------------------
-- location testing is not implemented
-- minimum sequence is not implemented
and fc.fare_lockin_date between date_add(r1t.rec_add_date, interval -1 day) and if(r1t.rec_cnx_date = ''9999-12-31'', r1t.rec_cnx_date, date_add(r1t.rec_cnx_date, interval 1 day))	
and fc.fare_lockin_date between date_add(r1t.rec_add_date, interval -1 day) and r1t.tvl_dis_date
and fc.jrny_dep_date between r1t.tvl_eff_date and r1t.tvl_dis_date
and (f.ow_rt_ind = r1.ow_rt_ind or (f.ow_rt_ind = 3 and r1.ow_rt_ind = 1))
and (f.rtg_nbr = r1.rtg_nbr or r1.rtg_nbr = ''99999'')
and if(r1.ftnt = '''', true, if(length(f.ftnt) = 2 and (left(f.ftnt,1) between ''A'' and ''Z''), left(f.ftnt,1) = r1.ftnt or right(f.ftnt,1) = r1.ftnt, f.ftnt = r1.ftnt))
and (r1s.pax_type = t989.bf_pax_type or (t989.bf_pax_type = ''ADT'' and r1s.pax_type = ''''))

#----------------------- check all conditions that come with cx_dw.temp_fbcx_fbr -------------------------------------------
and fc.fare_lockin_date between date_add(fbcx.rec_add_date, interval -1 day) and if(fbcx.rec_cnx_date = ''9999-12-31'', fbcx.rec_cnx_date, date_add(fbcx.rec_cnx_date, interval 1 day))	
and fc.fare_lockin_date between date_add(fbcx.rec_add_date, interval -1 day) and fbcx.tvl_dis_date
and fc.jrny_dep_date between fbcx.tvl_eff_date and fbcx.tvl_dis_date

and (fbcx.ow_rt_ind = '''' or f.ow_rt_ind = fbcx.ow_rt_ind or (f.ow_rt_ind = 3 and fbcx.ow_rt_ind = 1))
and (r1.fare_type = fbcx.fare_type or fbcx.fare_type = '''')		-- Match fare type
and (r1.ssn_type = fbcx.ssn_type or fbcx.ssn_type = '''')			-- Match Season Type
and (r1.dow_type = fbcx.dow_type or fbcx.dow_type = '''')			-- Match Day of Week Type
and (f.rtg_nbr = fbcx.rtg_nbr or (f.rtg_nbr <> ''00000'' and fbcx.rtg_nbr = ''88888'') or fbcx.rtg_nbr = ''99999'')

-- r1.tkt_dsg_mod <> '''' is not implemented, no real cases
-- and fc.fc_tkt_dsg = if(fbcx.dsg_rule = '''', r1s.tkt_dsg, fbcx.dsg_rule)
and if(fbcx.dsg_rule <> '''', fc.fc_tkt_dsg = fbcx.dsg_rule, if(r1s.tkt_dsg <> '''', fc.fc_tkt_dsg = r1s.tkt_dsg, true))

and fc.fc_fbc = if(fbcx.fbc_rule = '''', if(r1s.tkt_cd = '''', f.fare_cls, r1s.tkt_cd),
					(case
						when left(fbcx.fbc_rule,1) = ''*'' then concat(left(f.fare_cls, 1), right(fbcx.fbc_rule, length(fbcx.fbc_rule)-1))
						when left(fbcx.fbc_rule,1) = ''-'' then concat(f.fare_cls, right(fbcx.fbc_rule, length(fbcx.fbc_rule)-1))
						when right(fbcx.fbc_rule,1) = ''-'' then concat(left(fbcx.fbc_rule, 1), right(f.fare_cls, length(f.fare_cls)-1))
						else fbcx.fbc_rule
					end)
				)

-- The location check must be done last, otherwise it takes up a lot of time
-- !!! lesson: must use simple and effective logics to reduce workload?
and (case r2s.dir_ind
		when ''1'' then true	# only 31 instance, not checking it for now, pass
        when ''2'' then true	# only 20 
		when ''3'' then fo.fc_loc is not null and fd.fc_loc is not null
		when ''4'' then ro.fc_loc is not null and fd.fc_loc is not null
		else # blank, no direction, then test either one is true
				(fo.fc_loc is not null and fd.fc_loc is not null)
                or
                (ro.fc_loc is not null and fd.fc_loc is not null)
	end)

and if(fbcx.r2_fare_cls = '''', true, if(instr(fbcx.r2_fare_cls, ''-'') = 0, fbcx.r2_fare_cls = f.fare_cls, f.fare_cls regexp fbcx.r2_fare_cls_regex))
and ctrl.map_code <> ''2''
;');
insert zz_law.performance_record( action ) select concat('start ', @s);
PREPARE stmt FROM @s; EXECUTE stmt; DEALLOCATE PREPARE stmt;

# ---------------------------------------------------------------------------------------------------------------------------------------------------------------------------------
# match with tour code only, part 2
# where FBCX is no change ('') or append ('-CH')
# matching to r1 ticketing code
set @s = CONCAT('drop table if exists  zz_cx.temp_match_fbr_tc2_',t_id,';');
PREPARE stmt FROM @s; EXECUTE stmt; DEALLOCATE PREPARE stmt;

set @s = CONCAT('create table if not exists zz_cx.temp_match_fbr_tc2_',t_id,' engine = MyISAM

select distinct
fc.fc_all_id, r2.rule_id as r2_25_rule_id, r3.cat_id as r3_25_cat_id, f.fare_id,''R'' as map_type, ''T'' as map_code

from zz_cx.temp_match_fbr_ctrl_', t_id, ' ctrl

straight_join zz_cx.temp_tbl_fc_all fc on (ctrl.fc_all_id = fc.fc_all_id)

straight_join zz_cx.temp_map_fbcx_fbr fbcm on (fbcm.doc_nbr_prime = fc.doc_nbr_prime and fbcm.fc_cpn_nbr = fc.fc_cpn_nbr and fbcm.fbcx_mode = '''' and fbcm.tkt_cd_ind = ''Y'')

straight_join atpco_fare.atpco_r8_fbr r8 on (r8.tar_nbr = fbcm.frt_nbr and r8.carr_cd = fbcm.carr_cd and r8.rule_nbr = fbcm.rule_nbr and r8.proc_ind in (''N'', ''R''))
straight_join atpco_fare.atpco_r8_fbr_state r8t on (r8t.rule_id = r8.rule_id)

straight_join atpco_fare.atpco_r2_cat25_ctrl r2 on (r2.carr_cd = r8.carr_cd and r2.tar_nbr = r8.tar_nbr and r2.rule_nbr = r8.rule_nbr and r2.proc_ind in (''N'', ''R''))
straight_join atpco_fare.atpco_r2_cat25_ctrl_state r2t on (r2t.rule_id = r2.rule_id)
straight_join atpco_fare.atpco_r2_cat25_ctrl_sup r2s on (r2s.rule_id = r2.rule_id)

straight_join atpco_fare.atpco_cat25 r3 on (r3.cat_id = r2s.tbl_nbr)
left join atpco_fare.atpco_t994_date dor on (dor.tbl_nbr = r3.dt_ovrd_t994)
straight_join atpco_fare.atpco_t989_base_fare t989 on (t989.tbl_nbr = r3.base_tbl_t989 and t989.bf_appl <> ''N'')

straight_join zz_cx.g16_temp g16 on (g16.carr_cd = fc.fc_carr_cd and (g16.frt_nbr = t989.bf_rule_tar or (t989.bf_rule_tar = ''000'' and g16.pri_ind <> ''X'')))
straight_join zz_cx.temp_map_r1_tkt_cd tcd on (tcd.tkt_cd = fbcm.fbc_match and tcd.carr_cd = fc.fc_carr_cd)  # reverse look from ticketing code to fare class code

straight_join atpco_fare.atpco_fare f on (f.orig_city = fc.fc_orig and f.dest_city = fc.fc_dest and f.carr_cd = fc.fc_carr_cd and f.tar_nbr = g16.ff_nbr and f.fare_cls = tcd.fare_cls)
straight_join atpco_fare.atpco_fare_state ft on f.fare_id = ft.fare_id

straight_join atpco_fare.atpco_r1_fare_cls r1 on (r1.carr_cd = f.carr_cd and r1.tar_nbr = g16.frt_nbr and r1.rule_nbr = f.rule_nbr and r1.fare_cls = f.fare_cls and r1.proc_ind in (''N'', ''R''))
straight_join atpco_fare.atpco_r1_fare_cls_state r1t on (r1t.rule_id = r1.rule_id)
straight_join atpco_fare.atpco_r1_fare_cls_sup r1s on (r1s.rule_id = r1.rule_id and r1s.tkt_cd = fbcm.fbc_match)

straight_join zz_cx.temp_fbcx_fbr fbcx on (fbcx.r8_rule_id = r8.rule_id and fbcx.r2_25_rule_id = r2.rule_id and fbcx.r3_25_cat_id = r3.cat_id)
straight_join zz_cx.temp_tbl_fc_loc fc_loc on (fc_loc.doc_nbr_prime = fc.doc_nbr_prime and fc_loc.fc_cpn_nbr = fc.fc_cpn_nbr and fc_loc.map_di_ind = fc.map_di_ind)		
# to allow base fare matching and r2 loaction testing using somewhat different orig/dest

# forward direction location test
left join tmp.loc_m fo on (fo.fc_loc = fc_loc.fc_orig and fo.loc_type = fbcx.loc1_type and fo.loc = fbcx.loc1 and fo.loc_t = fbcx.loc1_t978)
left join tmp.loc_m fd on (fd.fc_loc = fc_loc.fc_dest and fd.loc_type = fbcx.loc2_type and fd.loc = fbcx.loc2 and fd.loc_t = fbcx.loc2_t978)

# reverse direction location test
left join tmp.loc_m ro on (ro.fc_loc = fc_loc.fc_dest and ro.loc_type = fbcx.loc1_type and ro.loc = fbcx.loc1 and ro.loc_t = fbcx.loc1_t978)
left join tmp.loc_m rd on (rd.fc_loc = fc_loc.fc_orig and rd.loc_type = fbcx.loc2_type and rd.loc = fbcx.loc2 and rd.loc_t = fbcx.loc2_t978)
where fc.doc_nbr_prime > 0
-- fc.doc_nbr_prime = 5918564205 and fc.fc_cpn_nbr = 1
-- and mod(fc.doc_nbr_prime, 1999) = 0
-- and fc.tkt_endorse_cd = ''''

#----------------------- record 8 to the fare component -------------------------------------------
-- loc testing is not implemented
and fc.fare_lockin_date between (r8t.rec_add_date - interval 1 day) and if(r8t.rec_cnx_date = ''9999-12-31'', r8t.rec_cnx_date, r8t.rec_cnx_date + interval 1 day)
and fc.jrny_dep_date between r8t.tvl_eff_date and r8t.tvl_dis_date

#----------------------- cat 25 -------------------------------------------
and r3.pax_type = r8.pax_type
and if(r3.rslt_fare_tkt_cd <> '''', r3.rslt_fare_tkt_cd = fbcm.fbc_rule, if(r3.rslt_fare_cls <> '''', r3.rslt_fare_cls = fbcm.fbc_rule, true))

#----------------------- table 994 date override -------------------------------------------
and if(dor.tbl_nbr is null, true, fc.trnsc_date between dor.tkt_fr_date and dor.tkt_to_date)
and if(dor.tbl_nbr is null, true, fc.fc_dep_date between dor.tvl_fr_date and dor.tvl_to_date)

#----------------------- matching fc to the base fare -------------------------------------------
and fc.fare_lockin_date between (ft.rec_add_date - interval 1 day) and if(ft.rec_cnx_date = ''9999-12-31'', ft.rec_cnx_date, ft.rec_cnx_date + interval 1 day)
and fc.jrny_dep_date between ft.tvl_eff_date and ft.tvl_dis_date
# not sure the logic is entirely right: and if(fc.map_di_ind = ''F'', f.ftnt <> ''T'', f.ftnt <> ''F'')		-- useful for domestic, for international, ftnt.di is always ''F'', international footnote can never be ''F'' or ''T''

#----------------------- applying record 2 to fc -------------------------------------------
-- loc testing is not implemented
and fc.fare_lockin_date between (r2t.rec_add_date - interval 1 day) and if(r2t.rec_cnx_date = ''9999-12-31'', r2t.rec_cnx_date, r2t.rec_cnx_date + interval 1 day)
and fc.jrny_dep_date between r2t.tvl_eff_date and r2t.tvl_dis_date

#----------------------- matching t989 to the base fare -------------------------------------------
and (f.rule_nbr = t989.bf_rule_nbr or t989.bf_rule_nbr = '''')
and if(t989.bf_fc = '''', true, f.fare_cls regexp t989.bf_fc_regex)
and (f.rtg_nbr = t989.bf_rtg_nbr or t989.bf_rtg_nbr = ''99999'')
and (f.ow_rt_ind = t989.bf_ow_rt or t989.bf_ow_rt = '''')
-- bf_type, not yet implemented
-- bf_ssn, not yet implemented
-- bf_dow, not yet implemented
-- ftnt, not yet implemented
-- and (fc.fc_pax_type = t989.c25_pax_type or t989.c25_pax_type in (''ADT'', ''JCB'', ''''))	-- JCB and ADT are general

#----------------------- conditions for fare record to r1 -------------------------------------------
-- location testing is not implemented
-- minimum sequence is not implemented
and fc.fare_lockin_date between date_add(r1t.rec_add_date, interval -1 day) and if(r1t.rec_cnx_date = ''9999-12-31'', r1t.rec_cnx_date, date_add(r1t.rec_cnx_date, interval 1 day))	
and fc.fare_lockin_date between date_add(r1t.rec_add_date, interval -1 day) and r1t.tvl_dis_date
and fc.jrny_dep_date between r1t.tvl_eff_date and r1t.tvl_dis_date
and (f.ow_rt_ind = r1.ow_rt_ind or (f.ow_rt_ind = 3 and r1.ow_rt_ind = 1))
and (f.rtg_nbr = r1.rtg_nbr or r1.rtg_nbr = ''99999'')
and if(r1.ftnt = '''', true, if(length(f.ftnt) = 2 and (left(f.ftnt,1) between ''A'' and ''Z''), left(f.ftnt,1) = r1.ftnt or right(f.ftnt,1) = r1.ftnt, f.ftnt = r1.ftnt))
and (r1s.pax_type = t989.bf_pax_type or (t989.bf_pax_type = ''ADT'' and r1s.pax_type = ''''))

#----------------------- check all conditions that come with cx_dw.temp_fbcx_fbr -------------------------------------------
and fc.fare_lockin_date between date_add(fbcx.rec_add_date, interval -1 day) and if(fbcx.rec_cnx_date = ''9999-12-31'', fbcx.rec_cnx_date, date_add(fbcx.rec_cnx_date, interval 1 day))	
and fc.fare_lockin_date between date_add(fbcx.rec_add_date, interval -1 day) and fbcx.tvl_dis_date
and fc.jrny_dep_date between fbcx.tvl_eff_date and fbcx.tvl_dis_date

and (fbcx.ow_rt_ind = '''' or f.ow_rt_ind = fbcx.ow_rt_ind or (f.ow_rt_ind = 3 and fbcx.ow_rt_ind = 1))
and (r1.fare_type = fbcx.fare_type or fbcx.fare_type = '''')		-- Match fare type
and (r1.ssn_type = fbcx.ssn_type or fbcx.ssn_type = '''')			-- Match Season Type
and (r1.dow_type = fbcx.dow_type or fbcx.dow_type = '''')			-- Match Day of Week Type
and (f.rtg_nbr = fbcx.rtg_nbr or (f.rtg_nbr <> ''00000'' and fbcx.rtg_nbr = ''88888'') or fbcx.rtg_nbr = ''99999'')

-- r1.tkt_dsg_mod <> '''' is not implemented, no real cases
-- and fc.fc_tkt_dsg = if(fbcx.dsg_rule = '''', r1s.tkt_dsg, fbcx.dsg_rule)
and if(fbcx.dsg_rule <> '''', fc.fc_tkt_dsg = fbcx.dsg_rule, if(r1s.tkt_dsg <> '''', fc.fc_tkt_dsg = r1s.tkt_dsg, true))

and fc.fc_fbc = if(fbcx.fbc_rule = '''', if(r1s.tkt_cd = '''', f.fare_cls, r1s.tkt_cd),
					(case
						when left(fbcx.fbc_rule,1) = ''*'' then concat(left(f.fare_cls, 1), right(fbcx.fbc_rule, length(fbcx.fbc_rule)-1))
						when left(fbcx.fbc_rule,1) = ''-'' then concat(f.fare_cls, right(fbcx.fbc_rule, length(fbcx.fbc_rule)-1))
						when right(fbcx.fbc_rule,1) = ''-'' then concat(left(fbcx.fbc_rule, 1), right(f.fare_cls, length(f.fare_cls)-1))
						else fbcx.fbc_rule
					end)
				)

-- The location check must be done last, otherwise it takes up a lot of time
-- !!! lesson: must use simple and effective logics to reduce workload?
and (case r2s.dir_ind
		when ''1'' then true	# only 31 instance, not checking it for now, pass
        when ''2'' then true	# only 20 
		when ''3'' then fo.fc_loc is not null and fd.fc_loc is not null
		when ''4'' then ro.fc_loc is not null and fd.fc_loc is not null
		else # blank, no direction, then test either one is true
				(fo.fc_loc is not null and fd.fc_loc is not null)
                or
                (ro.fc_loc is not null and fd.fc_loc is not null)
	end)

and if(fbcx.r2_fare_cls = '''', true, if(instr(fbcx.r2_fare_cls, ''-'') = 0, fbcx.r2_fare_cls = f.fare_cls, f.fare_cls regexp fbcx.r2_fare_cls_regex))
and ctrl.map_code <> ''2''
;');
insert zz_law.performance_record( action ) select concat('start ', @s);
PREPARE stmt FROM @s; EXECUTE stmt; DEALLOCATE PREPARE stmt;

# ---------------------------------------------------------------------------------------------------------------------------------------------------------------------------------
# match with tour code only, part 3
# where FBCX is replace (ie. XYZ >> ABC)
set @s = CONCAT('drop table if exists  zz_cx.temp_match_fbr_tc3_',t_id,';');
PREPARE stmt FROM @s; EXECUTE stmt; DEALLOCATE PREPARE stmt;

set @s = CONCAT('
create table if not exists zz_cx.temp_match_fbr_tc3_',t_id,' engine = MyISAM

select distinct
fc.fc_all_id, r2.rule_id as r2_25_rule_id, r3.cat_id as r3_25_cat_id, f.fare_id,''R'' as map_type, ''T'' as map_code

from zz_cx.temp_match_fbr_ctrl_', t_id, ' ctrl

straight_join zz_cx.temp_tbl_fc_all fc on (ctrl.fc_all_id = fc.fc_all_id)

straight_join zz_cx.temp_map_fbcx_fbr fbcm on (fbcm.doc_nbr_prime = fc.doc_nbr_prime and fbcm.fc_cpn_nbr = fc.fc_cpn_nbr and fbcm.fbcx_mode = ''X'')

straight_join atpco_fare.atpco_r8_fbr r8 on (r8.tar_nbr = fbcm.frt_nbr and r8.carr_cd = fbcm.carr_cd and r8.rule_nbr = fbcm.rule_nbr and r8.proc_ind in (''N'', ''R''))
straight_join atpco_fare.atpco_r8_fbr_state r8t on (r8t.rule_id = r8.rule_id)

straight_join atpco_fare.atpco_r2_cat25_ctrl r2 on (r2.carr_cd = r8.carr_cd and r2.tar_nbr = r8.tar_nbr and r2.rule_nbr = r8.rule_nbr and r2.proc_ind in (''N'', ''R''))
straight_join atpco_fare.atpco_r2_cat25_ctrl_state r2t on (r2t.rule_id = r2.rule_id)
straight_join atpco_fare.atpco_r2_cat25_ctrl_sup r2s on (r2s.rule_id = r2.rule_id)

straight_join atpco_fare.atpco_cat25 r3 on (r3.cat_id = r2s.tbl_nbr)
left join atpco_fare.atpco_t994_date dor on (dor.tbl_nbr = r3.dt_ovrd_t994)

straight_join zz_cx.temp_fbcx_fbr fbcx on (fbcx.r8_rule_id = r8.rule_id and fbcx.r2_25_rule_id = r2.rule_id and fbcx.r3_25_cat_id = r3.cat_id and fbcx.fbc_rule = fc.fc_fbc and (fbcx.dsg_rule = fc.fc_tkt_dsg or fbcx.dsg_rule = ''''))

straight_join atpco_fare.atpco_t989_base_fare t989 on (t989.tbl_nbr = r3.base_tbl_t989 and t989.bf_appl <> ''N'')

straight_join zz_cx.g16_temp g16 on (g16.carr_cd = fc.fc_carr_cd and (g16.frt_nbr = t989.bf_rule_tar or (t989.bf_rule_tar = ''000'' and g16.pri_ind <> ''X'')))

straight_join atpco_fare.atpco_fare f on (f.orig_city = fc.fc_orig and f.dest_city = fc.fc_dest and f.carr_cd = fc.fc_carr_cd and f.tar_nbr = g16.ff_nbr)
straight_join atpco_fare.atpco_fare_state ft on f.fare_id = ft.fare_id

straight_join atpco_fare.atpco_r1_fare_cls r1 on (r1.carr_cd = f.carr_cd and r1.tar_nbr = g16.frt_nbr and r1.rule_nbr = f.rule_nbr and r1.fare_cls = f.fare_cls and r1.proc_ind in (''N'', ''R''))
straight_join atpco_fare.atpco_r1_fare_cls_state r1t on (r1t.rule_id = r1.rule_id)
straight_join atpco_fare.atpco_r1_fare_cls_sup r1s on (r1s.rule_id = r1.rule_id)

straight_join zz_cx.temp_tbl_fc_loc fc_loc on (fc_loc.doc_nbr_prime = fc.doc_nbr_prime and fc_loc.fc_cpn_nbr = fc.fc_cpn_nbr and fc_loc.map_di_ind = fc.map_di_ind)		
# to allow base fare matching and r2 loaction testing using somewhat different orig/dest

# forward direction location test
left join tmp.loc_m fo on (fo.fc_loc = fc_loc.fc_orig and fo.loc_type = fbcx.loc1_type and fo.loc = fbcx.loc1 and fo.loc_t = fbcx.loc1_t978)
left join tmp.loc_m fd on (fd.fc_loc = fc_loc.fc_dest and fd.loc_type = fbcx.loc2_type and fd.loc = fbcx.loc2 and fd.loc_t = fbcx.loc2_t978)

# reverse direction location test
left join tmp.loc_m ro on (ro.fc_loc = fc_loc.fc_dest and ro.loc_type = fbcx.loc1_type and ro.loc = fbcx.loc1 and ro.loc_t = fbcx.loc1_t978)
left join tmp.loc_m rd on (rd.fc_loc = fc_loc.fc_orig and rd.loc_type = fbcx.loc2_type and rd.loc = fbcx.loc2 and rd.loc_t = fbcx.loc2_t978)
where fc.doc_nbr_prime > 0
-- fc.doc_nbr_prime = 5918564205 and fc.fc_cpn_nbr = 1
-- and mod(fc.doc_nbr_prime, 1999) = 0
-- and fc.tkt_endorse_cd = ''''

#----------------------- record 8 to the fare component -------------------------------------------
-- loc testing is not implemented
and fc.fare_lockin_date between (r8t.rec_add_date - interval 1 day) and if(r8t.rec_cnx_date = ''9999-12-31'', r8t.rec_cnx_date, r8t.rec_cnx_date + interval 1 day)
and fc.jrny_dep_date between r8t.tvl_eff_date and r8t.tvl_dis_date

#----------------------- cat 25 -------------------------------------------
and r3.pax_type = r8.pax_type
and if(r3.rslt_fare_tkt_cd <> '''', r3.rslt_fare_tkt_cd = fbcm.fbc_rule, if(r3.rslt_fare_cls <> '''', r3.rslt_fare_cls = fbcm.fbc_rule, true))

#----------------------- table 994 date override -------------------------------------------
and if(dor.tbl_nbr is null, true, fc.trnsc_date between dor.tkt_fr_date and dor.tkt_to_date)
and if(dor.tbl_nbr is null, true, fc.fc_dep_date between dor.tvl_fr_date and dor.tvl_to_date)

#----------------------- matching fc to the base fare -------------------------------------------
and fc.fare_lockin_date between (ft.rec_add_date - interval 1 day) and if(ft.rec_cnx_date = ''9999-12-31'', ft.rec_cnx_date, ft.rec_cnx_date + interval 1 day)
and fc.jrny_dep_date between ft.tvl_eff_date and ft.tvl_dis_date
# not sure the logic is entirely right: and if(fc.map_di_ind = ''F'', f.ftnt <> ''T'', f.ftnt <> ''F'')		-- useful for domestic, for international, ftnt.di is always ''F'', international footnote can never be ''F'' or ''T''

#----------------------- applying record 2 to fc -------------------------------------------
-- loc testing is not implemented
and fc.fare_lockin_date between (r2t.rec_add_date - interval 1 day) and if(r2t.rec_cnx_date = ''9999-12-31'', r2t.rec_cnx_date, r2t.rec_cnx_date + interval 1 day)
and fc.jrny_dep_date between r2t.tvl_eff_date and r2t.tvl_dis_date

#----------------------- matching t989 to the base fare -------------------------------------------
and (f.rule_nbr = t989.bf_rule_nbr or t989.bf_rule_nbr = '''')
and if(t989.bf_fc = '''', true, f.fare_cls regexp t989.bf_fc_regex)
and (f.rtg_nbr = t989.bf_rtg_nbr or t989.bf_rtg_nbr = ''99999'')
and (f.ow_rt_ind = t989.bf_ow_rt or t989.bf_ow_rt = '''')
-- bf_type, not yet implemented
-- bf_ssn, not yet implemented
-- bf_dow, not yet implemented
-- ftnt, not yet implemented
-- and (fc.fc_pax_type = t989.c25_pax_type or t989.c25_pax_type in (''ADT'', ''JCB'', ''''))	-- JCB and ADT are general

#----------------------- conditions for fare record to r1 -------------------------------------------
-- location testing is not implemented
-- minimum sequence is not implemented
and fc.fare_lockin_date between date_add(r1t.rec_add_date, interval -1 day) and if(r1t.rec_cnx_date = ''9999-12-31'', r1t.rec_cnx_date, date_add(r1t.rec_cnx_date, interval 1 day))	
and fc.fare_lockin_date between date_add(r1t.rec_add_date, interval -1 day) and r1t.tvl_dis_date
and fc.jrny_dep_date between r1t.tvl_eff_date and r1t.tvl_dis_date
and (f.ow_rt_ind = r1.ow_rt_ind or (f.ow_rt_ind = 3 and r1.ow_rt_ind = 1))
and (f.rtg_nbr = r1.rtg_nbr or r1.rtg_nbr = ''99999'')
and if(r1.ftnt = '''', true, if(length(f.ftnt) = 2 and (left(f.ftnt,1) between ''A'' and ''Z''), left(f.ftnt,1) = r1.ftnt or right(f.ftnt,1) = r1.ftnt, f.ftnt = r1.ftnt))
and (r1s.pax_type = t989.bf_pax_type or (t989.bf_pax_type = ''ADT'' and r1s.pax_type = ''''))

#----------------------- check all conditions that come with cx_dw.temp_fbcx_fbr -------------------------------------------
and fc.fare_lockin_date between date_add(fbcx.rec_add_date, interval -1 day) and if(fbcx.rec_cnx_date = ''9999-12-31'', fbcx.rec_cnx_date, date_add(fbcx.rec_cnx_date, interval 1 day))	
and fc.fare_lockin_date between date_add(fbcx.rec_add_date, interval -1 day) and fbcx.tvl_dis_date
and fc.jrny_dep_date between fbcx.tvl_eff_date and fbcx.tvl_dis_date

and (fbcx.ow_rt_ind = '''' or f.ow_rt_ind = fbcx.ow_rt_ind or (f.ow_rt_ind = 3 and fbcx.ow_rt_ind = 1))
and (r1.fare_type = fbcx.fare_type or fbcx.fare_type = '''')		-- Match fare type
and (r1.ssn_type = fbcx.ssn_type or fbcx.ssn_type = '''')			-- Match Season Type
and (r1.dow_type = fbcx.dow_type or fbcx.dow_type = '''')			-- Match Day of Week Type
and (f.rtg_nbr = fbcx.rtg_nbr or (f.rtg_nbr <> ''00000'' and fbcx.rtg_nbr = ''88888'') or fbcx.rtg_nbr = ''99999'')

-- r1.tkt_dsg_mod <> '''' is not implemented, no real cases
-- and fc.fc_tkt_dsg = if(fbcx.dsg_rule = '''', r1s.tkt_dsg, fbcx.dsg_rule)
and if(fbcx.dsg_rule <> '''', fc.fc_tkt_dsg = fbcx.dsg_rule, if(r1s.tkt_dsg <> '''', fc.fc_tkt_dsg = r1s.tkt_dsg, true))

and fc.fc_fbc = if(fbcx.fbc_rule = '''', if(r1s.tkt_cd = '''', f.fare_cls, r1s.tkt_cd),
					(case
						when left(fbcx.fbc_rule,1) = ''*'' then concat(left(f.fare_cls, 1), right(fbcx.fbc_rule, length(fbcx.fbc_rule)-1))
						when left(fbcx.fbc_rule,1) = ''-'' then concat(f.fare_cls, right(fbcx.fbc_rule, length(fbcx.fbc_rule)-1))
						when right(fbcx.fbc_rule,1) = ''-'' then concat(left(fbcx.fbc_rule, 1), right(f.fare_cls, length(f.fare_cls)-1))
						else fbcx.fbc_rule
					end)
				)

-- The location check must be done last, otherwise it takes up a lot of time
-- !!! lesson: must use simple and effective logics to reduce workload?
and (case r2s.dir_ind
		when ''1'' then true	# only 31 instance, not checking it for now, pass
        when ''2'' then true	# only 20 
		when ''3'' then fo.fc_loc is not null and fd.fc_loc is not null
		when ''4'' then ro.fc_loc is not null and fd.fc_loc is not null
		else # blank, no direction, then test either one is true
				(fo.fc_loc is not null and fd.fc_loc is not null)
                or
                (ro.fc_loc is not null and fd.fc_loc is not null)
	end)

and if(fbcx.r2_fare_cls = '''', true, if(instr(fbcx.r2_fare_cls, ''-'') = 0, fbcx.r2_fare_cls = f.fare_cls, f.fare_cls regexp fbcx.r2_fare_cls_regex))
and ctrl.map_code <> ''2''
;');
insert zz_law.performance_record( action ) select concat('start ', @s);
PREPARE stmt FROM @s; EXECUTE stmt; DEALLOCATE PREPARE stmt;

# ---------------------------------------------------------------------------------------------------------------------------------------------------------------------------------
# match with tour code only, part 4
# where one can only use the rule to find a match -- eg. F-
set @s = CONCAT('drop table if exists  zz_cx.temp_match_fbr_tc4_',t_id,';');
PREPARE stmt FROM @s; EXECUTE stmt; DEALLOCATE PREPARE stmt;
set @s = CONCAT('
create table if not exists zz_cx.temp_match_fbr_tc4_',t_id,' engine = MyISAM

select distinct
fc.fc_all_id, r2.rule_id as r2_25_rule_id, r3.cat_id as r3_25_cat_id, f.fare_id,''R'' as map_type, ''T'' as map_code

from zz_cx.temp_match_fbr_ctrl_', t_id, ' ctrl

straight_join zz_cx.temp_tbl_fc_all fc on (ctrl.fc_all_id = fc.fc_all_id)
straight_join zz_cx.temp_map_fbcx_fbr fbcm on (fbcm.doc_nbr_prime = fc.doc_nbr_prime and fbcm.fc_cpn_nbr = fc.fc_cpn_nbr and fbcm.fbcx_mode = ''*'')

straight_join atpco_fare.atpco_r8_fbr r8 on (r8.tar_nbr = fbcm.frt_nbr and r8.carr_cd = fbcm.carr_cd and r8.rule_nbr = fbcm.rule_nbr and r8.proc_ind in (''N'', ''R''))
straight_join atpco_fare.atpco_r8_fbr_state r8t on (r8t.rule_id = r8.rule_id)

straight_join atpco_fare.atpco_r2_cat25_ctrl r2 on (r2.carr_cd = r8.carr_cd and r2.tar_nbr = r8.tar_nbr and r2.rule_nbr = r8.rule_nbr and r2.proc_ind in (''N'', ''R''))
straight_join atpco_fare.atpco_r2_cat25_ctrl_state r2t on (r2t.rule_id = r2.rule_id)
straight_join atpco_fare.atpco_r2_cat25_ctrl_sup r2s on (r2s.rule_id = r2.rule_id)

straight_join atpco_fare.atpco_cat25 r3 on (r3.cat_id = r2s.tbl_nbr)
left join atpco_fare.atpco_t994_date dor on (dor.tbl_nbr = r3.dt_ovrd_t994)
straight_join atpco_fare.atpco_t989_base_fare t989 on (t989.tbl_nbr = r3.base_tbl_t989 and t989.bf_appl <> ''N'')

straight_join zz_cx.g16_temp g16 on (g16.carr_cd = fc.fc_carr_cd and (g16.frt_nbr = t989.bf_rule_tar or (t989.bf_rule_tar = ''000'' and g16.pri_ind <> ''X'')))

straight_join atpco_fare.atpco_fare f on (f.orig_city = fc.fc_orig and f.dest_city = fc.fc_dest and f.carr_cd = fc.fc_carr_cd and f.tar_nbr = g16.ff_nbr)
straight_join atpco_fare.atpco_fare_state ft on f.fare_id = ft.fare_id

straight_join atpco_fare.atpco_r1_fare_cls r1 on (r1.carr_cd = f.carr_cd and r1.tar_nbr = g16.frt_nbr and r1.rule_nbr = f.rule_nbr and r1.fare_cls = f.fare_cls and r1.proc_ind in (''N'', ''R''))
straight_join atpco_fare.atpco_r1_fare_cls_state r1t on (r1t.rule_id = r1.rule_id)
straight_join atpco_fare.atpco_r1_fare_cls_sup r1s on (r1s.rule_id = r1.rule_id)

straight_join zz_cx.temp_fbcx_fbr fbcx on (fbcx.r8_rule_id = r8.rule_id and fbcx.r2_25_rule_id = r2.rule_id and fbcx.r3_25_cat_id = r3.cat_id)
straight_join zz_cx.temp_tbl_fc_loc fc_loc on (fc_loc.doc_nbr_prime = fc.doc_nbr_prime and fc_loc.fc_cpn_nbr = fc.fc_cpn_nbr and fc_loc.map_di_ind = fc.map_di_ind)		
# to allow base fare matching and r2 loaction testing using somewhat different orig/dest

# forward direction location test
left join tmp.loc_m fo on (fo.fc_loc = fc_loc.fc_orig and fo.loc_type = fbcx.loc1_type and fo.loc = fbcx.loc1 and fo.loc_t = fbcx.loc1_t978)
left join tmp.loc_m fd on (fd.fc_loc = fc_loc.fc_dest and fd.loc_type = fbcx.loc2_type and fd.loc = fbcx.loc2 and fd.loc_t = fbcx.loc2_t978)

# reverse direction location test
left join tmp.loc_m ro on (ro.fc_loc = fc_loc.fc_dest and ro.loc_type = fbcx.loc1_type and ro.loc = fbcx.loc1 and ro.loc_t = fbcx.loc1_t978)
left join tmp.loc_m rd on (rd.fc_loc = fc_loc.fc_orig and rd.loc_type = fbcx.loc2_type and rd.loc = fbcx.loc2 and rd.loc_t = fbcx.loc2_t978)
where fc.doc_nbr_prime > 0
-- fc.doc_nbr_prime = 5918564205 and fc.fc_cpn_nbr = 1
-- and mod(fc.doc_nbr_prime, 1999) = 0
-- and fc.tkt_endorse_cd = ''''

#----------------------- record 8 to the fare component -------------------------------------------
-- loc testing is not implemented
and fc.fare_lockin_date between (r8t.rec_add_date - interval 1 day) and if(r8t.rec_cnx_date = ''9999-12-31'', r8t.rec_cnx_date, r8t.rec_cnx_date + interval 1 day)
and fc.jrny_dep_date between r8t.tvl_eff_date and r8t.tvl_dis_date

#----------------------- cat 25 -------------------------------------------
and r3.pax_type = r8.pax_type
and if(r3.rslt_fare_tkt_cd <> '''', r3.rslt_fare_tkt_cd = fbcm.fbc_rule, if(r3.rslt_fare_cls <> '''', r3.rslt_fare_cls = fbcm.fbc_rule, true))

#----------------------- table 994 date override -------------------------------------------
and if(dor.tbl_nbr is null, true, fc.trnsc_date between dor.tkt_fr_date and dor.tkt_to_date)
and if(dor.tbl_nbr is null, true, fc.fc_dep_date between dor.tvl_fr_date and dor.tvl_to_date)

#----------------------- matching fc to the base fare -------------------------------------------
and fc.fare_lockin_date between (ft.rec_add_date - interval 1 day) and if(ft.rec_cnx_date = ''9999-12-31'', ft.rec_cnx_date, ft.rec_cnx_date + interval 1 day)
and fc.jrny_dep_date between ft.tvl_eff_date and ft.tvl_dis_date
# not sure the logic is entirely right: and if(fc.map_di_ind = ''F'', f.ftnt <> ''T'', f.ftnt <> ''F'')		-- useful for domestic, for international, ftnt.di is always ''F'', international footnote can never be ''F'' or ''T''

#----------------------- applying record 2 to fc -------------------------------------------
-- loc testing is not implemented
and fc.fare_lockin_date between (r2t.rec_add_date - interval 1 day) and if(r2t.rec_cnx_date = ''9999-12-31'', r2t.rec_cnx_date, r2t.rec_cnx_date + interval 1 day)
and fc.jrny_dep_date between r2t.tvl_eff_date and r2t.tvl_dis_date

#----------------------- matching t989 to the base fare -------------------------------------------
and (f.rule_nbr = t989.bf_rule_nbr or t989.bf_rule_nbr = '''')
and if(t989.bf_fc = '''', true, f.fare_cls regexp t989.bf_fc_regex)
and (f.rtg_nbr = t989.bf_rtg_nbr or t989.bf_rtg_nbr = ''99999'')
and (f.ow_rt_ind = t989.bf_ow_rt or t989.bf_ow_rt = '''')
-- bf_type, not yet implemented
-- bf_ssn, not yet implemented
-- bf_dow, not yet implemented
-- ftnt, not yet implemented
-- and (fc.fc_pax_type = t989.c25_pax_type or t989.c25_pax_type in (''ADT'', ''JCB'', ''''))	-- JCB and ADT are general

#----------------------- conditions for fare record to r1 -------------------------------------------
-- location testing is not implemented
-- minimum sequence is not implemented
and fc.fare_lockin_date between date_add(r1t.rec_add_date, interval -1 day) and if(r1t.rec_cnx_date = ''9999-12-31'', r1t.rec_cnx_date, date_add(r1t.rec_cnx_date, interval 1 day))	
and fc.fare_lockin_date between date_add(r1t.rec_add_date, interval -1 day) and r1t.tvl_dis_date
and fc.jrny_dep_date between r1t.tvl_eff_date and r1t.tvl_dis_date
and (f.ow_rt_ind = r1.ow_rt_ind or (f.ow_rt_ind = 3 and r1.ow_rt_ind = 1))
and (f.rtg_nbr = r1.rtg_nbr or r1.rtg_nbr = ''99999'')
and if(r1.ftnt = '''', true, if(length(f.ftnt) = 2 and (left(f.ftnt,1) between ''A'' and ''Z''), left(f.ftnt,1) = r1.ftnt or right(f.ftnt,1) = r1.ftnt, f.ftnt = r1.ftnt))
and (r1s.pax_type = t989.bf_pax_type or (t989.bf_pax_type = ''ADT'' and r1s.pax_type = ''''))

#----------------------- check all conditions that come with cx_dw.temp_fbcx_fbr -------------------------------------------
and fc.fare_lockin_date between date_add(fbcx.rec_add_date, interval -1 day) and if(fbcx.rec_cnx_date = ''9999-12-31'', fbcx.rec_cnx_date, date_add(fbcx.rec_cnx_date, interval 1 day))	
and fc.fare_lockin_date between date_add(fbcx.rec_add_date, interval -1 day) and fbcx.tvl_dis_date
and fc.jrny_dep_date between fbcx.tvl_eff_date and fbcx.tvl_dis_date

and (fbcx.ow_rt_ind = '''' or f.ow_rt_ind = fbcx.ow_rt_ind or (f.ow_rt_ind = 3 and fbcx.ow_rt_ind = 1))
and (r1.fare_type = fbcx.fare_type or fbcx.fare_type = '''')		-- Match fare type
and (r1.ssn_type = fbcx.ssn_type or fbcx.ssn_type = '''')			-- Match Season Type
and (r1.dow_type = fbcx.dow_type or fbcx.dow_type = '''')			-- Match Day of Week Type
and (f.rtg_nbr = fbcx.rtg_nbr or (f.rtg_nbr <> ''00000'' and fbcx.rtg_nbr = ''88888'') or fbcx.rtg_nbr = ''99999'')

-- r1.tkt_dsg_mod <> '''' is not implemented, no real cases
-- and fc.fc_tkt_dsg = if(fbcx.dsg_rule = '''', r1s.tkt_dsg, fbcx.dsg_rule)
and if(fbcx.dsg_rule <> '''', fc.fc_tkt_dsg = fbcx.dsg_rule, if(r1s.tkt_dsg <> '''', fc.fc_tkt_dsg = r1s.tkt_dsg, true))

and fc.fc_fbc = if(fbcx.fbc_rule = '''', if(r1s.tkt_cd = '''', f.fare_cls, r1s.tkt_cd),
					(case
						when left(fbcx.fbc_rule,1) = ''*'' then concat(left(f.fare_cls, 1), right(fbcx.fbc_rule, length(fbcx.fbc_rule)-1))
						when left(fbcx.fbc_rule,1) = ''-'' then concat(f.fare_cls, right(fbcx.fbc_rule, length(fbcx.fbc_rule)-1))
						when right(fbcx.fbc_rule,1) = ''-'' then concat(left(fbcx.fbc_rule, 1), right(f.fare_cls, length(f.fare_cls)-1))
						else fbcx.fbc_rule
					end)
				)

-- The location check must be done last, otherwise it takes up a lot of time
-- !!! lesson: must use simple and effective logics to reduce workload?
and (case r2s.dir_ind
		when ''1'' then true	# only 31 instance, not checking it for now, pass
        when ''2'' then true	# only 20 
		when ''3'' then fo.fc_loc is not null and fd.fc_loc is not null
		when ''4'' then ro.fc_loc is not null and fd.fc_loc is not null
		else # blank, no direction, then test either one is true
				(fo.fc_loc is not null and fd.fc_loc is not null)
                or
                (ro.fc_loc is not null and fd.fc_loc is not null)
	end)

and if(fbcx.r2_fare_cls = '''', true, if(instr(fbcx.r2_fare_cls, ''-'') = 0, fbcx.r2_fare_cls = f.fare_cls, f.fare_cls regexp fbcx.r2_fare_cls_regex))
and ctrl.map_code <> ''2''
;');
insert zz_law.performance_record( action ) select concat('start ', @s);
PREPARE stmt FROM @s; EXECUTE stmt; DEALLOCATE PREPARE stmt;



set @s = CONCAT('insert into zz_cx.temp_match_fbr_tc_',t_id,'
 (fc_all_id, r2_25_rule_id, r3_25_cat_id, fare_id, map_type, map_code)
 select 
 fc_all_id, r2_25_rule_id, r3_25_cat_id, fare_id, map_type, map_code
 from zz_cx.temp_match_fbr_tc1_',t_id,';');
PREPARE stmt FROM @s; EXECUTE stmt; DEALLOCATE PREPARE stmt;
set @s = CONCAT('insert into zz_cx.temp_match_fbr_tc_',t_id,'
 (fc_all_id, r2_25_rule_id, r3_25_cat_id, fare_id, map_type, map_code)
 select 
 fc_all_id, r2_25_rule_id, r3_25_cat_id, fare_id, map_type, map_code
 from zz_cx.temp_match_fbr_tc2_',t_id,';');
PREPARE stmt FROM @s; EXECUTE stmt; DEALLOCATE PREPARE stmt;
set @s = CONCAT('insert into zz_cx.temp_match_fbr_tc_',t_id,'
 (fc_all_id, r2_25_rule_id, r3_25_cat_id, fare_id, map_type, map_code)
 select 
 fc_all_id, r2_25_rule_id, r3_25_cat_id, fare_id, map_type, map_code
 from zz_cx.temp_match_fbr_tc3_',t_id,';');
PREPARE stmt FROM @s; EXECUTE stmt; DEALLOCATE PREPARE stmt;
set @s = CONCAT('insert into zz_cx.temp_match_fbr_tc_',t_id,'
 (fc_all_id, r2_25_rule_id, r3_25_cat_id, fare_id, map_type, map_code)
 select 
 fc_all_id, r2_25_rule_id, r3_25_cat_id, fare_id, map_type, map_code
 from zz_cx.temp_match_fbr_tc4_',t_id,';');
PREPARE stmt FROM @s; EXECUTE stmt; DEALLOCATE PREPARE stmt;

set @s = CONCAT('update zz_cx.temp_match_fbr_ctrl_',t_id, ' ctrl
join zz_cx.temp_match_fbr_tc_',t_id, ' m on (ctrl.fc_all_id = m.fc_all_id)
#(ctrl.doc_nbr_prime = m.doc_nbr_prime and ctrl.fc_cpn_nbr = m.fc_cpn_nbr)
set ctrl.map_code = ''1'';		-- 1 for matched by both tour code or accound code (BCODE)');
PREPARE stmt FROM @s; EXECUTE stmt; DEALLOCATE PREPARE stmt;


# ---------------------------------------------------------------------------------------------------------------------------------------------------------------------------------
# send the result to a central table

SET @s = CONCAT('insert into zz_cx.temp_tbl_fc_fare_map_fbr
(doc_nbr_prime, doc_carr_nbr, trnsc_date, fare_lockin_date, fc_cpn_nbr, fc_orig, fc_orig_city, fc_orig_cntry, fc_dest, fc_dest_city, fc_dest_cntry, fc_carr_cd, 
fc_fbc, fc_mile_plus, fc_tkt_dsg, fc_pax_type, fc_curr_cd, fc_amt, fc_roe, fc_nuc_amt, jrny_dep_date, tkt_tour_cd, mod_tour_cd, 
map_di_ind, map_type, map_code, spec_fare_id,cat25_r2_id, c25_r3_cat_id)
select distinct 
 doc_nbr_prime, doc_carr_nbr, trnsc_date, fare_lockin_date, fc_cpn_nbr, fc_orig, fc_orig_c, fc_orig_n, fc_dest, fc_dest_c, fc_dest_n, fc_carr_cd, 
fc_fbc, fc_mile_plus, fc_tkt_dsg, fc_pax_type, fc_curr_cd, fc_amt, fc_roe, fc_nuc_amt, jrny_dep_date, tkt_tour_cd, mod_tour_cd, 
map_di_ind, map_type, map_code, fare_id, r2_25_rule_id, r3_25_cat_id
from zz_cx.temp_match_fbr_tc_ac_', t_id,' m
straight_join zz_cx.temp_tbl_fc_all fc on (m.fc_all_id = fc.fc_all_id)
;');
PREPARE stmt FROM @s; EXECUTE stmt; DEALLOCATE PREPARE stmt;

/* no longer needed, delete the following code at some point
SET @s = CONCAT('insert into zz_cx.temp_tbl_fc_fare_map_fbr
(doc_nbr_prime, doc_carr_nbr, trnsc_date, fare_lockin_date, fc_cpn_nbr, fc_orig, fc_orig_cntry, fc_dest, fc_dest_cntry, fc_carr_cd, 
fc_fbc, fc_mile_plus, fc_tkt_dsg, fc_pax_type, fc_curr_cd, fc_amt, fc_roe, fc_nuc_amt, jrny_dep_date, tkt_tour_cd, mod_tour_cd, 
map_di_ind, map_type, map_code, spec_fare_id,cat25_r2_id, c25_r3_cat_id)
select distinct 
 doc_nbr_prime, doc_carr_nbr, trnsc_date, fare_lockin_date, fc_cpn_nbr, fc_orig, fc_orig_cntry, fc_dest, fc_dest_cntry, fc_carr_cd, 
fc_fbc, fc_mile_plus, fc_tkt_dsg, fc_pax_type, fc_curr_cd, fc_amt, fc_roe, fc_nuc_amt, jrny_dep_date, tkt_tour_cd, mod_tour_cd, 
map_di_ind, map_type, map_code, fare_id, r2_25_rule_id, r3_25_cat_id
from zz_cx.temp_match_fbr_ac_', t_id,' m
straight_join zz_cx.temp_tbl_fc_all fc on (m.fc_all_id = fc.fc_all_id)
;');
*/

PREPARE stmt FROM @s; EXECUTE stmt; DEALLOCATE PREPARE stmt;
set @s = CONCAT('insert into zz_cx.temp_tbl_fc_fare_map_fbr
(doc_nbr_prime, doc_carr_nbr, trnsc_date, fare_lockin_date, fc_cpn_nbr, fc_orig, fc_orig_city, fc_orig_cntry, fc_dest, fc_dest_city, fc_dest_cntry, fc_carr_cd, 
fc_fbc, fc_mile_plus, fc_tkt_dsg, fc_pax_type, fc_curr_cd, fc_amt, fc_roe, fc_nuc_amt, jrny_dep_date, tkt_tour_cd, mod_tour_cd, 
map_di_ind, map_type, map_code, spec_fare_id,cat25_r2_id, c25_r3_cat_id)
select distinct 
 doc_nbr_prime, doc_carr_nbr, trnsc_date, fare_lockin_date, fc_cpn_nbr, fc_orig, fc_orig_c, fc_orig_n, fc_dest, fc_dest_c, fc_dest_n, fc_carr_cd, 
fc_fbc, fc_mile_plus, fc_tkt_dsg, fc_pax_type, fc_curr_cd, fc_amt, fc_roe, fc_nuc_amt, jrny_dep_date, tkt_tour_cd, mod_tour_cd, 
map_di_ind, map_type, map_code, fare_id, r2_25_rule_id, r3_25_cat_id
from zz_cx.temp_match_fbr_tc_', t_id,' m
straight_join zz_cx.temp_tbl_fc_all fc on (m.fc_all_id = fc.fc_all_id)
;');
PREPARE stmt FROM @s; EXECUTE stmt; DEALLOCATE PREPARE stmt;

END