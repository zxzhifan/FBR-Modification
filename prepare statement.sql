SET @s = CONCAT('DROP TABLE IF EXISTS zz_cx.temp_match_fbr_ctrl_', t_id);
PREPARE stmt FROM @s; EXECUTE stmt; DEALLOCATE PREPARE stmt;

SET @s = CONCAT('CREATE TABLE IF NOT EXISTS zz_cx.temp_match_fbr_ctrl_', t_id, '(
	fc_all_id int(20) NOT NULL,
	doc_nbr_prime bigint(20) NOT NULL,
	doc_carr_nbr char(3) NOT NULL,
	trnsc_date date NOT NULL,
	fc_cpn_nbr tinyint(3) unsigned NOT NULL,
	map_code char(1) DEFAULT '''',
	sys_proc_ts TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
	INDEX idx_doc (fc_all_id ASC)
	) ENGINE=InnoDB AUTO_INCREMENT=1 DEFAULT CHARSET=latin1;');
PREPARE stmt FROM @s; EXECUTE stmt; DEALLOCATE PREPARE stmt;

SET @s = CONCAT('insert into zz_cx.temp_match_fbr_ctrl_', t_id, '
(fc_all_id,doc_nbr_prime, doc_carr_nbr, trnsc_date, fc_cpn_nbr)
SELECT DISTINCT fc_all_id,doc_nbr_prime, doc_carr_nbr, trnsc_date, fc_cpn_nbr
FROM zz_cx.temp_tbl_fc_all fc
WHERE mod(fc.doc_nbr_prime, ', base, ') = ', t_id, ';');
PREPARE stmt FROM @s; EXECUTE stmt; DEALLOCATE PREPARE stmt;

set @s = CONCAT('DROP TABLE IF EXISTS zz_cx.temp_match_fbr_tc_',t_id,';');
PREPARE stmt FROM @s; EXECUTE stmt; DEALLOCATE PREPARE stmt;

SET @s = CONCAT('CREATE TABLE zz_cx.temp_match_fbr_tc_', t_id,'(
	fc_all_id int(11) NOT NULL,
	r2_25_rule_id int(11) NOT NULL,
	r3_25_cat_id int(11) NOT NULL,
	fare_id int(11) NOT NULL,
	map_type char(1) DEFAULT '''',
	map_code char(1) DEFAULT '''',
	sys_proc_ts TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP
	) ENGINE = InnoDB DEFAULT CHARSET=latin1;');
PREPARE stmt FROM @s; EXECUTE stmt; DEALLOCATE PREPARE stmt;

# FBR1-------------------------------------------------------------------------------------------------------------------------------------------------------------------------------
# match with both tour code AND account code (bcode for CX)

SET @s = CONCAT('DROP TABLE IF EXISTS zz_cx.fm_fbr1_fc_', t_id);
PREPARE stmt FROM @s; EXECUTE stmt; DEALLOCATE PREPARE stmt;

SET @s = CONCAT('CREATE TABLE IF NOT EXISTS zz_cx.fm_fbr1_fc_', t_id, ' ENGINE = InnoDB 
	SELECT DISTINCT ctrl.fc_all_id
	FROM zz_cx.temp_match_fbr_ctrl_', t_id,' ctrl
	STRAIGHT_JOIN zz_cx.temp_tbl_fc_all fc ON (ctrl.fc_all_id = fc.fc_all_id)
	STRAIGHT_JOIN tmp.temp_map_fbcx_fbr_r fbcm ON (fbcm.doc_nbr_prime = fc.doc_nbr_prime AND fbcm.fc_cpn_nbr = fc.fc_cpn_nbr)
	STRAIGHT_JOIN zz_cx.temp_map_bcode_acc mbc ON (mbc.bcode = fc.tkt_endorse_cd AND mbc.tar_nbr = fbcm.frt_nbr AND mbc.carr_cd = fbcm.carr_cd AND mbc.rule_nbr = fbcm.rule_nbr);');
PREPARE stmt FROM @s; EXECUTE stmt; DEALLOCATE PREPARE stmt;

SET @s = CONCAT('DROP TABLE IF EXISTS zz_cx.fm_fbr1_r3_', t_id);
PREPARE stmt FROM @s; EXECUTE stmt; DEALLOCATE PREPARE stmt;

SET @s = CONCAT('CREATE TABLE IF NOT EXISTS zz_cx.fm_fbr1_r3_', t_id,' ENGINE = InnoDB 
	SELECT DISTINCT fc.fc_all_id, r8.rule_id as r8_id, r2.rule_id as r2_id, r3.cat_id as r3_id, r2s.dir_ind, r3.base_tbl_t989, g16.ff_nbr
	FROM zz_cx.fm_fbr1_fc_', t_id,' ctrl
	STRAIGHT_JOIN zz_cx.temp_tbl_fc_all fc ON (ctrl.fc_all_id = fc.fc_all_id)
	STRAIGHT_JOIN tmp.temp_map_fbcx_fbr_r fbcm ON (fbcm.doc_nbr_prime = fc.doc_nbr_prime AND fbcm.fc_cpn_nbr = fc.fc_cpn_nbr)
	STRAIGHT_JOIN zz_cx.temp_map_bcode_acc mbc ON (mbc.bcode = fc.tkt_endorse_cd AND mbc.tar_nbr = fbcm.frt_nbr AND mbc.carr_cd = fbcm.carr_cd AND mbc.rule_nbr = fbcm.rule_nbr)

	STRAIGHT_JOIN atpco_fare.atpco_r8_fbr r8 ON (r8.tar_nbr = fbcm.frt_nbr AND r8.carr_cd = fbcm.carr_cd AND r8.rule_nbr = fbcm.rule_nbr AND r8.proc_ind in (''N'', ''R''))
	STRAIGHT_JOIN atpco_fare.atpco_r8_fbr_state r8t ON (r8t.rule_id = r8.rule_id)

	STRAIGHT_JOIN atpco_fare.atpco_r2_cat25_ctrl r2 ON (r2.carr_cd = r8.carr_cd AND r2.tar_nbr = r8.tar_nbr AND r2.rule_nbr = r8.rule_nbr AND r2.proc_ind in (''N'', ''R''))
	STRAIGHT_JOIN atpco_fare.atpco_r2_cat25_ctrl_state r2t ON (r2t.rule_id = r2.rule_id)
	STRAIGHT_JOIN atpco_fare.atpco_r2_cat25_ctrl_sup r2s ON (r2s.rule_id = r2.rule_id)

	STRAIGHT_JOIN atpco_fare.atpco_cat25 r3 ON (r3.cat_id = r2s.tbl_nbr AND r3.pax_type = r8.pax_type)

	LEFT JOIN atpco_fare.atpco_t994_date dor ON (dor.tbl_nbr = r3.dt_ovrd_t994)

	STRAIGHT_JOIN atpco_fare.atpco_t989_base_fare t989 ON (t989.tbl_nbr = r3.base_tbl_t989 AND t989.bf_appl <> ''N'')

	STRAIGHT_JOIN zz_cx.g16_temp g16 ON (g16.carr_cd = fc.fc_carr_cd AND (g16.frt_nbr = t989.bf_rule_tar or (t989.bf_rule_tar = ''000'' AND g16.pri_ind <> ''X'')))

	WHERE fc.tkt_endorse_cd <> ''''
	AND fc.fare_lockin_date between (r8t.rec_add_date - interval 1 day) AND if(r8t.rec_cnx_date = ''9999-12-31'', r8t.rec_cnx_date, r8t.rec_cnx_date + interval 1 day)
	AND fc.jrny_dep_date between r8t.tvl_eff_date AND r8t.tvl_dis_date
	AND if(mbc.pax_acc_cd <> '''' AND r8.pax_acc_cd <> '''', mbc.pax_acc_cd = r8.pax_acc_cd, true)

	AND fc.fare_lockin_date between (r2t.rec_add_date - interval 1 day) AND if(r2t.rec_cnx_date = ''9999-12-31'', r2t.rec_cnx_date, r2t.rec_cnx_date + interval 1 day)
	AND fc.jrny_dep_date between r2t.tvl_eff_date AND r2t.tvl_dis_date
	#----------------------- table 994 date override -------------------------------------------
	AND if(dor.tbl_nbr is null, true, fc.trnsc_date between dor.tkt_fr_date AND dor.tkt_to_date)
	AND if(dor.tbl_nbr is null, true, fc.fc_dep_date between dor.tvl_fr_date AND dor.tvl_to_date);');
PREPARE stmt FROM @s; EXECUTE stmt; DEALLOCATE PREPARE stmt;

SET @s = CONCAT('ALTER TABLE zz_cx.fm_fbr1_r3_', t_id,'
	ADD INDEX idx_tmp_id(fc_all_id, base_tbl_t989);');
PREPARE stmt FROM @s; EXECUTE stmt; DEALLOCATE PREPARE stmt;

SET @s = CONCAT('DROP TABLE IF EXISTS zz_cx.fm_fbr1_g16_', t_id,';');
PREPARE stmt FROM @s; EXECUTE stmt; DEALLOCATE PREPARE stmt;

SET @s = CONCAT('CREATE TABLE IF NOT EXISTS zz_cx.fm_fbr1_g16_', t_id,'
	SELECT DISTINCT fc_all_id, ff_nbr
	FROM zz_cx.fm_fbr1_r3_', t_id,';');
PREPARE stmt FROM @s; EXECUTE stmt; DEALLOCATE PREPARE stmt;

SET @s = CONCAT('DROP TABLE IF EXISTS zz_cx.fm_fbr1_t989_', t_id,';');
PREPARE stmt FROM @s; EXECUTE stmt; DEALLOCATE PREPARE stmt;

SET @s = CONCAT('CREATE TABLE IF NOT EXISTS zz_cx.fm_fbr1_t989_', t_id,'
	SELECT DISTINCT fc_all_id, base_tbl_t989
	FROM zz_cx.fm_fbr1_r3_', t_id,';');
PREPARE stmt FROM @s; EXECUTE stmt; DEALLOCATE PREPARE stmt;

SET @s = CONCAT('ALTER TABLE zz_cx.fm_fbr1_t989_', t_id,'
	ADD INDEX idx_fbr_t989_id(fc_all_id);');
PREPARE stmt FROM @s; EXECUTE stmt; DEALLOCATE PREPARE stmt;

SET @s = CONCAT('ALTER TABLE zz_cx.fm_fbr1_g16_', t_id,'
	ADD INDEX idx_fbr_g16_id(fc_all_id);');
PREPARE stmt FROM @s; EXECUTE stmt; DEALLOCATE PREPARE stmt;

SET @s = CONCAT('DROP TABLE IF EXISTS zz_cx.fm_fbr1_f_', t_id,';');
PREPARE stmt FROM @s; EXECUTE stmt; DEALLOCATE PREPARE stmt;

SET @s = CONCAT('CREATE TABLE IF NOT EXISTS zz_cx.fm_fbr1_f_', t_id,'
	SELECT DISTINCT ctrl.fc_all_id, f.fare_id, ctrl.ff_nbr
	FROM zz_cx.fm_fbr1_g16_', t_id,' ctrl
	STRAIGHT_JOIN zz_cx.temp_tbl_fc_all fc ON (ctrl.fc_all_id = fc.fc_all_id)
	/*
	STRAIGHT_JOIN tmp.temp_map_fbcx_fbr_r fbcm ON (fbcm.doc_nbr_prime = fc.doc_nbr_prime AND fbcm.fc_cpn_nbr = fc.fc_cpn_nbr)
	STRAIGHT_JOIN zz_cx.temp_map_bcode_acc mbc ON (mbc.bcode = fc.tkt_endorse_cd AND mbc.tar_nbr = fbcm.frt_nbr AND mbc.carr_cd = fbcm.carr_cd AND mbc.rule_nbr = fbcm.rule_nbr)
	*/
	STRAIGHT_JOIN atpco_fare.atpco_fare f ON (fc.fc_orig = f.orig_city AND fc.fc_dest = f.dest_city AND fc.fc_carr_cd = f.carr_cd AND f.tar_nbr = ctrl.ff_nbr) 
	STRAIGHT_JOIN atpco_fare.atpco_fare_state ft ON f.fare_id = ft.fare_id
	
	WHERE fc.tkt_endorse_cd <> ''''
	#----------------------- matching fc to the base fare -------------------------------------------
	AND fc.fare_lockin_date between (ft.rec_add_date - interval 1 day) AND if(ft.rec_cnx_date = ''9999-12-31'', ft.rec_cnx_date, ft.rec_cnx_date + interval 1 day)
	AND fc.jrny_dep_date between ft.tvl_eff_date AND ft.tvl_dis_date
	# not sure the logic is entirely right: AND if(fc.map_di_ind = ''F'', f.ftnt <> ''T'', f.ftnt <> ''F'')		-- useful for domestic, for international, ftnt.di is always ''F'', international footnote can never be ''F'' or ''T'';');
PREPARE stmt FROM @s; EXECUTE stmt; DEALLOCATE PREPARE stmt;

SET @s = CONCAT('DROP TABLE IF EXISTS zz_cx.fm_fbr1_syn_', t_id,';');
PREPARE stmt FROM @s; EXECUTE stmt; DEALLOCATE PREPARE stmt;

SET @s = CONCAT('CREATE TABLE IF NOT EXISTS zz_cx.fm_fbr1_syn_', t_id,'
	SELECT DISTINCT fc.fc_all_id, r2_id as r2_rule_id, f.fare_id as f_fare_id, r8_id as r8_rule_id, r3_id as r3_cat_id, cr.dir_ind, t989.tbl_nbr as t989_tbl_nbr, t989.bf_pax_type,  t989.bf_rule_tar as frt_nbr
	FROM zz_cx.fm_fbr1_f_', t_id,' ctrl -- fare part
	
	STRAIGHT_JOIN zz_cx.temp_tbl_fc_all fc ON (ctrl.fc_all_id = fc.fc_all_id)
	
	STRAIGHT_JOIN atpco_fare.atpco_fare f ON (ctrl.fare_id = f.fare_id AND ctrl.ff_nbr = f.tar_nbr)
	
	JOIN zz_cx.fm_fbr1_t989_', t_id,' ct ON (ctrl.fc_all_id = ct.fc_all_id)  -- r8 to r2
	
	STRAIGHT_JOIN atpco_fare.atpco_t989_base_fare t989 ON (t989.tbl_nbr = ct.base_tbl_t989 AND t989.bf_appl <> ''N'')
	
	STRAIGHT_JOIN zz_cx.g16_temp g16 ON (g16.carr_cd = fc.fc_carr_cd AND (g16.frt_nbr = t989.bf_rule_tar or (t989.bf_rule_tar = ''000'' AND g16.pri_ind <> ''X'')) AND ctrl.ff_nbr = g16.ff_nbr)
	
	STRAIGHT_JOIN zz_cx.fm_fbr1_r3_', t_id,' cr ON (ct.fc_all_id = cr.fc_all_id AND ct.base_tbl_t989 = cr.base_tbl_t989)
	
	WHERE  (f.rule_nbr = t989.bf_rule_nbr or t989.bf_rule_nbr = '''')
	AND if(t989.bf_fc_len = 0, true, 
		if(t989.bf_fc_wld = 0, f.fare_cls = t989.bf_fc, 
		if(t989.bf_fc_wld = t989.bf_fc_len, LEFT(f.fare_cls, t989.bf_fc_len-1) = LEFT(t989.bf_fc, t989.bf_fc_len-1),
		if(t989.bf_fc_wld = 1, instr(f.fare_cls, right(t989.bf_fc, t989.bf_fc_len-1)),
		f.fare_cls regexp t989.bf_fc_regex
		))))
	AND (f.rtg_nbr = t989.bf_rtg_nbr or t989.bf_rtg_nbr = ''99999'')
	AND (f.ow_rt_ind = t989.bf_ow_rt or t989.bf_ow_rt = '''')
	-- bf_type, not yet implemented
	-- bf_ssn, not yet implemented
	-- bf_dow, not yet implemented
	-- ftnt, not yet implemented
	-- AND (fc.fc_pax_type = t989.c25_pax_type or t989.c25_pax_type in (''ADT'', ''JCB'', ''''))	-- JCB AND ADT are general
	;');
PREPARE stmt FROM @s; EXECUTE stmt; DEALLOCATE PREPARE stmt;

SET @s = CONCAT('DROP TABLE IF EXISTS zz_cx.fm_fbr1_loc_', t_id,';');
PREPARE stmt FROM @s; EXECUTE stmt; DEALLOCATE PREPARE stmt;

SET @s = CONCAT('CREATE TABLE IF NOT EXISTS zz_cx.fm_fbr1_loc_', t_id,' 
	SELECT DISTINCT fc.doc_nbr_prime, fc.fc_cpn_nbr, ctrl.fc_all_id, ctrl.r2_rule_id as r2_25_rule_id, ctrl.r3_cat_id as r3_25_cat_id, f.fare_id, ''R'' as map_type, ''B'' as map_code
	FROM zz_cx.fm_fbr1_syn_', t_id,' ctrl
	JOIN zz_cx.temp_tbl_fc_all fc ON (ctrl.fc_all_id = fc.fc_all_id)
	JOIN atpco_fare.atpco_fare f ON (ctrl.f_fare_id = f.fare_id)

	JOIN atpco_fare.atpco_r1_fare_cls r1 ON (r1.carr_cd = f.carr_cd AND r1.tar_nbr = ctrl.frt_nbr AND r1.rule_nbr = f.rule_nbr AND r1.fare_cls = f.fare_cls AND r1.proc_ind in (''N'', ''R''))
	JOIN atpco_fare.atpco_r1_fare_cls_state r1t ON (r1t.rule_id = r1.rule_id)
	JOIN atpco_fare.atpco_r1_fare_cls_sup r1s ON (r1s.rule_id = r1.rule_id)

	JOIN zz_cx.temp_fbcx_fbr_r fbcx ON (fbcx.r8_rule_id = ctrl.r8_rule_id AND fbcx.r2_25_rule_id = ctrl.r2_rule_id AND fbcx.r3_25_cat_id = ctrl.r3_cat_id)			# matching based ON record 2 AND record 3  cat 25 id
	JOIN zz_cx.temp_tbl_fc_loc fc_loc ON (fc_loc.doc_nbr_prime = fc.doc_nbr_prime AND fc_loc.fc_cpn_nbr = fc.fc_cpn_nbr AND fc_loc.map_di_ind = fc.map_di_ind)		
	# to allow base fare matching AND r2 loaction testing using somewhat different orig/dest

	# forward direction location test
	LEFT JOIN zz_cx.loc_m fo ON (fo.fc_loc = fc_loc.fc_orig AND fo.loc_type = fbcx.loc1_type AND fo.loc = fbcx.loc1 AND fo.loc_t = fbcx.loc1_t978)
	LEFT JOIN zz_cx.loc_m fd ON (fd.fc_loc = fc_loc.fc_dest AND fd.loc_type = fbcx.loc2_type AND fd.loc = fbcx.loc2 AND fd.loc_t = fbcx.loc2_t978)
	
	# reverse direction location test
	LEFT JOIN zz_cx.loc_m ro ON (ro.fc_loc = fc_loc.fc_dest AND ro.loc_type = fbcx.loc1_type AND ro.loc = fbcx.loc1 AND ro.loc_t = fbcx.loc1_t978)
	LEFT JOIN zz_cx.loc_m rd ON (rd.fc_loc = fc_loc.fc_orig AND rd.loc_type = fbcx.loc2_type AND rd.loc = fbcx.loc2 AND rd.loc_t = fbcx.loc2_t978)

	WHERE fc.doc_nbr_prime > 0
	#----------------------- conditions for fare record to r1 -------------------------------------------
	-- location testing is not implemented
	-- minimum sequence is not implemented
	AND fc.fare_lockin_date between date_add(r1t.rec_add_date, interval -1 day) AND if(r1t.rec_cnx_date = ''9999-12-31'', r1t.rec_cnx_date, date_add(r1t.rec_cnx_date, interval 1 day))	
	AND fc.fare_lockin_date between date_add(r1t.rec_add_date, interval -1 day) AND r1t.tvl_dis_date
	AND fc.jrny_dep_date between r1t.tvl_eff_date AND r1t.tvl_dis_date
	AND (f.ow_rt_ind = r1.ow_rt_ind or (f.ow_rt_ind = 3 AND r1.ow_rt_ind = 1))
	AND (f.rtg_nbr = r1.rtg_nbr or r1.rtg_nbr = ''99999'')
	AND if(r1.ftnt = '''', true, if(length(f.ftnt) = 2 AND (LEFT(f.ftnt,1) between ''A'' AND ''Z''), LEFT(f.ftnt,1) = r1.ftnt or right(f.ftnt,1) = r1.ftnt, f.ftnt = r1.ftnt))
	AND (r1s.pax_type = ctrl.bf_pax_type or (ctrl.bf_pax_type = ''ADT'' AND r1s.pax_type = ''''))

	#----------------------- check all conditions that come with datawarehouse.temp_fbcx_fbr -------------------------------------------
	AND fc.fare_lockin_date between date_add(fbcx.rec_add_date, interval -1 day) AND if(fbcx.rec_cnx_date = ''9999-12-31'', fbcx.rec_cnx_date, date_add(fbcx.rec_cnx_date, interval 1 day))	
	AND fc.fare_lockin_date between date_add(fbcx.rec_add_date, interval -1 day) AND fbcx.tvl_dis_date
	AND fc.jrny_dep_date between fbcx.tvl_eff_date AND fbcx.tvl_dis_date

	AND (fbcx.ow_rt_ind = '''' or f.ow_rt_ind = fbcx.ow_rt_ind or (f.ow_rt_ind = 3 AND fbcx.ow_rt_ind = 1))
	AND (r1.fare_type = fbcx.fare_type or fbcx.fare_type = '''')		-- Match fare type
	AND (r1.ssn_type = fbcx.ssn_type or fbcx.ssn_type = '''')			-- Match Season Type
	AND (r1.dow_type = fbcx.dow_type or fbcx.dow_type = '''')			-- Match Day of Week Type
	AND (f.rtg_nbr = fbcx.rtg_nbr or (f.rtg_nbr <> ''00000'' AND fbcx.rtg_nbr = ''88888'') or fbcx.rtg_nbr = ''99999'')

	-- r1.tkt_dsg_mod <> '''' is not implemented, no real cases
	-- AND fc.fc_tkt_dsg = if(fbcx.dsg_rule = '''', r1s.tkt_dsg, fbcx.dsg_rule)
	AND if(fbcx.dsg_rule <> '''', fc.fc_tkt_dsg = fbcx.dsg_rule, if(r1s.tkt_dsg <> '''', fc.fc_tkt_dsg = r1s.tkt_dsg, true))
	
	AND fc.fc_fbc = if(fbcx.fbc_rule = '''', if(r1s.tkt_cd = '''', f.fare_cls, r1s.tkt_cd),
					(case
						when LEFT(fbcx.fbc_rule,1) = ''*'' then concat(LEFT(f.fare_cls, 1), right(fbcx.fbc_rule, length(fbcx.fbc_rule)-1))
						when LEFT(fbcx.fbc_rule,1) = ''-'' then concat(f.fare_cls, right(fbcx.fbc_rule, length(fbcx.fbc_rule)-1))
						when right(fbcx.fbc_rule,1) = ''-'' then concat(LEFT(fbcx.fbc_rule, 1), right(f.fare_cls, length(f.fare_cls)-1))
						else fbcx.fbc_rule
					end)
				)
	
	-- The location check must be done last, otherwise it takes up a lot of time
	-- !!! lesson: must use simple AND effective logics to reduce workload?
	AND (case ctrl.dir_ind
			when ''1'' then true	# only 31 instance, not checking it for now, pass
			when ''2'' then true	# only 20 
			when ''3'' then fo.fc_loc is not null AND fd.fc_loc is not null
			when ''4'' then ro.fc_loc is not null AND rd.fc_loc is not null
			else # blank, no direction, then test either one is true
				(fo.fc_loc is not null AND fd.fc_loc is not null)
                 or
                (ro.fc_loc is not null AND rd.fc_loc is not null)
		end)
	
	AND if(fbcx.r2_fare_cls = '''', true, if(instr(fbcx.r2_fare_cls, ''-'') = 0, fbcx.r2_fare_cls = f.fare_cls, f.fare_cls regexp fbcx.r2_fare_cls_regex));');
PREPARE stmt FROM @s; EXECUTE stmt; DEALLOCATE PREPARE stmt;

SET @s = CONCAT('update zz_cx.temp_match_fbr_ctrl_',t_id, ' ctrl
	JOIN zz_cx.fm_fbr1_loc_',t_id, ' m ON (ctrl.fc_all_id = m.fc_all_id)
	#(ctrl.doc_nbr_prime = m.doc_nbr_prime AND ctrl.fc_cpn_nbr = m.fc_cpn_nbr)
	set ctrl.map_code = ''2'';		-- 2 for matched by both tour code AND accound code (BCODE)');
PREPARE stmt FROM @s; EXECUTE stmt; DEALLOCATE PREPARE stmt;

# FBR2---------------------------------------------------------------------------------------------------------------------------------------------------------------------------------
# match with tour code only, part 1
# WHERE FBCX is no change ('') or append ('-CH')
# matching base fare fare class

SET @s = CONCAT('DROP TABLE IF EXISTS zz_cx.fm_fbr2_f_', t_id,';');
PREPARE stmt FROM @s; EXECUTE stmt; DEALLOCATE PREPARE stmt;

SET @s = CONCAT('CREATE TABLE IF NOT EXISTS zz_cx.fm_fbr2_f_', t_id,'
	SELECT DISTINCT	ctrl.fc_all_id, f.fare_id, fbc_match, f.tar_nbr
	FROM zz_cx.temp_match_fbr_ctrl_', t_id,' ctrl

	STRAIGHT_JOIN zz_cx.temp_tbl_fc_all fc ON (ctrl.fc_all_id = fc.fc_all_id)
	STRAIGHT_JOIN tmp.temp_map_fbcx_fbr_r fbcm ON (fbcm.doc_nbr_prime = fc.doc_nbr_prime AND fc.fc_cpn_nbr = fbcm.fc_cpn_nbr AND fbcm.fbcx_mode in ('''', ''-''))
	STRAIGHT_JOIN zz_cx.temp_tbl_fc_tar_spec ts ON (fc.fc_orig_n = ts.orig_cntry AND fc.fc_dest_n = ts.dest_cntry AND fc.fc_carr_cd = ts.carr_cd)
	STRAIGHT_JOIN atpco_fare.atpco_fare f ON (f.orig_city = fc.fc_orig AND f.dest_city = fc.fc_dest AND f.carr_cd = fc.fc_carr_cd  AND f.tar_nbr = ts.tar_nbr AND f.fare_cls = fbcm.fbc_match)
	STRAIGHT_JOIN atpco_fare.atpco_fare_state ft ON f.fare_id = ft.fare_id

	WHERE ctrl.map_code <> ''2''
	#----------------------- matching fc to the base fare -------------------------------------------
	AND fc.fare_lockin_date between (ft.rec_add_date - interval 1 day) AND if(ft.rec_cnx_date = ''9999-12-31'', ft.rec_cnx_date, ft.rec_cnx_date + interval 1 day)
	AND fc.jrny_dep_date between ft.tvl_eff_date AND ft.tvl_dis_date
	# not sure the logic is entirely right: AND if(fc.map_di_ind = ''F'', f.ftn t <> ''T'', f.ftnt <> ''F'')		-- useful for domestic, for international, ftnt.di is always ''F'', international footnote can never be ''F'' or ''T''
	;');
PREPARE stmt FROM @s; EXECUTE stmt; DEALLOCATE PREPARE stmt;

SET @s = CONCAT('DROP TABLE IF EXISTS zz_cx.fm_fbr2_r3_', t_id,';');
PREPARE stmt FROM @s; EXECUTE stmt; DEALLOCATE PREPARE stmt;

SET @s = CONCAT('CREATE TABLE zz_cx.fm_fbr2_r3_', t_id,'
	SELECT DISTINCT fc.fc_all_id, r8.rule_id as r8_id, r2.rule_id as r2_id, r3.cat_id as r3_id, r2s.dir_ind, r3.base_tbl_t989
	FROM zz_cx.fm_fbr2_f_', t_id,' ctrl
	STRAIGHT_JOIN zz_cx.temp_tbl_fc_all fc ON (ctrl.fc_all_id = fc.fc_all_id)

	STRAIGHT_JOIN tmp.temp_map_fbcx_fbr_r fbcm ON ((fbcm.doc_nbr_prime = fc.doc_nbr_prime AND fc.fc_cpn_nbr = fbcm.fc_cpn_nbr AND fbcm.fbcx_mode in ('''', ''-'')) AND ctrl.fbc_match = fbcm.fbc_match)
	STRAIGHT_JOIN atpco_fare.atpco_r8_fbr r8 ON (r8.tar_nbr = fbcm.frt_nbr AND r8.carr_cd = fbcm.carr_cd AND r8.rule_nbr = fbcm.rule_nbr AND r8.proc_ind in (''N'', ''R''))
	STRAIGHT_JOIN atpco_fare.atpco_r8_fbr_state r8t ON (r8t.rule_id = r8.rule_id)

	STRAIGHT_JOIN atpco_fare.atpco_r2_cat25_ctrl r2 ON (r2.carr_cd = r8.carr_cd AND r2.tar_nbr = r8.tar_nbr AND r2.rule_nbr = r8.rule_nbr AND r2.proc_ind in (''N'', ''R''))
	STRAIGHT_JOIN atpco_fare.atpco_r2_cat25_ctrl_state r2t ON (r2t.rule_id = r2.rule_id)
	STRAIGHT_JOIN atpco_fare.atpco_r2_cat25_ctrl_sup r2s ON (r2s.rule_id = r2.rule_id)

	STRAIGHT_JOIN atpco_fare.atpco_cat25 r3 ON (r3.cat_id = r2s.tbl_nbr)
	LEFT JOIN atpco_fare.atpco_t994_date dor ON (dor.tbl_nbr = r3.dt_ovrd_t994)

	WHERE 
	#----------------------- record 8 to the fare component -------------------------------------------
	-- loc testing is not implemented
	 fc.fare_lockin_date between (r8t.rec_add_date - interval 1 day) AND if(r8t.rec_cnx_date = ''9999-12-31'', r8t.rec_cnx_date, r8t.rec_cnx_date + interval 1 day)
	AND fc.jrny_dep_date between r8t.tvl_eff_date AND r8t.tvl_dis_date

	#----------------------- applying record 2 to fc -------------------------------------------
	-- loc testing is not implemented
	AND fc.fare_lockin_date between (r2t.rec_add_date - interval 1 day) AND if(r2t.rec_cnx_date = ''9999-12-31'', r2t.rec_cnx_date, r2t.rec_cnx_date + interval 1 day)
	AND fc.jrny_dep_date between r2t.tvl_eff_date AND r2t.tvl_dis_date

	#----------------------- cat 25 -------------------------------------------
	AND r3.pax_type = r8.pax_type
	AND if(r3.rslt_fare_tkt_cd <> '''', r3.rslt_fare_tkt_cd = fbcm.fbc_rule, if(r3.rslt_fare_cls <> '''', r3.rslt_fare_cls = fbcm.fbc_rule, true))

	#----------------------- table 994 date override -------------------------------------------
	AND if(dor.tbl_nbr is null, true, fc.trnsc_date between dor.tkt_fr_date AND dor.tkt_to_date)
	AND if(dor.tbl_nbr is null, true, fc.fc_dep_date between dor.tvl_fr_date AND dor.tvl_to_date);');
PREPARE stmt FROM @s; EXECUTE stmt; DEALLOCATE PREPARE stmt;

SET @s = CONCAT('ALTER TABLE zz_cx.fm_fbr2_r3_', t_id,'
	ADD INDEX idx_tmp_id(fc_all_id, base_tbl_t989);');
PREPARE stmt FROM @s; EXECUTE stmt; DEALLOCATE PREPARE stmt;

SET @s = CONCAT('DROP TABLE IF EXISTS zz_cx.fm_fbr2_t989_', t_id,';
	CREATE TABLE zz_cx.fm_fbr2_t989_', t_id,'
	SELECT DISTINCT fc_all_id, base_tbl_t989
	FROM zz_cx.fm_fbr2_r3_', t_id,';');
PREPARE stmt FROM @s; EXECUTE stmt; DEALLOCATE PREPARE stmt;

SET @s = CONCAT('ALTER TABLE zz_cx.fm_fbr2_t989_', t_id,'
	ADD INDEX idx_fbr_t989_id(fc_all_id);');
PREPARE stmt FROM @s; EXECUTE stmt; DEALLOCATE PREPARE stmt;

SET @s = CONCAT('DROP TABLE IF EXISTS zz_cx.fm_fbr2_syn_', t_id,';');
PREPARE stmt FROM @s; EXECUTE stmt; DEALLOCATE PREPARE stmt;

SET @s = CONCAT('CREATE TABLE IF NOT EXISTS zz_cx.fm_fbr2_syn_', t_id,'
	SELECT DISTINCT fc.fc_all_id, r2_id as r2_rule_id, r3_id as r3_25_cat_id, 
	f.fare_id as f_fare_id, r8_id as r8_rule_id, t989.tbl_nbr as t989_tbl_nbr, t989.bf_pax_type, cr.dir_ind, g16.frt_nbr
	FROM zz_cx.fm_fbr2_f_', t_id,' ctrl
	STRAIGHT_JOIN zz_cx.temp_tbl_fc_all fc ON (ctrl.fc_all_id = fc.fc_all_id)

	STRAIGHT_JOIN atpco_fare.atpco_fare f ON (f.fare_id = ctrl.fare_id)

	JOIN zz_cx.fm_fbr2_t989_', t_id,' ct ON (ctrl.fc_all_id = ct.fc_all_id) 

	STRAIGHT_JOIN atpco_fare.atpco_t989_base_fare t989 ON (t989.tbl_nbr = ct.base_tbl_t989 AND t989.bf_appl <> ''N'')

	STRAIGHT_JOIN zz_cx.fm_fbr2_r3_', t_id,' cr ON (ct.fc_all_id = cr.fc_all_id AND ct.base_tbl_t989 = cr.base_tbl_t989)

	STRAIGHT_JOIN zz_cx.g16_temp g16 ON ((g16.carr_cd = fc.fc_carr_cd AND (g16.frt_nbr = t989.bf_rule_tar or (t989.bf_rule_tar = ''000'' AND g16.pri_ind <> ''X''))) AND g16.ff_nbr = f.tar_nbr AND g16.ff_nbr = ctrl.tar_nbr)

	WHERE
	#----------------------- matching t989 to the base fare -------------------------------------------
	 (f.rule_nbr = t989.bf_rule_nbr or t989.bf_rule_nbr = '''')
	AND if(t989.bf_fc_len = 0, true, 
		if(t989.bf_fc_wld = 0, f.fare_cls = t989.bf_fc, 
		if(t989.bf_fc_wld = t989.bf_fc_len, LEFT(f.fare_cls, t989.bf_fc_len-1) = LEFT(t989.bf_fc, t989.bf_fc_len-1),
		if(t989.bf_fc_wld = 1, instr(f.fare_cls, right(t989.bf_fc, t989.bf_fc_len-1)),
		f.fare_cls regexp t989.bf_fc_regex
		))))
	AND (f.rtg_nbr = t989.bf_rtg_nbr or t989.bf_rtg_nbr = ''99999'')
	AND (f.ow_rt_ind = t989.bf_ow_rt or t989.bf_ow_rt = '''');');
PREPARE stmt FROM @s; EXECUTE stmt; DEALLOCATE PREPARE stmt;

SET @s = CONCAT('DROP TABLE IF EXISTS zz_cx.fm_fbr2_loc_', t_id,';');
PREPARE stmt FROM @s; EXECUTE stmt; DEALLOCATE PREPARE stmt;

SET @s = CONCAT('CREATE TABLE IF NOT EXISTS zz_cx.fm_fbr2_loc_', t_id,'
	SELECT DISTINCT fc.doc_nbr_prime, fc.fc_cpn_nbr, ctrl.fc_all_id, ctrl.r2_rule_id as r2_25_rule_id, ctrl.r3_25_cat_id as r3_25_cat_id, f.fare_id, ''R'' as map_type, ''T'' as map_code
	FROM zz_cx.fm_fbr2_syn_', t_id,' ctrl
	 
	JOIN zz_cx.temp_tbl_fc_all fc ON (ctrl.fc_all_id = fc.fc_all_id)
	JOIN atpco_fare.atpco_fare f ON ctrl.f_fare_id = f.fare_id

	JOIN atpco_fare.atpco_r1_fare_cls r1 ON (r1.carr_cd = f.carr_cd AND r1.tar_nbr = ctrl.frt_nbr AND r1.rule_nbr = f.rule_nbr AND r1.fare_cls = f.fare_cls AND r1.proc_ind in (''N'', ''R''))
	JOIN atpco_fare.atpco_r1_fare_cls_state r1t ON (r1t.rule_id = r1.rule_id)
	JOIN atpco_fare.atpco_r1_fare_cls_sup r1s ON (r1s.rule_id = r1.rule_id)

	JOIN zz_cx.temp_fbcx_fbr_r fbcx ON (fbcx.r8_rule_id = ctrl.r8_rule_id AND fbcx.r2_25_rule_id = ctrl.r2_rule_id AND fbcx.r3_25_cat_id = ctrl.r3_25_cat_id)
	JOIN zz_cx.temp_tbl_fc_loc fc_loc ON (fc_loc.doc_nbr_prime = fc.doc_nbr_prime AND fc_loc.fc_cpn_nbr = fc.fc_cpn_nbr AND fc_loc.map_di_ind = fc.map_di_ind)		
	# to allow base fare matching AND r2 loaction testing using somewhat different orig/dest

	# forward direction location test
	LEFT JOIN zz_cx.loc_m fo ON (fo.fc_loc = fc_loc.fc_orig AND fo.loc_type = fbcx.loc1_type AND fo.loc = fbcx.loc1 AND fo.loc_t = fbcx.loc1_t978)
	LEFT JOIN zz_cx.loc_m fd ON (fd.fc_loc = fc_loc.fc_dest AND fd.loc_type = fbcx.loc2_type AND fd.loc = fbcx.loc2 AND fd.loc_t = fbcx.loc2_t978)

	# reverse direction location test
	LEFT JOIN zz_cx.loc_m ro ON (ro.fc_loc = fc_loc.fc_dest AND ro.loc_type = fbcx.loc1_type AND ro.loc = fbcx.loc1 AND ro.loc_t = fbcx.loc1_t978)
	LEFT JOIN zz_cx.loc_m rd ON (rd.fc_loc = fc_loc.fc_orig AND rd.loc_type = fbcx.loc2_type AND rd.loc = fbcx.loc2 AND rd.loc_t = fbcx.loc2_t978)

	WHERE
	#----------------------- conditions for fare record to r1 -------------------------------------------
	-- location testing is not implemented
	-- minimum sequence is not implemented
	 fc.fare_lockin_date between date_add(r1t.rec_add_date, interval -1 day) AND if(r1t.rec_cnx_date = ''9999-12-31'', r1t.rec_cnx_date, date_add(r1t.rec_cnx_date, interval 1 day))	
	AND fc.fare_lockin_date between date_add(r1t.rec_add_date, interval -1 day) AND r1t.tvl_dis_date
	AND fc.jrny_dep_date between r1t.tvl_eff_date AND r1t.tvl_dis_date
	AND (f.ow_rt_ind = r1.ow_rt_ind or (f.ow_rt_ind = 3 AND r1.ow_rt_ind = 1))
	AND (f.rtg_nbr = r1.rtg_nbr or r1.rtg_nbr = ''99999'')
	AND if(r1.ftnt = '''', true, if(length(f.ftnt) = 2 AND (LEFT(f.ftnt,1) between ''A'' AND ''Z''), LEFT(f.ftnt,1) = r1.ftnt or right(f.ftnt,1) = r1.ftnt, f.ftnt = r1.ftnt))
	AND (r1s.pax_type = ctrl.bf_pax_type or (ctrl.bf_pax_type = ''ADT'' AND r1s.pax_type = ''''))

	#----------------------- check all conditions that come with datawarehouse.temp_fbcx_fbr -------------------------------------------
	AND fc.fare_lockin_date between date_add(fbcx.rec_add_date, interval -1 day) AND if(fbcx.rec_cnx_date = ''9999-12-31'', fbcx.rec_cnx_date, date_add(fbcx.rec_cnx_date, interval 1 day))	
	AND fc.fare_lockin_date between date_add(fbcx.rec_add_date, interval -1 day) AND fbcx.tvl_dis_date
	AND fc.jrny_dep_date between fbcx.tvl_eff_date AND fbcx.tvl_dis_date
	AND (fbcx.ow_rt_ind = '''' or f.ow_rt_ind = fbcx.ow_rt_ind or (f.ow_rt_ind = 3 AND fbcx.ow_rt_ind = 1))
	AND (r1.fare_type = fbcx.fare_type or fbcx.fare_type = '''')		-- Match fare type
	AND (r1.ssn_type = fbcx.ssn_type or fbcx.ssn_type = '''')			-- Match Season Type
	AND (r1.dow_type = fbcx.dow_type or fbcx.dow_type = '''')			-- Match Day of Week Type
	AND (f.rtg_nbr = fbcx.rtg_nbr or (f.rtg_nbr <> ''00000'' AND fbcx.rtg_nbr = ''88888'') or fbcx.rtg_nbr = ''99999'')

	-- r1.tkt_dsg_mod <> '''' is not implemented, no real cases
	-- AND fc.fc_tkt_dsg = if(fbcx.dsg_rule = '''', r1s.tkt_dsg, fbcx.dsg_rule)
	AND if(fbcx.dsg_rule <> '''', fc.fc_tkt_dsg = fbcx.dsg_rule, if(r1s.tkt_dsg <> '''', fc.fc_tkt_dsg = r1s.tkt_dsg, true))

	AND fc.fc_fbc = if(fbcx.fbc_rule = '''', if(r1s.tkt_cd = '''', f.fare_cls, r1s.tkt_cd),
						(case
							when LEFT(fbcx.fbc_rule,1) = ''*'' then concat(LEFT(f.fare_cls, 1), right(fbcx.fbc_rule, length(fbcx.fbc_rule)-1))
							when LEFT(fbcx.fbc_rule,1) = ''-'' then concat(f.fare_cls, right(fbcx.fbc_rule, length(fbcx.fbc_rule)-1))
							when right(fbcx.fbc_rule,1) = ''-'' then concat(LEFT(fbcx.fbc_rule, 1), right(f.fare_cls, length(f.fare_cls)-1))
							else fbcx.fbc_rule
						end)
					)

	-- The location check must be done last, otherwise it takes up a lot of time
	-- !!! lesson: must use simple AND effective logics to reduce workload?
	AND (case ctrl.dir_ind
			when ''1'' then true	# only 31 instance, not checking it for now, pass
			when ''2'' then true	# only 20 
			when ''3'' then fo.fc_loc is not null AND fd.fc_loc is not null
			when ''4'' then ro.fc_loc is not null AND rd.fc_loc is not null
			else # blank, no direction, then test either one is true
					(fo.fc_loc is not null AND fd.fc_loc is not null)
					 or
					(ro.fc_loc is not null AND rd.fc_loc is not null)
		end)

	AND if(fbcx.r2_fare_cls = '''', true, if(instr(fbcx.r2_fare_cls, ''-'') = 0, fbcx.r2_fare_cls = f.fare_cls, f.fare_cls regexp fbcx.r2_fare_cls_regex));');
PREPARE stmt FROM @s; EXECUTE stmt; DEALLOCATE PREPARE stmt;


# FBR3---------------------------------------------------------------------------------------------------------------------------------------------------------------------------------
# match with tour code only, part 2
# where FBCX is no change ('') or append ('-CH')
# matching to r1 ticketing code

SET @s = CONCAT('DROP TABLE IF EXISTS zz_cx.fm_fbr3_f_', t_id,';');
PREPARE stmt FROM @s; EXECUTE stmt; DEALLOCATE PREPARE stmt;

SET @s = CONCAT('CREATE TABLE IF NOT EXISTS zz_cx.fm_fbr3_f_', t_id,'
	SELECT DISTINCT ctrl.fc_all_id, f.fare_id, fbc_match
	FROM zz_cx.temp_match_fbr_ctrl_', t_id,' ctrl

	STRAIGHT_JOIN zz_cx.temp_tbl_fc_all fc ON (ctrl.fc_all_id = fc.fc_all_id)
	STRAIGHT_JOIN tmp.temp_map_fbcx_fbr_m fbcm ON (fbcm.doc_nbr_prime = fc.doc_nbr_prime AND fbcm.fc_cpn_nbr = fc.fc_cpn_nbr AND fbcm.fbcx_mode = '''' AND fbcm.tkt_cd_ind = ''Y'')
	#STRAIGHT_JOIN tmp.temp_map_fbcx_fbr_r fbcm ON (fbcm.doc_nbr_prime = fc.doc_nbr_prime AND fbcm.fc_cpn_nbr = fc.fc_cpn_nbr AND fbcm.fbcx_mode = '''' AND fbcm.tkt_cd_ind = ''Y'')
	
	STRAIGHT_JOIN zz_cx.temp_tbl_fc_tar_spec ts ON fc.fc_orig_n = ts.orig_cntry AND fc.fc_dest_n = ts.dest_cntry AND fc.fc_carr_cd = ts.carr_cd
	STRAIGHT_JOIN zz_cx.temp_map_r1_tkt_cd tcd ON (tcd.tkt_cd = fbcm.fbc_match AND tcd.carr_cd = fc.fc_carr_cd)  # reverse look FROM ticketing code to fare class code
	
	STRAIGHT_JOIN atpco_fare.atpco_fare f ON (f.orig_city = fc.fc_orig AND f.dest_city = fc.fc_dest AND f.carr_cd = fc.fc_carr_cd AND f.tar_nbr = ts.tar_nbr AND f.fare_cls = tcd.fare_cls)
	STRAIGHT_JOIN atpco_fare.atpco_fare_state ft ON f.fare_id = ft.fare_id
	
	WHERE fc.doc_nbr_prime > 0 AND ctrl.map_code <> ''2''
	#----------------------- matching fc to the base fare -------------------------------------------
	AND fc.fare_lockin_date between (ft.rec_add_date - interval 1 day) AND if(ft.rec_cnx_date = ''9999-12-31'', ft.rec_cnx_date, ft.rec_cnx_date + interval 1 day)
	AND fc.jrny_dep_date between ft.tvl_eff_date AND ft.tvl_dis_date
	# not sure the logic is entirely right: AND if(fc.map_di_ind = ''F'', f.ftnt <> ''T'', f.ftnt <> ''F'')		-- useful for domestic, for international, ftnt.di is always ''F'', international footnote can never be ''F'' or ''T''
	;');
PREPARE stmt FROM @s; EXECUTE stmt; DEALLOCATE PREPARE stmt;

SET @s = CONCAT('DROP TABLE IF EXISTS zz_cx.fm_fbr3_r2_', t_id,';');
PREPARE stmt FROM @s; EXECUTE stmt; DEALLOCATE PREPARE stmt;

SET @s = CONCAT('CREATE TABLE zz_cx.fm_fbr3_r2_', t_id,'
	SELECT DISTINCT fc.fc_all_id, r8.rule_id as r8_id, r8.tar_nbr, r8.carr_cd, r8.rule_nbr, r8.pax_type, fc.fc_fbc, fbcm.fbc_rule
	FROM zz_cx.fm_fbr3_f_', t_id,' ctrl

	STRAIGHT_JOIN zz_cx.temp_tbl_fc_all fc ON (ctrl.fc_all_id = fc.fc_all_id)

	STRAIGHT_JOIN tmp.temp_map_fbcx_fbr_r fbcm ON ((fbcm.doc_nbr_prime = fc.doc_nbr_prime AND fbcm.fc_cpn_nbr = fc.fc_cpn_nbr AND fbcm.fbcx_mode = '''' AND fbcm.tkt_cd_ind = ''Y'') AND ctrl.fbc_match = fbcm.fbc_match)

	STRAIGHT_JOIN atpco_fare.atpco_r8_fbr r8 ON (r8.tar_nbr = fbcm.frt_nbr AND r8.carr_cd = fbcm.carr_cd AND r8.rule_nbr = fbcm.rule_nbr AND r8.proc_ind in (''N'', ''R''))
	STRAIGHT_JOIN atpco_fare.atpco_r8_fbr_state r8t ON (r8t.rule_id = r8.rule_id)
	WHERE 
	#----------------------- record 8 to the fare component -------------------------------------------
	-- loc testing is not implemented
	 fc.fare_lockin_date between (r8t.rec_add_date - interval 1 day) AND if(r8t.rec_cnx_date = ''9999-12-31'', r8t.rec_cnx_date, r8t.rec_cnx_date + interval 1 day)
	AND fc.jrny_dep_date between r8t.tvl_eff_date AND r8t.tvl_dis_date;');
PREPARE stmt FROM @s; EXECUTE stmt; DEALLOCATE PREPARE stmt;

SET @s = CONCAT('DROP TABLE IF EXISTS zz_cx.fm_fbr3_r3_', t_id,';');
PREPARE stmt FROM @s; EXECUTE stmt; DEALLOCATE PREPARE stmt;

SET @s = CONCAT('CREATE TABLE zz_cx.fm_fbr3_r3_', t_id,'
	SELECT DISTINCT	fc.fc_all_id, ctrl.r8_id as r8_id, r2.rule_id as r2_id, r3.cat_id as r3_id, r2s.dir_ind, r3.base_tbl_t989
	FROM zz_cx.fm_fbr3_r2_', t_id,' ctrl

	STRAIGHT_JOIN zz_cx.temp_tbl_fc_all fc ON (ctrl.fc_all_id = fc.fc_all_id)

	STRAIGHT_JOIN atpco_fare.atpco_r2_cat25_ctrl r2 ON (r2.carr_cd = ctrl.carr_cd AND r2.tar_nbr = ctrl.tar_nbr AND r2.rule_nbr = ctrl.rule_nbr AND r2.proc_ind in (''N'', ''R''))
	STRAIGHT_JOIN atpco_fare.atpco_r2_cat25_ctrl_state r2t ON (r2t.rule_id = r2.rule_id)
	STRAIGHT_JOIN atpco_fare.atpco_r2_cat25_ctrl_sup r2s ON (r2s.rule_id = r2.rule_id)

	STRAIGHT_JOIN atpco_fare.atpco_cat25 r3 ON (r3.cat_id = r2s.tbl_nbr)
	LEFT JOIN atpco_fare.atpco_t994_date dor ON (dor.tbl_nbr = r3.dt_ovrd_t994)

	WHERE 
	#----------------------- cat 25 -------------------------------------------
	 r3.pax_type = ctrl.pax_type
	AND if(r3.rslt_fare_tkt_cd <> '''', r3.rslt_fare_tkt_cd = ctrl.fbc_rule, if(r3.rslt_fare_cls <> '''', r3.rslt_fare_cls = ctrl.fbc_rule, true))

	#----------------------- table 994 date override -------------------------------------------
	AND if(dor.tbl_nbr is null, true, fc.trnsc_date between dor.tkt_fr_date AND dor.tkt_to_date)
	AND if(dor.tbl_nbr is null, true, fc.fc_dep_date between dor.tvl_fr_date AND dor.tvl_to_date)

	#----------------------- applying record 2 to fc -------------------------------------------
	-- loc testing is not implemented
	AND fc.fare_lockin_date between (r2t.rec_add_date - interval 1 day) AND if(r2t.rec_cnx_date = ''9999-12-31'', r2t.rec_cnx_date, r2t.rec_cnx_date + interval 1 day)
	AND fc.jrny_dep_date between r2t.tvl_eff_date AND r2t.tvl_dis_date;');
PREPARE stmt FROM @s; EXECUTE stmt; DEALLOCATE PREPARE stmt;

SET @s = CONCAT('ALTER TABLE zz_cx.fm_fbr3_r3_', t_id,'
	ADD INDEX idx_tmp_id(fc_all_id, base_tbl_t989);');
PREPARE stmt FROM @s; EXECUTE stmt; DEALLOCATE PREPARE stmt;

SET @s = CONCAT('DROP TABLE IF EXISTS zz_cx.fm_fbr3_t989_', t_id,';');
PREPARE stmt FROM @s; EXECUTE stmt; DEALLOCATE PREPARE stmt;

SET @s = CONCAT('CREATE TABLE zz_cx.fm_fbr3_t989_', t_id,'
	SELECT DISTINCT fc_all_id, base_tbl_t989
	FROM zz_cx.fm_fbr3_r3_', t_id,';');
PREPARE stmt FROM @s; EXECUTE stmt; DEALLOCATE PREPARE stmt;

SET @s = CONCAT('ALTER TABLE zz_cx.fm_fbr3_t989_', t_id,'
	ADD INDEX idx_fbr_t989_id(fc_all_id);');
PREPARE stmt FROM @s; EXECUTE stmt; DEALLOCATE PREPARE stmt;

SET @s = CONCAT('DROP TABLE IF EXISTS zz_cx.fm_fbr3_syn_', t_id,';');
PREPARE stmt FROM @s; EXECUTE stmt; DEALLOCATE PREPARE stmt;

SET @s = CONCAT('CREATE TABLE IF NOT EXISTS zz_cx.fm_fbr3_syn_', t_id,'
	SELECT DISTINCT fc.fc_all_id, r2_id as r2_rule_id, r3_id as r3_25_cat_id, f.fare_id as f_fare_id, r8_id as r8_rule_id, t989.tbl_nbr as t989_tbl_nbr, t989.bf_pax_type, cr.dir_ind, g16.frt_nbr, ctrl.fbc_match
	FROM zz_cx.fm_fbr3_f_', t_id,' ctrl

	STRAIGHT_JOIN zz_cx.temp_tbl_fc_all fc ON (ctrl.fc_all_id = fc.fc_all_id)

	STRAIGHT_JOIN atpco_fare.atpco_fare f ON (f.fare_id = ctrl.fare_id)

	JOIN zz_cx.fm_fbr3_t989_', t_id,' ct ON (ctrl.fc_all_id = ct.fc_all_id) 

	STRAIGHT_JOIN atpco_fare.atpco_t989_base_fare t989 ON (t989.tbl_nbr = ct.base_tbl_t989 AND t989.bf_appl <> ''N'')

	STRAIGHT_JOIN zz_cx.g16_temp g16 ON (g16.carr_cd = fc.fc_carr_cd AND (g16.frt_nbr = t989.bf_rule_tar or (t989.bf_rule_tar = ''000'' AND g16.pri_ind <> ''X'')))

	STRAIGHT_JOIN zz_cx.fm_fbr3_r3_', t_id,' cr ON (ct.fc_all_id = cr.fc_all_id AND ct.base_tbl_t989 = cr.base_tbl_t989)

	WHERE fc.doc_nbr_prime > 0
	#----------------------- matching t989 to the base fare -------------------------------------------
	AND (f.rule_nbr = t989.bf_rule_nbr or t989.bf_rule_nbr = '''')
	AND if(t989.bf_fc_len = 0, true, 
		if(t989.bf_fc_wld = 0, f.fare_cls = t989.bf_fc, 
		if(t989.bf_fc_wld = t989.bf_fc_len, LEFT(f.fare_cls, t989.bf_fc_len-1) = LEFT(t989.bf_fc, t989.bf_fc_len-1),
		if(t989.bf_fc_wld = 1, instr(f.fare_cls, right(t989.bf_fc, t989.bf_fc_len-1)),
		f.fare_cls regexp t989.bf_fc_regex
		))))
	AND (f.rtg_nbr = t989.bf_rtg_nbr or t989.bf_rtg_nbr = ''99999'')
	AND (f.ow_rt_ind = t989.bf_ow_rt or t989.bf_ow_rt = '''')
	-- bf_type, not yet implemented
	-- bf_ssn, not yet implemented
	-- bf_dow, not yet implemented
	-- ftnt, not yet implemented
	-- AND (fc.fc_pax_type = t989.c25_pax_type or t989.c25_pax_type in (''ADT'', ''JCB'', ''''))	-- JCB AND ADT are general
	;');
PREPARE stmt FROM @s; EXECUTE stmt; DEALLOCATE PREPARE stmt;

SET @s = CONCAT('DROP TABLE IF EXISTS zz_cx.fm_fbr3_loc_', t_id,';');
PREPARE stmt FROM @s; EXECUTE stmt; DEALLOCATE PREPARE stmt;

SET @s = CONCAT('CREATE TABLE IF NOT EXISTS zz_cx.fm_fbr3_loc_', t_id,'
	SELECT DISTINCT fc.doc_nbr_prime, fc.fc_cpn_nbr, ctrl.fc_all_id, ctrl.r2_rule_id as r2_25_rule_id, ctrl.r3_25_cat_id as r3_25_cat_id, f.fare_id, ''R'' as map_type, ''T'' as map_code
	FROM zz_cx.fm_fbr3_syn_', t_id,' ctrl

	STRAIGHT_JOIN zz_cx.temp_tbl_fc_all fc ON (ctrl.fc_all_id = fc.fc_all_id)

	JOIN atpco_fare.atpco_fare f ON ctrl.f_fare_id = f.fare_id

	#STRAIGHT_JOIN tmp.temp_map_fbcx_fbr_r fbcm ON (fbcm.doc_nbr_prime = fc.doc_nbr_prime AND fbcm.fc_cpn_nbr = fc.fc_cpn_nbr AND fbcm.fbcx_mode = '''' AND fbcm.tkt_cd_ind = ''Y'')

	STRAIGHT_JOIN atpco_fare.atpco_r1_fare_cls r1 ON (r1.carr_cd = f.carr_cd AND r1.tar_nbr = ctrl.frt_nbr AND r1.rule_nbr = f.rule_nbr AND r1.fare_cls = f.fare_cls AND r1.proc_ind in (''N'', ''R''))
	STRAIGHT_JOIN atpco_fare.atpco_r1_fare_cls_state r1t ON (r1t.rule_id = r1.rule_id)
	STRAIGHT_JOIN atpco_fare.atpco_r1_fare_cls_sup r1s ON (r1s.rule_id = r1.rule_id AND r1s.tkt_cd = ctrl.fbc_match)

	STRAIGHT_JOIN zz_cx.temp_fbcx_fbr fbcx ON (fbcx.r8_rule_id = ctrl.r8_rule_id AND fbcx.r2_25_rule_id = ctrl.r2_rule_id AND fbcx.r3_25_cat_id = ctrl.r3_25_cat_id)
	STRAIGHT_JOIN zz_cx.temp_tbl_fc_loc fc_loc ON (fc_loc.doc_nbr_prime = fc.doc_nbr_prime AND fc_loc.fc_cpn_nbr = fc.fc_cpn_nbr AND fc_loc.map_di_ind = fc.map_di_ind)		
	# to allow base fare matching AND r2 loaction testing using somewhat different orig/dest

	# forward direction location test
	LEFT JOIN zz_cx.loc_m fo ON (fo.fc_loc = fc_loc.fc_orig AND fo.loc_type = fbcx.loc1_type AND fo.loc = fbcx.loc1 AND fo.loc_t = fbcx.loc1_t978)
	LEFT JOIN zz_cx.loc_m fd ON (fd.fc_loc = fc_loc.fc_dest AND fd.loc_type = fbcx.loc2_type AND fd.loc = fbcx.loc2 AND fd.loc_t = fbcx.loc2_t978)

	# reverse direction location test
	LEFT JOIN zz_cx.loc_m ro ON (ro.fc_loc = fc_loc.fc_dest AND ro.loc_type = fbcx.loc1_type AND ro.loc = fbcx.loc1 AND ro.loc_t = fbcx.loc1_t978)
	LEFT JOIN zz_cx.loc_m rd ON (rd.fc_loc = fc_loc.fc_orig AND rd.loc_type = fbcx.loc2_type AND rd.loc = fbcx.loc2 AND rd.loc_t = fbcx.loc2_t978)
	WHERE fc.doc_nbr_prime > 0
	#----------------------- conditions for fare record to r1 -------------------------------------------
	-- location testing is not implemented
	-- minimum sequence is not implemented
	AND fc.fare_lockin_date between date_add(r1t.rec_add_date, interval -1 day) AND if(r1t.rec_cnx_date = ''9999-12-31'', r1t.rec_cnx_date, date_add(r1t.rec_cnx_date, interval 1 day))	
	AND fc.fare_lockin_date between date_add(r1t.rec_add_date, interval -1 day) AND r1t.tvl_dis_date
	AND fc.jrny_dep_date between r1t.tvl_eff_date AND r1t.tvl_dis_date
	AND (f.ow_rt_ind = r1.ow_rt_ind or (f.ow_rt_ind = 3 AND r1.ow_rt_ind = 1))
	AND (f.rtg_nbr = r1.rtg_nbr or r1.rtg_nbr = ''99999'')
	AND if(r1.ftnt = '''', true, if(length(f.ftnt) = 2 AND (LEFT(f.ftnt,1) between ''A'' AND ''Z''), LEFT(f.ftnt,1) = r1.ftnt or right(f.ftnt,1) = r1.ftnt, f.ftnt = r1.ftnt))
	AND (r1s.pax_type = ctrl.bf_pax_type or (ctrl.bf_pax_type = ''ADT'' AND r1s.pax_type = ''''))

	#----------------------- check all conditions that come with datawarehouse.temp_fbcx_fbr -------------------------------------------
	AND fc.fare_lockin_date between date_add(fbcx.rec_add_date, interval -1 day) AND if(fbcx.rec_cnx_date = ''9999-12-31'', fbcx.rec_cnx_date, date_add(fbcx.rec_cnx_date, interval 1 day))	
	AND fc.fare_lockin_date between date_add(fbcx.rec_add_date, interval -1 day) AND fbcx.tvl_dis_date
	AND fc.jrny_dep_date between fbcx.tvl_eff_date AND fbcx.tvl_dis_date

	AND (fbcx.ow_rt_ind = '''' or f.ow_rt_ind = fbcx.ow_rt_ind or (f.ow_rt_ind = 3 AND fbcx.ow_rt_ind = 1))
	AND (r1.fare_type = fbcx.fare_type or fbcx.fare_type = '''')		-- Match fare type
	AND (r1.ssn_type = fbcx.ssn_type or fbcx.ssn_type = '''')			-- Match Season Type
	AND (r1.dow_type = fbcx.dow_type or fbcx.dow_type = '''')			-- Match Day of Week Type
	AND (f.rtg_nbr = fbcx.rtg_nbr or (f.rtg_nbr <> ''00000'' AND fbcx.rtg_nbr = ''88888'') or fbcx.rtg_nbr = ''99999'')

	-- r1.tkt_dsg_mod <> '''' is not implemented, no real cases
	-- AND fc.fc_tkt_dsg = if(fbcx.dsg_rule = '''', r1s.tkt_dsg, fbcx.dsg_rule)
	AND if(fbcx.dsg_rule <> '''', fc.fc_tkt_dsg = fbcx.dsg_rule, if(r1s.tkt_dsg <> '''', fc.fc_tkt_dsg = r1s.tkt_dsg, true))

	AND fc.fc_fbc = if(fbcx.fbc_rule = '''', if(r1s.tkt_cd = '''', f.fare_cls, r1s.tkt_cd),
						(case
							when LEFT(fbcx.fbc_rule,1) = ''*'' then concat(LEFT(f.fare_cls, 1), right(fbcx.fbc_rule, length(fbcx.fbc_rule)-1))
							when LEFT(fbcx.fbc_rule,1) = ''-'' then concat(f.fare_cls, right(fbcx.fbc_rule, length(fbcx.fbc_rule)-1))
							when right(fbcx.fbc_rule,1) = ''-'' then concat(LEFT(fbcx.fbc_rule, 1), right(f.fare_cls, length(f.fare_cls)-1))
							else fbcx.fbc_rule
						end)
					)

	-- The location check must be done last, otherwise it takes up a lot of time
	-- !!! lesson: must use simple AND effective logics to reduce workload?
	AND (case ctrl.dir_ind
			when ''1'' then true	# only 31 instance, not checking it for now, pass
			when ''2'' then true	# only 20 
			when ''3'' then fo.fc_loc is not null AND fd.fc_loc is not null
			when ''4'' then ro.fc_loc is not null AND rd.fc_loc is not null
			else # blank, no direction, then test either one is true
					(fo.fc_loc is not null AND fd.fc_loc is not null)
					or
					(ro.fc_loc is not null AND rd.fc_loc is not null)
		end)

	AND if(fbcx.r2_fare_cls = '''', true, if(instr(fbcx.r2_fare_cls, ''-'') = 0, fbcx.r2_fare_cls = f.fare_cls, f.fare_cls regexp fbcx.r2_fare_cls_regex));');
PREPARE stmt FROM @s; EXECUTE stmt; DEALLOCATE PREPARE stmt;

# FBR4---------------------------------------------------------------------------------------------------------------------------------------------------------------------------------
# match with tour code only, part 3
# where FBCX is replace (ie. XYZ >> ABC)

SET @s = CONCAT('DROP TABLE IF EXISTS zz_cx.fm_fbr4_f_', t_id,';');
PREPARE stmt FROM @s; EXECUTE stmt; DEALLOCATE PREPARE stmt;

SET @s = CONCAT('CREATE TABLE IF NOT EXISTS zz_cx.fm_fbr4_f_', t_id,'
	SELECT DISTINCT	ctrl.fc_all_id, f.fare_id, fbc_match
	FROM zz_cx.temp_match_fbr_ctrl_', t_id,' ctrl
	STRAIGHT_JOIN zz_cx.temp_tbl_fc_all fc ON (ctrl.fc_all_id = fc.fc_all_id)

	STRAIGHT_JOIN tmp.temp_map_fbcx_fbr_m fbcm ON (fbcm.doc_nbr_prime = fc.doc_nbr_prime AND fbcm.fc_cpn_nbr = fc.fc_cpn_nbr AND fbcm.fbcx_mode = ''X'')
	STRAIGHT_JOIN zz_cx.temp_tbl_fc_tar_spec ts ON fc.fc_orig_n = ts.orig_cntry AND fc.fc_dest_n = ts.dest_cntry AND fc.fc_carr_cd = ts.carr_cd
	STRAIGHT_JOIN atpco_fare.atpco_fare f ON (f.orig_city = fc.fc_orig AND f.dest_city = fc.fc_dest AND f.carr_cd = fc.fc_carr_cd AND f.tar_nbr = ts.tar_nbr)
	STRAIGHT_JOIN atpco_fare.atpco_fare_state ft ON f.fare_id = ft.fare_id
	WHERE fc.doc_nbr_prime > 0 AND ctrl.map_code <> ''2''

	#----------------------- matching fc to the base fare -------------------------------------------
	AND fc.fare_lockin_date between (ft.rec_add_date - interval 1 day) AND if(ft.rec_cnx_date = ''9999-12-31'', ft.rec_cnx_date, ft.rec_cnx_date + interval 1 day)
	AND fc.jrny_dep_date between ft.tvl_eff_date AND ft.tvl_dis_date
	# not sure the logic is entirely right: AND if(fc.map_di_ind = ''F'', f.ftnt <> ''T'', f.ftnt <> ''F'')		-- useful for domestic, for international, ftnt.di is always ''F'', international footnote can never be ''F'' or ''T''
	;');
PREPARE stmt FROM @s; EXECUTE stmt; DEALLOCATE PREPARE stmt;

SET @s = CONCAT('DROP TABLE IF EXISTS zz_cx.fm_fbr4_r2_', t_id,';');
PREPARE stmt FROM @s; EXECUTE stmt; DEALLOCATE PREPARE stmt;

SET @s = CONCAT('CREATE TABLE IF NOT EXISTS zz_cx.fm_fbr4_r2_', t_id,' 
	SELECT DISTINCT fc.fc_all_id, r8.tar_nbr, r8.carr_cd, r8.rule_nbr, r8.pax_type, fc.fc_fbc, fbcm.fbc_rule
	FROM zz_cx.temp_match_fbr_ctrl_', t_id,' ctrl
	STRAIGHT_JOIN zz_cx.temp_tbl_fc_all fc ON (ctrl.fc_all_id = fc.fc_all_id)  
	STRAIGHT_JOIN tmp.temp_map_fbcx_fbr_r fbcm ON (fbcm.doc_nbr_prime = fc.doc_nbr_prime AND fbcm.fc_cpn_nbr = fc.fc_cpn_nbr AND fbcm.fbcx_mode = ''X'')  
	
	STRAIGHT_JOIN atpco_fare.atpco_r8_fbr r8 ON (r8.tar_nbr = fbcm.frt_nbr AND r8.carr_cd = fbcm.carr_cd AND r8.rule_nbr = fbcm.rule_nbr AND r8.proc_ind in (''N'', ''R'')) 
	STRAIGHT_JOIN atpco_fare.atpco_r8_fbr_state r8t ON (r8t.rule_id = r8.rule_id)

	STRAIGHT_JOIN zz_cx.temp_fbcx_fbr_r fbcx ON (fbcx.r8_rule_id = r8.rule_id AND fbcx.fbc_rule = fc.fc_fbc AND fbcx.dsg_rule = fc.fc_tkt_dsg)  

	WHERE fc.doc_nbr_prime > 0 AND ctrl.map_code <> ''2''
	#----------------------- record 8 to the fare component ------------------------------------------- 
	-- loc testing is not implemented 
	AND fc.fare_lockin_date between (r8t.rec_add_date - interval 1 day) 
	AND if(r8t.rec_cnx_date = ''9999-12-31'', r8t.rec_cnx_date, r8t.rec_cnx_date + interval 1 day) 
	AND fc.jrny_dep_date between r8t.tvl_eff_date AND r8t.tvl_dis_date 

	#----------------------- check all conditions that come with datawarehouse.temp_fbcx_fbr ------------------------------------------- 
	AND fc.fare_lockin_date between date_add(fbcx.rec_add_date, interval -1 day) AND if(fbcx.rec_cnx_date = ''9999-12-31'', fbcx.rec_cnx_date, date_add(fbcx.rec_cnx_date, interval 1 day))   
	AND fc.fare_lockin_date between date_add(fbcx.rec_add_date, interval -1 day) AND fbcx.tvl_dis_date
	AND fbcx.tvl_dis_date AND fc.jrny_dep_date between fbcx.tvl_eff_date AND fbcx.tvl_dis_date;');
PREPARE stmt FROM @s; EXECUTE stmt; DEALLOCATE PREPARE stmt;

SET @s = CONCAT('ALTER TABLE zz_cx.fm_fbr4_r2_', t_id,'
	ADD UNIQUE INDEX idx_uni(fc_all_id, tar_nbr, carr_cd, rule_nbr, pax_type, fc_fbc, fbc_rule);');
PREPARE stmt FROM @s; EXECUTE stmt; DEALLOCATE PREPARE stmt;

SET @s = CONCAT('INSERT IGNORE INTO zz_cx.fm_fbr4_r2_', t_id,' 
	SELECT DISTINCT fc.fc_all_id, r8.tar_nbr, r8.carr_cd, r8.rule_nbr, r8.pax_type, fc.fc_fbc, fbcm.fbc_rule
	FROM zz_cx.temp_match_fbr_ctrl_', t_id,' ctrl
	STRAIGHT_JOIN zz_cx.temp_tbl_fc_all fc ON (ctrl.fc_all_id = fc.fc_all_id)  
	STRAIGHT_JOIN tmp.temp_map_fbcx_fbr_r fbcm ON (fbcm.doc_nbr_prime = fc.doc_nbr_prime AND fbcm.fc_cpn_nbr = fc.fc_cpn_nbr AND fbcm.fbcx_mode = ''X'')  
	
	STRAIGHT_JOIN atpco_fare.atpco_r8_fbr r8 ON (r8.tar_nbr = fbcm.frt_nbr AND r8.carr_cd = fbcm.carr_cd AND r8.rule_nbr = fbcm.rule_nbr AND r8.proc_ind in (''N'', ''R'')) 
	STRAIGHT_JOIN atpco_fare.atpco_r8_fbr_state r8t ON (r8t.rule_id = r8.rule_id)

	STRAIGHT_JOIN zz_cx.temp_fbcx_fbr_r fbcx ON (fbcx.r8_rule_id = r8.rule_id AND fbcx.fbc_rule = fc.fc_fbc AND fbcx.dsg_rule = '''')  

	WHERE fc.doc_nbr_prime > 0 AND ctrl.map_code <> ''2''
	#----------------------- record 8 to the fare component ------------------------------------------- 
	-- loc testing is not implemented 
	AND fc.fare_lockin_date between (r8t.rec_add_date - interval 1 day) 
	AND if(r8t.rec_cnx_date = ''9999-12-31'', r8t.rec_cnx_date, r8t.rec_cnx_date + interval 1 day) 
	AND fc.jrny_dep_date between r8t.tvl_eff_date AND r8t.tvl_dis_date 

	#----------------------- check all conditions that come with datawarehouse.temp_fbcx_fbr ------------------------------------------- 
	AND fc.fare_lockin_date between date_add(fbcx.rec_add_date, interval -1 day) AND if(fbcx.rec_cnx_date = ''9999-12-31'', fbcx.rec_cnx_date, date_add(fbcx.rec_cnx_date, interval 1 day))   
	AND fc.fare_lockin_date between date_add(fbcx.rec_add_date, interval -1 day) AND fbcx.tvl_dis_date
	AND fbcx.tvl_dis_date AND fc.jrny_dep_date between fbcx.tvl_eff_date AND fbcx.tvl_dis_date;');
PREPARE stmt FROM @s; EXECUTE stmt; DEALLOCATE PREPARE stmt;

SET @s = CONCAT('DROP TABLE IF EXISTS zz_cx.fm_fbr4_r3_', t_id,';');
PREPARE stmt FROM @s; EXECUTE stmt; DEALLOCATE PREPARE stmt;

SET @s = CONCAT('CREATE TABLE IF NOT EXISTS zz_cx.fm_fbr4_r3_', t_id,'
	SELECT DISTINCT fc.fc_all_id, r2.rule_id as r2_id, r3.cat_id as r3_id, r3.base_tbl_t989, ctrl.tar_nbr, ctrl.carr_cd, ctrl.rule_nbr, ctrl.fc_fbc, fbcx.rcid as fbcx_id
	FROM zz_cx.fm_fbr4_r2_', t_id,' ctrl
	STRAIGHT_JOIN zz_cx.temp_tbl_fc_all fc ON (ctrl.fc_all_id = fc.fc_all_id)
	#STRAIGHT_JOIN tmp.temp_map_fbcx_fbr_r fbcm ON (fbcm.doc_nbr_prime = fc.doc_nbr_prime AND fbcm.fc_cpn_nbr = fc.fc_cpn_nbr AND fbcm.fbcx_mode = ''X'')

	STRAIGHT_JOIN atpco_fare.atpco_r2_cat25_ctrl r2 ON (r2.carr_cd = ctrl.carr_cd AND r2.tar_nbr = ctrl.tar_nbr AND r2.rule_nbr = ctrl.rule_nbr AND r2.proc_ind in (''N'', ''R''))
	STRAIGHT_JOIN atpco_fare.atpco_r2_cat25_ctrl_state r2t ON (r2t.rule_id = r2.rule_id)
	STRAIGHT_JOIN atpco_fare.atpco_r2_cat25_ctrl_sup r2s ON (r2s.rule_id = r2.rule_id)

	STRAIGHT_JOIN atpco_fare.atpco_cat25 r3 ON (r3.cat_id = r2s.tbl_nbr)
	LEFT JOIN atpco_fare.atpco_t994_date dor ON (dor.tbl_nbr = r3.dt_ovrd_t994)

	STRAIGHT_JOIN zz_cx.temp_fbcx_fbr_r fbcx ON (fbcx.tar_nbr = ctrl.tar_nbr AND fbcx.carr_cd = ctrl.carr_cd AND fbcx.rule_nbr = ctrl.rule_nbr AND fbcx.r2_25_rule_id = r2.rule_id AND fbcx.r3_25_cat_id = r3.cat_id 
												AND fbcx.fbc_rule = fc.fc_fbc AND (fbcx.dsg_rule = fc.fc_tkt_dsg or fbcx.dsg_rule = ''''))

	STRAIGHT_JOIN zz_cx.temp_tbl_fc_loc fc_loc ON (fc_loc.doc_nbr_prime = fc.doc_nbr_prime AND fc_loc.fc_cpn_nbr = fc.fc_cpn_nbr AND fc_loc.map_di_ind = fc.map_di_ind)		
	# to allow base fare matching AND r2 loaction testing using somewhat different orig/dest

	# forward direction location test
	LEFT JOIN zz_cx.loc_m fo ON (fo.fc_loc = fc_loc.fc_orig AND fo.loc_type = fbcx.loc1_type AND fo.loc = fbcx.loc1 AND fo.loc_t = fbcx.loc1_t978)
	LEFT JOIN zz_cx.loc_m fd ON (fd.fc_loc = fc_loc.fc_dest AND fd.loc_type = fbcx.loc2_type AND fd.loc = fbcx.loc2 AND fd.loc_t = fbcx.loc2_t978)

	# reverse direction location test
	LEFT JOIN zz_cx.loc_m ro ON (ro.fc_loc = fc_loc.fc_dest AND ro.loc_type = fbcx.loc1_type AND ro.loc = fbcx.loc1 AND ro.loc_t = fbcx.loc1_t978)
	LEFT JOIN zz_cx.loc_m rd ON (rd.fc_loc = fc_loc.fc_orig AND rd.loc_type = fbcx.loc2_type AND rd.loc = fbcx.loc2 AND rd.loc_t = fbcx.loc2_t978)

	WHERE fc.doc_nbr_prime > 0

	#----------------------- applying record 2 to fc -------------------------------------------
	-- loc testing is not implemented
	AND fc.fare_lockin_date between (r2t.rec_add_date - interval 1 day) AND if(r2t.rec_cnx_date = ''9999-12-31'', r2t.rec_cnx_date, r2t.rec_cnx_date + interval 1 day)
	AND fc.jrny_dep_date between r2t.tvl_eff_date AND r2t.tvl_dis_date
	
	#----------------------- cat 25 -------------------------------------------
	AND r3.pax_type = ctrl.pax_type
	AND if(r3.rslt_fare_tkt_cd <> '''', r3.rslt_fare_tkt_cd = ctrl.fbc_rule, if(r3.rslt_fare_cls <> '''', r3.rslt_fare_cls = ctrl.fbc_rule, true))

	#----------------------- table 994 date override -------------------------------------------
	AND if(dor.tbl_nbr is null, true, fc.trnsc_date between dor.tkt_fr_date AND dor.tkt_to_date)
	AND if(dor.tbl_nbr is null, true, fc.fc_dep_date between dor.tvl_fr_date AND dor.tvl_to_date)

	-- The location check must be done last, otherwise it takes up a lot of time
	-- !!! lesson: must use simple AND effective logics to reduce workload?
	AND (case r2s.dir_ind
			when ''1'' then true	# only 31 instance, not checking it for now, pass
			when ''2'' then true	# only 20 
			when ''3'' then fo.fc_loc is not null AND fd.fc_loc is not null
			when ''4'' then ro.fc_loc is not null AND rd.fc_loc is not null
			else # blank, no direction, then test either one is true
					(fo.fc_loc is not null AND fd.fc_loc is not null)
					or
					(ro.fc_loc is not null AND rd.fc_loc is not null)
		end);');
PREPARE stmt FROM @s; EXECUTE stmt; DEALLOCATE PREPARE stmt;

SET @s = CONCAT('ALTER TABLE zz_cx.fm_fbr4_r3_', t_id,'
	ADD INDEX idx_tmp_id(fc_all_id, base_tbl_t989);');
PREPARE stmt FROM @s; EXECUTE stmt; DEALLOCATE PREPARE stmt;

SET @s = CONCAT('DROP TABLE IF EXISTS zz_cx.fm_fbr4_t989_', t_id,';');
PREPARE stmt FROM @s; EXECUTE stmt; DEALLOCATE PREPARE stmt;

SET @s = CONCAT('CREATE TABLE zz_cx.fm_fbr4_t989_', t_id,'
	SELECT DISTINCT fc_all_id, base_tbl_t989
	FROM zz_cx.fm_fbr4_r3_', t_id,';');
PREPARE stmt FROM @s; EXECUTE stmt; DEALLOCATE PREPARE stmt;

SET @s = CONCAT('ALTER TABLE zz_cx.fm_fbr4_t989_', t_id,'
	ADD INDEX idx_fbr_t989_id(fc_all_id);');
PREPARE stmt FROM @s; EXECUTE stmt; DEALLOCATE PREPARE stmt;

SET @s = CONCAT('DROP TABLE IF EXISTS zz_cx.fm_fbr4_syn_', t_id,';');
PREPARE stmt FROM @s; EXECUTE stmt; DEALLOCATE PREPARE stmt;

SET @s = CONCAT('CREATE TABLE IF NOT EXISTS zz_cx.fm_fbr4_syn_', t_id,'
	SELECT DISTINCT fc.fc_all_id, r2_id as r2_rule_id, r3_id as r3_25_cat_id, f.fare_id as f_fare_id, t989.bf_pax_type, g16.frt_nbr, cr.fbcx_id
	FROM zz_cx.fm_fbr4_f_', t_id,' ctrl
	STRAIGHT_JOIN zz_cx.temp_tbl_fc_all fc ON (ctrl.fc_all_id = fc.fc_all_id)

	STRAIGHT_JOIN atpco_fare.atpco_fare f ON (f.fare_id = ctrl.fare_id)
	JOIN zz_cx.fm_fbr4_t989_', t_id,' ct ON (ctrl.fc_all_id = ct.fc_all_id)
	STRAIGHT_JOIN atpco_fare.atpco_t989_base_fare t989 ON (t989.tbl_nbr = ct.base_tbl_t989 AND t989.bf_appl <> ''N'')

	STRAIGHT_JOIN zz_cx.g16_temp g16 ON (g16.carr_cd = fc.fc_carr_cd AND (g16.frt_nbr = t989.bf_rule_tar or (t989.bf_rule_tar = ''000'' AND g16.pri_ind <> ''X'')))
	STRAIGHT_JOIN zz_cx.fm_fbr4_r3_', t_id,' cr ON (ct.fc_all_id = cr.fc_all_id AND ct.base_tbl_t989 = cr.base_tbl_t989)
	WHERE fc.doc_nbr_prime > 0

	#----------------------- matching t989 to the base fare -------------------------------------------
	AND (f.rule_nbr = t989.bf_rule_nbr or t989.bf_rule_nbr = '''')
	AND if(t989.bf_fc_len = 0, true, 
		if(t989.bf_fc_wld = 0, f.fare_cls = t989.bf_fc, 
		if(t989.bf_fc_wld = t989.bf_fc_len, LEFT(f.fare_cls, t989.bf_fc_len-1) = LEFT(t989.bf_fc, t989.bf_fc_len-1),
		if(t989.bf_fc_wld = 1, instr(f.fare_cls, right(t989.bf_fc, t989.bf_fc_len-1)),
		f.fare_cls regexp t989.bf_fc_regex
		))))
	AND (f.rtg_nbr = t989.bf_rtg_nbr or t989.bf_rtg_nbr = ''99999'')
	AND (f.ow_rt_ind = t989.bf_ow_rt or t989.bf_ow_rt = '''')
	-- bf_type, not yet implemented
	-- bf_ssn, not yet implemented
	-- bf_dow, not yet implemented
	-- ftnt, not yet implemented
	-- AND (fc.fc_pax_type = t989.c25_pax_type or t989.c25_pax_type in (''ADT'', ''JCB'', ''''))	-- JCB AND ADT are general
	;');
PREPARE stmt FROM @s; EXECUTE stmt; DEALLOCATE PREPARE stmt;

SET @s = CONCAT('DROP TABLE IF EXISTS zz_cx.fm_fbr4_loc_', t_id,';');
PREPARE stmt FROM @s; EXECUTE stmt; DEALLOCATE PREPARE stmt;


SET @s = CONCAT('CREATE TABLE IF NOT EXISTS zz_cx.fm_fbr4_loc_', t_id,'
	SELECT DISTINCT fc.doc_nbr_prime, fc.fc_cpn_nbr, ctrl.fc_all_id, ctrl.r2_rule_id as r2_25_rule_id, ctrl.r3_25_cat_id as r3_25_cat_id, f.fare_id, ''R'' as map_type, ''T'' as map_code
	FROM zz_cx.fm_fbr4_syn_', t_id,' ctrl

	STRAIGHT_JOIN zz_cx.temp_tbl_fc_all fc ON (ctrl.fc_all_id = fc.fc_all_id)
	JOIN atpco_fare.atpco_fare f ON (ctrl.f_fare_id = f.fare_id)
	STRAIGHT_JOIN zz_cx.temp_fbcx_fbr_r fbcx ON (fbcx.rcid = ctrl.fbcx_id)

	STRAIGHT_JOIN atpco_fare.atpco_r1_fare_cls r1 ON (r1.carr_cd = f.carr_cd AND r1.tar_nbr = ctrl.frt_nbr AND r1.rule_nbr = f.rule_nbr AND r1.fare_cls = f.fare_cls AND r1.proc_ind in (''N'', ''R''))
	STRAIGHT_JOIN atpco_fare.atpco_r1_fare_cls_state r1t ON (r1t.rule_id = r1.rule_id)
	STRAIGHT_JOIN atpco_fare.atpco_r1_fare_cls_sup r1s ON (r1s.rule_id = r1.rule_id)
	WHERE fc.doc_nbr_prime > 0
	#----------------------- conditions for fare record to r1 -------------------------------------------
	-- location testing is not implemented
	-- minimum sequence is not implemented
	AND fc.fare_lockin_date between date_add(r1t.rec_add_date, interval -1 day) AND if(r1t.rec_cnx_date = ''9999-12-31'', r1t.rec_cnx_date, date_add(r1t.rec_cnx_date, interval 1 day))	
	AND fc.fare_lockin_date between date_add(r1t.rec_add_date, interval -1 day) AND r1t.tvl_dis_date
	AND fc.jrny_dep_date between r1t.tvl_eff_date AND r1t.tvl_dis_date
	AND (f.ow_rt_ind = r1.ow_rt_ind or (f.ow_rt_ind = 3 AND r1.ow_rt_ind = 1))
	AND (f.rtg_nbr = r1.rtg_nbr or r1.rtg_nbr = ''99999'')
	AND if(r1.ftnt = '''', true, if(length(f.ftnt) = 2 AND (LEFT(f.ftnt,1) between ''A'' AND ''Z''), LEFT(f.ftnt,1) = r1.ftnt or right(f.ftnt,1) = r1.ftnt, f.ftnt = r1.ftnt))
	AND (r1s.pax_type = ctrl.bf_pax_type or (ctrl.bf_pax_type = ''ADT'' AND r1s.pax_type = ''''))

	#----------------------- check all conditions that come with datawarehouse.temp_fbcx_fbr -------------------------------------------
	AND fc.fare_lockin_date between date_add(fbcx.rec_add_date, interval -1 day) AND if(fbcx.rec_cnx_date = ''9999-12-31'', fbcx.rec_cnx_date, date_add(fbcx.rec_cnx_date, interval 1 day))	
	AND fc.fare_lockin_date between date_add(fbcx.rec_add_date, interval -1 day) AND fbcx.tvl_dis_date
	AND fc.jrny_dep_date between fbcx.tvl_eff_date AND fbcx.tvl_dis_date

	AND (fbcx.ow_rt_ind = '''' or f.ow_rt_ind = fbcx.ow_rt_ind or (f.ow_rt_ind = 3 AND fbcx.ow_rt_ind = 1))
	AND (r1.fare_type = fbcx.fare_type or fbcx.fare_type = '''')		-- Match fare type
	AND (r1.ssn_type = fbcx.ssn_type or fbcx.ssn_type = '''')			-- Match Season Type
	AND (r1.dow_type = fbcx.dow_type or fbcx.dow_type = '''')			-- Match Day of Week Type
	AND (f.rtg_nbr = fbcx.rtg_nbr or (f.rtg_nbr <> ''00000'' AND fbcx.rtg_nbr = ''88888'') or fbcx.rtg_nbr = ''99999'')

	-- r1.tkt_dsg_mod <> '''' is not implemented, no real cases
	-- AND fc.fc_tkt_dsg = if(fbcx.dsg_rule = '''', r1s.tkt_dsg, fbcx.dsg_rule)

	AND if(fbcx.dsg_rule <> '''', fc.fc_tkt_dsg = fbcx.dsg_rule, if(r1s.tkt_dsg <> '''', fc.fc_tkt_dsg = r1s.tkt_dsg, true))

	AND fc.fc_fbc = if(fbcx.fbc_rule = '''', if(r1s.tkt_cd = '''', f.fare_cls, r1s.tkt_cd),
						(case
							when LEFT(fbcx.fbc_rule,1) = ''*'' then concat(LEFT(f.fare_cls, 1), right(fbcx.fbc_rule, length(fbcx.fbc_rule)-1))
							when LEFT(fbcx.fbc_rule,1) = ''-'' then concat(f.fare_cls, right(fbcx.fbc_rule, length(fbcx.fbc_rule)-1))
							when right(fbcx.fbc_rule,1) = ''-'' then concat(LEFT(fbcx.fbc_rule, 1), right(f.fare_cls, length(f.fare_cls)-1))
							else fbcx.fbc_rule
						end)
					)
					
	AND if(fbcx.r2_fare_cls = '''', true, if(instr(fbcx.r2_fare_cls, ''-'') = 0, fbcx.r2_fare_cls = f.fare_cls, f.fare_cls regexp fbcx.r2_fare_cls_regex));');
PREPARE stmt FROM @s; EXECUTE stmt; DEALLOCATE PREPARE stmt;

# FBR5---------------------------------------------------------------------------------------------------------------------------------------------------------------------------------
# match with tour code only, part 4
# where one can only use the rule to find a match -- eg. F-

SET @s = CONCAT('DROP TABLE IF EXISTS zz_cx.fm_fbr5_loc_', t_id,';');
PREPARE stmt FROM @s; EXECUTE stmt; DEALLOCATE PREPARE stmt;

SET @s = CONCAT('CREATE TABLE IF NOT EXISTS zz_cx.fm_fbr5_loc_', t_id,' ENGINE = InnoDB
	SELECT DISTINCT fc.fc_all_id, r2.rule_id as r2_25_rule_id, r3.cat_id as r3_25_cat_id, f.fare_id,''R'' as map_type, ''T'' as map_code
	FROM zz_cx.temp_match_fbr_ctrl_', t_id,' ctrl

	STRAIGHT_JOIN zz_cx.temp_tbl_fc_all fc ON (ctrl.fc_all_id = fc.fc_all_id)
	STRAIGHT_JOIN tmp.temp_map_fbcx_fbr_r fbcm ON (fbcm.doc_nbr_prime = fc.doc_nbr_prime AND fbcm.fc_cpn_nbr = fc.fc_cpn_nbr AND fbcm.fbcx_mode = ''*'')

	STRAIGHT_JOIN atpco_fare.atpco_r8_fbr r8 ON (r8.tar_nbr = fbcm.frt_nbr AND r8.carr_cd = fbcm.carr_cd AND r8.rule_nbr = fbcm.rule_nbr AND r8.proc_ind in (''N'', ''R''))
	STRAIGHT_JOIN atpco_fare.atpco_r8_fbr_state r8t ON (r8t.rule_id = r8.rule_id)

	STRAIGHT_JOIN atpco_fare.atpco_r2_cat25_ctrl r2 ON (r2.carr_cd = r8.carr_cd AND r2.tar_nbr = r8.tar_nbr AND r2.rule_nbr = r8.rule_nbr AND r2.proc_ind in (''N'', ''R''))
	STRAIGHT_JOIN atpco_fare.atpco_r2_cat25_ctrl_state r2t ON (r2t.rule_id = r2.rule_id)
	STRAIGHT_JOIN atpco_fare.atpco_r2_cat25_ctrl_sup r2s ON (r2s.rule_id = r2.rule_id)

	STRAIGHT_JOIN atpco_fare.atpco_cat25 r3 ON (r3.cat_id = r2s.tbl_nbr)
	LEFT JOIN atpco_fare.atpco_t994_date dor ON (dor.tbl_nbr = r3.dt_ovrd_t994)
	STRAIGHT_JOIN atpco_fare.atpco_t989_base_fare t989 ON (t989.tbl_nbr = r3.base_tbl_t989 AND t989.bf_appl <> ''N'')

	STRAIGHT_JOIN zz_cx.g16_temp g16 ON (g16.carr_cd = fc.fc_carr_cd AND (g16.frt_nbr = t989.bf_rule_tar or (t989.bf_rule_tar = ''000'' AND g16.pri_ind <> ''X'')))

	STRAIGHT_JOIN atpco_fare.atpco_fare f ON (f.orig_city = fc.fc_orig AND f.dest_city = fc.fc_dest AND f.carr_cd = fc.fc_carr_cd AND f.tar_nbr = g16.ff_nbr)
	STRAIGHT_JOIN atpco_fare.atpco_fare_state ft ON f.fare_id = ft.fare_id

	STRAIGHT_JOIN atpco_fare.atpco_r1_fare_cls r1 ON (r1.carr_cd = f.carr_cd AND r1.tar_nbr = g16.frt_nbr AND r1.rule_nbr = f.rule_nbr AND r1.fare_cls = f.fare_cls AND r1.proc_ind in (''N'', ''R''))
	STRAIGHT_JOIN atpco_fare.atpco_r1_fare_cls_state r1t ON (r1t.rule_id = r1.rule_id)
	STRAIGHT_JOIN atpco_fare.atpco_r1_fare_cls_sup r1s ON (r1s.rule_id = r1.rule_id)

	STRAIGHT_JOIN zz_cx.temp_fbcx_fbr fbcx ON (fbcx.r8_rule_id = r8.rule_id AND fbcx.r2_25_rule_id = r2.rule_id AND fbcx.r3_25_cat_id = r3.cat_id)
	STRAIGHT_JOIN zz_cx.temp_tbl_fc_loc fc_loc ON (fc_loc.doc_nbr_prime = fc.doc_nbr_prime AND fc_loc.fc_cpn_nbr = fc.fc_cpn_nbr AND fc_loc.map_di_ind = fc.map_di_ind)		
	# to allow base fare matching AND r2 loaction testing using somewhat different orig/dest

	# forward direction location test
	LEFT JOIN zz_cx.loc_m fo ON (fo.fc_loc = fc_loc.fc_orig AND fo.loc_type = fbcx.loc1_type AND fo.loc = fbcx.loc1 AND fo.loc_t = fbcx.loc1_t978)
	LEFT JOIN zz_cx.loc_m fd ON (fd.fc_loc = fc_loc.fc_dest AND fd.loc_type = fbcx.loc2_type AND fd.loc = fbcx.loc2 AND fd.loc_t = fbcx.loc2_t978)

	# reverse direction location test
	LEFT JOIN zz_cx.loc_m ro ON (ro.fc_loc = fc_loc.fc_dest AND ro.loc_type = fbcx.loc1_type AND ro.loc = fbcx.loc1 AND ro.loc_t = fbcx.loc1_t978)
	LEFT JOIN zz_cx.loc_m rd ON (rd.fc_loc = fc_loc.fc_orig AND rd.loc_type = fbcx.loc2_type AND rd.loc = fbcx.loc2 AND rd.loc_t = fbcx.loc2_t978)
	WHERE fc.doc_nbr_prime > 0 AND ctrl.map_code <> ''2''
	-- fc.doc_nbr_prime = 5918564205 AND fc.fc_cpn_nbr = 1
	-- AND mod(fc.doc_nbr_prime, 1999) = 0
	-- AND fc.tkt_endorse_cd = ''''

	#----------------------- record 8 to the fare component -------------------------------------------
	-- loc testing is not implemented
	AND fc.fare_lockin_date between (r8t.rec_add_date - interval 1 day) AND if(r8t.rec_cnx_date = ''9999-12-31'', r8t.rec_cnx_date, r8t.rec_cnx_date + interval 1 day)
	AND fc.jrny_dep_date between r8t.tvl_eff_date AND r8t.tvl_dis_date

	#----------------------- cat 25 -------------------------------------------
	AND r3.pax_type = r8.pax_type
	AND if(r3.rslt_fare_tkt_cd <> '''', r3.rslt_fare_tkt_cd = fbcm.fbc_rule, if(r3.rslt_fare_cls <> '''', r3.rslt_fare_cls = fbcm.fbc_rule, true))

	#----------------------- table 994 date override -------------------------------------------
	AND if(dor.tbl_nbr is null, true, fc.trnsc_date between dor.tkt_fr_date AND dor.tkt_to_date)
	AND if(dor.tbl_nbr is null, true, fc.fc_dep_date between dor.tvl_fr_date AND dor.tvl_to_date)

	#----------------------- matching fc to the base fare -------------------------------------------
	AND fc.fare_lockin_date between (ft.rec_add_date - interval 1 day) AND if(ft.rec_cnx_date = ''9999-12-31'', ft.rec_cnx_date, ft.rec_cnx_date + interval 1 day)
	AND fc.jrny_dep_date between ft.tvl_eff_date AND ft.tvl_dis_date
	# not sure the logic is entirely right: AND if(fc.map_di_ind = ''F'', f.ftnt <> ''T'', f.ftnt <> ''F'')		-- useful for domestic, for international, ftnt.di is always ''F'', international footnote can never be ''F'' or ''T''

	#----------------------- applying record 2 to fc -------------------------------------------
	-- loc testing is not implemented
	AND fc.fare_lockin_date between (r2t.rec_add_date - interval 1 day) AND if(r2t.rec_cnx_date = ''9999-12-31'', r2t.rec_cnx_date, r2t.rec_cnx_date + interval 1 day)
	AND fc.jrny_dep_date between r2t.tvl_eff_date AND r2t.tvl_dis_date

	#----------------------- matching t989 to the base fare -------------------------------------------
	AND (t989.bf_rule_nbr = '''' or f.rule_nbr = t989.bf_rule_nbr)
	AND if(t989.bf_fc = '''', true, f.fare_cls regexp t989.bf_fc_regex)
	AND (t989.bf_rtg_nbr = ''99999'' or f.rtg_nbr = t989.bf_rtg_nbr)
	AND (t989.bf_ow_rt = '''' or f.ow_rt_ind = t989.bf_ow_rt)
	-- bf_type, not yet implemented
	-- bf_ssn, not yet implemented
	-- bf_dow, not yet implemented
	-- ftnt, not yet implemented
	-- AND (fc.fc_pax_type = t989.c25_pax_type or t989.c25_pax_type in (''ADT'', ''JCB'', ''''))	-- JCB AND ADT are general

	#----------------------- conditions for fare record to r1 -------------------------------------------
	-- location testing is not implemented
	-- minimum sequence is not implemented
	AND fc.fare_lockin_date between date_add(r1t.rec_add_date, interval -1 day) AND if(r1t.rec_cnx_date = ''9999-12-31'', r1t.rec_cnx_date, date_add(r1t.rec_cnx_date, interval 1 day))	
	AND fc.fare_lockin_date between date_add(r1t.rec_add_date, interval -1 day) AND r1t.tvl_dis_date
	AND fc.jrny_dep_date between r1t.tvl_eff_date AND r1t.tvl_dis_date
	AND (f.ow_rt_ind = r1.ow_rt_ind or (f.ow_rt_ind = 3 AND r1.ow_rt_ind = 1))
	AND (f.rtg_nbr = r1.rtg_nbr or r1.rtg_nbr = ''99999'')
	AND if(r1.ftnt = '''', true, if(length(f.ftnt) = 2 AND (LEFT(f.ftnt,1) between ''A'' AND ''Z''), LEFT(f.ftnt,1) = r1.ftnt or right(f.ftnt,1) = r1.ftnt, f.ftnt = r1.ftnt))
	AND (r1s.pax_type = t989.bf_pax_type or (t989.bf_pax_type = ''ADT'' AND r1s.pax_type = ''''))

	#----------------------- check all conditions that come with datawarehouse.temp_fbcx_fbr -------------------------------------------
	AND fc.fare_lockin_date between date_add(fbcx.rec_add_date, interval -1 day) AND if(fbcx.rec_cnx_date = ''9999-12-31'', fbcx.rec_cnx_date, date_add(fbcx.rec_cnx_date, interval 1 day))	
	AND fc.fare_lockin_date between date_add(fbcx.rec_add_date, interval -1 day) AND fbcx.tvl_dis_date
	AND fc.jrny_dep_date between fbcx.tvl_eff_date AND fbcx.tvl_dis_date

	AND (fbcx.ow_rt_ind = '''' or f.ow_rt_ind = fbcx.ow_rt_ind or (f.ow_rt_ind = 3 AND fbcx.ow_rt_ind = 1))
	AND (r1.fare_type = fbcx.fare_type or fbcx.fare_type = '''')		-- Match fare type
	AND (r1.ssn_type = fbcx.ssn_type or fbcx.ssn_type = '''')			-- Match Season Type
	AND (r1.dow_type = fbcx.dow_type or fbcx.dow_type = '''')			-- Match Day of Week Type
	AND (f.rtg_nbr = fbcx.rtg_nbr or (f.rtg_nbr <> ''00000'' AND fbcx.rtg_nbr = ''88888'') or fbcx.rtg_nbr = ''99999'')

	-- r1.tkt_dsg_mod <> '''' is not implemented, no real cases
	-- AND fc.fc_tkt_dsg = if(fbcx.dsg_rule = '''', r1s.tkt_dsg, fbcx.dsg_rule)
	AND if(fbcx.dsg_rule <> '''', fc.fc_tkt_dsg = fbcx.dsg_rule, if(r1s.tkt_dsg <> '''', fc.fc_tkt_dsg = r1s.tkt_dsg, true))

	AND fc.fc_fbc = if(fbcx.fbc_rule = '''', if(r1s.tkt_cd = '''', f.fare_cls, r1s.tkt_cd),
						(case
							when LEFT(fbcx.fbc_rule,1) = ''*'' then concat(LEFT(f.fare_cls, 1), right(fbcx.fbc_rule, length(fbcx.fbc_rule)-1))
							when LEFT(fbcx.fbc_rule,1) = ''-'' then concat(f.fare_cls, right(fbcx.fbc_rule, length(fbcx.fbc_rule)-1))
							when right(fbcx.fbc_rule,1) = ''-'' then concat(LEFT(fbcx.fbc_rule, 1), right(f.fare_cls, length(f.fare_cls)-1))
							else fbcx.fbc_rule
						end)
					)

	-- The location check must be done last, otherwise it takes up a lot of time
	-- !!! lesson: must use simple AND effective logics to reduce workload?
	AND (case r2s.dir_ind
			when ''1'' then true	# only 31 instance, not checking it for now, pass
			when ''2'' then true	# only 20 
			when ''3'' then fo.fc_loc is not null AND fd.fc_loc is not null
			when ''4'' then ro.fc_loc is not null AND rd.fc_loc is not null
			else # blank, no direction, then test either one is true
					(fo.fc_loc is not null AND fd.fc_loc is not null)
					or
					(ro.fc_loc is not null AND rd.fc_loc is not null)
		end)

	AND if(fbcx.r2_fare_cls = '''', true, if(instr(fbcx.r2_fare_cls, ''-'') = 0, fbcx.r2_fare_cls = f.fare_cls, f.fare_cls regexp fbcx.r2_fare_cls_regex));');
PREPARE stmt FROM @s; EXECUTE stmt; DEALLOCATE PREPARE stmt;

