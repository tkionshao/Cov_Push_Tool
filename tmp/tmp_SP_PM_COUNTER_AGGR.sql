CREATE DEFINER=`covmo`@`%` PROCEDURE `SP_PM_COUNTER_AGGR`(GT_DB VARCHAR(100), GT_NT VARCHAR(100), TECH_MASK TINYINT(4))
BEGIN
	DECLARE VENDOR_ID INT(11); 
	DECLARE DATA_HR VARCHAR(2) DEFAULT LEFT(gt_strtok(GT_DB,4,'_'),2);
	DECLARE table_pm_pdf_ericsson_sum_str VARCHAR(50000) DEFAULT
		'
		SUM(IFNULL(Column0,0)),
		SUM(IFNULL(Column1,0)),
		SUM(IFNULL(Column2,0)),
		SUM(IFNULL(Column3,0)),
		SUM(IFNULL(Column4,0)),
		SUM(IFNULL(Column5,0)),
		SUM(IFNULL(Column6,0)),
		SUM(IFNULL(Column7,0)),
		SUM(IFNULL(Column8,0)),
		SUM(IFNULL(Column9,0)),
		SUM(IFNULL(Column10,0)),
		SUM(IFNULL(Column11,0)),
		SUM(IFNULL(Column12,0)),
		SUM(IFNULL(Column13,0)),
		SUM(IFNULL(Column14,0)),
		SUM(IFNULL(Column15,0)),
		SUM(IFNULL(Column16,0)),
		SUM(IFNULL(Column17,0)),
		SUM(IFNULL(Column18,0)),
		SUM(IFNULL(Column19,0)),
		SUM(IFNULL(Column20,0)),
		SUM(IFNULL(Column21,0)),
		SUM(IFNULL(Column22,0)),
		SUM(IFNULL(Column23,0)),
		SUM(IFNULL(Column24,0)),
		SUM(IFNULL(Column25,0)),
		SUM(IFNULL(Column26,0)),
		SUM(IFNULL(Column27,0)),
		SUM(IFNULL(Column28,0)),
		SUM(IFNULL(Column29,0)),
		SUM(IFNULL(Column30,0)),
		SUM(IFNULL(Column31,0)),
		SUM(IFNULL(Column32,0)),
		SUM(IFNULL(Column33,0)),
		SUM(IFNULL(Column34,0)),
		SUM(IFNULL(Column35,0)),
		SUM(IFNULL(Column36,0)),
		SUM(IFNULL(Column37,0)),
		SUM(IFNULL(Column38,0)),
		SUM(IFNULL(Column39,0)),
		SUM(IFNULL(Column40,0)),
		SUM(IFNULL(Column41,0)),
		SUM(IFNULL(Column42,0)),
		SUM(IFNULL(Column43,0)),
		SUM(IFNULL(Column44,0)),
		SUM(IFNULL(Column45,0)),
		SUM(IFNULL(Column46,0)),
		SUM(IFNULL(Column47,0)),
		SUM(IFNULL(Column48,0)),
		SUM(IFNULL(Column49,0)),
		SUM(IFNULL(Column50,0)),
		SUM(IFNULL(Column51,0)),
		SUM(IFNULL(Column52,0)),
		SUM(IFNULL(Column53,0)),
		SUM(IFNULL(Column54,0)),
		SUM(IFNULL(Column55,0)),
		SUM(IFNULL(Column56,0)),
		SUM(IFNULL(Column57,0)),
		SUM(IFNULL(Column58,0)),
		SUM(IFNULL(Column59,0)),
		SUM(IFNULL(Column60,0)),
		SUM(IFNULL(Column61,0)),
		SUM(IFNULL(Column62,0)),
		SUM(IFNULL(Column63,0)),
		SUM(IFNULL(Column64,0)),
		SUM(IFNULL(Column65,0)),
		SUM(IFNULL(Column66,0)),
		SUM(IFNULL(Column67,0)),
		SUM(IFNULL(Column68,0)),
		SUM(IFNULL(Column69,0)),
		SUM(IFNULL(Column70,0)),
		SUM(IFNULL(Column71,0)),
		SUM(IFNULL(Column72,0)),
		SUM(IFNULL(Column73,0)),
		SUM(IFNULL(Column74,0)),
		SUM(IFNULL(Column75,0)),
		SUM(IFNULL(Column76,0)),
		SUM(IFNULL(Column77,0)),
		SUM(IFNULL(Column78,0)),
		SUM(IFNULL(Column79,0)),
		SUM(IFNULL(Column80,0)),
		SUM(IFNULL(Column81,0)),
		SUM(IFNULL(Column82,0)),
		SUM(IFNULL(Column83,0)),
		SUM(IFNULL(Column84,0)),
		SUM(IFNULL(Column85,0)),
		SUM(IFNULL(Column86,0)),
		SUM(IFNULL(Column87,0)),
		SUM(IFNULL(Column88,0)),
		SUM(IFNULL(Column89,0)),
		SUM(IFNULL(Column90,0)),
		SUM(IFNULL(Column91,0)),
		SUM(IFNULL(Column92,0)),
		SUM(IFNULL(Column93,0)),
		SUM(IFNULL(Column94,0)),
		SUM(IFNULL(Column95,0)),
		SUM(IFNULL(Column96,0)),
		SUM(IFNULL(Column97,0)),
		SUM(IFNULL(Column98,0)),
		SUM(IFNULL(Column99,0)),
		SUM(IFNULL(Column100,0)),
		SUM(IFNULL(Column101,0)),
		SUM(IFNULL(Column102,0)),
		SUM(IFNULL(Column103,0)),
		SUM(IFNULL(Column104,0)),
		SUM(IFNULL(Column105,0)),
		SUM(IFNULL(Column106,0)),
		SUM(IFNULL(Column107,0)),
		SUM(IFNULL(Column108,0)),
		SUM(IFNULL(Column109,0)),
		SUM(IFNULL(Column110,0)),
		SUM(IFNULL(Column111,0)),
		SUM(IFNULL(Column112,0)),
		SUM(IFNULL(Column113,0)),
		SUM(IFNULL(Column114,0)),
		SUM(IFNULL(Column115,0)),
		SUM(IFNULL(Column116,0)),
		SUM(IFNULL(Column117,0)),
		SUM(IFNULL(Column118,0)),
		SUM(IFNULL(Column119,0)),
		SUM(IFNULL(Column120,0)),
		SUM(IFNULL(Column121,0)),
		SUM(IFNULL(Column122,0)),
		SUM(IFNULL(Column123,0)),
		SUM(IFNULL(Column124,0)),
		SUM(IFNULL(Column125,0)),
		SUM(IFNULL(Column126,0)),
		SUM(IFNULL(Column127,0)),
		SUM(IFNULL(Column128,0)),
		SUM(IFNULL(Column129,0)),
		SUM(IFNULL(Column130,0)),
		SUM(IFNULL(Column131,0)),
		SUM(IFNULL(Column132,0)),
		SUM(IFNULL(Column133,0)),
		SUM(IFNULL(Column134,0)),
		SUM(IFNULL(Column135,0)),
		SUM(IFNULL(Column136,0)),
		SUM(IFNULL(Column137,0)),
		SUM(IFNULL(Column138,0)),
		SUM(IFNULL(Column139,0)),
		SUM(IFNULL(Column140,0)),
		SUM(IFNULL(Column141,0)),
		SUM(IFNULL(Column142,0)),
		SUM(IFNULL(Column143,0)),
		SUM(IFNULL(Column144,0)),
		SUM(IFNULL(Column145,0)),
		SUM(IFNULL(Column146,0)),
		SUM(IFNULL(Column147,0)),
		SUM(IFNULL(Column148,0)),
		SUM(IFNULL(Column149,0)),
		SUM(IFNULL(Column150,0)),
		SUM(IFNULL(Column151,0)),
		SUM(IFNULL(Column152,0)),
		SUM(IFNULL(Column153,0)),
		SUM(IFNULL(Column154,0)),
		SUM(IFNULL(Column155,0)),
		SUM(IFNULL(Column156,0)),
		SUM(IFNULL(Column157,0)),
		SUM(IFNULL(Column158,0)),
		SUM(IFNULL(Column159,0)),
		SUM(IFNULL(Column160,0)),
		SUM(IFNULL(Column161,0)),
		SUM(IFNULL(Column162,0)),
		SUM(IFNULL(Column163,0)),
		SUM(IFNULL(Column164,0)),
		SUM(IFNULL(Column165,0)),
		SUM(IFNULL(Column166,0)),
		SUM(IFNULL(Column167,0)),
		SUM(IFNULL(Column168,0)),
		SUM(IFNULL(Column169,0)),
		SUM(IFNULL(Column170,0)),
		SUM(IFNULL(Column171,0)),
		SUM(IFNULL(Column172,0)),
		SUM(IFNULL(Column173,0)),
		SUM(IFNULL(Column174,0)),
		SUM(IFNULL(Column175,0)),
		SUM(IFNULL(Column176,0)),
		SUM(IFNULL(Column177,0)),
		SUM(IFNULL(Column178,0)),
		SUM(IFNULL(Column179,0)),
		SUM(IFNULL(Column180,0)),
		SUM(IFNULL(Column181,0)),
		SUM(IFNULL(Column182,0)),
		SUM(IFNULL(Column183,0)),
		SUM(IFNULL(Column184,0)),
		SUM(IFNULL(Column185,0)),
		SUM(IFNULL(Column186,0)),
		SUM(IFNULL(Column187,0)),
		SUM(IFNULL(Column188,0)),
		SUM(IFNULL(Column189,0)),
		SUM(IFNULL(Column190,0)),
		SUM(IFNULL(Column191,0)),
		SUM(IFNULL(Column192,0)),
		SUM(IFNULL(Column193,0)),
		SUM(IFNULL(Column194,0)),
		SUM(IFNULL(Column195,0)),
		SUM(IFNULL(Column196,0)),
		SUM(IFNULL(Column197,0)),
		SUM(IFNULL(Column198,0)),
		SUM(IFNULL(Column199,0)),
		SUM(IFNULL(Column200,0)),
		SUM(IFNULL(Column201,0)),
		SUM(IFNULL(Column202,0)),
		SUM(IFNULL(Column203,0)),
		SUM(IFNULL(Column204,0)),
		SUM(IFNULL(Column205,0)),
		SUM(IFNULL(Column206,0)),
		SUM(IFNULL(Column207,0)),
		SUM(IFNULL(Column208,0)),
		SUM(IFNULL(Column209,0)),
		SUM(IFNULL(Column210,0)),
		SUM(IFNULL(Column211,0)),
		SUM(IFNULL(Column212,0)),
		SUM(IFNULL(Column213,0)),
		SUM(IFNULL(Column214,0)),
		SUM(IFNULL(Column215,0)),
		SUM(IFNULL(Column216,0)),
		SUM(IFNULL(Column217,0)),
		SUM(IFNULL(Column218,0)),
		SUM(IFNULL(Column219,0)),
		SUM(IFNULL(Column220,0)),
		SUM(IFNULL(Column221,0)),
		SUM(IFNULL(Column222,0)),
		SUM(IFNULL(Column223,0)),
		SUM(IFNULL(Column224,0)),
		SUM(IFNULL(Column225,0)),
		SUM(IFNULL(Column226,0)),
		SUM(IFNULL(Column227,0)),
		SUM(IFNULL(Column228,0)),
		SUM(IFNULL(Column229,0)),
		SUM(IFNULL(Column230,0)),
		SUM(IFNULL(Column231,0)),
		SUM(IFNULL(Column232,0)),
		SUM(IFNULL(Column233,0)),
		SUM(IFNULL(Column234,0)),
		SUM(IFNULL(Column235,0)),
		SUM(IFNULL(Column236,0)),
		SUM(IFNULL(Column237,0)),
		SUM(IFNULL(Column238,0)),
		SUM(IFNULL(Column239,0)),
		SUM(IFNULL(Column240,0)),
		SUM(IFNULL(Column241,0)),
		SUM(IFNULL(Column242,0)),
		SUM(IFNULL(Column243,0)),
		SUM(IFNULL(Column244,0)),
		SUM(IFNULL(Column245,0)),
		SUM(IFNULL(Column246,0)),
		SUM(IFNULL(Column247,0)),
		SUM(IFNULL(Column248,0)),
		SUM(IFNULL(Column249,0)),
		SUM(IFNULL(Column250,0)),
		SUM(IFNULL(Column251,0)),
		SUM(IFNULL(Column252,0)),
		SUM(IFNULL(Column253,0)),
		SUM(IFNULL(Column254,0)),
		SUM(IFNULL(Column255,0)),
		SUM(IFNULL(Column256,0)),
		SUM(IFNULL(Column257,0)),
		SUM(IFNULL(Column258,0)),
		SUM(IFNULL(Column259,0)),
		SUM(IFNULL(Column260,0)),
		SUM(IFNULL(Column261,0)),
		SUM(IFNULL(Column262,0)),
		SUM(IFNULL(Column263,0)),
		SUM(IFNULL(Column264,0)),
		SUM(IFNULL(Column265,0)),
		SUM(IFNULL(Column266,0)),
		SUM(IFNULL(Column267,0)),
		SUM(IFNULL(Column268,0)),
		SUM(IFNULL(Column269,0)),
		SUM(IFNULL(Column270,0)),
		SUM(IFNULL(Column271,0)),
		SUM(IFNULL(Column272,0)),
		SUM(IFNULL(Column273,0)),
		SUM(IFNULL(Column274,0)),
		SUM(IFNULL(Column275,0)),
		SUM(IFNULL(Column276,0)),
		SUM(IFNULL(Column277,0)),
		SUM(IFNULL(Column278,0)),
		SUM(IFNULL(Column279,0)),
		SUM(IFNULL(Column280,0)),
		SUM(IFNULL(Column281,0)),
		SUM(IFNULL(Column282,0)),
		SUM(IFNULL(Column283,0)),
		SUM(IFNULL(Column284,0)),
		SUM(IFNULL(Column285,0)),
		SUM(IFNULL(Column286,0)),
		SUM(IFNULL(Column287,0)),
		SUM(IFNULL(Column288,0)),
		SUM(IFNULL(Column289,0)),
		SUM(IFNULL(Column290,0)),
		SUM(IFNULL(Column291,0)),
		SUM(IFNULL(Column292,0)),
		SUM(IFNULL(Column293,0)),
		SUM(IFNULL(Column294,0)),
		SUM(IFNULL(Column295,0)),
		SUM(IFNULL(Column296,0)),
		SUM(IFNULL(Column297,0)),
		SUM(IFNULL(Column298,0)),
		SUM(IFNULL(Column299,0)),
		SUM(IFNULL(Column300,0)),
		SUM(IFNULL(Column301,0)),
		SUM(IFNULL(Column302,0)),
		SUM(IFNULL(Column303,0)),
		SUM(IFNULL(Column304,0)),
		SUM(IFNULL(Column305,0)),
		SUM(IFNULL(Column306,0)),
		SUM(IFNULL(Column307,0)),
		SUM(IFNULL(Column308,0)),
		SUM(IFNULL(Column309,0)),
		SUM(IFNULL(Column310,0)),
		SUM(IFNULL(Column311,0)),
		SUM(IFNULL(Column312,0)),
		SUM(IFNULL(Column313,0)),
		SUM(IFNULL(Column314,0)),
		SUM(IFNULL(Column315,0)),
		SUM(IFNULL(Column316,0)),
		SUM(IFNULL(Column317,0)),
		SUM(IFNULL(Column318,0)),
		SUM(IFNULL(Column319,0)),
		SUM(IFNULL(Column320,0)),
		SUM(IFNULL(Column321,0)),
		SUM(IFNULL(Column322,0)),
		SUM(IFNULL(Column323,0)),
		SUM(IFNULL(Column324,0)),
		SUM(IFNULL(Column325,0)),
		SUM(IFNULL(Column326,0)),
		SUM(IFNULL(Column327,0)),
		SUM(IFNULL(Column328,0)),
		SUM(IFNULL(Column329,0)),
		SUM(IFNULL(Column330,0)),
		SUM(IFNULL(Column331,0)),
		SUM(IFNULL(Column332,0)),
		SUM(IFNULL(Column333,0)),
		SUM(IFNULL(Column334,0)),
		SUM(IFNULL(Column335,0)),
		SUM(IFNULL(Column336,0)),
		SUM(IFNULL(Column337,0)),
		SUM(IFNULL(Column338,0)),
		SUM(IFNULL(Column339,0)),
		SUM(IFNULL(Column340,0)),
		SUM(IFNULL(Column341,0)),
		SUM(IFNULL(Column342,0)),
		SUM(IFNULL(Column343,0)),
		SUM(IFNULL(Column344,0)),
		SUM(IFNULL(Column345,0)),
		SUM(IFNULL(Column346,0)),
		SUM(IFNULL(Column347,0)),
		SUM(IFNULL(Column348,0)),
		SUM(IFNULL(Column349,0)),
		SUM(IFNULL(Column350,0)),
		SUM(IFNULL(Column351,0)),
		SUM(IFNULL(Column352,0)),
		SUM(IFNULL(Column353,0)),
		SUM(IFNULL(Column354,0)),
		SUM(IFNULL(Column355,0)),
		SUM(IFNULL(Column356,0)),
		SUM(IFNULL(Column357,0)),
		SUM(IFNULL(Column358,0)),
		SUM(IFNULL(Column359,0)),
		SUM(IFNULL(Column360,0)),
		SUM(IFNULL(Column361,0)),
		SUM(IFNULL(Column362,0)),
		SUM(IFNULL(Column363,0)),
		SUM(IFNULL(Column364,0)),
		SUM(IFNULL(Column365,0)),
		SUM(IFNULL(Column366,0)),
		SUM(IFNULL(Column367,0)),
		SUM(IFNULL(Column368,0)),
		SUM(IFNULL(Column369,0)),
		SUM(IFNULL(Column370,0)),
		SUM(IFNULL(Column371,0)),
		SUM(IFNULL(Column372,0)),
		SUM(IFNULL(Column373,0)),
		SUM(IFNULL(Column374,0)),
		SUM(IFNULL(Column375,0)),
		SUM(IFNULL(Column376,0)),
		SUM(IFNULL(Column377,0)),
		SUM(IFNULL(Column378,0)),
		SUM(IFNULL(Column379,0)),
		SUM(IFNULL(Column380,0)),
		SUM(IFNULL(Column381,0)),
		SUM(IFNULL(Column382,0)),
		SUM(IFNULL(Column383,0)),
		SUM(IFNULL(Column384,0)),
		SUM(IFNULL(Column385,0)),
		SUM(IFNULL(Column386,0)),
		SUM(IFNULL(Column387,0)),
		SUM(IFNULL(Column388,0)),
		SUM(IFNULL(Column389,0)),
		SUM(IFNULL(Column390,0)),
		SUM(IFNULL(Column391,0)),
		SUM(IFNULL(Column392,0)),
		SUM(IFNULL(Column393,0)),
		SUM(IFNULL(Column394,0)),
		SUM(IFNULL(Column395,0)),
		SUM(IFNULL(Column396,0)),
		SUM(IFNULL(Column397,0)),
		SUM(IFNULL(Column398,0)),
		SUM(IFNULL(Column399,0)),
		SUM(IFNULL(Column400,0)),
		SUM(IFNULL(Column401,0)),
		SUM(IFNULL(Column402,0)),
		SUM(IFNULL(Column403,0)),
		SUM(IFNULL(Column404,0)),
		SUM(IFNULL(Column405,0)),
		SUM(IFNULL(Column406,0)),
		SUM(IFNULL(Column407,0)),
		SUM(IFNULL(Column408,0)),
		SUM(IFNULL(Column409,0)),
		SUM(IFNULL(Column410,0)),
		SUM(IFNULL(Column411,0)),
		SUM(IFNULL(Column412,0)),
		SUM(IFNULL(Column413,0)),
		SUM(IFNULL(Column414,0)),
		SUM(IFNULL(Column415,0)),
		SUM(IFNULL(Column416,0)),
		SUM(IFNULL(Column417,0)),
		SUM(IFNULL(Column418,0)),
		SUM(IFNULL(Column419,0)),
		SUM(IFNULL(Column420,0)),
		SUM(IFNULL(Column421,0)),
		SUM(IFNULL(Column422,0)),
		SUM(IFNULL(Column423,0)),
		SUM(IFNULL(Column424,0)),
		SUM(IFNULL(Column425,0)),
		SUM(IFNULL(Column426,0)),
		SUM(IFNULL(Column427,0)),
		SUM(IFNULL(Column428,0)),
		SUM(IFNULL(Column429,0)),
		SUM(IFNULL(Column430,0)),
		SUM(IFNULL(Column431,0)),
		SUM(IFNULL(Column432,0)),
		SUM(IFNULL(Column433,0)),
		SUM(IFNULL(Column434,0)),
		SUM(IFNULL(Column435,0)),
		SUM(IFNULL(Column436,0)),
		SUM(IFNULL(Column437,0)),
		SUM(IFNULL(Column438,0)),
		SUM(IFNULL(Column439,0)),
		SUM(IFNULL(Column440,0)),
		SUM(IFNULL(Column441,0)),
		SUM(IFNULL(Column442,0)),
		SUM(IFNULL(Column443,0)),
		SUM(IFNULL(Column444,0)),
		SUM(IFNULL(Column445,0)),
		SUM(IFNULL(Column446,0)),
		SUM(IFNULL(Column447,0)),
		SUM(IFNULL(Column448,0)),
		SUM(IFNULL(Column449,0)),
		SUM(IFNULL(Column450,0)),
		SUM(IFNULL(Column451,0)),
		SUM(IFNULL(Column452,0)),
		SUM(IFNULL(Column453,0)),
		SUM(IFNULL(Column454,0)),
		SUM(IFNULL(Column455,0)),
		SUM(IFNULL(Column456,0)),
		SUM(IFNULL(Column457,0)),
		SUM(IFNULL(Column458,0)),
		SUM(IFNULL(Column459,0)),
		SUM(IFNULL(Column460,0)),
		SUM(IFNULL(Column461,0)),
		SUM(IFNULL(Column462,0)),
		SUM(IFNULL(Column463,0)),
		SUM(IFNULL(Column464,0)),
		SUM(IFNULL(Column465,0)),
		SUM(IFNULL(Column466,0)),
		SUM(IFNULL(Column467,0)),
		SUM(IFNULL(Column468,0)),
		SUM(IFNULL(Column469,0)),
		SUM(IFNULL(Column470,0)),
		SUM(IFNULL(Column471,0)),
		SUM(IFNULL(Column472,0)),
		SUM(IFNULL(Column473,0)),
		SUM(IFNULL(Column474,0)),
		SUM(IFNULL(Column475,0)),
		SUM(IFNULL(Column476,0)),
		SUM(IFNULL(Column477,0)),
		SUM(IFNULL(Column478,0)),
		SUM(IFNULL(Column479,0)),
		SUM(IFNULL(Column480,0)),
		SUM(IFNULL(Column481,0)),
		SUM(IFNULL(Column482,0)),
		SUM(IFNULL(Column483,0)),
		SUM(IFNULL(Column484,0)),
		SUM(IFNULL(Column485,0)),
		SUM(IFNULL(Column486,0)),
		SUM(IFNULL(Column487,0)),
		SUM(IFNULL(Column488,0)),
		SUM(IFNULL(Column489,0)),
		SUM(IFNULL(Column490,0)),
		SUM(IFNULL(Column491,0)),
		SUM(IFNULL(Column492,0)),
		SUM(IFNULL(Column493,0)),
		SUM(IFNULL(Column494,0)),
		SUM(IFNULL(Column495,0)),
		SUM(IFNULL(Column496,0)),
		SUM(IFNULL(Column497,0)),
		SUM(IFNULL(Column498,0)),
		SUM(IFNULL(Column499,0)),
		SUM(IFNULL(Column500,0)),
		SUM(IFNULL(Column501,0)),
		SUM(IFNULL(Column502,0)),
		SUM(IFNULL(Column503,0)),
		SUM(IFNULL(Column504,0)),
		SUM(IFNULL(Column505,0)),
		SUM(IFNULL(Column506,0)),
		SUM(IFNULL(Column507,0)),
		SUM(IFNULL(Column508,0)),
		SUM(IFNULL(Column509,0)),
		SUM(IFNULL(Column510,0)),
		SUM(IFNULL(Column511,0))
		';
	
	SET SESSION group_concat_max_len=@@max_allowed_packet;
	SELECT (VALUE*1) INTO VENDOR_ID FROM gt_gw_main.integration_param WHERE gt_group = 'sp' AND gt_name = 'VENDOR_ID' ;
	SELECT REPLACE(GT_DB,RIGHT(GT_DB,9),'0000_0000') INTO GT_DB;
	
	IF TECH_MASK IN (0,2) THEN
		IF (VENDOR_ID & 1)>0 THEN
			SET @SqlCmd = CONCAT('TRUNCATE TABLE ',GT_DB,'.table_pm_ericsson_umts_aggr_',DATA_HR,';');
			PREPARE Stmt FROM @SqlCmd;
			EXECUTE Stmt;
			DEALLOCATE PREPARE Stmt;
			
			SET @SqlCmd=CONCAT('SELECT GROUP_CONCAT(IF( counter_type LIKE ''%peg%'',CONCAT(''SUM(IFNULL('',PM_COUNTER_NAME,'',0)) AS '',PM_COUNTER_NAME),PM_COUNTER_NAME)SEPARATOR '','') INTO @all_column_eri_umts_sum
						FROM ',GT_NT,'.dim_pm_ericsson_umts;');
			PREPARE Stmt FROM @SqlCmd;
			EXECUTE Stmt;
			DEALLOCATE PREPARE Stmt;	
			
			SET @SqlCmd = CONCAT('INSERT INTO ',GT_DB,'.table_pm_ericsson_umts_aggr_',DATA_HR,' SELECT ',@all_column_eri_umts_sum,' FROM ',GT_DB,'.table_pm_ericsson_umts WHERE data_hour=',DATA_HR,' GROUP BY DATA_DATE,DATA_HOUR,RNC_NAME,CELL_ID;');
			PREPARE Stmt FROM @SqlCmd;
			EXECUTE Stmt;
			DEALLOCATE PREPARE Stmt;
			
			SET @SqlCmd = CONCAT( 'UPDATE ',GT_DB,'.table_pm_ericsson_umts_aggr_',DATA_HR,' a 
						INNER JOIN ',GT_NT,'.nt_current b ON b.cell_name LIKE CONCAT(''%'',a.cell_id,''%'') 
						SET a.cell_name = b.cell_name, a.cell_id=b.cell_id; ');
			PREPARE Stmt FROM @SqlCmd;
			EXECUTE Stmt;
			DEALLOCATE PREPARE Stmt; 
			SET @SqlCmd = CONCAT( 'UPDATE ',GT_DB,'.table_pm_ericsson_umts_aggr_',DATA_HR,' a 
						INNER JOIN ',GT_NT,'.nt_rnc_current b ON a.rnc_name = b.rnc_name
						SET a.rnc_id = b.rnc_id;');
			PREPARE Stmt FROM @SqlCmd;
			EXECUTE Stmt;
			DEALLOCATE PREPARE Stmt; 
			SET @SqlCmd = CONCAT('TRUNCATE TABLE ',GT_DB,'.table_pm_pdf_ericsson_umts_aggr_',DATA_HR,';');
			PREPARE Stmt FROM @SqlCmd;
			EXECUTE Stmt;
			DEALLOCATE PREPARE Stmt;
			SET @SqlCmd = CONCAT('INSERT INTO ',GT_DB,'.table_pm_pdf_ericsson_umts_aggr_',DATA_HR,' SELECT 
						data_date, data_hour, rnc_id, rnc_name, cell_id, cell_name, pdf_index,
						',table_pm_pdf_ericsson_sum_str,' FROM ',GT_DB,'.table_pm_pdf_ericsson_umts WHERE data_hour=',DATA_HR,' GROUP BY DATA_DATE,DATA_HOUR,cell_id,rnc_id;');
			PREPARE Stmt FROM @SqlCmd;
			EXECUTE Stmt;
			DEALLOCATE PREPARE Stmt;
			SET @SqlCmd = CONCAT( 'UPDATE ',GT_DB,'.table_pm_pdf_ericsson_umts_aggr_',DATA_HR,' a 
						INNER JOIN ',GT_NT,'.nt_current b 
						ON a.cell_id = b.cell_id
						SET a.cell_name = b.cell_name;');
			PREPARE Stmt FROM @SqlCmd;
			EXECUTE Stmt;
			DEALLOCATE PREPARE Stmt;  
			SET @SqlCmd = CONCAT( 'UPDATE ',GT_DB,'.table_pm_pdf_ericsson_umts_aggr_',DATA_HR,' a 
						INNER JOIN ',GT_NT,'.nt_rnc_current b ON a.rnc_name = b.rnc_name
						SET a.rnc_id = b.rnc_id;');
			PREPARE Stmt FROM @SqlCmd;
			EXECUTE Stmt;
			DEALLOCATE PREPARE Stmt;
		END IF;
	
		IF (VENDOR_ID & 2)>0 THEN
			SET @SqlCmd = CONCAT('INSERT INTO ',GT_DB,'.table_pm_nokia_umts_aggr_',DATA_HR,' SELECT DATA_DATE');
			SET @i = 2;
			SELECT COUNT(*) INTO @total_count FROM GT_NT.dim_pm_nokia_umts;
			WHILE @i<=@total_count DO
			BEGIN
				SELECT COUNTER_TYPE, PM_COUNTER_NAME
				INTO @counter_type, @column_name 
				FROM GT_NT.dim_pm_nokia_umts A WHERE A.INDEX=@i;
				IF @counter_type LIKE '%peg%' THEN 
					SET @SqlCmd = CONCAT(@SqlCmd, ',SUM(', @column_name, ') AS ',@column_name); 
				ELSE 
					SET @SqlCmd = CONCAT(@SqlCmd, ',', @column_name); 
				END IF;  
				SET @i = @i+1;
			END;
			END WHILE;
			
			SET @SqlCmd = CONCAT(@SqlCmd, ' FROM ',GT_DB,'.table_pm_nokia_umts WHERE data_hour=',DATA_HR,' GROUP BY DATA_DATE,DATA_HOUR,RNC_NAME,CELL_ID;');
			PREPARE Stmt FROM @SqlCmd;
			EXECUTE Stmt;
			DEALLOCATE PREPARE Stmt;
		END IF;
	
		IF (VENDOR_ID & 4)>0 THEN
			SET @SqlCmd = CONCAT('INSERT INTO ',GT_DB,'.table_pm_huawei_umts_aggr_',DATA_HR,' SELECT DATA_DATE');
			SET @i = 2;
			SELECT COUNT(*) INTO @total_count FROM GT_NT.dim_pm_huawei_umts;
			WHILE @i<=@total_count DO
			BEGIN
				SELECT COUNTER_TYPE, PM_COUNTER_NAME
				INTO @counter_type, @column_name 
				FROM GT_NT.dim_pm_nokia_umts A WHERE A.INDEX=@i;				
				IF @counter_type LIKE '%peg%' THEN 
					SET @SqlCmd = CONCAT(@SqlCmd, ',SUM(', @column_name, ') AS ',@column_name); 
				ELSE 
					SET @SqlCmd = CONCAT(@SqlCmd, ',', @column_name); 
				END IF;  
				SET @i = @i+1;
			END;
			END WHILE; 	
					
			SET @SqlCmd = CONCAT(@SqlCmd, ' FROM ',GT_DB,'.table_pm_huawei_umts WHERE data_hour=',DATA_HR,' GROUP BY DATA_DATE,DATA_HOUR,RNC_NAME,CELL_ID;');
			SELECT @SqlCmd;
			PREPARE Stmt FROM @SqlCmd;
			EXECUTE Stmt;
			DEALLOCATE PREPARE Stmt;
		END IF;
	END IF;
	
	IF TECH_MASK IN (0,4) THEN 
		IF (VENDOR_ID & 1)>0 THEN
			SET @SqlCmd = CONCAT('TRUNCATE TABLE ',GT_DB,'.table_pm_ericsson_lte_aggr_',DATA_HR,';');
			PREPARE Stmt FROM @SqlCmd;
			EXECUTE Stmt;
			DEALLOCATE PREPARE Stmt;
			
			SET @SqlCmd=CONCAT('SELECT GROUP_CONCAT(
						IF( counter_type LIKE ''%single%'', 
						CONCAT(''SUM(IFNULL('',PM_COUNTER_NAME,'',0)) AS '',PM_COUNTER_NAME),  
						PM_COUNTER_NAME)
						SEPARATOR '','') INTO @all_column_eri_lte_sum
						FROM ',GT_NT,'.dim_pm_ericsson_lte;');						
			PREPARE Stmt FROM @SqlCmd;
			EXECUTE Stmt;
			DEALLOCATE PREPARE Stmt;
				
			SET @SqlCmd = CONCAT('INSERT INTO ',GT_DB,'.table_pm_ericsson_lte_aggr_',DATA_HR,' SELECT ',@all_column_eri_lte_sum,' FROM ',GT_DB,'.table_pm_ericsson_lte WHERE data_hour=',DATA_HR,' GROUP BY DATA_DATE,DATA_HOUR,ENODEB_ID,CELL_ID;');
			PREPARE Stmt FROM @SqlCmd;
			EXECUTE Stmt;
			DEALLOCATE PREPARE Stmt;
			
			SET @SqlCmd = CONCAT( 'UPDATE ',GT_DB,'.table_pm_ericsson_lte_aggr_',DATA_HR,' a 
						INNER JOIN ',GT_NT,'.nt_cell_current_lte b ON a.cell_id = b.cell_id AND a.enodeb_id = b.enodeb_id
						SET a.enodeb_name = b.enodeb_name,  a.cell_name = b.cell_name');
			PREPARE Stmt FROM @SqlCmd;
			EXECUTE Stmt;
			DEALLOCATE PREPARE Stmt; 
	
            
			SET @SqlCmd = CONCAT('TRUNCATE TABLE ',GT_DB,'.table_pm_pdf_ericsson_lte_aggr_',DATA_HR,';');
			PREPARE Stmt FROM @SqlCmd;
			EXECUTE Stmt;
			DEALLOCATE PREPARE Stmt;
			SET @SqlCmd = CONCAT('INSERT INTO ',GT_DB,'.table_pm_pdf_ericsson_lte_aggr_',DATA_HR,' 
						SELECT data_date, data_hour, enodeb_id, enodeb_name, cell_id, cell_name, pdf_index,
						',table_pm_pdf_ericsson_sum_str,' FROM ',GT_DB,'.table_pm_pdf_ericsson_lte WHERE data_hour=',DATA_HR,' GROUP BY cell_id, enodeb_id;');
			PREPARE Stmt FROM @SqlCmd;
			EXECUTE Stmt;
			DEALLOCATE PREPARE Stmt;
			SET @SqlCmd = CONCAT( 'UPDATE ',GT_DB,'.table_pm_pdf_ericsson_lte_aggr_',DATA_HR,' a 
						INNER JOIN ',GT_NT,'.nt_cell_current_lte b 
						ON b.cell_id = a.cell_id and  b.enodeb_id = a.enodeb_id
						SET a.cell_name = b.cell_name, a.enodeb_name=b.enodeb_name; ');
			PREPARE Stmt FROM @SqlCmd;
			EXECUTE Stmt;
			DEALLOCATE PREPARE Stmt;
		END IF;
	
		IF (VENDOR_ID & 4)>0 THEN
			SET @SqlCmd = CONCAT('INSERT INTO ',GT_DB,'.table_pm_huawei_lte_aggr_',DATA_HR,' SELECT DATA_DATE');
			SET @i = 2;
			SELECT COUNT(*) INTO @total_count FROM GT_NT.dim_pm_huawei_lte;
			WHILE @i<=@total_count DO
			BEGIN
				SELECT COUNTER_TYPE, PM_COUNTER_NAME
				INTO @counter_type, @column_name 
				FROM GT_NT.dim_pm_huawei_lte A WHERE A.INDEX=@i;
				IF @counter_type LIKE '%single%' THEN 
					SET @SqlCmd = CONCAT(@SqlCmd, ',SUM(', @column_name, ') AS ',@column_name); 
				ELSE 
					SET @SqlCmd = CONCAT(@SqlCmd, ',', @column_name); 
				END IF;  
				SET @i = @i+1;
			END;
			END WHILE; 
			
			SET @SqlCmd = CONCAT(@SqlCmd, ' FROM ',GT_DB,'.table_pm_huawei_lte WHERE data_hour=',DATA_HR,' GROUP BY DATA_DATE,DATA_HOUR,ENODEB_ID,CELL_ID;');
			PREPARE Stmt FROM @SqlCmd;
			EXECUTE Stmt;
			DEALLOCATE PREPARE Stmt;
		END IF;
	END IF;
