DELIMITER $$
USE `gt_gw_main`$$
DROP PROCEDURE IF EXISTS `SP_CreateDB_LTE`$$
CREATE DEFINER=`covmo`@`%` PROCEDURE `SP_CreateDB_PM_HUA`(IN GT_DB VARCHAR(50))
BEGIN
	SET @SqlCmd=CONCAT('CREATE TABLE ',GT_DB,'.`Table_pm_huawei_umts` 
				(
				`RNC_NAME` varchar(50) CHARACTER SET utf8 NOT NULL,
				`RNC_ID` mediumint(9) DEFAULT NULL,
				`CELL_ID` mediumint(9) NOT NULL,
				`TIMESTAMP` DATETIME NOT NULL,
				`DATA_DATE` DATE DEFAULT NULL,
				`DATA_HOUR` TINYINT(4) DEFAULT NULL,
				`67179299` CHAR(8) DEFAULT NULL,
				`67179298` CHAR(8) DEFAULT NULL,
				`67179329` CHAR(8) DEFAULT NULL,
				`67179330` CHAR(8) DEFAULT NULL,
				`67179331` CHAR(8) DEFAULT NULL,
				`67179332` CHAR(8) DEFAULT NULL,
				`67179333` CHAR(8) DEFAULT NULL,
				`67179334` CHAR(8) DEFAULT NULL,
				`67179335` CHAR(8) DEFAULT NULL,
				`67179336` CHAR(8) DEFAULT NULL,
				`67179337` CHAR(8) DEFAULT NULL,
				`67179457` CHAR(8) DEFAULT NULL,
				`67179458` CHAR(8) DEFAULT NULL,
				`67179459` CHAR(8) DEFAULT NULL,
				`67179460` CHAR(8) DEFAULT NULL,
				`67179461` CHAR(8) DEFAULT NULL,
				`67179462` CHAR(8) DEFAULT NULL,
				`67179463` CHAR(8) DEFAULT NULL,
				`67179464` CHAR(8) DEFAULT NULL,
				`67179465` CHAR(8) DEFAULT NULL,
				`67179777` CHAR(8) DEFAULT NULL,
				`67179778` CHAR(8) DEFAULT NULL,
				`67179779` CHAR(8) DEFAULT NULL,
				`67179780` CHAR(8) DEFAULT NULL,
				`67179781` CHAR(8) DEFAULT NULL,
				`67179782` CHAR(8) DEFAULT NULL,
				`67179825` CHAR(8) DEFAULT NULL,
				`67179826` CHAR(8) DEFAULT NULL,
				`67179827` CHAR(8) DEFAULT NULL,
				`67179828` CHAR(8) DEFAULT NULL,
				`67179921` CHAR(8) DEFAULT NULL,
				`67179922` CHAR(8) DEFAULT NULL,
				`67179923` CHAR(8) DEFAULT NULL,
				`67179924` CHAR(8) DEFAULT NULL,
				`67179925` CHAR(8) DEFAULT NULL,
				`67179926` CHAR(8) DEFAULT NULL,
				`67179927` CHAR(8) DEFAULT NULL,
				`67179928` CHAR(8) DEFAULT NULL,
				`67184200` CHAR(8) DEFAULT NULL,
				`67184201` CHAR(8) DEFAULT NULL,
				`67189754` CHAR(8) DEFAULT NULL,
				`67189755` CHAR(8) DEFAULT NULL
				,PRIMARY KEY(RNC_NAME,CELL_ID,TIMESTAMP)
				) ENGINE=MYISAM DEFAULT CHARSET=latin1 DELAY_KEY_WRITE=1' 
				);
	PREPARE Stmt FROM @SqlCmd;
	EXECUTE Stmt;
	DEALLOCATE PREPARE Stmt; 
	
	SET @SqlCmd=CONCAT('CREATE TABLE ',GT_DB,'.`Table_pm_67109365` 
				(
				`RNC_NAME` varchar(50) CHARACTER SET utf8 DEFAULT NULL,
				`CELL_ID` mediumint(9) DEFAULT NULL,
				`TIMESTAMP` DATETIME DEFAULT NULL,
				`67179298` CHAR(8) DEFAULT NULL,
				`67179299` CHAR(8) DEFAULT NULL,
				`67179302` CHAR(8) DEFAULT NULL,
				`67179303` CHAR(8) DEFAULT NULL,
				`67179304` CHAR(8) DEFAULT NULL,
				`67179329` CHAR(8) DEFAULT NULL,
				`67179330` CHAR(8) DEFAULT NULL,
				`67179331` CHAR(8) DEFAULT NULL,
				`67179332` CHAR(8) DEFAULT NULL,
				`67179333` CHAR(8) DEFAULT NULL,
				`67179334` CHAR(8) DEFAULT NULL,
				`67179335` CHAR(8) DEFAULT NULL,
				`67179336` CHAR(8) DEFAULT NULL,
				`67179337` CHAR(8) DEFAULT NULL,
				`67179338` CHAR(8) DEFAULT NULL,
				`67179339` CHAR(8) DEFAULT NULL,
				`67179340` CHAR(8) DEFAULT NULL,
				`67179341` CHAR(8) DEFAULT NULL,
				`67179342` CHAR(8) DEFAULT NULL,
				`67179343` CHAR(8) DEFAULT NULL,
				`67179344` CHAR(8) DEFAULT NULL,
				`67179345` CHAR(8) DEFAULT NULL,
				`67179346` CHAR(8) DEFAULT NULL,
				`67179347` CHAR(8) DEFAULT NULL,
				`67179348` CHAR(8) DEFAULT NULL,
				`67179457` CHAR(8) DEFAULT NULL,
				`67179458` CHAR(8) DEFAULT NULL,
				`67179459` CHAR(8) DEFAULT NULL,
				`67179460` CHAR(8) DEFAULT NULL,
				`67179461` CHAR(8) DEFAULT NULL,
				`67179462` CHAR(8) DEFAULT NULL,
				`67179463` CHAR(8) DEFAULT NULL,
				`67179464` CHAR(8) DEFAULT NULL,
				`67179465` CHAR(8) DEFAULT NULL,
				`67179466` CHAR(8) DEFAULT NULL,
				`67179467` CHAR(8) DEFAULT NULL,
				`67179468` CHAR(8) DEFAULT NULL,
				`67179469` CHAR(8) DEFAULT NULL,
				`67179470` CHAR(8) DEFAULT NULL,
				`67179471` CHAR(8) DEFAULT NULL,
				`67179472` CHAR(8) DEFAULT NULL,
				`67179473` CHAR(8) DEFAULT NULL,
				`67179474` CHAR(8) DEFAULT NULL,
				`67179475` CHAR(8) DEFAULT NULL,
				`67179476` CHAR(8) DEFAULT NULL,
				`67179633` CHAR(8) DEFAULT NULL,
				`67179634` CHAR(8) DEFAULT NULL,
				`67179649` CHAR(8) DEFAULT NULL,
				`67179650` CHAR(8) DEFAULT NULL,
				`67189400` CHAR(8) DEFAULT NULL,
				`67189401` CHAR(8) DEFAULT NULL,
				`67190586` CHAR(8) DEFAULT NULL,
				`67190587` CHAR(8) DEFAULT NULL,
				`67190588` CHAR(8) DEFAULT NULL,
				`67190589` CHAR(8) DEFAULT NULL,
				`67192607` CHAR(8) DEFAULT NULL,
				`67195590` CHAR(8) DEFAULT NULL,
				`67195964` CHAR(8) DEFAULT NULL,
				`67195965` CHAR(8) DEFAULT NULL,
				`67195966` CHAR(8) DEFAULT NULL,
				`67195967` CHAR(8) DEFAULT NULL,
				`67196198` CHAR(8) DEFAULT NULL,
				`67196199` CHAR(8) DEFAULT NULL,
				`67196200` CHAR(8) DEFAULT NULL,
				`67196201` CHAR(8) DEFAULT NULL,
				`67199510` CHAR(8) DEFAULT NULL,
				`73403798` CHAR(8) DEFAULT NULL,
				`73410489` CHAR(8) DEFAULT NULL,
				`73410490` CHAR(8) DEFAULT NULL,
				`73410505` CHAR(8) DEFAULT NULL,
				`73421887` CHAR(8) DEFAULT NULL,
				`73421888` CHAR(8) DEFAULT NULL,
				`73421889` CHAR(8) DEFAULT NULL,
				`73423388` CHAR(8) DEFAULT NULL,
				`73423389` CHAR(8) DEFAULT NULL,
				`73423390` CHAR(8) DEFAULT NULL,
				`73423391` CHAR(8) DEFAULT NULL,
				`73423392` CHAR(8) DEFAULT NULL,
				`73423393` CHAR(8) DEFAULT NULL,
				`73423394` CHAR(8) DEFAULT NULL,
				`73423395` CHAR(8) DEFAULT NULL,
				`73423396` CHAR(8) DEFAULT NULL,
				`73423397` CHAR(8) DEFAULT NULL,
				`73423398` CHAR(8) DEFAULT NULL,
				`73423399` CHAR(8) DEFAULT NULL,
				`73423400` CHAR(8) DEFAULT NULL,
				`73423401` CHAR(8) DEFAULT NULL,
				`73423402` CHAR(8) DEFAULT NULL,
				`73423403` CHAR(8) DEFAULT NULL,
				`73423404` CHAR(8) DEFAULT NULL,
				`73423405` CHAR(8) DEFAULT NULL,
				`73423406` CHAR(8) DEFAULT NULL,
				`73423407` CHAR(8) DEFAULT NULL,
				`73423485` CHAR(8) DEFAULT NULL,
				`73423486` CHAR(8) DEFAULT NULL,
				`73423487` CHAR(8) DEFAULT NULL,
				`73423488` CHAR(8) DEFAULT NULL,
				`73423489` CHAR(8) DEFAULT NULL,
				`73423490` CHAR(8) DEFAULT NULL,
				`73423491` CHAR(8) DEFAULT NULL,
				`73423492` CHAR(8) DEFAULT NULL,
				`73423493` CHAR(8) DEFAULT NULL,
				`73423494` CHAR(8) DEFAULT NULL,
				`73423495` CHAR(8) DEFAULT NULL,
				`73423496` CHAR(8) DEFAULT NULL,
				`73423497` CHAR(8) DEFAULT NULL,
				`73423498` CHAR(8) DEFAULT NULL,
				`73423501` CHAR(8) DEFAULT NULL,
				`73423502` CHAR(8) DEFAULT NULL,
				`73423503` CHAR(8) DEFAULT NULL,
				`73423504` CHAR(8) DEFAULT NULL,
				`73423505` CHAR(8) DEFAULT NULL,
				`73423506` CHAR(8) DEFAULT NULL,
				`73423507` CHAR(8) DEFAULT NULL,
				`73423508` CHAR(8) DEFAULT NULL,
				`73423509` CHAR(8) DEFAULT NULL,
				`73423510` CHAR(8) DEFAULT NULL,
				`73424223` CHAR(8) DEFAULT NULL,
				`73424224` CHAR(8) DEFAULT NULL,
				`73424225` CHAR(8) DEFAULT NULL,
				`73424226` CHAR(8) DEFAULT NULL,
				`73424227` CHAR(8) DEFAULT NULL,
				`73424228` CHAR(8) DEFAULT NULL,
				`73424229` CHAR(8) DEFAULT NULL,
				`73424230` CHAR(8) DEFAULT NULL,
				`73424231` CHAR(8) DEFAULT NULL,
				`73424232` CHAR(8) DEFAULT NULL,
				`73424233` CHAR(8) DEFAULT NULL,
				`73424234` CHAR(8) DEFAULT NULL,
				`73424476` CHAR(8) DEFAULT NULL,
				`73424477` CHAR(8) DEFAULT NULL,
				`73424691` CHAR(8) DEFAULT NULL,
				`73424692` CHAR(8) DEFAULT NULL,
				`73424693` CHAR(8) DEFAULT NULL,
				`73424694` CHAR(8) DEFAULT NULL,
				`73424695` CHAR(8) DEFAULT NULL,
				`73424968` CHAR(8) DEFAULT NULL,
				`73424969` CHAR(8) DEFAULT NULL,
				`73425017` CHAR(8) DEFAULT NULL,
				`73425018` CHAR(8) DEFAULT NULL,
				`73425019` CHAR(8) DEFAULT NULL,
				`73425020` CHAR(8) DEFAULT NULL,
				`73425914` CHAR(8) DEFAULT NULL,
				`73425915` CHAR(8) DEFAULT NULL,
				`73425916` CHAR(8) DEFAULT NULL,
				`73425917` CHAR(8) DEFAULT NULL,
				`73426415` CHAR(8) DEFAULT NULL,
				`73426416` CHAR(8) DEFAULT NULL,
				`73427913` CHAR(8) DEFAULT NULL,
				`73427914` CHAR(8) DEFAULT NULL,
				`73427915` CHAR(8) DEFAULT NULL,
				`73427916` CHAR(8) DEFAULT NULL,
				`73427998` CHAR(8) DEFAULT NULL,
				`73427999` CHAR(8) DEFAULT NULL,
				`73428000` CHAR(8) DEFAULT NULL,
				`73428001` CHAR(8) DEFAULT NULL,
				`73428002` CHAR(8) DEFAULT NULL,
				`73428003` CHAR(8) DEFAULT NULL,
				`73428004` CHAR(8) DEFAULT NULL,
				`73428005` CHAR(8) DEFAULT NULL,
				`73428006` CHAR(8) DEFAULT NULL,
				`73428007` CHAR(8) DEFAULT NULL,
				`73428008` CHAR(8) DEFAULT NULL,
				`73428009` CHAR(8) DEFAULT NULL,
				`73439969` CHAR(8) DEFAULT NULL,
				`73441146` CHAR(8) DEFAULT NULL,
				`73441147` CHAR(8) DEFAULT NULL,
				`73441148` CHAR(8) DEFAULT NULL,
				`73441149` CHAR(8) DEFAULT NULL,
				`73441150` CHAR(8) DEFAULT NULL,
				`73441151` CHAR(8) DEFAULT NULL,
				`73441152` CHAR(8) DEFAULT NULL,
				`73441153` CHAR(8) DEFAULT NULL,
				`73441154` CHAR(8) DEFAULT NULL,
				`73441155` CHAR(8) DEFAULT NULL,
				`73441156` CHAR(8) DEFAULT NULL,
				`73441157` CHAR(8) DEFAULT NULL
				,KEY `IX_TIME` (`TIMESTAMP`)
				) ENGINE=MYISAM DEFAULT CHARSET=latin1 DELAY_KEY_WRITE=1');	
	PREPARE Stmt FROM @SqlCmd;
	EXECUTE Stmt;
	DEALLOCATE PREPARE Stmt; 
	SET @SqlCmd=CONCAT('CREATE TABLE ',GT_DB,'.`Table_pm_67109368` 
				(
				`RNC_NAME` varchar(50) CHARACTER SET utf8 DEFAULT NULL,
				`CELL_ID` mediumint(9) DEFAULT NULL,
				`TIMESTAMP` DATETIME DEFAULT NULL,
				`67179825` CHAR(8) DEFAULT NULL,
				`67179826` CHAR(8) DEFAULT NULL,
				`67179827` CHAR(8) DEFAULT NULL,
				`67179828` CHAR(8) DEFAULT NULL,
				`67179858` CHAR(8) DEFAULT NULL,
				`67179859` CHAR(8) DEFAULT NULL,
				`67179860` CHAR(8) DEFAULT NULL,
				`67179861` CHAR(8) DEFAULT NULL,
				`67190457` CHAR(8) DEFAULT NULL,
				`67190458` CHAR(8) DEFAULT NULL,
				`67190460` CHAR(8) DEFAULT NULL,
				`67190461` CHAR(8) DEFAULT NULL,
				`67190462` CHAR(8) DEFAULT NULL,
				`67190464` CHAR(8) DEFAULT NULL,
				`67192120` CHAR(8) DEFAULT NULL,
				`67192121` CHAR(8) DEFAULT NULL,
				`67196202` CHAR(8) DEFAULT NULL,
				`67196203` CHAR(8) DEFAULT NULL,
				`67196232` CHAR(8) DEFAULT NULL,
				`67204853` CHAR(8) DEFAULT NULL,
				`73393916` CHAR(8) DEFAULT NULL,
				`73393918` CHAR(8) DEFAULT NULL,
				`73393920` CHAR(8) DEFAULT NULL,
				`73393966` CHAR(8) DEFAULT NULL,
				`73393967` CHAR(8) DEFAULT NULL,
				`73394277` CHAR(8) DEFAULT NULL,
				`73394278` CHAR(8) DEFAULT NULL,
				`73394279` CHAR(8) DEFAULT NULL,
				`73394280` CHAR(8) DEFAULT NULL,
				`73394281` CHAR(8) DEFAULT NULL,
				`73394282` CHAR(8) DEFAULT NULL,
				`73394283` CHAR(8) DEFAULT NULL,
				`73394284` CHAR(8) DEFAULT NULL,
				`73394285` CHAR(8) DEFAULT NULL,
				`73394286` CHAR(8) DEFAULT NULL,
				`73394287` CHAR(8) DEFAULT NULL,
				`73394288` CHAR(8) DEFAULT NULL,
				`73394289` CHAR(8) DEFAULT NULL,
				`73394290` CHAR(8) DEFAULT NULL,
				`73394291` CHAR(8) DEFAULT NULL,
				`73394292` CHAR(8) DEFAULT NULL,
				`73394293` CHAR(8) DEFAULT NULL,
				`73394294` CHAR(8) DEFAULT NULL,
				`73425918` CHAR(8) DEFAULT NULL,
				`73425919` CHAR(8) DEFAULT NULL,
				`73425920` CHAR(8) DEFAULT NULL,
				`73425921` CHAR(8) DEFAULT NULL,
				`73426335` CHAR(8) DEFAULT NULL,
				`73426336` CHAR(8) DEFAULT NULL,
				`73426337` CHAR(8) DEFAULT NULL,
				`73426338` CHAR(8) DEFAULT NULL,
				`73426417` CHAR(8) DEFAULT NULL,
				`73426418` CHAR(8) DEFAULT NULL,
				`73426419` CHAR(8) DEFAULT NULL,
				`73426420` CHAR(8) DEFAULT NULL,
				`73427962` CHAR(8) DEFAULT NULL,
				`73427963` CHAR(8) DEFAULT NULL
				,KEY `IX_TIME` (`TIMESTAMP`)
				) ENGINE=MYISAM DEFAULT CHARSET=latin1 DELAY_KEY_WRITE=1');	
	PREPARE Stmt FROM @SqlCmd;
	EXECUTE Stmt;
	DEALLOCATE PREPARE Stmt; 
	SET @SqlCmd=CONCAT('CREATE TABLE ',GT_DB,'.`Table_pm_67109372` 
				(
				`RNC_NAME` varchar(50) CHARACTER SET utf8 DEFAULT NULL,
				`CELL_ID` mediumint(9) DEFAULT NULL,
				`TIMESTAMP` DATETIME DEFAULT NULL,
				`67179921` CHAR(8) DEFAULT NULL,
				`67179922` CHAR(8) DEFAULT NULL,
				`67179923` CHAR(8) DEFAULT NULL,
				`67179924` CHAR(8) DEFAULT NULL,
				`67179925` CHAR(8) DEFAULT NULL,
				`67179926` CHAR(8) DEFAULT NULL,
				`67179927` CHAR(8) DEFAULT NULL,
				`67179928` CHAR(8) DEFAULT NULL,
				`67196301` CHAR(8) DEFAULT NULL,
				`73393917` CHAR(8) DEFAULT NULL,
				`73393919` CHAR(8) DEFAULT NULL,
				`73393921` CHAR(8) DEFAULT NULL,
				`73393968` CHAR(8) DEFAULT NULL,
				`73393969` CHAR(8) DEFAULT NULL,
				`73394075` CHAR(8) DEFAULT NULL,
				`73394076` CHAR(8) DEFAULT NULL,
				`73403808` CHAR(8) DEFAULT NULL,
				`73403809` CHAR(8) DEFAULT NULL,
				`73423517` CHAR(8) DEFAULT NULL,
				`73423518` CHAR(8) DEFAULT NULL,
				`73425922` CHAR(8) DEFAULT NULL,
				`73425923` CHAR(8) DEFAULT NULL,
				`73425924` CHAR(8) DEFAULT NULL,
				`73425925` CHAR(8) DEFAULT NULL,
				`73426339` CHAR(8) DEFAULT NULL,
				`73426340` CHAR(8) DEFAULT NULL,
				`73426341` CHAR(8) DEFAULT NULL,
				`73426342` CHAR(8) DEFAULT NULL,
				`73427964` CHAR(8) DEFAULT NULL,
				`73427965` CHAR(8) DEFAULT NULL,
				`73430323` CHAR(8) DEFAULT NULL,
				`73430780` CHAR(8) DEFAULT NULL
				,KEY `IX_TIME` (`TIMESTAMP`)
				) ENGINE=MYISAM DEFAULT CHARSET=latin1 DELAY_KEY_WRITE=1');	
	PREPARE Stmt FROM @SqlCmd;
	EXECUTE Stmt;
	DEALLOCATE PREPARE Stmt; 
	
	SET @SqlCmd=CONCAT('CREATE TABLE ',GT_DB,'.`Table_pm_67109376` 
				(
				`RNC_NAME` varchar(50) CHARACTER SET utf8 DEFAULT NULL,
				`CELL_ID` mediumint(9) DEFAULT NULL,
				`TIMESTAMP` DATETIME DEFAULT NULL,
				`67179777` CHAR(8) DEFAULT NULL,
				`67179778` CHAR(8) DEFAULT NULL,
				`67179779` CHAR(8) DEFAULT NULL,
				`67179780` CHAR(8) DEFAULT NULL,
				`67179781` CHAR(8) DEFAULT NULL,
				`67179782` CHAR(8) DEFAULT NULL,
				`67180067` CHAR(8) DEFAULT NULL,
				`67180068` CHAR(8) DEFAULT NULL,
				`67180069` CHAR(8) DEFAULT NULL,
				`67180074` CHAR(8) DEFAULT NULL,
				`67180076` CHAR(8) DEFAULT NULL,
				`67180077` CHAR(8) DEFAULT NULL,
				`67180078` CHAR(8) DEFAULT NULL,
				`67180079` CHAR(8) DEFAULT NULL,
				`67180080` CHAR(8) DEFAULT NULL,
				`67180081` CHAR(8) DEFAULT NULL,
				`67180082` CHAR(8) DEFAULT NULL,
				`67180083` CHAR(8) DEFAULT NULL,
				`67189568` CHAR(8) DEFAULT NULL,
				`67189572` CHAR(8) DEFAULT NULL,
				`67190467` CHAR(8) DEFAULT NULL,
				`67190468` CHAR(8) DEFAULT NULL,
				`67190469` CHAR(8) DEFAULT NULL,
				`67190470` CHAR(8) DEFAULT NULL,
				`67190505` CHAR(8) DEFAULT NULL,
				`67190506` CHAR(8) DEFAULT NULL,
				`67190518` CHAR(8) DEFAULT NULL,
				`67190840` CHAR(8) DEFAULT NULL,
				`67190841` CHAR(8) DEFAULT NULL,
				`67191786` CHAR(8) DEFAULT NULL,
				`67191791` CHAR(8) DEFAULT NULL,
				`67191818` CHAR(8) DEFAULT NULL,
				`67191835` CHAR(8) DEFAULT NULL,
				`67192201` CHAR(8) DEFAULT NULL,
				`67192202` CHAR(8) DEFAULT NULL,
				`67192203` CHAR(8) DEFAULT NULL,
				`67192204` CHAR(8) DEFAULT NULL,
				`67192205` CHAR(8) DEFAULT NULL,
				`67192206` CHAR(8) DEFAULT NULL,
				`67192207` CHAR(8) DEFAULT NULL,
				`67192208` CHAR(8) DEFAULT NULL,
				`67192209` CHAR(8) DEFAULT NULL,
				`67192597` CHAR(8) DEFAULT NULL,
				`67192598` CHAR(8) DEFAULT NULL,
				`67192599` CHAR(8) DEFAULT NULL,
				`67192600` CHAR(8) DEFAULT NULL,
				`67192601` CHAR(8) DEFAULT NULL,
				`67192602` CHAR(8) DEFAULT NULL,
				`67192975` CHAR(8) DEFAULT NULL,
				`67192976` CHAR(8) DEFAULT NULL,
				`67192977` CHAR(8) DEFAULT NULL,
				`67192978` CHAR(8) DEFAULT NULL,
				`67192979` CHAR(8) DEFAULT NULL,
				`67192980` CHAR(8) DEFAULT NULL,
				`67196204` CHAR(8) DEFAULT NULL,
				`67196205` CHAR(8) DEFAULT NULL,
				`67196233` CHAR(8) DEFAULT NULL,
				`67196302` CHAR(8) DEFAULT NULL,
				`73393843` CHAR(8) DEFAULT NULL,
				`73393845` CHAR(8) DEFAULT NULL,
				`73393846` CHAR(8) DEFAULT NULL,
				`73394051` CHAR(8) DEFAULT NULL,
				`73394052` CHAR(8) DEFAULT NULL,
				`73394073` CHAR(8) DEFAULT NULL,
				`73394074` CHAR(8) DEFAULT NULL,
				`73403805` CHAR(8) DEFAULT NULL,
				`73403806` CHAR(8) DEFAULT NULL,
				`73403807` CHAR(8) DEFAULT NULL,
				`73403833` CHAR(8) DEFAULT NULL,
				`73403834` CHAR(8) DEFAULT NULL,
				`73403835` CHAR(8) DEFAULT NULL,
				`73403836` CHAR(8) DEFAULT NULL,
				`73403837` CHAR(8) DEFAULT NULL,
				`73403838` CHAR(8) DEFAULT NULL,
				`73403839` CHAR(8) DEFAULT NULL,
				`73403840` CHAR(8) DEFAULT NULL,
				`73403841` CHAR(8) DEFAULT NULL,
				`73403842` CHAR(8) DEFAULT NULL,
				`73403843` CHAR(8) DEFAULT NULL,
				`73421882` CHAR(8) DEFAULT NULL,
				`73421883` CHAR(8) DEFAULT NULL,
				`73421886` CHAR(8) DEFAULT NULL,
				`73422166` CHAR(8) DEFAULT NULL,
				`73422169` CHAR(8) DEFAULT NULL,
				`73423939` CHAR(8) DEFAULT NULL,
				`73423940` CHAR(8) DEFAULT NULL,
				`73425979` CHAR(8) DEFAULT NULL,
				`73425980` CHAR(8) DEFAULT NULL,
				`73425981` CHAR(8) DEFAULT NULL,
				`73426205` CHAR(8) DEFAULT NULL,
				`73426206` CHAR(8) DEFAULT NULL,
				`73426207` CHAR(8) DEFAULT NULL,
				`73426208` CHAR(8) DEFAULT NULL,
				`73426209` CHAR(8) DEFAULT NULL,
				`73426881` CHAR(8) DEFAULT NULL,
				`73426882` CHAR(8) DEFAULT NULL,
				`73428670` CHAR(8) DEFAULT NULL,
				`73428671` CHAR(8) DEFAULT NULL,
				`73428672` CHAR(8) DEFAULT NULL,
				`73428903` CHAR(8) DEFAULT NULL,
				`73428904` CHAR(8) DEFAULT NULL,
				`73429092` CHAR(8) DEFAULT NULL,
				`73429099` CHAR(8) DEFAULT NULL,
				`73429102` CHAR(8) DEFAULT NULL,
				`73429470` CHAR(8) DEFAULT NULL,
				`73429471` CHAR(8) DEFAULT NULL,
				`73430189` CHAR(8) DEFAULT NULL,
				`73430190` CHAR(8) DEFAULT NULL,
				`73430220` CHAR(8) DEFAULT NULL,
				`73430480` CHAR(8) DEFAULT NULL,
				`73430481` CHAR(8) DEFAULT NULL,
				`73430482` CHAR(8) DEFAULT NULL,
				`73430483` CHAR(8) DEFAULT NULL,
				`73430484` CHAR(8) DEFAULT NULL,
				`73430485` CHAR(8) DEFAULT NULL,
				`73430559` CHAR(8) DEFAULT NULL,
				`73430562` CHAR(8) DEFAULT NULL
				,KEY `IX_TIME` (`TIMESTAMP`)
				) ENGINE=MYISAM DEFAULT CHARSET=latin1 DELAY_KEY_WRITE=1');	
	PREPARE Stmt FROM @SqlCmd;
	EXECUTE Stmt;
	DEALLOCATE PREPARE Stmt; 
	
	SET @SqlCmd=CONCAT('CREATE TABLE ',GT_DB,'.`Table_pm_67109381` 
				(
				`RNC_NAME` varchar(50) CHARACTER SET utf8 DEFAULT NULL,
				`CELL_ID` mediumint(9) DEFAULT NULL,
				`TIMESTAMP` DATETIME DEFAULT NULL,
				`67189729` CHAR(8) DEFAULT NULL,
				`67189730` CHAR(8) DEFAULT NULL,
				`67189732` CHAR(8) DEFAULT NULL,
				`67189739` CHAR(8) DEFAULT NULL,
				`67189740` CHAR(8) DEFAULT NULL,
				`67189741` CHAR(8) DEFAULT NULL,
				`67189742` CHAR(8) DEFAULT NULL,
				`67189743` CHAR(8) DEFAULT NULL,
				`67189744` CHAR(8) DEFAULT NULL,
				`67189745` CHAR(8) DEFAULT NULL,
				`67189746` CHAR(8) DEFAULT NULL,
				`67189747` CHAR(8) DEFAULT NULL,
				`67189748` CHAR(8) DEFAULT NULL,
				`67189749` CHAR(8) DEFAULT NULL,
				`67189750` CHAR(8) DEFAULT NULL,
				`67189751` CHAR(8) DEFAULT NULL,
				`67189752` CHAR(8) DEFAULT NULL,
				`67189753` CHAR(8) DEFAULT NULL,
				`67189754` CHAR(8) DEFAULT NULL,
				`67189755` CHAR(8) DEFAULT NULL,
				`67189756` CHAR(8) DEFAULT NULL,
				`67189757` CHAR(8) DEFAULT NULL,
				`67189758` CHAR(8) DEFAULT NULL,
				`67189759` CHAR(8) DEFAULT NULL,
				`67189760` CHAR(8) DEFAULT NULL,
				`67189761` CHAR(8) DEFAULT NULL,
				`67189762` CHAR(8) DEFAULT NULL,
				`67189763` CHAR(8) DEFAULT NULL,
				`67190410` CHAR(8) DEFAULT NULL,
				`67190411` CHAR(8) DEFAULT NULL,
				`67190412` CHAR(8) DEFAULT NULL,
				`67190413` CHAR(8) DEFAULT NULL,
				`67190414` CHAR(8) DEFAULT NULL,
				`67190476` CHAR(8) DEFAULT NULL,
				`67190477` CHAR(8) DEFAULT NULL,
				`67190593` CHAR(8) DEFAULT NULL,
				`67190856` CHAR(8) DEFAULT NULL,
				`67190857` CHAR(8) DEFAULT NULL,
				`67190858` CHAR(8) DEFAULT NULL,
				`67190859` CHAR(8) DEFAULT NULL,
				`67191155` CHAR(8) DEFAULT NULL,
				`67191156` CHAR(8) DEFAULT NULL,
				`67192186` CHAR(8) DEFAULT NULL,
				`67192187` CHAR(8) DEFAULT NULL,
				`67192506` CHAR(8) DEFAULT NULL,
				`67192507` CHAR(8) DEFAULT NULL,
				`67192654` CHAR(8) DEFAULT NULL,
				`67192655` CHAR(8) DEFAULT NULL,
				`67192656` CHAR(8) DEFAULT NULL,
				`67192657` CHAR(8) DEFAULT NULL,
				`67192658` CHAR(8) DEFAULT NULL,
				`67192659` CHAR(8) DEFAULT NULL,
				`67192660` CHAR(8) DEFAULT NULL,
				`67192661` CHAR(8) DEFAULT NULL,
				`67192662` CHAR(8) DEFAULT NULL,
				`67192663` CHAR(8) DEFAULT NULL,
				`67192664` CHAR(8) DEFAULT NULL,
				`67192665` CHAR(8) DEFAULT NULL,
				`67192666` CHAR(8) DEFAULT NULL,
				`67192667` CHAR(8) DEFAULT NULL,
				`67192668` CHAR(8) DEFAULT NULL,
				`67192669` CHAR(8) DEFAULT NULL,
				`67192670` CHAR(8) DEFAULT NULL,
				`67192671` CHAR(8) DEFAULT NULL,
				`67192672` CHAR(8) DEFAULT NULL,
				`67192673` CHAR(8) DEFAULT NULL,
				`67192674` CHAR(8) DEFAULT NULL,
				`67192675` CHAR(8) DEFAULT NULL,
				`67192676` CHAR(8) DEFAULT NULL,
				`67192677` CHAR(8) DEFAULT NULL,
				`67192678` CHAR(8) DEFAULT NULL,
				`67192679` CHAR(8) DEFAULT NULL,
				`67192680` CHAR(8) DEFAULT NULL,
				`67192681` CHAR(8) DEFAULT NULL,
				`67192682` CHAR(8) DEFAULT NULL,
				`67193399` CHAR(8) DEFAULT NULL,
				`67193400` CHAR(8) DEFAULT NULL,
				`67193405` CHAR(8) DEFAULT NULL,
				`67193406` CHAR(8) DEFAULT NULL,
				`67193407` CHAR(8) DEFAULT NULL,
				`67193408` CHAR(8) DEFAULT NULL,
				`67196299` CHAR(8) DEFAULT NULL,
				`67196300` CHAR(8) DEFAULT NULL,
				`73393922` CHAR(8) DEFAULT NULL,
				`73393923` CHAR(8) DEFAULT NULL,
				`73393924` CHAR(8) DEFAULT NULL,
				`73393925` CHAR(8) DEFAULT NULL,
				`73393926` CHAR(8) DEFAULT NULL,
				`73393927` CHAR(8) DEFAULT NULL,
				`73393928` CHAR(8) DEFAULT NULL,
				`73393929` CHAR(8) DEFAULT NULL,
				`73393930` CHAR(8) DEFAULT NULL,
				`73393931` CHAR(8) DEFAULT NULL,
				`73393932` CHAR(8) DEFAULT NULL,
				`73393933` CHAR(8) DEFAULT NULL,
				`73394014` CHAR(8) DEFAULT NULL,
				`73394015` CHAR(8) DEFAULT NULL,
				`73394016` CHAR(8) DEFAULT NULL,
				`73394017` CHAR(8) DEFAULT NULL,
				`73394018` CHAR(8) DEFAULT NULL,
				`73394019` CHAR(8) DEFAULT NULL,
				`73394020` CHAR(8) DEFAULT NULL,
				`73394021` CHAR(8) DEFAULT NULL,
				`73394022` CHAR(8) DEFAULT NULL,
				`73423107` CHAR(8) DEFAULT NULL,
				`73423288` CHAR(8) DEFAULT NULL,
				`73423289` CHAR(8) DEFAULT NULL,
				`73423290` CHAR(8) DEFAULT NULL,
				`73425007` CHAR(8) DEFAULT NULL,
				`73425008` CHAR(8) DEFAULT NULL,
				`73425009` CHAR(8) DEFAULT NULL,
				`73425010` CHAR(8) DEFAULT NULL,
				`73425011` CHAR(8) DEFAULT NULL,
				`73425012` CHAR(8) DEFAULT NULL,
				`73425013` CHAR(8) DEFAULT NULL,
				`73425820` CHAR(8) DEFAULT NULL,
				`73425821` CHAR(8) DEFAULT NULL,
				`73425822` CHAR(8) DEFAULT NULL,
				`73425823` CHAR(8) DEFAULT NULL,
				`73425926` CHAR(8) DEFAULT NULL,
				`73425927` CHAR(8) DEFAULT NULL,
				`73425954` CHAR(8) DEFAULT NULL,
				`73425955` CHAR(8) DEFAULT NULL,
				`73425956` CHAR(8) DEFAULT NULL,
				`73425957` CHAR(8) DEFAULT NULL,
				`73426146` CHAR(8) DEFAULT NULL,
				`73426147` CHAR(8) DEFAULT NULL,
				`73426148` CHAR(8) DEFAULT NULL,
				`73426856` CHAR(8) DEFAULT NULL,
				`73426857` CHAR(8) DEFAULT NULL,
				`73426858` CHAR(8) DEFAULT NULL,
				`73426859` CHAR(8) DEFAULT NULL,
				`73427898` CHAR(8) DEFAULT NULL,
				`73427899` CHAR(8) DEFAULT NULL,
				`73427900` CHAR(8) DEFAULT NULL,
				`73427977` CHAR(8) DEFAULT NULL,
				`73427978` CHAR(8) DEFAULT NULL,
				`73427979` CHAR(8) DEFAULT NULL,
				`73427980` CHAR(8) DEFAULT NULL,
				`73427981` CHAR(8) DEFAULT NULL,
				`73427982` CHAR(8) DEFAULT NULL,
				`73427983` CHAR(8) DEFAULT NULL,
				`73427984` CHAR(8) DEFAULT NULL,
				`73427985` CHAR(8) DEFAULT NULL,
				`73427986` CHAR(8) DEFAULT NULL,
				`73427987` CHAR(8) DEFAULT NULL,
				`73428084` CHAR(8) DEFAULT NULL,
				`73428085` CHAR(8) DEFAULT NULL,
				`73428677` CHAR(8) DEFAULT NULL,
				`73428678` CHAR(8) DEFAULT NULL,
				`73428679` CHAR(8) DEFAULT NULL,
				`73428680` CHAR(8) DEFAULT NULL,
				`73428681` CHAR(8) DEFAULT NULL,
				`73428682` CHAR(8) DEFAULT NULL,
				`73428683` CHAR(8) DEFAULT NULL,
				`73428684` CHAR(8) DEFAULT NULL,
				`73428685` CHAR(8) DEFAULT NULL,
				`73428686` CHAR(8) DEFAULT NULL,
				`73428687` CHAR(8) DEFAULT NULL,
				`73428688` CHAR(8) DEFAULT NULL,
				`73428827` CHAR(8) DEFAULT NULL,
				`73428828` CHAR(8) DEFAULT NULL,
				`73428829` CHAR(8) DEFAULT NULL,
				`73430224` CHAR(8) DEFAULT NULL,
				`73430225` CHAR(8) DEFAULT NULL,
				`73430230` CHAR(8) DEFAULT NULL,
				`73430231` CHAR(8) DEFAULT NULL
				,KEY `IX_TIME` (`TIMESTAMP`)
				) ENGINE=MYISAM DEFAULT CHARSET=latin1 DELAY_KEY_WRITE=1');	
	PREPARE Stmt FROM @SqlCmd;
	EXECUTE Stmt;
	DEALLOCATE PREPARE Stmt; 
END$$
DELIMITER ;
