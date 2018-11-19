-- MySQL dump 10.13  Distrib 5.5.60, for Linux (x86_64)
--
-- Host: 192.168.1.221    Database: gt_gw_main
-- ------------------------------------------------------
-- Server version	5.5.60

/*!40101 SET @OLD_CHARACTER_SET_CLIENT=@@CHARACTER_SET_CLIENT */;
/*!40101 SET @OLD_CHARACTER_SET_RESULTS=@@CHARACTER_SET_RESULTS */;
/*!40101 SET @OLD_COLLATION_CONNECTION=@@COLLATION_CONNECTION */;
/*!40101 SET NAMES utf8 */;
/*!40103 SET @OLD_TIME_ZONE=@@TIME_ZONE */;
/*!40103 SET TIME_ZONE='+00:00' */;
/*!40014 SET @OLD_UNIQUE_CHECKS=@@UNIQUE_CHECKS, UNIQUE_CHECKS=0 */;
/*!40014 SET @OLD_FOREIGN_KEY_CHECKS=@@FOREIGN_KEY_CHECKS, FOREIGN_KEY_CHECKS=0 */;
/*!40101 SET @OLD_SQL_MODE=@@SQL_MODE, SQL_MODE='NO_AUTO_VALUE_ON_ZERO' */;
/*!40111 SET @OLD_SQL_NOTES=@@SQL_NOTES, SQL_NOTES=0 */;

--
-- Table structure for table `sp_version`
--

DROP TABLE IF EXISTS `sp_version`;
/*!40101 SET @saved_cs_client     = @@character_set_client */;
/*!40101 SET character_set_client = utf8 */;
CREATE TABLE `sp_version` (
  `SP_VERSION` varchar(10) DEFAULT NULL,
  `LTE_SCHEMA` char(10) DEFAULT NULL,
  `LTE_NT` char(10) DEFAULT NULL,
  `LTE_POS` char(10) DEFAULT NULL,
  `LTE_RPT` char(15) DEFAULT NULL,
  `UMTS_SCHEMA` char(10) DEFAULT NULL,
  `UMTS_NT` char(10) DEFAULT NULL,
  `UMTS_POS` char(10) DEFAULT NULL,
  `UMTS_RPT` char(10) DEFAULT NULL,
  `GSM_SCHEMA` char(10) DEFAULT NULL,
  `GSM_NT` char(10) DEFAULT NULL,
  `GSM_POS` char(10) DEFAULT NULL,
  `GSM_RPT` char(10) DEFAULT NULL,
  `LTE_NT2` char(10) DEFAULT NULL,
  `UMTS_NT2` char(10) DEFAULT NULL,
  `GSM_NT2` char(10) DEFAULT NULL,
  `NOTE` varchar(50) DEFAULT NULL
) ENGINE=MyISAM DEFAULT CHARSET=utf8;
/*!40101 SET character_set_client = @saved_cs_client */;

--
-- Dumping data for table `sp_version`
--

LOCK TABLES `sp_version` WRITE;
/*!40000 ALTER TABLE `sp_version` DISABLE KEYS */;
INSERT INTO `sp_version` VALUES ('777_09.02','L20180628','LN20180312','LP20180430','LR20180830','','UN20180312','','','','GN20180530','','','LN20180822','UN20180822','GN20180822',NULL),(NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL);
/*!40000 ALTER TABLE `sp_version` ENABLE KEYS */;
UNLOCK TABLES;
/*!40103 SET TIME_ZONE=@OLD_TIME_ZONE */;

/*!40101 SET SQL_MODE=@OLD_SQL_MODE */;
/*!40014 SET FOREIGN_KEY_CHECKS=@OLD_FOREIGN_KEY_CHECKS */;
/*!40014 SET UNIQUE_CHECKS=@OLD_UNIQUE_CHECKS */;
/*!40101 SET CHARACTER_SET_CLIENT=@OLD_CHARACTER_SET_CLIENT */;
/*!40101 SET CHARACTER_SET_RESULTS=@OLD_CHARACTER_SET_RESULTS */;
/*!40101 SET COLLATION_CONNECTION=@OLD_COLLATION_CONNECTION */;
/*!40111 SET SQL_NOTES=@OLD_SQL_NOTES */;

-- Dump completed on 2018-11-19  3:50:01
