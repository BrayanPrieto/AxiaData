-- MySQL dump 10.13  Distrib 8.0.34, for Win64 (x86_64)
--
-- Host: localhost    Database: bdPrestamosNormalizada
-- ------------------------------------------------------
-- Server version	8.1.0

/*!40101 SET @OLD_CHARACTER_SET_CLIENT=@@CHARACTER_SET_CLIENT */;
/*!40101 SET @OLD_CHARACTER_SET_RESULTS=@@CHARACTER_SET_RESULTS */;
/*!40101 SET @OLD_COLLATION_CONNECTION=@@COLLATION_CONNECTION */;
/*!50503 SET NAMES utf8 */;
/*!40103 SET @OLD_TIME_ZONE=@@TIME_ZONE */;
/*!40103 SET TIME_ZONE='+00:00' */;
/*!40014 SET @OLD_UNIQUE_CHECKS=@@UNIQUE_CHECKS, UNIQUE_CHECKS=0 */;
/*!40014 SET @OLD_FOREIGN_KEY_CHECKS=@@FOREIGN_KEY_CHECKS, FOREIGN_KEY_CHECKS=0 */;
/*!40101 SET @OLD_SQL_MODE=@@SQL_MODE, SQL_MODE='NO_AUTO_VALUE_ON_ZERO' */;
/*!40111 SET @OLD_SQL_NOTES=@@SQL_NOTES, SQL_NOTES=0 */;

--
-- Table structure for table `dim_sub_grade`
--

DROP TABLE IF EXISTS `dim_sub_grade`;
/*!40101 SET @saved_cs_client     = @@character_set_client */;
/*!50503 SET character_set_client = utf8mb4 */;
CREATE TABLE `dim_sub_grade` (
  `sub_grade_id` int NOT NULL AUTO_INCREMENT,
  `sub_grade_code` char(2) NOT NULL,
  `grade_id` int NOT NULL,
  PRIMARY KEY (`sub_grade_id`),
  UNIQUE KEY `uq_sub_grade_code` (`sub_grade_code`),
  KEY `fk_sub_grade_grade` (`grade_id`),
  CONSTRAINT `fk_sub_grade_grade` FOREIGN KEY (`grade_id`) REFERENCES `dim_grade` (`grade_id`) ON DELETE RESTRICT ON UPDATE RESTRICT
) ENGINE=InnoDB AUTO_INCREMENT=100 DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci;
/*!40101 SET character_set_client = @saved_cs_client */;

--
-- Dumping data for table `dim_sub_grade`
--

LOCK TABLES `dim_sub_grade` WRITE;
/*!40000 ALTER TABLE `dim_sub_grade` DISABLE KEYS */;
INSERT INTO `dim_sub_grade` VALUES (37,'C4',8),(38,'C1',8),(39,'B4',9),(40,'C5',8),(41,'F1',10),(42,'C3',8),(43,'B2',9),(44,'B1',9),(45,'A2',11),(46,'B5',9),(47,'C2',8),(48,'E2',12),(49,'A4',11),(50,'E3',12),(51,'A1',11),(52,'D4',13),(53,'F3',10),(54,'D1',13),(55,'B3',9),(56,'E4',12),(57,'D3',13),(58,'D2',13),(59,'D5',13),(60,'A5',11),(61,'F2',10),(62,'E1',12),(63,'F5',10),(64,'E5',12),(65,'A3',11),(66,'G2',14),(67,'G1',14),(68,'G3',14),(69,'G4',14),(70,'F4',10),(71,'G5',14);
/*!40000 ALTER TABLE `dim_sub_grade` ENABLE KEYS */;
UNLOCK TABLES;
/*!40103 SET TIME_ZONE=@OLD_TIME_ZONE */;

/*!40101 SET SQL_MODE=@OLD_SQL_MODE */;
/*!40014 SET FOREIGN_KEY_CHECKS=@OLD_FOREIGN_KEY_CHECKS */;
/*!40014 SET UNIQUE_CHECKS=@OLD_UNIQUE_CHECKS */;
/*!40101 SET CHARACTER_SET_CLIENT=@OLD_CHARACTER_SET_CLIENT */;
/*!40101 SET CHARACTER_SET_RESULTS=@OLD_CHARACTER_SET_RESULTS */;
/*!40101 SET COLLATION_CONNECTION=@OLD_COLLATION_CONNECTION */;
/*!40111 SET SQL_NOTES=@OLD_SQL_NOTES */;

-- Dump completed on 2025-09-05 15:21:19
