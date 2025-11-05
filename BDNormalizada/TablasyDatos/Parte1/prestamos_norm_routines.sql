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
-- Temporary view structure for view `vw_loan_fact`
--

DROP TABLE IF EXISTS `vw_loan_fact`;
/*!50001 DROP VIEW IF EXISTS `vw_loan_fact`*/;
SET @saved_cs_client     = @@character_set_client;
/*!50503 SET character_set_client = utf8mb4 */;
/*!50001 CREATE VIEW `vw_loan_fact` AS SELECT 
 1 AS `loan_id`,
 1 AS `application_id`,
 1 AS `application_date`,
 1 AS `loan_status_code`,
 1 AS `grade_code`,
 1 AS `sub_grade_code`,
 1 AS `term_months`,
 1 AS `requested_amount`,
 1 AS `funded_amount`,
 1 AS `int_rate`,
 1 AS `last_pymnt_d`,
 1 AS `next_pymnt_d`,
 1 AS `out_prncp`,
 1 AS `total_pymnt`,
 1 AS `state_code`,
 1 AS `zip3`*/;
SET character_set_client = @saved_cs_client;

--
-- Temporary view structure for view `vw_borrower_capacity`
--

DROP TABLE IF EXISTS `vw_borrower_capacity`;
/*!50001 DROP VIEW IF EXISTS `vw_borrower_capacity`*/;
SET @saved_cs_client     = @@character_set_client;
/*!50503 SET character_set_client = utf8mb4 */;
/*!50001 CREATE VIEW `vw_borrower_capacity` AS SELECT 
 1 AS `application_id`,
 1 AS `annual_inc`,
 1 AS `dti`,
 1 AS `installment`,
 1 AS `est_disposable_monthly`*/;
SET character_set_client = @saved_cs_client;

--
-- Final view structure for view `vw_loan_fact`
--

/*!50001 DROP VIEW IF EXISTS `vw_loan_fact`*/;
/*!50001 SET @saved_cs_client          = @@character_set_client */;
/*!50001 SET @saved_cs_results         = @@character_set_results */;
/*!50001 SET @saved_col_connection     = @@collation_connection */;
/*!50001 SET character_set_client      = utf8mb4 */;
/*!50001 SET character_set_results     = utf8mb4 */;
/*!50001 SET collation_connection      = utf8mb4_0900_ai_ci */;
/*!50001 CREATE ALGORITHM=UNDEFINED */
/*!50013 DEFINER=`root`@`localhost` SQL SECURITY DEFINER */
/*!50001 VIEW `vw_loan_fact` AS select `l`.`loan_id` AS `loan_id`,`a`.`application_id` AS `application_id`,`a`.`application_date` AS `application_date`,`ls`.`loan_status_code` AS `loan_status_code`,`g`.`grade_code` AS `grade_code`,`sg`.`sub_grade_code` AS `sub_grade_code`,`t`.`term_months` AS `term_months`,`lt`.`requested_amount` AS `requested_amount`,`lt`.`funded_amount` AS `funded_amount`,`lt`.`int_rate` AS `int_rate`,`ps`.`last_pymnt_d` AS `last_pymnt_d`,`ps`.`next_pymnt_d` AS `next_pymnt_d`,`ps`.`out_prncp` AS `out_prncp`,`ps`.`total_pymnt` AS `total_pymnt`,`st`.`state_code` AS `state_code`,`z`.`zip3` AS `zip3` from ((((((((((`loan` `l` join `application` `a` on((`a`.`application_id` = `l`.`application_id`))) left join `loan_terms` `lt` on((`lt`.`loan_id` = `l`.`loan_id`))) left join `payment_status_snapshot` `ps` on((`ps`.`loan_id` = `l`.`loan_id`))) left join `dim_loan_status` `ls` on((`ls`.`loan_status_id` = `l`.`loan_status_id`))) left join `dim_grade` `g` on((`g`.`grade_id` = `l`.`grade_id`))) left join `dim_sub_grade` `sg` on((`sg`.`sub_grade_id` = `l`.`sub_grade_id`))) left join `dim_term` `t` on((`t`.`term_id` = `l`.`term_id`))) left join `application_address` `aa` on((`aa`.`application_id` = `a`.`application_id`))) left join `dim_state` `st` on((`st`.`state_id` = `aa`.`state_id`))) left join `dim_zip3` `z` on((`z`.`zip3_id` = `aa`.`zip3_id`))) */;
/*!50001 SET character_set_client      = @saved_cs_client */;
/*!50001 SET character_set_results     = @saved_cs_results */;
/*!50001 SET collation_connection      = @saved_col_connection */;

--
-- Final view structure for view `vw_borrower_capacity`
--

/*!50001 DROP VIEW IF EXISTS `vw_borrower_capacity`*/;
/*!50001 SET @saved_cs_client          = @@character_set_client */;
/*!50001 SET @saved_cs_results         = @@character_set_results */;
/*!50001 SET @saved_col_connection     = @@collation_connection */;
/*!50001 SET character_set_client      = utf8mb4 */;
/*!50001 SET character_set_results     = utf8mb4 */;
/*!50001 SET collation_connection      = utf8mb4_0900_ai_ci */;
/*!50001 CREATE ALGORITHM=UNDEFINED */
/*!50013 DEFINER=`root`@`localhost` SQL SECURITY DEFINER */
/*!50001 VIEW `vw_borrower_capacity` AS select `a`.`application_id` AS `application_id`,`af`.`annual_inc` AS `annual_inc`,`af`.`dti` AS `dti`,`lt`.`installment` AS `installment`,(case when ((`af`.`annual_inc` is not null) and (`lt`.`installment` is not null)) then ((`af`.`annual_inc` / 12) - `lt`.`installment`) else NULL end) AS `est_disposable_monthly` from (((`application` `a` left join `applicant_financials_snapshot` `af` on((`af`.`application_id` = `a`.`application_id`))) left join `loan` `l` on((`l`.`application_id` = `a`.`application_id`))) left join `loan_terms` `lt` on((`lt`.`loan_id` = `l`.`loan_id`))) */;
/*!50001 SET character_set_client      = @saved_cs_client */;
/*!50001 SET character_set_results     = @saved_cs_results */;
/*!50001 SET collation_connection      = @saved_col_connection */;
/*!40103 SET TIME_ZONE=@OLD_TIME_ZONE */;

/*!40101 SET SQL_MODE=@OLD_SQL_MODE */;
/*!40014 SET FOREIGN_KEY_CHECKS=@OLD_FOREIGN_KEY_CHECKS */;
/*!40014 SET UNIQUE_CHECKS=@OLD_UNIQUE_CHECKS */;
/*!40101 SET CHARACTER_SET_CLIENT=@OLD_CHARACTER_SET_CLIENT */;
/*!40101 SET CHARACTER_SET_RESULTS=@OLD_CHARACTER_SET_RESULTS */;
/*!40101 SET COLLATION_CONNECTION=@OLD_COLLATION_CONNECTION */;
/*!40111 SET SQL_NOTES=@OLD_SQL_NOTES */;

-- Dump completed on 2025-09-05 15:21:19
