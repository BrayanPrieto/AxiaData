-- MySQL Data Warehouse Schema for Loan Analytics (Star Schema)
-- Targets: origination trends, customer profiles, delinquency/cancellation by purpose, 
--          relationship between indebtedness and default, geo performance

SET @OLD_UNIQUE_CHECKS=@@UNIQUE_CHECKS, UNIQUE_CHECKS=0;
SET @OLD_FOREIGN_KEY_CHECKS=@@FOREIGN_KEY_CHECKS, FOREIGN_KEY_CHECKS=0;
SET @OLD_SQL_MODE=@@SQL_MODE, SQL_MODE='ONLY_FULL_GROUP_BY,STRICT_TRANS_TABLES,NO_ZERO_IN_DATE,NO_ZERO_DATE,ERROR_FOR_DIVISION_BY_ZERO,NO_ENGINE_SUBSTITUTION';

-- -----------------------------------------------------
-- Schema prestamos_dw (Dimensional Warehouse)
-- -----------------------------------------------------
CREATE DATABASE IF NOT EXISTS `prestamos_dw` DEFAULT CHARACTER SET utf8mb4 COLLATE utf8mb4_0900_ai_ci;
USE `prestamos_dw`;

-- -----------------------------------------------------
-- Dimension: Date (conformed)
-- Note: Populate via calendar ETL. date_id = YYYYMMDD
-- -----------------------------------------------------
CREATE TABLE IF NOT EXISTS `dim_date` (
  `date_id` INT NOT NULL,
  `full_date` DATE NOT NULL,
  `year` SMALLINT NOT NULL,
  `quarter` TINYINT NOT NULL,
  `quarter_name` VARCHAR(6) NOT NULL,
  `month` TINYINT NOT NULL,
  `month_name` VARCHAR(9) NOT NULL,
  `day` TINYINT NOT NULL,
  `day_of_week` TINYINT NOT NULL,
  `day_name` VARCHAR(9) NOT NULL,
  `week_of_year` TINYINT NULL DEFAULT NULL,
  PRIMARY KEY (`date_id`),
  UNIQUE KEY `uq_dim_date_full_date` (`full_date`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci;

-- -----------------------------------------------------
-- Dimension: Purpose
-- Source: prestamos_norm.dim_purpose
-- -----------------------------------------------------
CREATE TABLE IF NOT EXISTS `dim_purpose` (
  `purpose_key` INT NOT NULL AUTO_INCREMENT,
  `purpose_code` VARCHAR(64) NOT NULL,
  PRIMARY KEY (`purpose_key`),
  UNIQUE KEY `uq_dw_purpose_code` (`purpose_code`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci;

-- -----------------------------------------------------
-- Dimension: Application Type
-- Source: prestamos_norm.dim_application_type
-- -----------------------------------------------------
CREATE TABLE IF NOT EXISTS `dim_application_type` (
  `application_type_key` INT NOT NULL AUTO_INCREMENT,
  `application_type_code` VARCHAR(16) NOT NULL,
  PRIMARY KEY (`application_type_key`),
  UNIQUE KEY `uq_dw_application_type_code` (`application_type_code`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci;

-- -----------------------------------------------------
-- Dimension: Verification Status
-- Source: prestamos_norm.dim_verification_status
-- -----------------------------------------------------
CREATE TABLE IF NOT EXISTS `dim_verification_status` (
  `verification_status_key` INT NOT NULL AUTO_INCREMENT,
  `verification_status_code` VARCHAR(32) NOT NULL,
  PRIMARY KEY (`verification_status_key`),
  UNIQUE KEY `uq_dw_verification_status_code` (`verification_status_code`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci;

-- -----------------------------------------------------
-- Dimension: Policy Code
-- Source: prestamos_norm.dim_policy_code
-- -----------------------------------------------------
CREATE TABLE IF NOT EXISTS `dim_policy_code` (
  `policy_code_key` INT NOT NULL AUTO_INCREMENT,
  `policy_code` VARCHAR(16) NOT NULL,
  PRIMARY KEY (`policy_code_key`),
  UNIQUE KEY `uq_dw_policy_code` (`policy_code`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci;

-- -----------------------------------------------------
-- Dimension: Disbursement Method
-- Source: prestamos_norm.dim_disbursement_method
-- -----------------------------------------------------
CREATE TABLE IF NOT EXISTS `dim_disbursement_method` (
  `disbursement_method_key` INT NOT NULL AUTO_INCREMENT,
  `disbursement_method_code` VARCHAR(16) NOT NULL,
  PRIMARY KEY (`disbursement_method_key`),
  UNIQUE KEY `uq_dw_disbursement_method_code` (`disbursement_method_code`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci;

-- -----------------------------------------------------
-- Dimension: Grade
-- Source: prestamos_norm.dim_grade
-- -----------------------------------------------------
CREATE TABLE IF NOT EXISTS `dim_grade` (
  `grade_key` INT NOT NULL AUTO_INCREMENT,
  `grade_code` CHAR(1) NOT NULL,
  PRIMARY KEY (`grade_key`),
  UNIQUE KEY `uq_dw_grade_code` (`grade_code`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci;

-- -----------------------------------------------------
-- Dimension: Sub Grade (child of Grade)
-- Source: prestamos_norm.dim_sub_grade
-- -----------------------------------------------------
CREATE TABLE IF NOT EXISTS `dim_sub_grade` (
  `sub_grade_key` INT NOT NULL AUTO_INCREMENT,
  `sub_grade_code` CHAR(2) NOT NULL,
  `grade_key` INT NOT NULL,
  PRIMARY KEY (`sub_grade_key`),
  UNIQUE KEY `uq_dw_sub_grade_code` (`sub_grade_code`),
  KEY `fk_dw_sub_grade_grade` (`grade_key`),
  CONSTRAINT `fk_dw_sub_grade_grade`
    FOREIGN KEY (`grade_key`) REFERENCES `dim_grade` (`grade_key`)
    ON DELETE RESTRICT ON UPDATE RESTRICT
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci;

-- -----------------------------------------------------
-- Dimension: Term (months)
-- Source: prestamos_norm.dim_term
-- -----------------------------------------------------
CREATE TABLE IF NOT EXISTS `dim_term` (
  `term_key` INT NOT NULL AUTO_INCREMENT,
  `term_months` TINYINT NOT NULL,
  PRIMARY KEY (`term_key`),
  UNIQUE KEY `uq_dw_term_months` (`term_months`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci;

-- -----------------------------------------------------
-- Dimension: Home Ownership
-- Source: prestamos_norm.dim_home_ownership
-- -----------------------------------------------------
CREATE TABLE IF NOT EXISTS `dim_home_ownership` (
  `home_ownership_key` INT NOT NULL AUTO_INCREMENT,
  `home_ownership_code` VARCHAR(16) NOT NULL,
  PRIMARY KEY (`home_ownership_key`),
  UNIQUE KEY `uq_dw_home_ownership_code` (`home_ownership_code`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci;

-- -----------------------------------------------------
-- Dimension: Employment Length
-- Source: prestamos_norm.dim_emp_length
-- -----------------------------------------------------
CREATE TABLE IF NOT EXISTS `dim_employment_length` (
  `employment_length_key` INT NOT NULL AUTO_INCREMENT,
  `years` TINYINT NULL DEFAULT NULL,
  `original_text` VARCHAR(32) NOT NULL,
  PRIMARY KEY (`employment_length_key`),
  UNIQUE KEY `uq_dw_emp_length_text` (`original_text`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci;

-- -----------------------------------------------------
-- Dimension: Location (state, zip3)
-- Source: prestamos_norm.application_address + dim_state + dim_zip3
-- Note: Single conformed dim to avoid snowflaking
-- -----------------------------------------------------
CREATE TABLE IF NOT EXISTS `dim_location` (
  `location_key` INT NOT NULL AUTO_INCREMENT,
  `state_code` VARCHAR(16) NULL DEFAULT NULL,
  `zip3` CHAR(3) NULL DEFAULT NULL,
  PRIMARY KEY (`location_key`),
  UNIQUE KEY `uq_dw_location_state_zip3` (`state_code`, `zip3`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci;

-- -----------------------------------------------------
-- Dimension: Loan Status (performance)
-- Source: prestamos_norm.dim_loan_status
-- -----------------------------------------------------
CREATE TABLE IF NOT EXISTS `dim_loan_status` (
  `loan_status_key` INT NOT NULL AUTO_INCREMENT,
  `loan_status_code` VARCHAR(64) NOT NULL,
  PRIMARY KEY (`loan_status_key`),
  UNIQUE KEY `uq_dw_loan_status_code` (`loan_status_code`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci;

-- -----------------------------------------------------
-- Dimension: Settlement Status (optional)
-- Source: prestamos_norm.dim_settlement_status
-- -----------------------------------------------------
CREATE TABLE IF NOT EXISTS `dim_settlement_status` (
  `settlement_status_key` INT NOT NULL AUTO_INCREMENT,
  `settlement_status_code` VARCHAR(64) NOT NULL,
  PRIMARY KEY (`settlement_status_key`),
  UNIQUE KEY `uq_dw_settlement_status_code` (`settlement_status_code`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci;

-- -----------------------------------------------------
-- Dimension: Hardship Type/Status (optional for stress cases)
-- Source: prestamos_norm.dim_hardship_type/status/loan_status
-- -----------------------------------------------------
CREATE TABLE IF NOT EXISTS `dim_hardship_type` (
  `hardship_type_key` INT NOT NULL AUTO_INCREMENT,
  `hardship_type_code` VARCHAR(64) NOT NULL,
  PRIMARY KEY (`hardship_type_key`),
  UNIQUE KEY `uq_dw_hardship_type_code` (`hardship_type_code`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci;

CREATE TABLE IF NOT EXISTS `dim_hardship_status` (
  `hardship_status_key` INT NOT NULL AUTO_INCREMENT,
  `hardship_status_code` VARCHAR(64) NOT NULL,
  PRIMARY KEY (`hardship_status_key`),
  UNIQUE KEY `uq_dw_hardship_status_code` (`hardship_status_code`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci;

CREATE TABLE IF NOT EXISTS `dim_hardship_loan_status` (
  `hardship_loan_status_key` INT NOT NULL AUTO_INCREMENT,
  `hardship_loan_status_code` VARCHAR(64) NOT NULL,
  PRIMARY KEY (`hardship_loan_status_key`),
  UNIQUE KEY `uq_dw_hardship_loan_status_code` (`hardship_loan_status_code`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci;

-- =====================================================
-- FACTS
-- =====================================================

-- -----------------------------------------------------
-- Fact: Loan Originations (one row per loan at decision)
-- Answers: evolution of approved amount and rate, segmentation by profile
-- Grain: loan_id (decision-level)
-- -----------------------------------------------------
CREATE TABLE IF NOT EXISTS `fact_originations` (
  -- Degenerate/natural keys
  `loan_id` BIGINT NOT NULL,
  `application_id` BIGINT NULL DEFAULT NULL,

  -- Dates
  `application_date_id` INT NULL DEFAULT NULL,
  `decision_date_id` INT NULL DEFAULT NULL,

  -- Conformed dimensions
  `purpose_key` INT NULL DEFAULT NULL,
  `application_type_key` INT NULL DEFAULT NULL,
  `verification_status_key` INT NULL DEFAULT NULL,
  `policy_code_key` INT NULL DEFAULT NULL,
  `disbursement_method_key` INT NULL DEFAULT NULL,
  `grade_key` INT NULL DEFAULT NULL,
  `sub_grade_key` INT NULL DEFAULT NULL,
  `term_key` INT NULL DEFAULT NULL,
  `home_ownership_key` INT NULL DEFAULT NULL,
  `employment_length_key` INT NULL DEFAULT NULL,
  `location_key` INT NULL DEFAULT NULL,

  -- Measures (origination and applicant profile at decision)
  `requested_amount` INT NULL DEFAULT NULL,
  `funded_amount` INT NULL DEFAULT NULL,
  `funded_amount_inv` INT NULL DEFAULT NULL,
  `installment` DECIMAL(14,2) NULL DEFAULT NULL,
  `int_rate` DECIMAL(6,2) NULL DEFAULT NULL,

  `annual_inc` DECIMAL(14,2) NULL DEFAULT NULL,
  `annual_inc_joint` DECIMAL(14,2) NULL DEFAULT NULL,
  `dti` DECIMAL(6,2) NULL DEFAULT NULL,
  `dti_joint` DECIMAL(6,2) NULL DEFAULT NULL,

  -- Credit history snapshot features (for risk appetite analysis)
  `inq_last_6mths` SMALLINT NULL DEFAULT NULL,
  `delinq_2yrs` SMALLINT NULL DEFAULT NULL,
  `mths_since_last_delinq` SMALLINT NULL DEFAULT NULL,
  `mths_since_recent_inq` SMALLINT NULL DEFAULT NULL,
  `pub_rec` SMALLINT NULL DEFAULT NULL,
  `total_acc` SMALLINT NULL DEFAULT NULL,
  `open_acc` SMALLINT NULL DEFAULT NULL,
  `revol_bal` INT NULL DEFAULT NULL,
  `revol_util` DECIMAL(6,1) NULL DEFAULT NULL,

  PRIMARY KEY (`loan_id`),
  KEY `idx_factorg_app_dt` (`application_date_id`),
  KEY `idx_factorg_decision_dt` (`decision_date_id`),
  KEY `idx_factorg_purpose` (`purpose_key`),
  KEY `idx_factorg_grade` (`grade_key`,`sub_grade_key`),
  KEY `idx_factorg_term` (`term_key`),
  KEY `idx_factorg_geo` (`location_key`),
  CONSTRAINT `fk_factorg_app_date` FOREIGN KEY (`application_date_id`) REFERENCES `dim_date` (`date_id`),
  CONSTRAINT `fk_factorg_decision_date` FOREIGN KEY (`decision_date_id`) REFERENCES `dim_date` (`date_id`),
  CONSTRAINT `fk_factorg_purpose` FOREIGN KEY (`purpose_key`) REFERENCES `dim_purpose` (`purpose_key`),
  CONSTRAINT `fk_factorg_application_type` FOREIGN KEY (`application_type_key`) REFERENCES `dim_application_type` (`application_type_key`),
  CONSTRAINT `fk_factorg_ver_status` FOREIGN KEY (`verification_status_key`) REFERENCES `dim_verification_status` (`verification_status_key`),
  CONSTRAINT `fk_factorg_policy_code` FOREIGN KEY (`policy_code_key`) REFERENCES `dim_policy_code` (`policy_code_key`),
  CONSTRAINT `fk_factorg_disb_method` FOREIGN KEY (`disbursement_method_key`) REFERENCES `dim_disbursement_method` (`disbursement_method_key`),
  CONSTRAINT `fk_factorg_grade` FOREIGN KEY (`grade_key`) REFERENCES `dim_grade` (`grade_key`),
  CONSTRAINT `fk_factorg_sub_grade` FOREIGN KEY (`sub_grade_key`) REFERENCES `dim_sub_grade` (`sub_grade_key`),
  CONSTRAINT `fk_factorg_term` FOREIGN KEY (`term_key`) REFERENCES `dim_term` (`term_key`),
  CONSTRAINT `fk_factorg_home_ownership` FOREIGN KEY (`home_ownership_key`) REFERENCES `dim_home_ownership` (`home_ownership_key`),
  CONSTRAINT `fk_factorg_emp_length` FOREIGN KEY (`employment_length_key`) REFERENCES `dim_employment_length` (`employment_length_key`),
  CONSTRAINT `fk_factorg_location` FOREIGN KEY (`location_key`) REFERENCES `dim_location` (`location_key`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci COMMENT='Origination-level fact at loan decision';

-- -----------------------------------------------------
-- Fact: Loan Performance Snapshot (one row per loan at export)
-- Answers: delinquency, cancellations/charge-offs, recoveries by purpose/geo, etc.
-- Grain: loan_id (latest known snapshot)
-- -----------------------------------------------------
CREATE TABLE IF NOT EXISTS `fact_performance_snapshot` (
  `loan_id` BIGINT NOT NULL,

  -- Conformed and time dimensions
  `loan_status_key` INT NULL DEFAULT NULL,
  `last_payment_date_id` INT NULL DEFAULT NULL,
  `next_payment_date_id` INT NULL DEFAULT NULL,

  -- Financial performance measures
  `last_pymnt_amnt` DECIMAL(14,2) NULL DEFAULT NULL,
  `total_pymnt` DECIMAL(16,2) NULL DEFAULT NULL,
  `total_pymnt_inv` DECIMAL(16,2) NULL DEFAULT NULL,
  `total_rec_prncp` DECIMAL(16,2) NULL DEFAULT NULL,
  `total_rec_int` DECIMAL(16,2) NULL DEFAULT NULL,
  `total_rec_late_fee` DECIMAL(14,2) NULL DEFAULT NULL,
  `recoveries` DECIMAL(14,2) NULL DEFAULT NULL,
  `collection_recovery_fee` DECIMAL(14,2) NULL DEFAULT NULL,
  `out_prncp` DECIMAL(16,2) NULL DEFAULT NULL,
  `out_prncp_inv` DECIMAL(16,2) NULL DEFAULT NULL,
  `pymnt_plan` TINYINT NULL DEFAULT NULL,

  -- Settlement (optional)
  `settlement_status_key` INT NULL DEFAULT NULL,
  `debt_settlement_flag` TINYINT NULL DEFAULT NULL,
  `settlement_amount` DECIMAL(14,2) NULL DEFAULT NULL,
  `settlement_percentage` DECIMAL(6,2) NULL DEFAULT NULL,
  `settlement_date_id` INT NULL DEFAULT NULL,

  -- Hardship (optional)
  `hardship_type_key` INT NULL DEFAULT NULL,
  `hardship_status_key` INT NULL DEFAULT NULL,
  `hardship_loan_status_key` INT NULL DEFAULT NULL,
  `hardship_amount` DECIMAL(14,2) NULL DEFAULT NULL,
  `hardship_start_date_id` INT NULL DEFAULT NULL,
  `hardship_end_date_id` INT NULL DEFAULT NULL,
  `payment_plan_start_date_id` INT NULL DEFAULT NULL,
  `deferral_term` SMALLINT NULL DEFAULT NULL,
  `hardship_length` SMALLINT NULL DEFAULT NULL,
  `hardship_dpd` SMALLINT NULL DEFAULT NULL,

  PRIMARY KEY (`loan_id`),
  KEY `idx_factperf_status` (`loan_status_key`),
  KEY `idx_factperf_last_pay_dt` (`last_payment_date_id`),
  KEY `idx_factperf_next_pay_dt` (`next_payment_date_id`),
  CONSTRAINT `fk_factperf_loan_status` FOREIGN KEY (`loan_status_key`) REFERENCES `dim_loan_status` (`loan_status_key`),
  CONSTRAINT `fk_factperf_last_pay_dt` FOREIGN KEY (`last_payment_date_id`) REFERENCES `dim_date` (`date_id`),
  CONSTRAINT `fk_factperf_next_pay_dt` FOREIGN KEY (`next_payment_date_id`) REFERENCES `dim_date` (`date_id`),
  CONSTRAINT `fk_factperf_settlement_status` FOREIGN KEY (`settlement_status_key`) REFERENCES `dim_settlement_status` (`settlement_status_key`),
  CONSTRAINT `fk_factperf_settlement_date` FOREIGN KEY (`settlement_date_id`) REFERENCES `dim_date` (`date_id`),
  CONSTRAINT `fk_factperf_hardship_type` FOREIGN KEY (`hardship_type_key`) REFERENCES `dim_hardship_type` (`hardship_type_key`),
  CONSTRAINT `fk_factperf_hardship_status` FOREIGN KEY (`hardship_status_key`) REFERENCES `dim_hardship_status` (`hardship_status_key`),
  CONSTRAINT `fk_factperf_hardship_loan_status` FOREIGN KEY (`hardship_loan_status_key`) REFERENCES `dim_hardship_loan_status` (`hardship_loan_status_key`),
  CONSTRAINT `fk_factperf_hardship_start_dt` FOREIGN KEY (`hardship_start_date_id`) REFERENCES `dim_date` (`date_id`),
  CONSTRAINT `fk_factperf_hardship_end_dt` FOREIGN KEY (`hardship_end_date_id`) REFERENCES `dim_date` (`date_id`),
  CONSTRAINT `fk_factperf_payplan_start_dt` FOREIGN KEY (`payment_plan_start_date_id`) REFERENCES `dim_date` (`date_id`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci COMMENT='Latest performance snapshot by loan';

-- -----------------------------------------------------
-- Notes for ETL mapping (guidance)
--  - Load conformed dims from prestamos_norm.* reference tables (1:1 code copies) to DW dims.
--  - Build dim_location by joining prestamos_norm.application_address -> dim_state/zip3.
--  - fact_originations joins:
--      loan -> application -> application_date/decision_date
--      loan_terms for requested/funded/installment/int_rate
--      applicant_financials_snapshot for income/dti
--      credit_history_snapshot for risk variables
--      employment for home_ownership/emp_length
--      application for purpose/application_type/verification_status/policy/disbursement
--  - fact_performance_snapshot joins:
--      payment_status_snapshot for payment metrics and dates
--      loan for current loan_status
--      settlement_case, hardship_case for optional attributes

SET SQL_MODE=@OLD_SQL_MODE;
SET FOREIGN_KEY_CHECKS=@OLD_FOREIGN_KEY_CHECKS;
SET UNIQUE_CHECKS=@OLD_UNIQUE_CHECKS;


