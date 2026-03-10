-- SQL Server Data Warehouse Schema for Loan Analytics (Star Schema)
-- Targets: origination trends, profiles, delinquency/cancellation, DTI vs default, geo performance

IF DB_ID(N'prestamos_dw') IS NULL
BEGIN
    CREATE DATABASE [prestamos_dw];
END
GO

USE [prestamos_dw];
GO

-- ======================
-- Dimensions
-- ======================

-- dim_date (conformed calendar) - date_id = YYYYMMDD
IF OBJECT_ID(N'dbo.dim_date', N'U') IS NULL
CREATE TABLE dbo.dim_date (
    date_id INT NOT NULL CONSTRAINT PK_dim_date PRIMARY KEY,
    full_date DATE NOT NULL,
    [year] SMALLINT NOT NULL,
    [quarter] TINYINT NOT NULL,
    quarter_name VARCHAR(6) NOT NULL,
    [month] TINYINT NOT NULL,
    month_name VARCHAR(9) NOT NULL,
    [day] TINYINT NOT NULL,
    day_of_week TINYINT NOT NULL,
    day_name VARCHAR(9) NOT NULL,
    week_of_year TINYINT NULL,
    CONSTRAINT UQ_dim_date_full_date UNIQUE (full_date)
);
GO

-- dim_purpose
IF OBJECT_ID(N'dbo.dim_purpose', N'U') IS NULL
CREATE TABLE dbo.dim_purpose (
    purpose_key INT IDENTITY(1,1) NOT NULL CONSTRAINT PK_dim_purpose PRIMARY KEY,
    purpose_code VARCHAR(64) NOT NULL,
    CONSTRAINT UQ_dim_purpose_code UNIQUE (purpose_code)
);
GO

-- dim_application_type
IF OBJECT_ID(N'dbo.dim_application_type', N'U') IS NULL
CREATE TABLE dbo.dim_application_type (
    application_type_key INT IDENTITY(1,1) NOT NULL CONSTRAINT PK_dim_application_type PRIMARY KEY,
    application_type_code VARCHAR(16) NOT NULL,
    CONSTRAINT UQ_dim_application_type_code UNIQUE (application_type_code)
);
GO

-- dim_verification_status
IF OBJECT_ID(N'dbo.dim_verification_status', N'U') IS NULL
CREATE TABLE dbo.dim_verification_status (
    verification_status_key INT IDENTITY(1,1) NOT NULL CONSTRAINT PK_dim_verification_status PRIMARY KEY,
    verification_status_code VARCHAR(32) NOT NULL,
    CONSTRAINT UQ_dim_verification_status_code UNIQUE (verification_status_code)
);
GO

-- dim_policy_code
IF OBJECT_ID(N'dbo.dim_policy_code', N'U') IS NULL
CREATE TABLE dbo.dim_policy_code (
    policy_code_key INT IDENTITY(1,1) NOT NULL CONSTRAINT PK_dim_policy_code PRIMARY KEY,
    policy_code VARCHAR(16) NOT NULL,
    CONSTRAINT UQ_dim_policy_code UNIQUE (policy_code)
);
GO

-- dim_disbursement_method
IF OBJECT_ID(N'dbo.dim_disbursement_method', N'U') IS NULL
CREATE TABLE dbo.dim_disbursement_method (
    disbursement_method_key INT IDENTITY(1,1) NOT NULL CONSTRAINT PK_dim_disbursement_method PRIMARY KEY,
    disbursement_method_code VARCHAR(16) NOT NULL,
    CONSTRAINT UQ_dim_disbursement_method_code UNIQUE (disbursement_method_code)
);
GO

-- dim_grade
IF OBJECT_ID(N'dbo.dim_grade', N'U') IS NULL
CREATE TABLE dbo.dim_grade (
    grade_key INT IDENTITY(1,1) NOT NULL CONSTRAINT PK_dim_grade PRIMARY KEY,
    grade_code CHAR(1) NOT NULL,
    CONSTRAINT UQ_dim_grade_code UNIQUE (grade_code)
);
GO

-- dim_sub_grade
IF OBJECT_ID(N'dbo.dim_sub_grade', N'U') IS NULL
CREATE TABLE dbo.dim_sub_grade (
    sub_grade_key INT IDENTITY(1,1) NOT NULL CONSTRAINT PK_dim_sub_grade PRIMARY KEY,
    sub_grade_code CHAR(2) NOT NULL,
    grade_key INT NOT NULL,
    CONSTRAINT UQ_dim_sub_grade_code UNIQUE (sub_grade_code),
    CONSTRAINT FK_dim_sub_grade_grade FOREIGN KEY (grade_key) REFERENCES dbo.dim_grade(grade_key)
);
GO

-- dim_term
IF OBJECT_ID(N'dbo.dim_term', N'U') IS NULL
CREATE TABLE dbo.dim_term (
    term_key INT IDENTITY(1,1) NOT NULL CONSTRAINT PK_dim_term PRIMARY KEY,
    term_months TINYINT NOT NULL,
    CONSTRAINT UQ_dim_term_months UNIQUE (term_months)
);
GO

-- dim_home_ownership
IF OBJECT_ID(N'dbo.dim_home_ownership', N'U') IS NULL
CREATE TABLE dbo.dim_home_ownership (
    home_ownership_key INT IDENTITY(1,1) NOT NULL CONSTRAINT PK_dim_home_ownership PRIMARY KEY,
    home_ownership_code VARCHAR(16) NOT NULL,
    CONSTRAINT UQ_dim_home_ownership_code UNIQUE (home_ownership_code)
);
GO

-- dim_employment_length
IF OBJECT_ID(N'dbo.dim_employment_length', N'U') IS NULL
CREATE TABLE dbo.dim_employment_length (
    employment_length_key INT IDENTITY(1,1) NOT NULL CONSTRAINT PK_dim_employment_length PRIMARY KEY,
    years TINYINT NULL,
    original_text VARCHAR(32) NOT NULL,
    CONSTRAINT UQ_dim_employment_length_text UNIQUE (original_text)
);
GO

-- dim_location (state, zip3)
IF OBJECT_ID(N'dbo.dim_location', N'U') IS NULL
CREATE TABLE dbo.dim_location (
    location_key INT IDENTITY(1,1) NOT NULL CONSTRAINT PK_dim_location PRIMARY KEY,
    state_code VARCHAR(16) NULL,
    zip3 CHAR(3) NULL,
    CONSTRAINT UQ_dim_location_state_zip3 UNIQUE (state_code, zip3)
);
GO

-- dim_loan_status
IF OBJECT_ID(N'dbo.dim_loan_status', N'U') IS NULL
CREATE TABLE dbo.dim_loan_status (
    loan_status_key INT IDENTITY(1,1) NOT NULL CONSTRAINT PK_dim_loan_status PRIMARY KEY,
    loan_status_code VARCHAR(64) NOT NULL,
    CONSTRAINT UQ_dim_loan_status_code UNIQUE (loan_status_code)
);
GO

-- dim_settlement_status
IF OBJECT_ID(N'dbo.dim_settlement_status', N'U') IS NULL
CREATE TABLE dbo.dim_settlement_status (
    settlement_status_key INT IDENTITY(1,1) NOT NULL CONSTRAINT PK_dim_settlement_status PRIMARY KEY,
    settlement_status_code VARCHAR(64) NOT NULL,
    CONSTRAINT UQ_dim_settlement_status_code UNIQUE (settlement_status_code)
);
GO

-- dim_hardship_type
IF OBJECT_ID(N'dbo.dim_hardship_type', N'U') IS NULL
CREATE TABLE dbo.dim_hardship_type (
    hardship_type_key INT IDENTITY(1,1) NOT NULL CONSTRAINT PK_dim_hardship_type PRIMARY KEY,
    hardship_type_code VARCHAR(64) NOT NULL,
    CONSTRAINT UQ_dim_hardship_type_code UNIQUE (hardship_type_code)
);
GO

-- dim_hardship_status
IF OBJECT_ID(N'dbo.dim_hardship_status', N'U') IS NULL
CREATE TABLE dbo.dim_hardship_status (
    hardship_status_key INT IDENTITY(1,1) NOT NULL CONSTRAINT PK_dim_hardship_status PRIMARY KEY,
    hardship_status_code VARCHAR(64) NOT NULL,
    CONSTRAINT UQ_dim_hardship_status_code UNIQUE (hardship_status_code)
);
GO

-- dim_hardship_loan_status
IF OBJECT_ID(N'dbo.dim_hardship_loan_status', N'U') IS NULL
CREATE TABLE dbo.dim_hardship_loan_status (
    hardship_loan_status_key INT IDENTITY(1,1) NOT NULL CONSTRAINT PK_dim_hardship_loan_status PRIMARY KEY,
    hardship_loan_status_code VARCHAR(64) NOT NULL,
    CONSTRAINT UQ_dim_hardship_loan_status_code UNIQUE (hardship_loan_status_code)
);
GO

-- ======================
-- Facts
-- ======================

-- fact_originations (one row per loan at decision)
IF OBJECT_ID(N'dbo.fact_originations', N'U') IS NULL
CREATE TABLE dbo.fact_originations (
    loan_id BIGINT NOT NULL CONSTRAINT PK_fact_originations PRIMARY KEY,
    application_id BIGINT NULL,

    application_date_id INT NULL,
    decision_date_id INT NULL,

    purpose_key INT NULL,
    application_type_key INT NULL,
    verification_status_key INT NULL,
    policy_code_key INT NULL,
    disbursement_method_key INT NULL,
    grade_key INT NULL,
    sub_grade_key INT NULL,
    term_key INT NULL,
    home_ownership_key INT NULL,
    employment_length_key INT NULL,
    location_key INT NULL,

    requested_amount INT NULL,
    funded_amount INT NULL,
    funded_amount_inv INT NULL,
    installment DECIMAL(14,2) NULL,
    int_rate DECIMAL(6,2) NULL,

    annual_inc DECIMAL(14,2) NULL,
    annual_inc_joint DECIMAL(14,2) NULL,
    dti DECIMAL(6,2) NULL,
    dti_joint DECIMAL(6,2) NULL,

    inq_last_6mths SMALLINT NULL,
    delinq_2yrs SMALLINT NULL,
    mths_since_last_delinq SMALLINT NULL,
    mths_since_recent_inq SMALLINT NULL,
    pub_rec SMALLINT NULL,
    total_acc SMALLINT NULL,
    open_acc SMALLINT NULL,
    revol_bal INT NULL,
    revol_util DECIMAL(6,1) NULL,

    CONSTRAINT FK_factorg_app_date     FOREIGN KEY (application_date_id)   REFERENCES dbo.dim_date(date_id),
    CONSTRAINT FK_factorg_decision_dt  FOREIGN KEY (decision_date_id)      REFERENCES dbo.dim_date(date_id),
    CONSTRAINT FK_factorg_purpose      FOREIGN KEY (purpose_key)           REFERENCES dbo.dim_purpose(purpose_key),
    CONSTRAINT FK_factorg_app_type     FOREIGN KEY (application_type_key)  REFERENCES dbo.dim_application_type(application_type_key),
    CONSTRAINT FK_factorg_ver_status   FOREIGN KEY (verification_status_key) REFERENCES dbo.dim_verification_status(verification_status_key),
    CONSTRAINT FK_factorg_policy_code  FOREIGN KEY (policy_code_key)       REFERENCES dbo.dim_policy_code(policy_code_key),
    CONSTRAINT FK_factorg_disb_method  FOREIGN KEY (disbursement_method_key) REFERENCES dbo.dim_disbursement_method(disbursement_method_key),
    CONSTRAINT FK_factorg_grade        FOREIGN KEY (grade_key)             REFERENCES dbo.dim_grade(grade_key),
    CONSTRAINT FK_factorg_sub_grade    FOREIGN KEY (sub_grade_key)         REFERENCES dbo.dim_sub_grade(sub_grade_key),
    CONSTRAINT FK_factorg_term         FOREIGN KEY (term_key)              REFERENCES dbo.dim_term(term_key),
    CONSTRAINT FK_factorg_home_owner   FOREIGN KEY (home_ownership_key)    REFERENCES dbo.dim_home_ownership(home_ownership_key),
    CONSTRAINT FK_factorg_emp_length   FOREIGN KEY (employment_length_key) REFERENCES dbo.dim_employment_length(employment_length_key),
    CONSTRAINT FK_factorg_location     FOREIGN KEY (location_key)          REFERENCES dbo.dim_location(location_key)
);
GO

CREATE INDEX IX_factorg_app_dt   ON dbo.fact_originations (application_date_id);
CREATE INDEX IX_factorg_dec_dt   ON dbo.fact_originations (decision_date_id);
CREATE INDEX IX_factorg_purpose  ON dbo.fact_originations (purpose_key);
CREATE INDEX IX_factorg_grade    ON dbo.fact_originations (grade_key, sub_grade_key);
CREATE INDEX IX_factorg_term     ON dbo.fact_originations (term_key);
CREATE INDEX IX_factorg_geo      ON dbo.fact_originations (location_key);
GO

-- fact_performance_snapshot (latest snapshot per loan)
IF OBJECT_ID(N'dbo.fact_performance_snapshot', N'U') IS NULL
CREATE TABLE dbo.fact_performance_snapshot (
    loan_id BIGINT NOT NULL CONSTRAINT PK_fact_performance_snapshot PRIMARY KEY,

    loan_status_key INT NULL,
    last_payment_date_id INT NULL,
    next_payment_date_id INT NULL,

    last_pymnt_amnt DECIMAL(14,2) NULL,
    total_pymnt DECIMAL(16,2) NULL,
    total_pymnt_inv DECIMAL(16,2) NULL,
    total_rec_prncp DECIMAL(16,2) NULL,
    total_rec_int DECIMAL(16,2) NULL,
    total_rec_late_fee DECIMAL(14,2) NULL,
    recoveries DECIMAL(14,2) NULL,
    collection_recovery_fee DECIMAL(14,2) NULL,
    out_prncp DECIMAL(16,2) NULL,
    out_prncp_inv DECIMAL(16,2) NULL,
    pymnt_plan BIT NULL,

    settlement_status_key INT NULL,
    debt_settlement_flag BIT NULL,
    settlement_amount DECIMAL(14,2) NULL,
    settlement_percentage DECIMAL(6,2) NULL,
    settlement_date_id INT NULL,

    hardship_type_key INT NULL,
    hardship_status_key INT NULL,
    hardship_loan_status_key INT NULL,
    hardship_amount DECIMAL(14,2) NULL,
    hardship_start_date_id INT NULL,
    hardship_end_date_id INT NULL,
    payment_plan_start_date_id INT NULL,
    deferral_term SMALLINT NULL,
    hardship_length SMALLINT NULL,
    hardship_dpd SMALLINT NULL,

    CONSTRAINT FK_factperf_loan_status     FOREIGN KEY (loan_status_key)        REFERENCES dbo.dim_loan_status(loan_status_key),
    CONSTRAINT FK_factperf_last_pay_dt     FOREIGN KEY (last_payment_date_id)   REFERENCES dbo.dim_date(date_id),
    CONSTRAINT FK_factperf_next_pay_dt     FOREIGN KEY (next_payment_date_id)   REFERENCES dbo.dim_date(date_id),
    CONSTRAINT FK_factperf_settlement_stat FOREIGN KEY (settlement_status_key)  REFERENCES dbo.dim_settlement_status(settlement_status_key),
    CONSTRAINT FK_factperf_settlement_dt   FOREIGN KEY (settlement_date_id)     REFERENCES dbo.dim_date(date_id),
    CONSTRAINT FK_factperf_hardship_type   FOREIGN KEY (hardship_type_key)      REFERENCES dbo.dim_hardship_type(hardship_type_key),
    CONSTRAINT FK_factperf_hardship_status FOREIGN KEY (hardship_status_key)    REFERENCES dbo.dim_hardship_status(hardship_status_key),
    CONSTRAINT FK_factperf_hardship_loan   FOREIGN KEY (hardship_loan_status_key) REFERENCES dbo.dim_hardship_loan_status(hardship_loan_status_key),
    CONSTRAINT FK_factperf_hardship_start  FOREIGN KEY (hardship_start_date_id) REFERENCES dbo.dim_date(date_id),
    CONSTRAINT FK_factperf_hardship_end    FOREIGN KEY (hardship_end_date_id)   REFERENCES dbo.dim_date(date_id),
    CONSTRAINT FK_factperf_payplan_start   FOREIGN KEY (payment_plan_start_date_id) REFERENCES dbo.dim_date(date_id)
);
GO

CREATE INDEX IX_factperf_status     ON dbo.fact_performance_snapshot (loan_status_key);
CREATE INDEX IX_factperf_last_pay   ON dbo.fact_performance_snapshot (last_payment_date_id);
CREATE INDEX IX_factperf_next_pay   ON dbo.fact_performance_snapshot (next_payment_date_id);
GO


