#!/usr/bin/env python3
"""
Genera un dataset resumido (metrics.json) a partir de los dumps SQL
normalizados. El objetivo es alimentar el dashboard estático ubicado en
dashboard/site.
"""
from __future__ import annotations

import ast
import json
import math
import re
from dataclasses import dataclass
from pathlib import Path
from typing import Dict, Iterable, List, Sequence

import numpy as np
import pandas as pd


PROJECT_ROOT = Path(__file__).resolve().parents[1]
SQL_BASE = PROJECT_ROOT / "BDNormalizada" / "TablasyDatos"
OUTPUT_PATH = Path(__file__).with_name("data") / "metrics.json"

STATE_LABELS = {
    "AL": "Alabama",
    "AK": "Alaska",
    "AZ": "Arizona",
    "AR": "Arkansas",
    "CA": "California",
    "CO": "Colorado",
    "CT": "Connecticut",
    "DC": "Distrito de Columbia",
    "DE": "Delaware",
    "FL": "Florida",
    "GA": "Georgia",
    "HI": "Hawái",
    "IA": "Iowa",
    "ID": "Idaho",
    "IL": "Illinois",
    "IN": "Indiana",
    "KS": "Kansas",
    "KY": "Kentucky",
    "LA": "Luisiana",
    "MA": "Massachusetts",
    "MD": "Maryland",
    "ME": "Maine",
    "MI": "Míchigan",
    "MN": "Minnesota",
    "MO": "Misuri",
    "MS": "Misisipi",
    "MT": "Montana",
    "NC": "Carolina del Norte",
    "ND": "Dakota del Norte",
    "NE": "Nebraska",
    "NH": "Nuevo Hampshire",
    "NJ": "Nueva Jersey",
    "NM": "Nuevo México",
    "NV": "Nevada",
    "NY": "Nueva York",
    "OH": "Ohio",
    "OK": "Oklahoma",
    "OR": "Oregón",
    "PA": "Pensilvania",
    "PR": "Puerto Rico",
    "RI": "Rhode Island",
    "SC": "Carolina del Sur",
    "SD": "Dakota del Sur",
    "TN": "Tennessee",
    "TX": "Texas",
    "UT": "Utah",
    "VA": "Virginia",
    "VT": "Vermont",
    "WA": "Washington",
    "WI": "Wisconsin",
    "WV": "Virginia Occidental",
    "WY": "Wyoming",
    "Unspecified": "Sin dato",
    "NA": "Sin dato",
}

HOME_LABELS = {
    "MORTGAGE": "Con hipoteca",
    "RENT": "En arriendo",
    "OWN": "Propietario",
    "ANY": "Tipo indistinto",
    "UNSPECIFIED": "Sin información",
    None: "Sin información",
}


# --------------------------------------------------------------------------- #
#                               SQL PARSING                                  #
# --------------------------------------------------------------------------- #

INSERT_SPLIT_RE = re.compile(r"\),\s*\(")


def iter_insert_rows(sql_path: Path) -> Iterable[tuple]:
    """Yield tuples from INSERT statements contained in a MySQL dump."""
    buffer: List[str] = []
    with sql_path.open(encoding="utf-8") as handle:
        for raw_line in handle:
            line = raw_line.strip()
            if not line or line.startswith("--") or line.startswith("LOCK ") or line.startswith("UNLOCK "):
                continue
            if line.startswith("/*!"):
                # Comentarios con metadatos de MySQL, se pueden ignorar.
                continue
            if line.upper().startswith("INSERT INTO"):
                buffer = [line]
            elif buffer:
                buffer.append(line)
            if buffer and line.endswith(";"):
                insert_stmt = " ".join(buffer)
                values_chunk = insert_stmt.split("VALUES", 1)[1].rstrip(";").strip()
                if not values_chunk:
                    buffer = []
                    continue
                if values_chunk.startswith("(") and values_chunk.endswith(")"):
                    values_chunk = values_chunk[1:-1]
                normalized = INSERT_SPLIT_RE.sub(")\n(", values_chunk)
                for row in normalized.splitlines():
                    row = row.strip().rstrip(",")
                    if not row:
                        continue
                    if not row.startswith("("):
                        row = f"({row}"
                    if not row.endswith(")"):
                        row = f"{row})"
                    python_row = row.replace("NULL", "None")
                    yield ast.literal_eval(python_row)
                buffer = []


def load_table(sql_path: Path, columns: Sequence[str]) -> pd.DataFrame:
    """Load a dump file into a DataFrame with the provided column names."""
    data = list(iter_insert_rows(sql_path))
    return pd.DataFrame(data, columns=columns) if data else pd.DataFrame(columns=columns)


# --------------------------------------------------------------------------- #
#                          DATA PREPARATION                                   #
# --------------------------------------------------------------------------- #

@dataclass
class DimMaps:
    loan_status: Dict[int, str]
    purpose: Dict[int, str]
    emp_length: Dict[int, str]
    home_ownership: Dict[int, str]
    state: Dict[int, str]
    grade: Dict[int, str]


def load_dimensions() -> DimMaps:
    dim_loan_status = load_table(
        SQL_BASE / "Parte2" / "prestamos_norm_dim_loan_status.sql",
        ["loan_status_id", "loan_status_code"],
    )
    dim_purpose = load_table(
        SQL_BASE / "Parte2" / "prestamos_norm_dim_purpose.sql",
        ["purpose_id", "purpose_code"],
    )
    dim_emp_length = load_table(
        SQL_BASE / "Parte3" / "prestamos_norm_dim_emp_length.sql",
        ["emp_length_id", "years", "original_text"],
    )
    dim_home_ownership = load_table(
        SQL_BASE / "Parte2" / "prestamos_norm_dim_home_ownership.sql",
        ["home_ownership_id", "home_ownership_code"],
    )
    dim_state = load_table(
        SQL_BASE / "Parte1" / "prestamos_norm_dim_state.sql",
        ["state_id", "state_code"],
    )
    dim_grade = load_table(
        SQL_BASE / "Parte3" / "prestamos_norm_dim_grade.sql",
        ["grade_id", "grade_code"],
    )
    return DimMaps(
        loan_status=dict(zip(dim_loan_status.loan_status_id, dim_loan_status.loan_status_code)),
        purpose=dict(zip(dim_purpose.purpose_id, dim_purpose.purpose_code)),
        emp_length=dict(zip(dim_emp_length.emp_length_id, dim_emp_length.original_text)),
        home_ownership=dict(zip(dim_home_ownership.home_ownership_id, dim_home_ownership.home_ownership_code)),
        state=dict(zip(dim_state.state_id, dim_state.state_code)),
        grade=dict(zip(dim_grade.grade_id, dim_grade.grade_code)),
    )


def build_dataset(dim_maps: DimMaps) -> pd.DataFrame:
    """Join the main fact tables into a single analytics-ready DataFrame."""
    loan = load_table(
        SQL_BASE / "Parte4" / "prestamos_norm_loan.sql",
        ["loan_id", "application_id", "decision_id", "decision_date", "loan_status_id", "sub_grade_id", "grade_id", "term_id"],
    )[["loan_id", "application_id", "loan_status_id", "grade_id"]]
    loan["loan_status"] = loan.loan_status_id.map(dim_maps.loan_status).fillna("Unspecified")
    loan["grade_code"] = loan.grade_id.map(dim_maps.grade).fillna("NA")

    loan_terms = load_table(
        SQL_BASE / "Parte3" / "prestamos_norm_loan_terms.sql",
        ["loan_id", "requested_amount", "funded_amount", "funded_amount_inv", "installment", "int_rate"],
    )
    numeric_cols = ["requested_amount", "funded_amount", "funded_amount_inv", "installment", "int_rate"]
    loan_terms[numeric_cols] = loan_terms[numeric_cols].apply(pd.to_numeric, errors="coerce")

    application = load_table(
        SQL_BASE / "Parte2" / "prestamos_norm_application.sql",
        [
            "application_id",
            "borrower_id",
            "source_system_code",
            "external_application_key",
            "application_date",
            "verification_status_id",
            "purpose_id",
            "application_type_id",
            "policy_code_id",
            "disbursement_method_id",
            "listing_id",
        ],
    )[["application_id", "application_date", "purpose_id"]]
    application["application_date"] = pd.to_datetime(application.application_date, errors="coerce")
    application["purpose"] = application.purpose_id.map(dim_maps.purpose).fillna("other")

    applicant_fin = load_table(
        SQL_BASE / "Parte1" / "prestamos_norm_applicant_financials_snapshot.sql",
        ["application_id", "annual_inc", "annual_inc_joint", "dti", "dti_joint"],
    )[["application_id", "annual_inc", "dti"]]
    applicant_fin[["annual_inc", "dti"]] = applicant_fin[["annual_inc", "dti"]].apply(pd.to_numeric, errors="coerce")

    employment = load_table(
        SQL_BASE / "Parte2" / "prestamos_norm_employment.sql",
        ["application_id", "emp_title_id", "emp_length_id", "home_ownership_id"],
    )[["application_id", "emp_length_id", "home_ownership_id"]]
    employment["emp_length_label"] = employment.emp_length_id.map(dim_maps.emp_length).fillna("Sin dato")
    employment["home_ownership_code"] = employment.home_ownership_id.map(dim_maps.home_ownership)
    employment["home_ownership"] = employment["home_ownership_code"].map(HOME_LABELS).fillna("Sin información")

    application_address = load_table(
        SQL_BASE / "Parte1" / "prestamos_norm_application_address.sql",
        ["application_id", "state_id", "zip3_id"],
    )[["application_id", "state_id"]]
    application_address["state_code"] = application_address.state_id.map(dim_maps.state).fillna("NA")
    application_address["state_name"] = application_address["state_code"].map(STATE_LABELS).fillna("Sin dato")

    credit_history = load_table(
        SQL_BASE / "Parte3" / "prestamos_norm_credit_history_snapshot.sql",
        [
            "application_id",
            "earliest_cr_line",
            "last_credit_pull_d",
            "inq_last_6mths",
            "delinq_2yrs",
            "mths_since_last_delinq",
            "mths_since_last_record",
            "mths_since_recent_inq",
            "pub_rec",
            "total_acc",
            "open_acc",
            "revol_bal",
            "revol_util",
        ],
    )[
        ["application_id", "inq_last_6mths", "delinq_2yrs", "mths_since_recent_inq", "total_acc", "open_acc", "revol_bal", "revol_util"]
    ]
    credit_cols = ["inq_last_6mths", "delinq_2yrs", "mths_since_recent_inq", "total_acc", "open_acc", "revol_bal", "revol_util"]
    credit_history[credit_cols] = credit_history[credit_cols].apply(pd.to_numeric, errors="coerce")

    payment_status = load_table(
        SQL_BASE / "Parte3" / "prestamos_norm_payment_status_snapshot.sql",
        [
            "loan_id",
            "last_pymnt_d",
            "next_pymnt_d",
            "last_pymnt_amnt",
            "total_pymnt",
            "total_pymnt_inv",
            "total_rec_prncp",
            "total_rec_int",
            "total_rec_late_fee",
            "recoveries",
            "collection_recovery_fee",
            "out_prncp",
            "out_prncp_inv",
            "pymnt_plan",
        ],
    )[
        ["loan_id", "last_pymnt_d", "total_pymnt", "total_rec_prncp", "recoveries", "out_prncp"]
    ]
    payment_status[["total_pymnt", "total_rec_prncp", "recoveries", "out_prncp"]] = payment_status[
        ["total_pymnt", "total_rec_prncp", "recoveries", "out_prncp"]
    ].apply(pd.to_numeric, errors="coerce")

    dataset = (
        loan.merge(loan_terms, on="loan_id", how="left")
        .merge(application, on="application_id", how="left")
        .merge(applicant_fin, on="application_id", how="left")
        .merge(employment, on="application_id", how="left")
        .merge(application_address, on="application_id", how="left")
        .merge(credit_history, on="application_id", how="left")
        .merge(payment_status, on="loan_id", how="left")
    )
    dataset["year"] = dataset.application_date.dt.year
    dataset["income_bracket"] = dataset.annual_inc.apply(classify_income)
    dataset["performance_score"] = (dataset.total_pymnt / dataset.funded_amount).replace([np.inf, -np.inf], np.nan)
    dataset["residual_balance_ratio"] = (dataset.out_prncp / dataset.funded_amount).replace([np.inf, -np.inf], np.nan)
    return dataset


# --------------------------------------------------------------------------- #
#                             FEATURE ENGINEERING                             #
# --------------------------------------------------------------------------- #


def classify_income(amount: float) -> str:
    if pd.isna(amount):
        return "Sin dato"
    value = float(amount)
    if value < 40000:
        return "<40k"
    if value < 60000:
        return "40-60k"
    if value < 80000:
        return "60-80k"
    if value < 120000:
        return "80-120k"
    if value < 150000:
        return "120-150k"
    return ">150k"


def pct_change(a: float, b: float) -> float:
    if not a or np.isclose(a, 0):
        return math.nan
    return (b - a) / a


# --------------------------------------------------------------------------- #
#                              KPI BUILDERS                                   #
# --------------------------------------------------------------------------- #

HIGH_RISK_GRADES = {"D", "E", "F", "G"}
DELINQ_STATUSES = {"Late (16-30 days)", "Late (31-120 days)", "Default", "In Grace Period"}
DEFAULT_STATUSES = {"Charged Off", "Default"}
RISKY_STATUSES = DELINQ_STATUSES | DEFAULT_STATUSES
GOOD_STATUSES = {"Fully Paid", "Current"}


def build_question_one(dataset: pd.DataFrame) -> dict:
    df = dataset.dropna(subset=["year", "funded_amount", "int_rate"]).copy()
    df = df[df.year.between(2010, 2020)]
    q1 = (
        df.groupby("year")
        .agg(
            avg_funded=("funded_amount", "mean"),
            avg_rate=("int_rate", "mean"),
            high_risk_share=("grade_code", lambda s: s.isin(HIGH_RISK_GRADES).mean()),
        )
        .sort_index()
    )
    series = [
        {
            "year": int(idx),
            "avg_funded": round(row.avg_funded, 2),
            "avg_rate": round(row.avg_rate, 2),
            "high_risk_share": round(row.high_risk_share, 4),
        }
        for idx, row in q1.iterrows()
    ]
    insight = "Sin datos suficientes para la serie temporal."
    if len(series) >= 2:
        first, last = series[0], series[-1]
        funded_delta = pct_change(first["avg_funded"], last["avg_funded"])
        rate_delta = pct_change(first["avg_rate"], last["avg_rate"])
        risk_delta = last["high_risk_share"] - first["high_risk_share"]
        insight = (
            f"El monto promedio aprobado pasó de USD {first['avg_funded']:,.0f} en {first['year']} "
            f"a USD {last['avg_funded']:,.0f} ({funded_delta:+.1%}). "
            f"Las tasas promedio cambiaron {rate_delta:+.1%} y la proporción de colocaciones en grados D-G "
            f"varió {risk_delta:+.1%}, lo que refleja ajustes en la tolerancia al riesgo."
        )
    return {"series": series, "insight": insight}


def build_question_two(dataset: pd.DataFrame) -> dict:
    mask = (
        dataset.loan_status.isin(GOOD_STATUSES)
        & dataset.performance_score.ge(0.85)
        & dataset.funded_amount.gt(0)
    )
    df = dataset[mask].dropna(subset=["annual_inc", "state_code"])
    if df.empty:
        return {"segments": [], "insight": "Sin registros con buen desempeño para perfilar."}
    grouped = (
        df.groupby(["income_bracket", "emp_length_label", "state_name", "grade_code", "home_ownership"])
        .agg(
            avg_funded=("funded_amount", "mean"),
            avg_income=("annual_inc", "mean"),
            perf_index=("performance_score", "mean"),
            loans=("loan_id", "count"),
        )
        .reset_index()
    )
    grouped = grouped[grouped.loans >= 15]
    grouped = grouped.sort_values("avg_funded", ascending=False).head(8)
    segments = [
        {
            "income_bracket": row.income_bracket,
            "emp_length": row.emp_length_label,
            "state": row.state_name,
            "grade": row.grade_code,
            "home": row.home_ownership,
            "avg_funded": round(row.avg_funded, 2),
            "avg_income": round(row.avg_income, 2),
            "perf_index": round(row.perf_index, 4),
            "loans": int(row.loans),
        }
        for _, row in grouped.iterrows()
    ]
    insight = "Los segmentos con mayor monto aprobado combinan ingresos altos, empleo estable y grados crediticios A-B."
    if segments:
        leader = segments[0]
        insight = (
            f"Los montos más altos (USD {leader['avg_funded']:,.0f}) se concentran en hogares {leader['home']} de {leader['state']}, "
            f"con ingresos {leader['income_bracket']} y antigüedad laboral '{leader['emp_length']}'. "
            f"Su índice de pago es {leader['perf_index']:.2f}, señalando un nicho rentable de grado {leader['grade']}."
        )
    return {"segments": segments, "insight": insight}


def build_question_three(dataset: pd.DataFrame) -> dict:
    df = dataset.dropna(subset=["purpose"])
    grouped = (
        df.groupby("purpose")
        .agg(
            total=("loan_id", "count"),
            delinquent=("loan_status", lambda s: s.isin(DELINQ_STATUSES).sum()),
            charged_off=("loan_status", lambda s: s.isin(DEFAULT_STATUSES).sum()),
        )
        .reset_index()
    )
    grouped = grouped[grouped.total >= 30]
    grouped["delinquency_rate"] = grouped.delinquent / grouped.total
    grouped["chargeoff_rate"] = grouped.charged_off / grouped.total
    grouped = grouped.sort_values("total", ascending=False).head(10)
    purpose_rows = [
        {
            "purpose": row.purpose,
            "delinquency_rate": round(row.delinquency_rate, 4),
            "chargeoff_rate": round(row.chargeoff_rate, 4),
            "loans": int(row.total),
        }
        for _, row in grouped.iterrows()
    ]
    insight = "Los propósitos orientados a consolidación y negocio concentran la morosidad más alta."
    if purpose_rows:
        worst = max(purpose_rows, key=lambda r: r["delinquency_rate"])
        best = min(purpose_rows, key=lambda r: r["chargeoff_rate"])
        insight = (
            f"'{worst['purpose']}' registra la mora más alta ({worst['delinquency_rate']:.1%}), "
            f"mientras que '{best['purpose']}' mantiene cancelaciones por debajo de {best['chargeoff_rate']:.1%}."
        )
    return {"purposes": purpose_rows, "insight": insight}


def build_question_four(dataset: pd.DataFrame) -> dict:
    df = dataset.dropna(subset=["dti", "open_acc", "revol_bal"]).copy()
    if df.empty:
        return {"buckets": [], "insight": "No hay datos de DTI suficientes."}
    bins = list(range(0, 46, 5)) + [100]
    labels = [f"{bins[i]}-{bins[i+1]}" for i in range(len(bins) - 1)]
    df["dti_bucket"] = pd.cut(df.dti, bins=bins, labels=labels, include_lowest=True, right=False)
    grouped = (
        df.dropna(subset=["dti_bucket"])
        .groupby("dti_bucket")
        .agg(
            avg_dti=("dti", "mean"),
            avg_open_acc=("open_acc", "mean"),
            avg_revol_bal=("revol_bal", "mean"),
            delinquent=("loan_status", lambda s: s.isin(RISKY_STATUSES).sum()),
            total=("loan_id", "count"),
        )
        .reset_index()
    )
    grouped["delinquency_rate"] = grouped.delinquent / grouped.total
    buckets = [
        {
            "bucket": str(row.dti_bucket),
            "avg_open_acc": round(row.avg_open_acc, 2),
            "avg_revol_bal": round(row.avg_revol_bal / 1000.0, 2),  # miles USD
            "delinquency_rate": round(row.delinquency_rate, 4),
        }
        for _, row in grouped.iterrows()
    ]
    insight = "El riesgo aumenta cuando la relación deuda-ingreso supera el 25%."
    if buckets:
        peak = max(buckets, key=lambda b: b["delinquency_rate"])
        base = min(buckets, key=lambda b: b["delinquency_rate"])
        insight = (
            f"La mora salta de {base['delinquency_rate']:.1%} en el tramo {base['bucket']} "
            f"a {peak['delinquency_rate']:.1%} cuando la relación deuda-ingreso supera {peak['bucket'].split('-')[0]}%, "
            f"acompañado de saldos revolventes medios de USD {peak['avg_revol_bal']:,.0f}k."
        )
    return {"buckets": buckets, "insight": insight}


def build_question_five(dataset: pd.DataFrame) -> dict:
    df = dataset.dropna(subset=["state_name"])
    grouped = (
        df.groupby("state_name")
        .agg(
            total_loans=("loan_id", "count"),
            total_funded=("funded_amount", "sum"),
            defaults=("loan_status", lambda s: s.isin(RISKY_STATUSES).sum()),
            recoveries=("recoveries", "sum"),
            rec_principal=("total_rec_prncp", "sum"),
        )
        .reset_index()
    )
    grouped = grouped[grouped.total_loans >= 30]
    grouped["default_rate"] = grouped.defaults / grouped.total_loans
    grouped["recovery_rate"] = grouped.recoveries / grouped.rec_principal.replace(0, np.nan)
    grouped["recovery_rate"] = grouped["recovery_rate"].fillna(0)
    grouped["funded_millions"] = grouped.total_funded / 1_000_000

    if grouped.empty:
        return {"states": [], "insight": "No hay volumen suficiente por estado."}

    top_volume = grouped.nlargest(10, "funded_millions").index.tolist()
    top_risk = grouped.nlargest(5, "default_rate").index.tolist()
    selected = grouped[grouped.index.isin(set(top_volume) | set(top_risk))]

    states = [
        {
            "state": idx,
            "funded_millions": round(row.funded_millions, 2),
            "recovery_rate": round(row.recovery_rate, 4),
            "default_rate": round(row.default_rate, 4),
            "segment": "alto_riesgo" if idx in top_risk else "alto_volumen",
        }
        for idx, row in selected.iterrows()
    ]
    best = min(states, key=lambda s: s["default_rate"])
    worst = max(states, key=lambda s: s["default_rate"])
    insight = (
        f"{best['state']} combina el mayor retorno (recuperación {best['recovery_rate']:.1%}) "
        f"con baja mora ({best['default_rate']:.1%}), mientras que {worst['state']} concentra "
        f"riesgos por encima de {worst['default_rate']:.1%}."
    )
    return {"states": states, "insight": insight}


# --------------------------------------------------------------------------- #
#                                MAIN ENTRY                                   #
# --------------------------------------------------------------------------- #

def main() -> None:
    dim_maps = load_dimensions()
    dataset = build_dataset(dim_maps)
    payload = {
        "q1": build_question_one(dataset),
        "q2": build_question_two(dataset),
        "q3": build_question_three(dataset),
        "q4": build_question_four(dataset),
        "q5": build_question_five(dataset),
        "meta": {
            "records": int(len(dataset)),
            "source_path": str(SQL_BASE),
        },
    }
    OUTPUT_PATH.parent.mkdir(parents=True, exist_ok=True)
    with OUTPUT_PATH.open("w", encoding="utf-8") as fh:
        json.dump(payload, fh, indent=2, ensure_ascii=False)
    print(f"Dataset generado en {OUTPUT_PATH}")


if __name__ == "__main__":
    main()
