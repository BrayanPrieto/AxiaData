const palette = {
  cyan: "#06b6d4",
  indigo: "#6366f1",
  orange: "#fb923c",
  slate: "#1e293b",
  emerald: "#10b981",
  rose: "#fb7185",
  amber: "#fbbf24"
};

async function bootstrap() {
  const response = await fetch("../data/metrics.json");
  const data = await response.json();
  renderQuestionOne(data.q1);
  renderQuestionTwo(data.q2);
  renderQuestionThree(data.q3);
  renderQuestionFour(data.q4);
  renderQuestionFive(data.q5);
  fillAnalysis(data);
}

function fillAnalysis(data) {
  document.getElementById("analysis-q1").textContent = data.q1.insight;
  document.getElementById("analysis-q2").textContent = data.q2.insight;
  document.getElementById("analysis-q3").textContent = data.q3.insight;
  document.getElementById("analysis-q4").textContent = data.q4.insight;
  document.getElementById("analysis-q5").textContent = data.q5.insight;
}

function renderQuestionOne(q1) {
  const ctx = document.getElementById("chart-q1");
  const labels = q1.series.map((item) => item.year);
  const funded = q1.series.map((item) => item.avg_funded);
  const rate = q1.series.map((item) => item.avg_rate);
  const riskShare = q1.series.map((item) => item.high_risk_share * 100);

  new Chart(ctx, {
    type: "bar",
    data: {
      labels,
      datasets: [
        {
          type: "bar",
          label: "Monto aprobado (USD)",
          data: funded,
          backgroundColor: "rgba(6, 182, 212, 0.4)",
          borderColor: palette.cyan,
          borderWidth: 1.5,
          yAxisID: "y"
        },
        {
          type: "line",
          label: "Tasa promedio (%)",
          data: rate,
          borderColor: palette.orange,
          backgroundColor: palette.orange,
          borderWidth: 2.4,
          tension: 0.35,
          yAxisID: "y1"
        },
        {
          type: "line",
          label: "Portafolio D-G (%)",
          data: riskShare,
          borderColor: palette.indigo,
          backgroundColor: palette.indigo,
          borderDash: [6, 4],
          borderWidth: 2,
          tension: 0.3,
          yAxisID: "y1"
        }
      ]
    },
    options: {
      responsive: true,
      stacked: false,
      plugins: {
        legend: { position: "bottom" },
        tooltip: { mode: "index", intersect: false }
      },
      scales: {
        y: {
          title: { display: true, text: "USD promedio" },
          beginAtZero: true
        },
        y1: {
          position: "right",
          beginAtZero: true,
          grid: { drawOnChartArea: false },
          ticks: { callback: (value) => `${value}%` }
        }
      }
    }
  });
}

function renderQuestionTwo(q2) {
  const ctx = document.getElementById("chart-q2");
  const dataset = q2.segments.map((segment) => ({
    x: segment.avg_funded,
    y: segment.perf_index * 100,
    r: Math.min(24, Math.max(6, Math.sqrt(segment.loans) * 2.5)),
    segment
  }));

  new Chart(ctx, {
    type: "bubble",
    data: {
      datasets: [
        {
          label: "Segmentos rentables",
          data: dataset,
          backgroundColor: "rgba(99, 102, 241, 0.35)",
          borderColor: palette.indigo
        }
      ]
    },
    options: {
      plugins: {
        legend: { display: false },
        tooltip: {
          callbacks: {
            label(ctx) {
              const seg = ctx.raw.segment;
              return [
                `${seg.state} · ${seg.grade} (${seg.loans} préstamos)`,
                `Ingreso: ${seg.income_bracket} · Vivienda: ${seg.home}`,
                `Empleo: ${seg.emp_length}`,
                `Ticket medio: USD ${seg.avg_funded.toLocaleString()}`,
                `Índice de pago: ${(seg.perf_index * 100).toFixed(1)}%`
              ];
            }
          }
        }
      },
      scales: {
        x: {
          title: { display: true, text: "Monto aprobado (USD)" }
        },
        y: {
          title: { display: true, text: "Índice de pago (%)" },
          min: 80,
          max: 130
        }
      }
    }
  });
}

function renderQuestionThree(q3) {
  const ctx = document.getElementById("chart-q3");
  const labels = q3.purposes.map((item) => item.purpose);
  const delinquency = q3.purposes.map((item) => item.delinquency_rate * 100);
  const chargeOff = q3.purposes.map((item) => item.chargeoff_rate * 100);

  new Chart(ctx, {
    type: "bar",
    data: {
      labels,
      datasets: [
        {
          label: "Mora %",
          data: delinquency,
          backgroundColor: "rgba(251, 146, 60, 0.5)",
          borderColor: palette.orange,
          borderWidth: 1.5
        },
        {
          label: "Cancelación %",
          data: chargeOff,
          backgroundColor: "rgba(248, 113, 113, 0.45)",
          borderColor: palette.rose,
          borderWidth: 1.5
        }
      ]
    },
    options: {
      plugins: {
        legend: { position: "bottom" },
        tooltip: {
          callbacks: {
            label: (ctx) => `${ctx.dataset.label}: ${ctx.formattedValue}%`
          }
        }
      },
      scales: {
        y: {
          beginAtZero: true,
          ticks: { callback: (value) => `${value}%` },
          title: { display: true, text: "% sobre préstamos del propósito" }
        },
        x: { ticks: { maxRotation: 45, minRotation: 45 } }
      }
    }
  });
}

function renderQuestionFour(q4) {
  const ctx = document.getElementById("chart-q4");
  const labels = q4.buckets.map((item) => item.bucket);
  const openAccounts = q4.buckets.map((item) => item.avg_open_acc);
  const revolBal = q4.buckets.map((item) => item.avg_revol_bal);
  const delinquency = q4.buckets.map((item) => item.delinquency_rate * 100);

  new Chart(ctx, {
    data: {
      labels,
      datasets: [
        {
          type: "bar",
          label: "Cuentas abiertas (prom.)",
          data: openAccounts,
          backgroundColor: "rgba(94, 234, 212, 0.5)",
          borderColor: palette.emerald,
          yAxisID: "y"
        },
        {
          type: "bar",
          label: "Saldo revolvente (USD miles)",
          data: revolBal,
          backgroundColor: "rgba(56, 189, 248, 0.4)",
          borderColor: palette.cyan,
          yAxisID: "y"
        },
        {
          type: "line",
          label: "Prob. mora (%)",
          data: delinquency,
          borderColor: palette.rose,
          backgroundColor: palette.rose,
          tension: 0.35,
          borderWidth: 2.5,
          yAxisID: "y1"
        }
      ]
    },
    options: {
      plugins: {
        legend: { position: "bottom" }
      },
      scales: {
        y: {
          beginAtZero: true,
          title: { display: true, text: "Nº de cuentas / USD miles" }
        },
        y1: {
          position: "right",
          beginAtZero: true,
          grid: { drawOnChartArea: false },
          ticks: { callback: (value) => `${value}%` },
          title: { display: true, text: "Probabilidad de mora" }
        }
      }
    }
  });
}

function renderQuestionFive(q5) {
  const ctx = document.getElementById("chart-q5");
  const colors = q5.states.map((state) =>
    state.segment === "alto_riesgo" ? "rgba(248, 113, 113, 0.55)" : "rgba(59, 130, 246, 0.55)"
  );

  const data = q5.states.map((state, idx) => ({
    x: state.funded_millions,
    y: state.recovery_rate * 100,
    r: Math.max(6, state.default_rate * 400),
    state,
    backgroundColor: colors[idx],
    borderColor: colors[idx].replace("0.55", "1")
  }));

  new Chart(ctx, {
    type: "bubble",
    data: {
      datasets: data.map((entry) => ({
        label: entry.state.state,
        data: [{ x: entry.x, y: entry.y, r: entry.r }],
        backgroundColor: entry.backgroundColor,
        borderColor: entry.borderColor,
        borderWidth: 1.5,
        state: entry.state
      }))
    },
    options: {
      plugins: {
        legend: { display: false },
        tooltip: {
          callbacks: {
            label(ctx) {
              const st = ctx.dataset.state;
              return [
                `${st.state} (${st.segment === "alto_riesgo" ? "Riesgo" : "Volumen"})`,
                `Monto aprobado: USD ${st.funded_millions.toFixed(2)}M`,
                `Recuperación: ${(st.recovery_rate * 100).toFixed(1)}%`,
                `Default: ${(st.default_rate * 100).toFixed(1)}%`
              ];
            }
          }
        }
      },
      scales: {
        x: {
          title: { display: true, text: "Monto aprobado (USD millones)" },
          beginAtZero: true
        },
        y: {
          title: { display: true, text: "Recuperación (%)" },
          beginAtZero: true,
          ticks: { callback: (value) => `${value}%` }
        }
      }
    }
  });
}

bootstrap();
