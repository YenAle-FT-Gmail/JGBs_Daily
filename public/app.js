/* ------------------------------------------------------------------ */
/*  JGBsDaily – Frontend Logic & Chart.js Rendering                  */
/* ------------------------------------------------------------------ */

(function () {
    "use strict";

    var DATA_URL = "data/yields.json";

    // Colours for overlay lines (index by activation order)
    var OVERLAY_COLOURS = [
        "#f59e0b", "#a78bfa", "#f87171", "#34d399",
        "#fb923c", "#38bdf8", "#e879f9", "#facc15", "#4ade80",
    ];

    var appData = null;
    var chart = null;
    var activeOverlays = []; // list of delta-key strings currently toggled on

    // Custom plugin: draw bps diff labels at each data point
    var diffLabelPlugin = {
        id: "diffLabels",
        afterDatasetsDraw: function (chartInstance) {
            var datasets = chartInstance.data.datasets;
            if (datasets.length < 2) return;
            var ctx = chartInstance.ctx;
            var todayDs = datasets[0];
            var histDs = datasets[1];
            var todayMeta = chartInstance.getDatasetMeta(0);
            var histMeta = chartInstance.getDatasetMeta(1);
            var isMobile = chartInstance.width < 500;
            var fontSize = isMobile ? 9 : 11;
            ctx.save();
            ctx.font = "bold " + fontSize + "px " + (isMobile ? "sans-serif" : "Inter, sans-serif");
            ctx.textAlign = "center";
            for (var i = 0; i < todayDs.data.length; i++) {
                var todayVal = todayDs.data[i];
                var histVal = histDs.data[i];
                if (todayVal == null || histVal == null) continue;
                var diffBps = (todayVal - histVal) * 100;
                if (Math.abs(diffBps) < 0.05) continue;
                var sign = diffBps > 0 ? "+" : "";
                var label = sign + diffBps.toFixed(1);
                var todayPt = todayMeta.data[i];
                var histPt = histMeta.data[i];
                var x = todayPt.x;
                var offset = isMobile ? 10 : 14;
                var y;
                if (diffBps > 0) {
                    y = Math.min(todayPt.y, histPt.y) - offset;
                } else {
                    y = Math.max(todayPt.y, histPt.y) + offset + fontSize;
                }
                ctx.fillStyle = diffBps > 0 ? "#f87171" : "#34d399";
                ctx.fillText(label, x, y);
            }
            ctx.restore();
        }
    };

    // ---- DOM refs (set in init) ----
    var toggle, labelSimple, labelCompound, dataDateEl, updatedAtEl;
    var tbody, thead, canvas, periodBtnContainer;

    // ---- Helpers ----
    function currentMode() {
        return toggle.checked ? "compound" : "simple";
    }

    function formatDelta(val) {
        if (val === null || val === undefined) return "—";
        var sign = val > 0 ? "+" : "";
        return sign + val.toFixed(1);
    }

    function deltaClass(val) {
        if (val === null || val === undefined) return "delta-zero";
        if (val > 0) return "delta-pos";
        if (val < 0) return "delta-neg";
        return "delta-zero";
    }

    // ---- Build period buttons ----
    function buildPeriodButtons() {
        periodBtnContainer.innerHTML = "";
        var keys = appData.delta_keys;
        keys.forEach(function (key) {
            var btn = document.createElement("button");
            btn.className = "period-btn";
            btn.textContent = key;
            btn.dataset.key = key;
            btn.addEventListener("click", function () {
                toggleOverlay(key);
            });
            periodBtnContainer.appendChild(btn);
        });
    }

    function toggleOverlay(key) {
        // Only one overlay at a time
        if (activeOverlays.length === 1 && activeOverlays[0] === key) {
            activeOverlays = [];
        } else {
            activeOverlays = [key];
        }
        // Update button active state
        var buttons = periodBtnContainer.querySelectorAll(".period-btn");
        buttons.forEach(function (btn) {
            btn.classList.toggle("active", activeOverlays.indexOf(btn.dataset.key) !== -1);
        });
        render();
    }

    // ---- Render Table ----
    function renderTable(section) {
        var keys = appData.delta_keys;
        var colCount = 2 + keys.length;
        var simpleSection = appData.simple;
        var compoundSection = appData.compound;
        thead.innerHTML = "";
        var headerRow = document.createElement("tr");
        var thTenor = document.createElement("th");
        thTenor.textContent = "Tenor";
        headerRow.appendChild(thTenor);
        var thYield = document.createElement("th");
        thYield.textContent = "Yield (%)";
        headerRow.appendChild(thYield);
        keys.forEach(function (k) {
            var th = document.createElement("th");
            th.textContent = k + " (bps)";
            headerRow.appendChild(th);
        });
        thead.appendChild(headerRow);

        tbody.innerHTML = "";
        var tenors = appData.tenors;
        tenors.forEach(function (tenor) {
            // --- Main row ---
            var row = document.createElement("tr");
            row.className = "tenor-row";
            row.style.cursor = "pointer";

            var tdTenor = document.createElement("td");
            tdTenor.innerHTML = '<span class="expand-arrow">&#9654;</span> ' + tenor;
            row.appendChild(tdTenor);

            var tdYield = document.createElement("td");
            var yieldVal = section.yields[tenor];
            tdYield.textContent = yieldVal !== null && yieldVal !== undefined ? yieldVal.toFixed(3) : "—";
            row.appendChild(tdYield);

            keys.forEach(function (dk) {
                var td = document.createElement("td");
                var dv = section.deltas[dk] ? section.deltas[dk][tenor] : null;
                td.textContent = formatDelta(dv);
                td.className = deltaClass(dv);
                row.appendChild(td);
            });
            tbody.appendChild(row);

            // --- Expandable sub-rows (SY High, SY Low, CY High, CY Low) ---
            var subLabels = [
                { label: "SY High", src: simpleSection, field: "high" },
                { label: "SY Low",  src: simpleSection, field: "low"  },
                { label: "CY High", src: compoundSection, field: "high" },
                { label: "CY Low",  src: compoundSection, field: "low"  },
            ];
            subLabels.forEach(function (sub) {
                var subRow = document.createElement("tr");
                subRow.className = "hl-sub-row";

                var tdLabel = document.createElement("td");
                tdLabel.className = "hl-label";
                tdLabel.textContent = sub.label;
                subRow.appendChild(tdLabel);

                // Blank yield column for sub-row
                var tdBlank = document.createElement("td");
                tdBlank.textContent = "";
                subRow.appendChild(tdBlank);

                keys.forEach(function (dk) {
                    var td = document.createElement("td");
                    td.className = "hl-val";
                    var hl = sub.src.high_low && sub.src.high_low[dk] && sub.src.high_low[dk][tenor];
                    if (hl && hl[sub.field] !== null && hl[sub.field] !== undefined) {
                        var dateStr = hl[sub.field + "_date"] || "";
                        var shortDate = dateStr.slice(5); // MM/DD
                        td.textContent = hl[sub.field].toFixed(3);
                        td.title = shortDate;
                        td.className += sub.field === "high" ? " hl-high" : " hl-low";
                    } else {
                        td.textContent = "—";
                    }
                    subRow.appendChild(td);
                });
                tbody.appendChild(subRow);
            });

            // Toggle expand/collapse
            row.addEventListener("click", function () {
                var expanded = row.classList.toggle("expanded");
                var next = row.nextElementSibling;
                while (next && next.classList.contains("hl-sub-row")) {
                    next.style.display = expanded ? "table-row" : "none";
                    next = next.nextElementSibling;
                }
            });
        });
    }

    // ---- Render Chart ----
    function renderChart(section) {
        var tenors = appData.tenors;
        var yields = tenors.map(function (t) { return section.yields[t]; });

        var datasets = [
            {
                label: "Today (" + (section.date || "—") + ")",
                data: yields,
                borderColor: "#4f8ff7",
                borderWidth: 2.5,
                pointRadius: 4,
                pointBackgroundColor: "#4f8ff7",
                tension: 0.3,
                fill: false,
                order: 1,
            },
        ];

        // Add overlay datasets for each active period
        activeOverlays.forEach(function (key, i) {
            var curve = section.curves[key];
            if (!curve) return;
            var colour = OVERLAY_COLOURS[i % OVERLAY_COLOURS.length];
            var histYields = tenors.map(function (t) { return curve.yields[t]; });
            datasets.push({
                label: key + " ago (" + (curve.date || "—") + ")",
                data: histYields,
                borderColor: colour,
                borderWidth: 2,
                pointRadius: 3,
                pointBackgroundColor: colour,
                tension: 0.3,
                fill: {
                    target: 0,
                    above: "rgba(52, 211, 153, 0.45)",
                    below: "rgba(248, 113, 113, 0.45)",
                },
                order: 0,
            });
        });

        var options = {
            responsive: true,
            maintainAspectRatio: true,
            interaction: { mode: "index", intersect: false },
            plugins: {
                filler: { propagate: false },
                legend: {
                    labels: { color: "#e4e6ed", font: { size: 12 } },
                },
                tooltip: {
                    callbacks: {
                        label: function (ctx) {
                            return ctx.dataset.label + ": " + (ctx.parsed.y !== null ? ctx.parsed.y.toFixed(3) : "N/A") + "%";
                        },
                    },
                },
            },
            scales: {
                x: {
                    title: { display: true, text: "Tenor", color: "#8b8fa3" },
                    ticks: { color: "#8b8fa3" },
                    grid: { color: "rgba(42,45,58,0.6)" },
                },
                y: {
                    title: { display: true, text: "Yield (%)", color: "#8b8fa3" },
                    ticks: { color: "#8b8fa3" },
                    grid: { color: "rgba(42,45,58,0.6)" },
                },
            },
        };

        if (chart) {
            chart.data = { labels: tenors, datasets: datasets };
            chart.options = options;
            chart.update();
        } else {
            chart = new Chart(canvas, { type: "line", data: { labels: tenors, datasets: datasets }, options: options, plugins: [diffLabelPlugin] });
        }
    }

    // ---- Render Everything ----
    function render() {
        if (!appData) return;

        var mode = currentMode();
        var section = appData[mode];

        labelSimple.classList.toggle("active", mode === "simple");
        labelCompound.classList.toggle("active", mode === "compound");

        dataDateEl.textContent = "Data Date: " + (section.date || "—");
        updatedAtEl.textContent = "Pipeline: " + (appData.updated_at || "—");

        renderChart(section);
        renderTable(section);
    }

    // ---- Init ----
    function init() {
        // Grab DOM refs now that the DOM is guaranteed ready
        toggle = document.getElementById("yield-toggle");
        labelSimple = document.getElementById("label-simple");
        labelCompound = document.getElementById("label-compound");
        dataDateEl = document.getElementById("data-date");
        updatedAtEl = document.getElementById("updated-at");
        tbody = document.getElementById("yield-tbody");
        thead = document.getElementById("yield-thead");
        canvas = document.getElementById("yield-chart");
        periodBtnContainer = document.getElementById("period-buttons");

        fetch(DATA_URL)
            .then(function (res) {
                if (!res.ok) throw new Error("HTTP " + res.status);
                return res.json();
            })
            .then(function (json) {
                appData = json;
                labelSimple.classList.add("active");
                buildPeriodButtons();
                render();
            })
            .catch(function (err) {
                console.error("Failed to load yield data:", err);
                dataDateEl.textContent = "Failed to load data.";
            });

        toggle.addEventListener("change", render);
    }

    if (document.readyState === "loading") {
        document.addEventListener("DOMContentLoaded", init);
    } else {
        init();
    }
})();
