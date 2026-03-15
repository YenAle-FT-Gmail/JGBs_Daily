/* ------------------------------------------------------------------ */
/*  JGBsDaily – Frontend Logic & Chart.js Rendering                  */
/* ------------------------------------------------------------------ */

(function () {
    "use strict";

    var DATA_URLS = {
        jgb: "data/yields.json",
        ust: "data/ust_yields.json",
        egb: "data/egb_yields.json",
    };

    var TAB_META = {
        jgb: { subtitle: "Japanese Government Bond Yield Curves", footer: 'Data sourced from the <a href="https://www.mof.go.jp/english/policy/jgbs/reference/interest_rate/index.htm" target="_blank" rel="noopener">Japanese Ministry of Finance</a>. Yields are semiannual compound rates on a constant maturity basis, derived from JSDA reference prices at market close (3pm JST). Updated every Japanese business day.' },
        ust: { subtitle: "US Treasury Yield Curves", footer: 'Data sourced from the <a href="https://home.treasury.gov/resource-center/data-chart-center/interest-rates/Pages/TextView.aspx?data=yield" target="_blank" rel="noopener">US Department of the Treasury</a>. Updated every US business day.' },
        egb: { subtitle: "Euro Area Government Bond Yield Curves", footer: 'Data sourced from the <a href="https://www.ecb.europa.eu/stats/financial_markets_and_interest_rates/euro_area_yield_curves/html/index.en.html" target="_blank" rel="noopener">European Central Bank</a>. Updated every ECB business day.' },
    };

    // Colours for overlay lines (index by activation order)
    var OVERLAY_COLOURS = [
        "#f59e0b", "#a78bfa", "#f87171", "#34d399",
        "#fb923c", "#38bdf8", "#e879f9", "#facc15", "#4ade80",
    ];

    var allData = {};   // { jgb: {...}, ust: {...}, egb: {...} }
    var activeTab = "jgb";
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
    var dataDateEl, updatedAtEl;
    var tbody, thead, canvas, periodBtnContainer;
    var jgbSections, subtitleEl, footerTextEl;

    // ---- Helpers ----
    function getAppData() {
        return allData[activeTab] || null;
    }

    function isJgb() {
        return activeTab === "jgb";
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
        var data = getAppData();
        if (!data) return;
        var keys = data.delta_keys;
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
    function renderTable(section, appData) {
        var keys = appData.delta_keys;
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

        var subLabels = [
            { label: "High", src: section, field: "high" },
            { label: "Low", src: section, field: "low" },
        ];

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

            // --- Expandable sub-rows ---
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
    function renderChart(section, appData) {
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
        var appData = getAppData();
        if (!appData) return;

        var section = appData;

        dataDateEl.textContent = "Data Date: " + (section.date || "—");
        updatedAtEl.textContent = "Pipeline: " + (appData.updated_at || "—");

        renderChart(section, appData);
        renderTable(section, appData);
        if (isJgb()) {
            renderRvTable("spread", appData.spread_keys, section, appData);
            renderRvTable("fly", appData.fly_keys, section, appData);
            renderForwardTable("fwd-matrix", appData.fwd_matrix_keys || [], section, "matrix", appData);
            renderForwardTable("rate-path", appData.rate_path_keys || [], section, "path", appData);
        }
    }

    // ---- Render RV Table (spreads or butterflies) ----
    function renderRvTable(prefix, rvKeys, section, appData) {
        var theadEl = document.getElementById(prefix + "-thead");
        var tbodyEl = document.getElementById(prefix + "-tbody");
        var deltaKeys = appData.delta_keys;
        var rvType = prefix === "spread" ? "spreads" : "butterflies";
        var rvData = section.rv ? section.rv[rvType] : null;

        // Header
        theadEl.innerHTML = "";
        var hr = document.createElement("tr");
        var thName = document.createElement("th"); thName.textContent = prefix === "spread" ? "Spread" : "Fly"; hr.appendChild(thName);
        var thCur = document.createElement("th"); thCur.textContent = "Current"; hr.appendChild(thCur);
        deltaKeys.forEach(function (k) {
            var th = document.createElement("th"); th.textContent = k; hr.appendChild(th);
        });
        theadEl.appendChild(hr);

        // Body
        tbodyEl.innerHTML = "";
        rvKeys.forEach(function (key) {
            var item = rvData ? rvData[key] : null;
            var row = document.createElement("tr");
            row.className = "tenor-row";
            row.style.cursor = "pointer";

            var tdName = document.createElement("td");
            tdName.innerHTML = '<span class="expand-arrow">&#9654;</span> ' + key;
            row.appendChild(tdName);

            var tdCur = document.createElement("td");
            tdCur.textContent = item && item.current !== null ? item.current.toFixed(1) : "\u2014";
            row.appendChild(tdCur);

            deltaKeys.forEach(function (dk) {
                var td = document.createElement("td");
                var dv = item && item.deltas ? item.deltas[dk] : null;
                td.textContent = formatDelta(dv);
                td.className = deltaClass(dv);
                row.appendChild(td);
            });
            tbodyEl.appendChild(row);

            // Expandable high/low sub-rows
            var subLabels = [
                { label: "High", src: rvData, field: "high" },
                { label: "Low", src: rvData, field: "low" },
            ];
            subLabels.forEach(function (sub) {
                var subRow = document.createElement("tr");
                subRow.className = "hl-sub-row";
                var tdLabel = document.createElement("td");
                tdLabel.className = "hl-label";
                tdLabel.textContent = sub.label;
                subRow.appendChild(tdLabel);
                // blank current column
                var tdBlank = document.createElement("td"); tdBlank.textContent = ""; subRow.appendChild(tdBlank);
                deltaKeys.forEach(function (dk) {
                    var td = document.createElement("td");
                    td.className = "hl-val";
                    var hl = sub.src && sub.src[key] && sub.src[key].high_low && sub.src[key].high_low[dk];
                    if (hl && hl[sub.field] !== null && hl[sub.field] !== undefined) {
                        td.textContent = hl[sub.field].toFixed(1);
                        td.title = (hl[sub.field + "_date"] || "").slice(5);
                        td.className += sub.field === "high" ? " hl-high" : " hl-low";
                    } else {
                        td.textContent = "\u2014";
                    }
                    subRow.appendChild(td);
                });
                tbodyEl.appendChild(subRow);
            });

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

    // ---- Render Forward Table (matrix or rate path) ----
    function renderForwardTable(prefix, fwdKeys, section, subKey, appData) {
        var theadEl = document.getElementById(prefix + "-thead");
        var tbodyEl = document.getElementById(prefix + "-tbody");
        var deltaKeys = appData.delta_keys;
        var fwdData = section.forwards ? section.forwards[subKey] : null;

        // Header
        theadEl.innerHTML = "";
        var hr = document.createElement("tr");
        var thName = document.createElement("th");
        thName.textContent = subKey === "matrix" ? "Forward" : "Horizon";
        hr.appendChild(thName);
        var thCur = document.createElement("th"); thCur.textContent = "Current"; hr.appendChild(thCur);
        deltaKeys.forEach(function (k) {
            var th = document.createElement("th"); th.textContent = k; hr.appendChild(th);
        });
        theadEl.appendChild(hr);

        // Body
        tbodyEl.innerHTML = "";
        fwdKeys.forEach(function (key) {
            var item = fwdData ? fwdData[key] : null;
            var row = document.createElement("tr");
            row.className = "tenor-row";
            row.style.cursor = "pointer";

            var tdName = document.createElement("td");
            tdName.innerHTML = '<span class="expand-arrow">&#9654;</span> ' + key;
            row.appendChild(tdName);

            var tdCur = document.createElement("td");
            tdCur.textContent = item && item.current !== null ? item.current.toFixed(3) : "\u2014";
            row.appendChild(tdCur);

            deltaKeys.forEach(function (dk) {
                var td = document.createElement("td");
                var dv = item && item.deltas ? item.deltas[dk] : null;
                td.textContent = formatDelta(dv);
                td.className = deltaClass(dv);
                row.appendChild(td);
            });
            tbodyEl.appendChild(row);

            // Expandable high/low sub-rows (rates in %, 3dp)
            var subLabels = [
                { label: "High", src: fwdData, field: "high" },
                { label: "Low", src: fwdData, field: "low" },
            ];
            subLabels.forEach(function (sub) {
                var subRow = document.createElement("tr");
                subRow.className = "hl-sub-row";
                var tdLabel = document.createElement("td");
                tdLabel.className = "hl-label";
                tdLabel.textContent = sub.label;
                subRow.appendChild(tdLabel);
                var tdBlank = document.createElement("td"); tdBlank.textContent = ""; subRow.appendChild(tdBlank);
                deltaKeys.forEach(function (dk) {
                    var td = document.createElement("td");
                    td.className = "hl-val";
                    var hl = sub.src && sub.src[key] && sub.src[key].high_low && sub.src[key].high_low[dk];
                    if (hl && hl[sub.field] !== null && hl[sub.field] !== undefined) {
                        td.textContent = hl[sub.field].toFixed(3);
                        td.title = (hl[sub.field + "_date"] || "").slice(5);
                        td.className += sub.field === "high" ? " hl-high" : " hl-low";
                    } else {
                        td.textContent = "\u2014";
                    }
                    subRow.appendChild(td);
                });
                tbodyEl.appendChild(subRow);
            });

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

    // ---- Tab switching ----
    function switchTab(tab) {
        activeTab = tab;
        activeOverlays = [];

        // Update tab button active state
        document.querySelectorAll(".tab-btn").forEach(function (btn) {
            btn.classList.toggle("active", btn.dataset.tab === tab);
        });

        // Show/hide JGB-only elements
        jgbSections.style.display = isJgb() ? "block" : "none";

        // Update subtitle and footer
        subtitleEl.textContent = TAB_META[tab].subtitle;
        footerTextEl.innerHTML = TAB_META[tab].footer;

        // Reset chart so it's recreated with proper data
        if (chart) { chart.destroy(); chart = null; }

        buildPeriodButtons();
        render();
    }

    // ---- Init ----
    function init() {
        // Grab DOM refs now that the DOM is guaranteed ready
        dataDateEl = document.getElementById("data-date");
        updatedAtEl = document.getElementById("updated-at");
        tbody = document.getElementById("yield-tbody");
        thead = document.getElementById("yield-thead");
        canvas = document.getElementById("yield-chart");
        periodBtnContainer = document.getElementById("period-buttons");
        jgbSections = document.getElementById("jgb-sections");
        subtitleEl = document.getElementById("page-subtitle");
        footerTextEl = document.getElementById("footer-text");

        // Load all data files in parallel
        var tabs = ["jgb", "ust", "egb"];
        var promises = tabs.map(function (tab) {
            return fetch(DATA_URLS[tab])
                .then(function (res) {
                    if (!res.ok) throw new Error("HTTP " + res.status);
                    return res.json();
                })
                .then(function (json) {
                    allData[tab] = json;
                })
                .catch(function (err) {
                    console.warn("Failed to load " + tab + " data:", err);
                    allData[tab] = null;
                });
        });

        Promise.all(promises).then(function () {
            buildPeriodButtons();
            render();
        });

        // Tab click handlers
        document.querySelectorAll(".tab-btn").forEach(function (btn) {
            btn.addEventListener("click", function () {
                switchTab(btn.dataset.tab);
            });
        });

    }

    if (document.readyState === "loading") {
        document.addEventListener("DOMContentLoaded", init);
    } else {
        init();
    }
})();
