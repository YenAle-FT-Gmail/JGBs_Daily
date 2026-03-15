# JGBsDaily

Auto-updating Japanese Government Bond (JGB) yield curve dashboard.

## Overview

Displays **Simple** and **Compound** JGB yield curves with historical delta calculations (DoD, 2D, 3D, 1W) in basis points.

- **Data Source**: [Japanese Ministry of Finance](https://www.mof.go.jp/english/policy/jgbs/reference/interest_rate/index.htm)
- **Tenors**: 1Y, 2Y, 3Y, 4Y, 5Y, 6Y, 7Y, 8Y, 9Y, 10Y, 15Y, 20Y, 25Y, 30Y, 40Y
- **Updates**: Every Japanese business day via GitHub Actions (09:00 UTC / 18:00 JST)

## Tech Stack

| Layer | Technology |
|-------|-----------|
| Data Processing | Python 3.12, pandas, requests |
| Frontend | HTML5, CSS3, Vanilla JS, Chart.js |
| CI/CD | GitHub Actions (scheduled cron) |
| Hosting | GitHub Pages (static, free) |
| Alerting | Python smtplib via Gmail SMTP |

## Project Structure

```
JGBsDaily/
├── .github/workflows/daily_update.yml   # Cron job
├── src/data_fetcher.py                  # Fetch, process, output JSON
├── public/
│   ├── index.html                       # Frontend UI
│   ├── app.js                           # Chart.js rendering & table
│   ├── style.css                        # Dark-theme styling
│   └── data/yields.json                 # Auto-generated data
├── requirements.txt
└── README.md
```

## Setup

### 1. Local Development

```bash
pip install -r requirements.txt
python src/data_fetcher.py
# Open public/index.html in a browser (or use a local server)
```

### 2. GitHub Pages

1. Push this repo to GitHub.
2. Go to **Settings → Pages** and set the source to the `main` branch, folder `/public`.
3. The site will be live at `https://<username>.github.io/<repo>/`.

### 3. Email Alerts (Optional)

Add these as **Repository Secrets** in GitHub (Settings → Secrets → Actions):

| Secret | Description |
|--------|-------------|
| `EMAIL_SENDER` | Gmail address to send from |
| `EMAIL_PASSWORD` | Gmail App Password (not your regular password) |
| `EMAIL_RECEIVER` | Address to receive failure alerts |

## License

MIT
