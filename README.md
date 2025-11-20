# ThaiPropertyGEMS — Setup

This project scrapes Thai foreclosure listings, stores them in a Neon (Postgres) database, and displays them in a Streamlit dashboard with bilingual support (Thai/English).

### Recent highlights

- Foreclosure-first labeling: standard sales are explicitly shown as **Foreclosure Property**, while auctions and other channels keep their own badges.
- Saved-properties workspace: authenticated users can tap the 🤍/💙 icon on any card, then review all favorites inside the **Saved** tab.
- Cleaner cards by default: listings render with sanitized text, on-the-fly translations, and always load with every asset visible until the user applies filters.

## Quick setup (local)

1. Create and activate a Python virtualenv:

```bash
python3 -m venv venv
source venv/bin/activate
```

2. Install dependencies:

```bash
pip install -r requirements.txt
```

3. Database (Neon / Postgres)

- Ensure your `DATABASE_URL` environment variable points to your Neon/Postgres DB.
- Run the SQL schema to create tables:

```bash
psql "$DATABASE_URL" -f schema.sql
```

4. Google Cloud Translate (recommended)

You now have two secure options:

**A. Service account (preferred for servers)**

- Create a Google Cloud project and enable the Cloud Translation API.
- Create a service account, grant it `Cloud Translation API User`, and download the JSON key file.
- Set environment variables:

```bash
export GOOGLE_APPLICATION_CREDENTIALS="/path/to/service-account.json"
export GOOGLE_CLOUD_PROJECT="your-gcp-project-id"
```

**B. API key (quick client-side testing)**

- Create a restricted API key in the Google Cloud console (limit it to the Cloud Translation API and lock to trusted origins/IPs per [Google’s best practices](https://cloud.google.com/docs/authentication/api-keys-best-practices)).
- Store it in `.env` or Streamlit secrets:

```bash
export GOOGLE_TRANSLATE_API_KEY="your-locked-down-api-key"
```

When both a service account and API key are present, the app uses the service account first, then the key, and finally the unofficial public endpoint if neither is available.

5. Generate a metadata snapshot + delta plan (recommended before every scraper run, scheduled nightly):

```bash
python bam_snapshot.py
```

This records the current BAM feed sizes in `bam_feed_snapshot` and writes `bam_delta_plan.json`, which tells the scraper which head/tail pages to refresh.

6. Run the scraper (populate DB):

```bash
python sniper_engine.py
```

7. Run the Streamlit dashboard:

```bash
streamlit run main.py
```

7. (Optional) Seed saved-properties table manually:

The app will auto-create a `saved_properties` table on first launch, but you can also apply it yourself:

```bash
psql "$DATABASE_URL" <<'SQL'
CREATE TABLE IF NOT EXISTS saved_properties (
	id SERIAL PRIMARY KEY,
	username TEXT NOT NULL,
	property_id INTEGER NOT NULL,
	saved_at TIMESTAMPTZ DEFAULT NOW(),
	UNIQUE(username, property_id)
);
SQL
```

## Notes & Recommendations

- The public translate endpoint is unofficial and may be rate-limited. For production, use the official Google Cloud Translate API with a service account.
- Translations are persisted in the DB (`*_en` columns) to reduce repeated API calls.
- Consider setting up scheduled snapshot + scraping (cron or background worker) to keep data fresh.
- Saved lists are tied to the authenticated username; ensure login accounts are unique per user so hearts remain scoped correctly.

### Automated nightly refresh (Thai time)

1. Every night around midnight **Asia/Bangkok** time (UTC+7), run:

	```bash
	./scripts/nightly_refresh.sh
	```

	The helper script activates `.venv`, runs `python bam_snapshot.py`, then `python sniper_engine.py`. `bam_snapshot.py` refreshes feed metadata and writes `bam_delta_plan.json`; `sniper_engine.py` consumes that plan so only the changed pages plus safety head/tail windows are scraped.

2. If your server uses UTC, midnight Bangkok corresponds to **17:00 UTC**. Add this cron entry (after `crontab -e`):

	```cron
	0 17 * * * cd /path/to/ThaiPropertyGEMS && ./scripts/nightly_refresh.sh >> cron.log 2>&1
	```

	Ensure `DATABASE_URL` (and any API keys) are either exported inside the script or set globally for the cron user. On macOS you can achieve the same cadence via a `launchd` plist running the same script at 00:00 Bangkok time.

3. The scraper deletes the plan file after each successful run, so the next night's snapshot always starts fresh.

If you want, I can:
- Switch translation calls to a paid provider or handle batching.
- Add DB migration scripts (Alembic) and automated deploy steps.
