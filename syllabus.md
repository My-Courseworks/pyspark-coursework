# PySpark: Interview Crash Course

A short, opinionated tour of PySpark for a **senior data scientist interview**. The goal is not to make you a Spark engineer. It is to give you a correct mental model, the vocabulary interviewers listen for, and three or four "here's a trade-off I've made" stories.

## Scope (frozen)

| Knob | Value |
|---|---|
| Learner | Senior DS who knows pandas/sklearn well and has used Spark lightly; a Spark beginner can follow too, because terms are defined at first use |
| Reference stack | Apache Spark 3.x, PySpark DataFrame API (not RDDs) |
| Backends | NONE (works on Databricks, EMR, Dataproc, or local `pip install pyspark`) |
| Labs | Code snippets only. They use `s3://shopco/...` paths for realism; each of M1–M4 has a local-mode setup with toy data you can run instead. None of the snippets have been executed by the author |
| Capstone / assessment | NONE. M5 is a talk-track instead |
| Depth | Interview depth. **Light mode:** the skill's verification gates and literature survey were deliberately skipped |

## Domain spine

**ShopCo**: an online retailer. You're building a **churn model**: will a customer stop buying in the next 30 days? Every module uses these three tables:

- `events`: ~2B clickstream rows/month, Parquet on S3, partitioned by `dt`
  - `user_id`, `ts` (timestamp), `event_type` (`view` / `cart` / `purchase` …), `product_id`, `price`, `dt` (date string, one folder per day)
- `customers`: ~20M rows, one per customer
  - `user_id`, `country`, `region`, `signup_date`
  - M3 joins it to `events`; M4 joins it to M2's training set to get `country` and `region`
- `products`: ~50K rows, but wide (long text columns)
  - `product_id`, `category`, `title`, `description`

## Modules

| # | Module | Core question | Status |
|---|---|---|---|
| M1 | [Mental model](modules/01-mental-model/README.md) | What actually happens when I call `.count()`? | 🟡 written |
| M2 | [DataFrame API for DS work](modules/02-dataframe-api/README.md) | How do I write feature engineering that Spark can optimize? | 🟡 written |
| M3 | [Performance: shuffles, skew, joins](modules/03-performance/README.md) | Why is my job slow, and what do I check first? | 🟡 written |
| M4 | [ML at scale](modules/04-ml-at-scale/README.md) | When do I use MLlib, pandas UDFs, or no Spark at all? | 🟡 written |
| M5 | [Interview talk-track](modules/05-interview-talk-track/README.md) | What do I actually say? | 🟡 written |

Status vocabulary: `⬜ planned` · `🟡 written` · `✅ verified`. Nothing here is `✅` because verification was skipped on purpose.

## Companion lab (runnable code)

The [`lab/`](lab/README.md) folder is this course as a working system: it generates ShopCo's clickstream at 2M / 500M / 2B events, runs the M2–M3 feature pipeline, and records every step's jobs, tasks, shuffle bytes and skew in a report. It runs on a laptop (`local/`) and on Google Cloud with Dataproc Serverless, BigQuery, Cloud Workflows and Monitoring (`gcp/`). Every design choice is an ADR in `local/adr/` and `gcp/adr/`.

## Cross-cutting threads

- **Failure modes.** Each module names the mistake it prevents.
- **Tradeoff ledger.** Each decision is written as "X costs Y; accepted because Z". These lines are what you quote in an interview.
