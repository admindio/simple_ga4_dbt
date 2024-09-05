# simple-ga4-dbt

## About
This dbt package provides a simple, cost-effective way to process raw GA4 data into easy-to-access marts, with opinionated, majority-of-markters-approve defaults.

While it has substantial standalone value to data engineers, it also serves as the open source core for the GA4 portions the [Molly](https://www.admindanaltics.com/molly/?utm_source=github%20simple-ga4-dbt&utm_medium=referral&utm_content=README) GA4 addon. 

Specifically: 

- [Molly Segments](https://www.admindanalytics.com/segmentation/?utm_source=github%20simple-ga4-dbt&utm_medium=referral&utm_content=README): the no-code data-cleaner/segment-maker for GA4 data in BigQuery. The benefits of web analytics in a warehouse without the need to manage infrastructure or write SQL.
- [Molly Quality Monitor](https://www.admindanalytics.com/data-quality-monitoring/?utm_source=github%20simple-ga4-dbt&utm_medium=referral&utm_content=README): Continuous data quality scores that guarantee you can trust the GA4 data you're looking at. Bye bye ObservePoint and DataTrue.
- [Molly Cause & Effect](https://www.mollydata.io/?utm_source=github%20simple-ga4-dbt&utm_medium=referral&utm_content=README): ongoing performance change detection to make sure performance changes never fly under your radar again.
- [Molly Reports](https://www.admindanalytics.com/molly-reports/?utm_source=github%20simple-ga4-dbt&utm_medium=referral&utm_content=README): Looker Studio templates that 
  - recover all lost Universal Analytics functionality (ecommerce shopping behavior, product list performance, user explorer, and many more)
  - expose Molly Segments for instant analysis
  - warn about data quality scores from Molly Quality Monitor that would compromise analysis 
  - expose key performance changes and the causal annotations from Molly Cause & Effect
  - integrate cost data and CRM data
  - connect aggregate analysis with user recordings

### Join the Molly Beta
Molly is currently in a closed beta - if you've been thinking about these problems and think your experience could help shape Molly's value, [contact us](www.admindanalytics.com/contact/?utm_source=github%20simple-ga4-dbt&utm_medium=referral&utm_content=README)

### Opinions
To Fill Out Later:
- sessions
- attribution
- incremental
- aggregation ahead of viz
- no dbt deps

### Notes
- comparison to GA4 UI
- 

## Installation and Running
- fill in vars in profile
- CLI
  - gcloud
  - python (3.10)
- DBT Cloud
  - todo