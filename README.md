# Bitcoin Data Analysis

A repo containing notebooks and scripts for analyzing the bitcoin blockchain. This repo analyzes data from the BigQuery public dataset published by google. You can query the data directly from the BigQuery UI [here](https://console.cloud.google.com/bigquery?p=bigquery-public-data&d=crypto_bitcoin&page=dataset) For more info about the datasets and how they are published, check out:

* [BigQuery cryptocurrency public datasets](https://cloud.google.com/blog/products/data-analytics/introducing-six-new-cryptocurrencies-in-bigquery-public-datasets-and-how-to-analyze-them)
* [github.com/blockchain-etl](https://github.com/blockchain-etl)

## Setup

These instructions assume you already have a google cloud account and `gcloud` setup. If not, you can sign up for a free account with $300 in credits at [https://cloud.google.com/free/docs/gcp-free-tier](https://cloud.google.com/free/docs/gcp-free-tier). You can also follow installation instructions for `gcloud` [here](https://cloud.google.com/sdk/docs/install). Alternatively, you can do all of these steps from the google console. The basic steps are:

1. Create a new project
2. Create a service account in that project
3. Add the correct roles to the service account
4. Download the keys to your local computer

### Create a new project

The following command creates a project under your account. This will prompt you to accept the generated ID. Make sure to copy the ID as this is what you will use for `<PROJECT_ID>` for the remaining commands.

```sh
gcloud projects create --name="<NAME>"
```

### Authentication

There are multiple ways you can authenticate, but the simplest is to create a service account, attach it to your project, and then copy the keys locally. Create the service account using the project id from above:

```sh
gcloud iam service-accounts create <NAME> --project <PROJECT_ID>
```

Grant permissions on the service account. Notice we are adding the `bigquery.dataViewer` and `bigquery.user` roles (feel free to add any additional roles you need):

```sh
gcloud projects add-iam-policy-binding <PROJECT_ID> --member="serviceAccount:<NAME>@<PROJECT_ID>.iam.gserviceaccount.com" --role="roles/bigquery.dataViewer" --role="roles/bigquery.user"
```

Generate the key file:

```sh
gcloud iam service-accounts keys create <FILE_NAME>.json --iam-account=<NAME>@<PROJECT_ID>.iam.gserviceaccount.com
```

Set the environment variable with the path to the file you just created:

```sh
export GOOGLE_APPLICATION_CREDENTIALS="<path/to/FILE_NAME.json>"
```

You should be all set! If you run into any issues, you can find more detailed instructions for authentication [here](https://cloud.google.com/docs/authentication/getting-started)

### Using Jupyter

The simplest method is to use the extension and magic commands like so:

```python
%load_ext google.cloud.bigquery
```

and 

```python
%%bigquery <df_name>

SELECT 1 FROM `bigquery-public-data.crypto_bitcoin.transactions`
```

This stores the results of the query in a dataframe called `df_name`. For more examples, checkout [visualize in jupyter](https://cloud.google.com/bigquery/docs/visualize-jupyter)

### Using R

Using R is a bit easier to setup due to the excellent `bigrquery` package. To use, install the following packages:

```R
install.packages("bigrquery")
install.packages("httpuv")
```

In your R session, you can access BigQuery and DBI like so:

```R
library(bigrquery)
library(DBI)

project_id <- "<PROJEC_ID>"

con <- dbConnect(
  bigrquery::bigquery(),
  project = "bitcoin-data-analysis",
  dataset = "crypto_bitcoin",
  billing = project_id
)

sql <- "SELECT `hash`, fee FROM `bigquery-public-data.crypto_bitcoin.transactions` LIMIT 10"

dbGetQuery(con, sql, n = 10)
```

This will prompt you to authenticate on the first query, which will open a browser page for you to authenticate R to use your google account. This caches a token, so you will not need to re-authenticate on your next session. For more info on `bigrquery` and examples on using with `dplyr`, check out the repo: [https://github.com/r-dbi/bigrquery](https://github.com/r-dbi/bigrquery)
