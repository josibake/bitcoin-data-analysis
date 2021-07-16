# Bitcoin Data Analysis

A repo containing notebooks and scripts for analyzing the bitcoin blockchain. This repo analyzes data from the BigQuery public dataset published by google. For more info about the datasets and how they are published, check out:

* [BigQuery cryptocurrency public datasets](https://cloud.google.com/blog/products/data-analytics/introducing-six-new-cryptocurrencies-in-bigquery-public-datasets-and-how-to-analyze-them)
* [github.com/blockchain-etl](https://github.com/blockchain-etl)

## Setup

These instructions assume you already have `gcloud` setup. If not, you can follow installation instructions [here](https://cloud.google.com/sdk/docs/install). Alternatively, you can do all of these steps from the google console, but only the `gcloud` commands are provided here for brevity.

### Create a new project

The following command creates a project under your account with the given name (feel free to choose your own). This will prompt you to accept the generated ID. Make sure to copy the ID as this is what you will use for `<PROJECT_ID>` for the remaining commands.

```sh
gcloud projects create --name="Bitcoin data analysis"
```

### Authentication

There are multiple ways you can authenticate, but the simplest is to create a service account, attach it to your project, and then copy the keys locally.

Create the service account under the project you created before:

```sh
gcloud iam service-accounts create <NAME> --project <PROJECT_ID>
```

Grant permissions. Specify a name for the service account and use the the project id from before. Notice we are adding the `bigquery.dataViewer` and `bigquery.user` roles (feel free to add any additional roles you need):

```sh
gcloud projects add-iam-policy-binding <PROJECT_ID> --member="serviceAccount:<NAME>@<PROJECT_ID>.iam.gserviceaccount.com" --role="roles/bigquery.dataViewer/bigquery.user"
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

tbd
