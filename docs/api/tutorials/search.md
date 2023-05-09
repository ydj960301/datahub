import Tabs from '@theme/Tabs';
import TabItem from '@theme/TabItem';

# Search

## Why Would You Search Entities?

The difference between search and scroll:

### Goal Of This Guide

This guide will show you how to search and scroll specific entities or across entities.

## Prerequisites

For this tutorial, you need to deploy DataHub Quickstart and ingest sample data.
For detailed steps, please refer to [Datahub Quickstart Guide](/docs/quickstart.md).

:::note
Before updating deprecation, you need to ensure the targeted dataset is already present in your datahub.
If you attempt to manipulate entities that do not exist, your operation will fail.
In this guide, we will be using data from a sample ingestion.
:::

## Search Entities

### Search Specific Entity

In GraphQL, there are 2 mutations with searching : `search` and `searchAcrossEntities`.

`search` allows you to search against a specific entity - for example, Dataset or Tag.
On the other hand, `searchAcrossEntiteis` allows you to search across entities. You can specify the list of entities you want to search in `types`.

<Tabs>
<TabItem value="graphql" label="GraphQL" default>

```json
{
  search(input: {type: DATASET , query: "fct", start: 0, count: 100}) {
    searchResults {
      entity {
        ... on Dataset {
          urn
          type
          name
        }
      }
    }
  }
}
```

If you see the following response, the operation was successful:

```python
{
  "data": {
    "search": {
      "searchResults": [
        {
          "entity": {
            "urn": "urn:li:dataset:(urn:li:dataPlatform:hive,fct_users_deleted,PROD)",
            "type": "DATASET",
            "name": "fct_users_deleted"
          }
        },
        {
          "entity": {
            "urn": "urn:li:dataset:(urn:li:dataPlatform:hive,fct_users_created,PROD)",
            "type": "DATASET",
            "name": "fct_users_created"
          }
        }
      ]
    }
  },
  "extensions": {}
}
```

</TabItem>

<TabItem value="curl" label="Curl">

```shell
curl --location --request POST 'http://localhost:8080/api/graphql' \
--header 'Authorization: Bearer <my-access-token>' \
--header 'Content-Type: application/json' \
--data-raw '{ "query": "{ search(input: {type: DATASET , query: \"fct\", start: 0, count: 100}) { searchResults { entity { ... on Dataset { urn type name } } } } }", "variables":{} }'
```

Expected Response:

```json
{
  "data": {
    "search": {
      "searchResults": [
        {
          "entity": {
            "urn": "urn:li:dataset:(urn:li:dataPlatform:hive,fct_users_created,PROD)",
            "type": "DATASET",
            "name": "fct_users_created"
          }
        },
        {
          "entity": {
            "urn": "urn:li:dataset:(urn:li:dataPlatform:hive,fct_users_deleted,PROD)",
            "type": "DATASET",
            "name": "fct_users_deleted"
          }
        }
      ]
    }
  },
  "extensions": {}
}
```

</TabItem>

<TabItem value="python" label="Python">

```python
{{ inline /metadata-ingestion/examples/library/dataset_query_deprecation.py show_path_as_comment }}
```

</TabItem>
</Tabs>

### Search Across Entities

<Tabs>
<TabItem value="graphql" label="GraphQL" default>

```python
{
  searchAcrossEntities(input: {
    types: [DATASET, TAG],
    query: "Legacy",
  }) {
    searchResults {
      entity {
        urn
        type
      }
    }
  }
}
```

If you see the following response, the operation was successful:

```python
{
  "data": {
    "searchAcrossEntities": {
      "searchResults": [
        {
          "entity": {
            "urn": "urn:li:tag:Legacy",
            "type": "TAG"
          }
        },
        {
          "entity": {
            "urn": "urn:li:dataset:(urn:li:dataPlatform:hdfs,SampleHdfsDataset,PROD)",
            "type": "DATASET"
          }
        },
        {
          "entity": {
            "urn": "urn:li:dataset:(urn:li:dataPlatform:hive,SampleHiveDataset,PROD)",
            "type": "DATASET"
          }
        }
      ]
    }
  },
  "extensions": {}
}
```

This will search all DATASET or TAG entities that matches with text `Legacy` - for example, this will return

- Tag named Legacy
- Dataset with Legacy Tag attached
- Dataset with field description contains `Legacy`
  ...

To see what exact field matched with the query, you can use `matchedFields` like below.

```python
{
  searchAcrossEntities(input: {
    types: [DATASET, TAG],
    query: "Legacy",
  }) {
    searchResults {
      entity {
        urn
        type
      }
      matchedFields {
        name
        value
      }
    }
  }
}
```

</TabItem>

<TabItem value="curl" label="Curl">

```shell
curl --location --request POST 'http://localhost:8080/api/graphql' \
--header 'Authorization: Bearer <my-access-token>' \
--header 'Content-Type: application/json' \
--data-raw '{ "query": "{ dataset(urn: \"urn:li:dataset:(urn:li:dataPlatform:hive,fct_users_created,PROD)\") { deprecation { deprecated decommissionTime } } }", "variables":{} }'
```

Expected Response:

```json
{
  "data": {
    "dataset": {
      "deprecation": { "deprecated": false, "decommissionTime": null }
    }
  },
  "extensions": {}
}
```

</TabItem>

<TabItem value="python" label="Python">

```python
{{ inline /metadata-ingestion/examples/library/dataset_query_deprecation.py show_path_as_comment }}
```

</TabItem>
</Tabs>

### Search Across Lineage

## Scroll Entities

### Scroll Across Entities

<Tabs>
<TabItem value="graphql" label="GraphQL" default>

```json

```

Also note that you can update deprecation status of multiple entities or subresource using `batchUpdateDeprecation`.

```json
mutation batchUpdateDeprecation {
    batchUpdateDeprecation(
      input: {
        deprecated: true,
        resources: [
          { resourceUrn:"urn:li:dataset:(urn:li:dataPlatform:hdfs,SampleHdfsDataset,PROD)"} ,
          { resourceUrn:"urn:li:dataset:(urn:li:dataPlatform:hive,fct_users_created,PROD)"} ,]
      }
    )
}

```

If you see the following response, the operation was successful:

```python
{
  "data": {
    "updateDeprecation": true
  },
  "extensions": {}
}
```

</TabItem>

<TabItem value="curl" label="Curl">

```shell
curl --location --request POST 'http://localhost:8080/api/graphql' \
--header 'Authorization: Bearer <my-access-token>' \
--header 'Content-Type: application/json' \
--data-raw '{ "query": "mutation updateDeprecation { updateDeprecation(input: { deprecated: true, urn: \"urn:li:dataset:(urn:li:dataPlatform:hive,fct_users_created,PROD)\" }) }", "variables":{}}'
```

Expected Response:

```json
{ "data": { "removeTag": true }, "extensions": {} }
```

</TabItem>

<TabItem value="python" label="Python">

</TabItem>
</Tabs>

### Scroll Across Lineage
