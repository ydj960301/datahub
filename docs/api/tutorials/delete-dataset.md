# Deleting Datasets

## Why Would You Delete Datasets? 
The dataset entity is one the most important entities in the metadata model.
It is possible to [delete entities via CLI](/docs/how/delete-metadata.md), but for scalability, a programmatic approach is necessary.

For more information about datasets, refer to [Dataset](/docs/generated/metamodel/entities/dataset.md).

### Goal Of This Guide
This guide will show you how to delete a dataset named `fct_user_deleted`.

## Prerequisites
For this tutorial, you need to deploy DataHub Quickstart and ingest sample data. 
For detailed steps, please refer to [Prepare Local DataHub Environment](/docs/api/tutorials/references/prepare-datahub.md).

## Delete Datasets With GraphQL (Not Supported)

> 🚫 Deleting a dataset via GraphQL is currently not supported.
> Please check out [API feature comparison table](/docs/api/datahub-apis.md#datahub-api-comparison) for more information, 

## Delete Datasets With Python SDK

The following code deletes a hive dataset named `fct_users_deleted` with three fields. 
You can refer to the complete code in [delete_dataset]().
```python
```

We're using the `MetdataChangeProposalWrapper` to change entities in this example.
For more information about the `MetadataChangeProposal`, please refer to [MetadataChangeProposal & MetadataChangeLog Events](/docs/advanced/mcp-mcl.md)


## Expected Outcomes
You can now see `fct_users_deleted` dataset has been deleted.

![dataset-deleted](../../imgs/apis/tutorials/dataset-deleted.png)



