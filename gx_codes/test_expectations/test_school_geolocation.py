import great_expectations as gx

context_root_dir = "./great_expectations/"
context = gx.get_context(context_root_dir=context_root_dir)

# CHECK: Retrieve data asset
my_asset = context.get_datasource("azure_blob_storage").get_asset("school_geolocation")

# Organize Batches
my_datasource = context.get_datasource("azure_blob_storage")
my_batch_request = my_asset.build_batch_request()
batches = my_asset.get_batch_list_from_batch_request(my_batch_request)

expectation_suite_name = "expectation_school_geolocation"
# expectation_suite = context.add_or_update_expectation_suite(
#     expectation_suite_name=expectation_suite_name
# )
checkpoint = context.add_or_update_checkpoint(
    name="school_geolocation_checkpoint",
    validations=[
        {
            "batch_request": my_batch_request,
            "expectation_suite_name": expectation_suite_name,
        },
    ],
)

checkpoint_result = checkpoint.run()
print("RESULTS")
print(checkpoint_result)
context.build_data_docs()
