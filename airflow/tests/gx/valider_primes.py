"""
Script de validation Great Expectations pour la table 'beneficiaires_primes_sport'
"""

from great_expectations.data_context import get_context
from great_expectations.core.batch import RuntimeBatchRequest

context = get_context(context_root_dir="tests/gx")

batch_request = RuntimeBatchRequest(
    datasource_name="sport_postgres",
    data_connector_name="default_runtime_connector_name",
    data_asset_name="beneficiaires_primes_sport",
    runtime_parameters={
        "query": "SELECT * FROM sportdata.beneficiaires_primes_sport"
    },
    batch_identifiers={
        "default_identifier_name": "primes_batch"
    }
)

checkpoint = context.add_or_update_checkpoint(
    name="checkpoint_primes",
    validations=[
        {
            "batch_request": batch_request,
            "expectation_suite_name": "primes_suite"
        }
    ]
)

results = checkpoint.run()
context.build_data_docs()
print(results)
