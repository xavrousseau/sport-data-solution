"""
Script de validation Great Expectations pour la table 'employes'
"""

from great_expectations.data_context import get_context
from great_expectations.core.batch import RuntimeBatchRequest

# Initialiser le contexte GE (dossier tests/gx)
context = get_context(context_root_dir="tests/gx")

# Définir la requête SQL dynamique pour le batch
batch_request = RuntimeBatchRequest(
    datasource_name="sport_postgres",
    data_connector_name="default_runtime_connector_name",
    data_asset_name="employes",
    runtime_parameters={
        "query": "SELECT * FROM sportdata.employes"
    },
    batch_identifiers={
        "default_identifier_name": "employes_batch"
    }
)

# Créer ou mettre à jour le checkpoint nommé
checkpoint = context.add_or_update_checkpoint(
    name="checkpoint_employes",
    validations=[
        {
            "batch_request": batch_request,
            "expectation_suite_name": "employes_suite"
        }
    ]
)

# Exécuter le checkpoint et générer les rapports HTML
results = checkpoint.run()
context.build_data_docs()
print(results)
