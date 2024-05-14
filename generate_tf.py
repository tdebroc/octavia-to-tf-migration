import json
import os
import re

import httpx
import yaml
from git.repo.base import Repo
import shutil
from pathlib import Path

from airbyte_helper import AirbyteHelper

OUTPUT = ""
PROD_ENV = "prod"
DP_NAME = os.environ["DP_NAME"]
# DP_NAME = "glb-degreed-learning"
GIT_ORGANIZATION = os.environ["GIT_ORGANIZATION"]

repo = f"{GIT_ORGANIZATION}/data-product-{DP_NAME.replace('-', '_')}"
FOLDER = "data_product"
airbyte_helpers = {
    "dev": AirbyteHelper(
        os.environ["DEV_AIRBYTE_URL"], os.environ["DEV_AIRBYTE_CLIENT_ID"], os.environ["DEV_AIRBYTE_CLIENT_SECRET"]
    ),
    "prod": AirbyteHelper(
        os.environ["PROD_AIRBYTE_URL"], os.environ["PROD_AIRBYTE_CLIENT_ID"], os.environ["PROD_AIRBYTE_CLIENT_SECRET"]
    )
}
all_envs = airbyte_helpers.keys()
sources = {}
connections = {}
destinations = {}

for env_value in all_envs:
    destinations[env_value] = airbyte_helpers[env_value].list_destinations()
    if len(destinations[env_value]) > 1 and "sp_lm" in DP_NAME:
        raise Exception("We made the script working for only one destination. Is it possible to delete 1 ?")
    sources[env_value] = airbyte_helpers[env_value].list_sources()
    connections[env_value] = airbyte_helpers[env_value].list_connections()


def init_repo_locally():
    if os.path.exists(FOLDER):
        shutil.rmtree(FOLDER)

    Repo.clone_from(
        f"git@github.com:{repo}.git",
        FOLDER,
        branch="main",
    )


TOKEN_GITHUB = os.environ["TOKEN_GITHUB"]
client = httpx.Client(
    base_url="https://graph.microsoft.com", headers={"Authorization": f"Bearer {TOKEN_GITHUB}"}
)


def init_output():
    add_to_output("""
terraform {
  required_providers {
    airbyte = {
      source  = "airbytehq/airbyte"
      version = "0.4.2"
    }
    google = {
      source  = "hashicorp/google"
      version = "5.10.0"
    }
  }
  backend "gcs" {
  }
}
    """)


def get_gh_secrets():
    response = client.get(f"https://api.github.com/repos/{repo}/actions/secrets")
    return response.json()


def get_airbyte_url():
    return os.environ["AIRBYTE_URL"]


def create_global_vars():
    add_to_output(
        f"""
variable "WORKSPACE_ID" {{
  description = "ID of the Airbyte Workspace."
}}

variable "ENV" {{
  description = "Environment. 'dev' or 'prod'"
}}

locals {{
  bigquery_gcp_project = "{DP_NAME.replace("_", "-")}-${{var.ENV}}"
}}

data \"google_secret_manager_secret_version\" \"airbyte_ingestion_account_secret\" {{
  project = local.bigquery_gcp_project
  secret  = \"${{local.bigquery_gcp_project}}-airbyte_ingestion_account_secret\"
  version = \"latest\"
}}

data \"google_secret_manager_secret_version\" \"airbyte_ingestion_airbyte_hmac_key_secret\" {{
  project = local.bigquery_gcp_project
  secret  = \"${{local.bigquery_gcp_project}}-airbyte_hmac_key_secret\"
  version = \"latest\"
}}

data \"google_secret_manager_secret_version\" \"airbyte_ingestion_airbyte_hmac_key_id\" {{
  project = local.bigquery_gcp_project
  secret  = \"${{local.bigquery_gcp_project}}-airbyte_hmac_key_id\"
  version = "latest"
}}



"""
    )


def create_vars_for_secrets():
    secrets = get_gh_secrets()
    unique_secrets = set([secret["name"].replace("DEV_", "").replace("PROD_", "") for secret in secrets["secrets"]])
    for secret in unique_secrets:
        if secret in ["INGESTION_ACCOUNT_HMAC_KEY_ID", "INGESTION_ACCOUNT_HMAC_KEY_SECRET",
                      "INGESTION_ACCOUNT_SECRET_JSON", "AIRBYTE_URL"]:
            # Already handled by Google Secrets
            continue
        var_content = f"""
variable "{secret}" {{
  description = "Variable for {secret}."
  sensitive   = true
}}
        """
        add_to_output(var_content)

    add_to_output(

        f"""
        
variable "AIRBYTE_URL" {{
  description = "Url of Airbyte."
}}

provider "airbyte" {{
  password = var.AIRBYTE_CLIENT_SECRET
  username = var.AIRBYTE_CLIENT_ID

  server_url = "${{var.AIRBYTE_URL}}/api/public/v1"
}}
            """
    )


def add_bq_tf():
    remote_destination_found = {}
    for env in all_envs:
        remote_destination_found[env] = [destination for destination in destinations[env]]
    add_import_for_all_envs(f'airbyte_destination_bigquery.bigquery', remote_destination_found, "destinationId")
    add_to_output(
        """
resource "airbyte_destination_bigquery" "bigquery" {
  name          = "BigQuery"
  configuration = {
    big_query_client_buffer_size_mb = 15
    credentials_json                = data.google_secret_manager_secret_version.airbyte_ingestion_account_secret.secret_data
    dataset_id                      = "airbyte_ingestion"
    dataset_location                = "EU"
    disable_type_dedupe             = false
    loading_method                  = {
      gcs_staging = {
        credential = {
          hmac_key = {
            hmac_key_access_id = data.google_secret_manager_secret_version.airbyte_ingestion_airbyte_hmac_key_id.secret_data  # noqa
            hmac_key_secret    = data.google_secret_manager_secret_version.airbyte_ingestion_airbyte_hmac_key_secret.secret_data  # noqa
          }
        }
        gcs_bucket_name          = "${local.bigquery_gcp_project}-airbyte-ingestion"
        gcs_bucket_path          = "gcs"
        keep_files_in_gcs_bucket = "Keep all tmp files in GCS"
      }
    }
    project_id              = "${local.bigquery_gcp_project}"
    transformation_priority = "interactive"
  }
  definition_id = "22f6c74f-5699-40ff-833c-4a879ea40133"
  workspace_id  = var.WORKSPACE_ID
}
    
        """)


def treat_all_octavia():
    for root, dirs, files in os.walk(f"{FOLDER}/airbyte/", topdown=True):
        dirs.sort(reverse=True)
        for name in files:
            print("#", root, dirs, files)
            if name == "configuration.yaml":
                file_path = f"{root}/configuration.yaml"
                content = yaml.safe_load(Path(file_path).read_text())
                if "/sources/" in file_path:
                    convert_source(file_path, content)
                if "/connections/" in file_path:
                    convert_connections(file_path, content)


def convert_source(file_path, content):
    source_name = file_path.split("/")[3]

    remote_source_found = {}
    for env in all_envs:
        remote_source_found[env] = [source for source in sources[env] if source["name"] == content["resource_name"]]
    if any(len(remote_source_found[env]) > 1 for env in all_envs):
        raise Exception("Too much sources found")

    del content["definition_type"]
    del content["definition_version"]
    content["name"] = content["resource_name"]
    del content["resource_name"]
    tf_package = content["definition_image"].replace("/", "_").replace("-", "_")
    del content["definition_image"]
    content["workspace_id"] = "${var.WORKSPACE_ID}"

    add_to_output("\n\n\n")

    source_tf_name = source_name.replace(' ', '_')
    source_path_tf = f"{tf_package}.{source_tf_name}"
    for env in all_envs:
        source_id_to_tf_name[remote_source_found[env][0]["sourceId"]] = source_path_tf

    add_import_for_all_envs(f'{source_path_tf}', remote_source_found, "sourceId")

    add_to_output(f"resource \"{tf_package}\" \"{source_name}\"")
    source_tf = json_to_tf(content)
    source_tf = add_var_to_secrets(source_tf)

    add_to_output(source_tf)


def fix_github_source(source_tf):
    founds = re.findall(r"credentials *= *{.*\"OAuth Credentials\"\n *}", source_tf, re.DOTALL)
    for found in founds:
        source_tf = source_tf.replace(
            found, "credentials = { personal_access_token = { personal_access_token = var.ACCESS_TOKEN } }"
        )
    return source_tf


def add_var_to_secrets(source_tf):
    founds = re.findall(r"\${[^var].*}", source_tf)
    for found in founds:
        source_tf = source_tf.replace(found, found.replace("${", "${var."))
    return source_tf


def add_import_for_all_envs(tf_path, remote_ids, id_key):
    add_to_output(f"""
import {{
  for_each = toset(var.ENV == \"dev\" ? [{{unique_id: \"{remote_ids["dev"][0][id_key]}\"}}] : [{{unique_id: \"{remote_ids["prod"][0][id_key]}\"}}])  # noqa
  to = {tf_path}
  id = each.value.unique_id
}}
        """)


def get_sync_mode(stream):
    sync_mode = stream["config"]["sync_mode"] + "_" + stream["config"]["destination_sync_mode"]
    return sync_mode.replace("incremental_append_dedup", "incremental_deduped_history")


def convert_connections(file_path, content):
    connection_name = file_path.split("/")[3]

    remote_connection_found = {}
    for env in all_envs:
        remote_connection_found[env] = [conn for conn in connections[env] if conn["name"] == content["resource_name"]]
    if any(len(remote_connection_found[env]) > 1 for env in all_envs):
        raise Exception("Too much remote_connections found")

    connection = remote_connection_found[PROD_ENV][0]
    connection_tf = {
        # "data_residency": "eu",
        "destination_id": "${airbyte_destination_bigquery.bigquery.destination_id}",
        "name": connection["name"],
        "namespace_definition": connection["namespaceDefinition"].replace("customformat", "custom_format"),
        "namespace_format": connection["namespaceFormat"],
        "non_breaking_schema_updates_behavior": connection["nonBreakingChangesPreference"],
        "source_id": f"${{{source_id_to_tf_name[connection['sourceId']]}.source_id}}",
        "status": connection["status"],
        "schedule": {
            "schedule_type": connection["scheduleType"]
        },
        "configurations": {

        }
    }
    if connection["scheduleType"] == "cron":
        connection_tf["schedule"]["cron"] = connection["scheduleData"]["cron"]["cronExpression"]
    elif connection["scheduleType"] != "manual":
        connection_tf["schedule"]["cron"] = "TODO: Convert to CRON"

    connection_tf["configurations"]["streams"] = []
    for stream in content["configuration"]["sync_catalog"]["streams"]:
        stream_tf = {
            "name": stream["stream"]["name"],
            "sync_mode": get_sync_mode(stream),
            "cursor_field": stream["stream"]["default_cursor_field"],
            "primary_key": stream["config"]["primary_key"],
        }
        connection_tf["configurations"]["streams"].append(stream_tf)

    tf_package = "airbyte_connection"
    connection_name_tf = f"{tf_package}_{connection_name.replace(' ', '_')}"
    connection_path_tf = f"{tf_package}.{connection_name_tf}"

    add_to_output("\n\n\n")

    add_import_for_all_envs(f'{connection_path_tf}', remote_connection_found, "connectionId")

    add_to_output(f"resource \"{tf_package}\" \"{connection_name_tf}\"")
    connection_tf = json_to_tf(connection_tf)
    # connection_tf = add_var_to_secrets(connection_tf)
    add_to_output(connection_tf)


def json_to_tf(json_content):
    tf_str = json.dumps(json_content, indent=4, separators=('', ' = '))
    tf_str = "\n".join([format_line(line) for line in tf_str.split("\n")])

    founds = re.findall(r"\[[^{]*]", tf_str, re.DOTALL)
    for found in founds:
        tf_str = tf_str.replace(found, found.replace("}", "},"))

    return tf_str


def format_line(line):
    if " = " in line:
        key, value = line.split(" = ")
        key = key.replace("\"", "")
        return f"{key} = {value}"
    return line


def add_to_output(content):
    global OUTPUT
    OUTPUT += content + "\n"


def write_tf_file(output_tf):
    with open('main.tf', 'w') as file:
        file.write(output_tf)
    print("terraform fmt")
    os.system('terraform fmt')


def clean_file_to_valid_tf():
    global OUTPUT

    # Remove all the double quotes around the variables:
    founds = re.findall(r"\"\${.*}\"", OUTPUT)
    for found in founds:
        if "SOURCE_NAMESPACE" in found:
            continue
        OUTPUT = OUTPUT.replace(found, found.replace("\"${", "").replace("}\"", ""))

    # Special case in Airbyte: If "${SOURCE_NAMESPACE}" then behaves like namespaceDefinition = 'source'.
    OUTPUT = OUTPUT.replace("${SOURCE_NAMESPACE}", "$${SOURCE_NAMESPACE}")
    # Needed for resource
    OUTPUT = OUTPUT.replace("\n{", "{")

    # Handles Arrays:
    founds = re.findall(r"] *\n *\[", OUTPUT)
    for found in founds:
        OUTPUT = OUTPUT.replace(found, found.replace("]", "],"))

    # Handles Arrays:
    founds = re.findall(r"} *\n *\{", OUTPUT)
    for found in founds:
        OUTPUT = OUTPUT.replace(found, found.replace("}", "},"))

    if "_github_" in DP_NAME:
        OUTPUT = fix_github_source(OUTPUT)

    # Due to a bug in Airbyte, https://github.com/airbytehq/terraform-provider-airbyte/issues/88
    # we need to replace definition_id by # definition_id:
    OUTPUT = OUTPUT.replace("definition_id", "# definition_id")

    OUTPUT = OUTPUT.replace("airbyte_source_declarative_manifest", "airbyte_source_custom")

    OUTPUT = OUTPUT.replace("primary_key = []", "")
    OUTPUT = OUTPUT.replace("cursor_field = []", "")




init_output()
source_id_to_tf_name = {}
init_repo_locally()
create_vars_for_secrets()
create_global_vars()
add_bq_tf()
treat_all_octavia()

clean_file_to_valid_tf()
print(OUTPUT)

write_tf_file(OUTPUT)
