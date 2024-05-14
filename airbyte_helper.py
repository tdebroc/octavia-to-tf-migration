import requests
import json


class AirbyteHelper:
    def __init__(self, airbyte_base_url, client_id, client_secret):
        self.airbyte_base_url = airbyte_base_url
        self.client_id = client_id
        self.client_secret = client_secret

    def launch_request(self, url_path, data):
        if data is None or len(data.keys()) == 0:
            headers = {}
        else:
            headers = {'Content-type': 'application/json'}
        resp = requests.post(self.airbyte_base_url + "/api" + url_path, data=json.dumps(data), headers=headers,
                             auth=(self.client_id, self.client_secret))
        if resp.status_code > 299:
            print(Exception(str(resp.status_code) + "_" + str(resp.content)))
        return resp

    # ======================================================================
    # Workspaces
    # ======================================================================
    def create_workspace(self, workspace):
        resp = self.launch_request("/v1/workspaces/create", workspace)
        return resp.json()

    def delete_workspace(self, workspace_id):
        resp = self.launch_request("/v1/workspaces/delete", {"workspaceId": workspace_id})
        return True

    def list_workspaces(self):
        resp = self.launch_request("/v1/workspaces/list", {})
        return resp.json()["workspaces"]

    def get_workspace(self, workspace_id):
        resp = self.launch_request("/v1/workspaces/get", {"workspaceId": workspace_id})
        return resp.json()

    def get_workspace_by_slug(self, slug):
        resp = self.launch_request("/v1/workspaces/get_by_slug", {"slug": slug})
        return resp.json()

    def get_first_workspace_id(self):
        return self.list_workspaces()[0]["workspaceId"]

    def get_workspace_by_connection_id(self, connection_id):
        resp = self.launch_request("/v1/workspaces/get_by_connection_id", {"connectionId": connection_id})
        return resp.json()

    def update_workspace(self, workspace):
        resp = self.launch_request("/v1/workspaces/update", workspace)
        return resp.json()

    def update_workspace_name(self, workspace_id, new_name):
        resp = self.launch_request("/v1/workspaces/update", {
            "workspaceId": workspace_id,
            "name": new_name
        })
        return resp.json()

    def update_workspace_tag_feedback_status_as_done(self, workspace_id):
        resp = self.launch_request("/v1/workspaces/tag_feedback_status_as_done", {"workspaceId": workspace_id})
        return resp.json()

    # ======================================================================
    # Sources
    # ======================================================================
    def create_sources(self, source):
        resp = self.launch_request("/v1/sources/create", source)
        return resp.json()

    def update_source(self, source):
        resp = self.launch_request("/v1/sources/update", source)
        return resp.json()

    def list_sources(self, workspace_id=None):
        if workspace_id is None:
            workspace_id = self.list_workspaces()[0]["workspaceId"]
        resp = self.launch_request("/v1/sources/list", {"workspaceId": workspace_id})
        return resp.json()["sources"]

    def get_source(self, source_id):
        resp = self.launch_request("/v1/sources/get", {"sourceId": source_id})
        return resp.json()

    def get_source_most_recent_source_actor_catalog(self, source_id):
        resp = self.launch_request("/v1/sources/get_most_recent_source_actor_catalog", {"sourceId": source_id})
        return resp.json()

    def search_source(self, search_source_body):
        resp = self.launch_request("/v1/sources/search", search_source_body)
        return resp.json()

    def clone_source(self, clone_source_body):
        resp = self.launch_request("/v1/sources/clone", clone_source_body)
        return resp.json()

    def delete_source(self, source_id):
        resp = self.launch_request("/v1/sources/delete", {"sourceId": source_id})
        return resp.json()

    def check_connection_source(self, source_id):
        resp = self.launch_request("/v1/sources/check_connection", {"sourceId": source_id})
        return resp.json()

    def check_connection_for_update_source(self, check_connection_for_update_body):
        resp = self.launch_request("/v1/sources/check_connection_for_update", check_connection_for_update_body)
        return resp.json()

    def discover_schema_source(self, source_id, connection_id, disable_cache, notify_schema_change):
        resp = self.launch_request("/v1/sources/check_connection_for_update", {
            "sourceId": source_id,
            "connectionId": connection_id,
            "disable_cache": disable_cache,
            "notifySchemaChange": notify_schema_change
        })
        return resp.json()

    def write_discover_catalog_result_source(self, write_discover_catalog_result_body):
        resp = self.launch_request("/v1/sources/write_discover_catalog_result", write_discover_catalog_result_body)
        return resp.json()

    # ======================================================================
    # Destinations
    # ======================================================================

    def create_destinations(self, destination):
        resp = self.launch_request("/v1/destination_definitions/create", destination)
        return resp.json()

    def update_destinations(self, destination):
        resp = self.launch_request("/v1/destination_definitions/update", destination)
        return resp.json()

    def list_destinations(self, workspace_id=None):
        if workspace_id is None:
            workspace_id = self.list_workspaces()[0]["workspaceId"]
        resp = self.launch_request("/v1/destinations/list", {"workspaceId": workspace_id})
        return resp.json()["destinations"]

    def get_destination(self, destination_id):
        resp = self.launch_request("/v1/destinations/get", {"destinationId": destination_id})
        return resp.json()

    def delete_destination(self, destination_id):
        self.launch_request("/v1/destinations/delete", {"destinationId": destination_id})
        return True

    def search_destination(self, search_destination_body):
        resp = self.launch_request("/v1/destinations/search", search_destination_body)
        return resp.json()

    def check_connection_destination(self, destination_id):
        resp = self.launch_request("/v1/destinations/check_connection", {"destinationId": destination_id})
        return resp.json()

    def check_connection_for_update_destination(self, check_connection_for_update_body):
        resp = self.launch_request("/v1/destinations/check_connection_for_update", check_connection_for_update_body)
        return resp.json()

    def clone_destination(self, clone_body):
        resp = self.launch_request("/v1/destinations/clone", clone_body)
        return resp.json()

    def delete_all_destinations(self, workspace_id=None):
        if workspace_id is None:
            workspace_id = self.list_workspaces()[0]["workspaceId"]
        print("Workspace ID", workspace_id)
        destinations = self.list_destinations(workspace_id)
        for destination in destinations:
            print("deleting", destination["destinationId"])
            self.delete_destination(destination["destinationId"])

    # ======================================================================
    # Connections
    # ======================================================================

    def create_connection(self, connection):
        resp = self.launch_request("/v1/connections/create", connection)
        return resp.json()

    def update_connection(self, connection):
        resp = self.launch_request("/v1/connections/update", connection)
        return resp.json()

    def list_connections(self, workspace_id=None):
        if workspace_id is None:
            workspace_id = self.list_workspaces()[0]["workspaceId"]
        resp = self.launch_request("/v1/connections/list", {"workspaceId": workspace_id})
        return resp.json()["connections"]

    def list_all_connections(self, workspace_id):
        resp = self.launch_request("/v1/connections/list_all", {"workspaceId": workspace_id})
        return resp.json()

    def get_connection(self, connection_id):
        resp = self.launch_request("/v1/connections/create", {"connectionId": connection_id})
        return resp.json()

    def trigger_connection_sync(self, connection_id):
        resp = self.launch_request("/v1/connections/sync", {"connectionId": connection_id})
        return resp.json()

    def delete_connection(self, connection_id):
        self.launch_request("/v1/connections/delete", {"connectionId": connection_id})
        return True

    def search_connections(self, search_connection_body):
        resp = self.launch_request("/v1/connections/search", search_connection_body)
        return resp.json()

    def reset_connection(self, connection_id):
        resp = self.launch_request("/v1/connections/reset", {"connectionId": connection_id})
        return resp.json()

    # ======================================================================
    # Connections
    # ======================================================================
    def get_logs(self):
        resp = self.launch_request("/v1/logs/get", {"logType": "server"})
        return resp.text