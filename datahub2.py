import logging
import os

import requests
import yaml

import os, uuid
from azure.storage.blob import BlobServiceClient, BlobClient, ContainerClient, __version__

class Datahub:
    cwd = os.getcwd()
    config = []

    def __init__(self, log_dir='/home/b4h'):
        self._log = logging.getLogger(__name__)
        self._log_dir = log_dir  # TODO: add this as a config option
        self.load_config()
        self.register()

    def _log_http_error(self, url, error):
        if self._log_dir is None:
            return
        with open(self._log_dir + '/http-error.log', 'a') as file:
            file.write('url={}\n'.format(url))
            file.write('  result={}\n'.format(error))

    def register(self):
        if "token" not in self.config.keys():
            self._log.info("Claiming apikey at datahub back-end")
            url = self.config["endpoint"] + '/participant/' + self.config["participant"] + '/claim'
            try:
                r = requests.put(url,
                                 json={"apiKey": self.config["apikey"]},
                                 headers={'Accept': 'application/json', 'Content-type': 'application/json'})
                if r.status_code == 200:
                    self.config["token"] = r.json()["jwt"]
                    self.dump_config()
                else:
                    self._log.error("Request '{}' resulted in error {}"
                                    .format(url, r.status_code))
                    self._log_http_error(url, r.text)
                    #sys.exit(-1)  # script can not function when this step fails
            except requests.exceptions.ConnectionError as e:
                self._log.error("Request '{}' resulted in a connection error: {}"
                                .format(url, e))
                self._log_http_error(url, e)
                #sys.exit(-1)  # script can not function when this step fails
            except requests.exceptions.RequestException as e:
                self._log.error("Request '{}' resulted in a request error: {}"
                                .format(url, e))
                self._log_http_error(url, e)
                #sys.exit(-1)  # script can not function when this step fails

    def upload(self, sessions_file, _callback=None):
        if 'token' not in self.config:
            self._log.warning("not uploading data, 'token' missing from configuration")
            return

        file_name = str(sessions_file)
        if len(sessions_file.split('/')) > 1:
            file_name = sessions_file.split('/')[len(sessions_file.split('/')) - 1]

        connect_str="DefaultEndpointsProtocol=https;AccountName=bathroom4healthstorage;AccountKey=9BRpcj3/dcRsT/5mvQQOiR4bGP1r8KSQo5WFua9K0B81lpRQeibxiIUTQ3O74fv2NTknuU/lH//tJf5ZnL9wtg==;EndpointSuffix=core.windows.net"
        blob_service_client = BlobServiceClient.from_connection_string(connect_str)
        container_name="aqilcontainer"
        blob_client = blob_service_client.get_blob_client(container=container_name, blob=sessions_file)
        print("\nUploading to Azure Storage as blob:\n\t" + sessions_file)
        with open(sessions_file, "rb") as data:
            blob_client.upload_blob(data)

    def load_config(self):
        with open(self.cwd + '/datahub-config.yaml', "r") as ymlfile:
            self.config = yaml.safe_load(ymlfile)

    def dump_config(self):
        self._log.info("Updating configuration file")
        with open(self.cwd + '/datahub-config.yaml', "w") as ymlfile:
            yaml.safe_dump(self.config, ymlfile, default_flow_style=False)
