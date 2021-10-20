# rs-couchbase is available under the MIT License. https://github.com/RoundServices/rs-couchbase/
# Copyright (c) 2020, Round Services LLC - https://roundservices.biz/
#
# Author: Gustavo J Gallardo - ggallard@roundservices.biz
#

import json
from couchbase.auth import PasswordAuthenticator
from couchbase.cluster import Cluster, ClusterOptions
import couchbase.subdocument as SD
from rs.utils.basics import Logger


########################################################################################################################
########## CLASSES #####################################################################################################
########################################################################################################################

class CouchbaseClient:
    def __init__(self, cb_uri, cb_username, cb_password, logger=Logger("CouchbaseClient")):
        self._logger = logger
        self._logger.debug("Connecting to: {}".format(cb_uri))
        self._cb_cluster = Cluster(cb_uri, ClusterOptions(PasswordAuthenticator(cb_username, cb_password)))

    def import_json_file(self, json_fn, cb_bucket, cb_documentid):
        self._logger.info("Importing file: {} into bucket: {}, document_id: {}", json_fn, cb_bucket, cb_documentid)
        with open(json_fn, "r") as json_file:
            json_data = json_file.read().replace('\n', '')
            json_file.close()
        json_document = json.loads(json_data)
        self.import_json(json_document, cb_bucket, cb_documentid)

    def export_json_file(self, cb_bucket, cb_documentid, json_fn):
        self._logger.info("Exporting document_id: {} from bucket: {} into JSON file: {}", cb_bucket, cb_documentid, json_fn)
        json_document = self.export_json(cb_bucket, cb_documentid)
        with open(json_fn, "w") as json_file:
            json.dump(json_document, json_file)
            json_file.close()

    def import_json(self, json_document, cb_bucket, cb_documentid):
        self._logger.info("Importing json document into bucket: {}, document_id: {}", cb_bucket, cb_documentid)
        self._logger.debug("Opening bucket: {}", cb_bucket)
        cbBucket = self._cb_cluster.bucket(cb_bucket)
        self._logger.debug("Inserting document_id: {}", cb_documentid)
        cbBucket.upsert(cb_documentid, json_document)

    def export_json(self, cb_bucket, cb_documentid):
        self._logger.info("Exporting document_id: {} from bucket: {} into JSON object", cb_documentid, cb_bucket)
        self._logger.debug("Opening bucket: {}", cb_bucket)
        cbBucket = self._cb_cluster.bucket(cb_bucket)
        self._logger.debug("Getting document_id: {}", cb_documentid)
        json_document = cbBucket.get(cb_documentid).value
        self._logger.trace("Returning json: {}", json_document)
        return json_document

    def export_key(self, cb_bucket, cb_documentid, cb_key):
        self._logger.info("Exporting key: {} from document_id: {} from bucket: {} into JSON object", cb_key, cb_documentid, cb_bucket)
        self._logger.debug("Opening bucket: {}", cb_bucket)
        cbBucket = self._cb_cluster.bucket(cb_bucket)
        self._logger.debug("Getting key: {} from document_id: {}", cb_key, cb_documentid)
        cbSubdoc = cbBucket.lookup_in(cb_documentid, [SD.get(cb_key)])
        json_key = cbSubdoc[0]
        self._logger.trace("Returning json: {}", json_key)
        return json_key

    def import_key(self, cb_bucket, cb_documentid, cb_key, key_value):
        self._logger.info("Upserting value: {} in key: {}. document_id: {}. bucket: {}.", key_value, cb_key, cb_documentid, cb_bucket)
        self._logger.debug("Opening bucket: {}", cb_bucket)
        cbBucket = self._cb_cluster.bucket(cb_bucket)
        cbBucket.mutate_in(cb_documentid, [SD.upsert(cb_key, key_value)])

    def list_users(self):
        self._logger.info("Listing users")
        cbManager = self._cb_cluster.cluster_manager()
        cbUsers = cbManager.users_get(AuthDomain.Local).value
        self._logger.debug("Local Users: {}, type: {}", cbUsers, type(cbUsers))
        return cbUsers

    def create_user(self, user_name, user_password, user_roles):
        cbManager = self._cb_cluster.cluster_manager()
        self._logger.info("Creating user '{}' with roles '{}'", user_name, user_roles)
        cbManager.user_upsert(AuthDomain.Local, user_name, user_password, user_roles)

########################################################################################################################
########## FUNCTIONS ###################################################################################################
########################################################################################################################


def json2couchbase(document_path, couchbase_uri, couchbase_username, couchbase_password, couchbase_bucket, document_id, logger):
    couchbaseClient = CouchbaseClient(couchbase_uri, couchbase_username, couchbase_password, logger)
    couchbaseClient.import_json_file(document_path, couchbase_bucket, document_id)


def couchbase2json(couchbase_uri, couchbase_username, couchbase_password, couchbase_bucket, document_id, document_path, logger):
    couchbaseClient = CouchbaseClient(couchbase_uri, couchbase_username, couchbase_password, logger)
    couchbaseClient.export_json_file(couchbase_bucket, document_id, document_path)


def import_document(couchbase_uri, couchbase_username, couchbase_password, couchbase_bucket, document_id, json_document, logger):
    couchbaseClient = CouchbaseClient(couchbase_uri, couchbase_username, couchbase_password, logger)
    couchbaseClient.import_json(json_document, couchbase_bucket, document_id)


def export_document(couchbase_uri, couchbase_username, couchbase_password, couchbase_bucket, document_id, logger):
    couchbaseClient = CouchbaseClient(couchbase_uri, couchbase_username, couchbase_password, logger)
    return couchbaseClient.export_json(couchbase_bucket, document_id)


def import_key(couchbase_uri, couchbase_username, couchbase_password, couchbase_bucket, document_id, document_key, key_value, logger):
    couchbaseClient = CouchbaseClient(couchbase_uri, couchbase_username, couchbase_password, logger)
    couchbaseClient.import_key(couchbase_bucket, document_id, document_key, key_value)


def export_key(couchbase_uri, couchbase_username, couchbase_password, couchbase_bucket, document_id, document_key, logger):
    couchbaseClient = CouchbaseClient(couchbase_uri, couchbase_username, couchbase_password, logger)
    return couchbaseClient.export_key(couchbase_bucket, document_id, document_key)