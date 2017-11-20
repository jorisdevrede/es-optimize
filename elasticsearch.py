"""Optimizes Elasticsearch indices"""

import sys
import uuid

from argparse import ArgumentParser
from logbook import Logger, StreamHandler
import requests

StreamHandler(sys.stdout).push_application()
LOG = Logger(__name__)


class Connection:
    """Creates a connection to Elasticsearch"""

    def __init__(self, baseurl):
        """Create a new connection to a master url

        :param baseurl: url to an Elasticsearch master (e.g.
        http://esmaster:9200)"""
        self._baseurl = baseurl

    def alias_update(self, index, alias, operation='add'):
        """Add or remove an alias for an index

        :param index: name of the index to create an alias for
        :param alias: name of the alias
        :param operation: either add or remove"""
        data = {'actions': [
            {operation: {'index': index, 'alias': alias}}
        ]}
        response = requests.post(self._baseurl + '/_aliases',
                                 json=data)
        LOG.debug('alias update: {} - {}'.format(response.status_code, response.text))

    def cluster_get_health(self, params):
        """Retrieves the health of a cluster"""
        response = requests.get(self._baseurl + '/_cluster/health',
                                params=params)
        LOG.debug('cluster_get_health: {} - {}'.format(response.status_code, response.text))
        return response.json()

    def cluster_get_stats(self):
        """Retrieves the statistics of a cluster"""
        response = requests.get(self._baseurl + '/_cluster/stats')
        LOG.debug('cluster_get_stat: {} - {}'.format(response.status_code, response.text))
        return response.json()

    def index_create(self, index, mappings=None, settings=None, timeout=None):
        """Creates a new index
        :param index: the index to create
        :param mappings: mappings dictionary
        :param settings: settings dictionary
        :param timeout: time to wait for completion (e.g. 1m)
        :returns: True when successful"""

        data = {}
        if mappings is not None:
            data['mappings'] = mappings

        if settings is not None:
            data['settings'] = settings

        params = {}
        if timeout is not None:
            params['wait_for_active_shards'] = 'all'
            params['timeout'] = timeout

        response = requests.put(self._baseurl + '/' + index,
                                json=data,
                                params=params)
        LOG.debug('index_create: {} - {}'.format(response.status_code, response.text))

        return response.ok

    def index_delete(self, index):
        """Deletes an existing index
        :param index: index name or alias to delete"""
        response = requests.delete(self._baseurl + '/' + index)
        LOG.debug('index_delete: {} - {}'.format(response.status_code, response.text))

        return response.ok

    def index_forcemerge(self, index):
        """Merges the segments of an index to 1"""
        params = {'max_num_segments': 1}
        response = requests.post(self._baseurl + '/' + index + '/_forcemerge',
                                 params=params)
        LOG.debug('index_forcemerge: {} - {}'.format(response.status_code, response.text))

    def index_get_config(self, index):
        """Retrieves the config of an index"""
        response = requests.get(self._baseurl + '/' + index)
        LOG.debug('index_get_config: {} - {}'.format(response.status_code, response.text))
        return response.json()

    def index_get_stats(self, index):
        """Retrieves the statistics of an index"""
        response = requests.get(self._baseurl + '/' + index + '/_stats')
        LOG.debug('index_get_stat: {} - {}'.format(response.status_code, response.text))
        return response.json()

    def index_reindex(self, index, new_index, slices=None, timeout=None):
        """Reindexes an index to a new index
        :param index: old index to reindex from
        :param new_index: new index
        :param slices: number of documents to batch in
        :param timeout: time to wait for completion (e.g. 1h)"""
        data = {'source': {'index': index},
                'dest': {'index': new_index}}

        LOG.info(
            'Reindexing index {} to new index {}'.format(
                index,
                new_index))

        params = {}
        if slices is not None:
            params['slices'] = slices

        if timeout is not None:
            params['wait_for_completion'] = 'true'
            params['timeout'] = timeout

        response = requests.post(self._baseurl + '/_reindex',
                                 json=data,
                                 params=params)
        LOG.debug('index_reindex: {} - {}'.format(response.status_code, response.text))

        return response.ok


class IndexOptimizer:
    """Calculates the correct optimization"""
    def __init__(self, baseurl):
        self._connection = Connection(baseurl)

        cluster_stats = self._connection.cluster_get_stats()
        self._datanodes = int(cluster_stats['nodes']['count']['data'])
        self._indices = int(cluster_stats['indices']['count'])
        self._shards = cluster_stats['indices']['shards']['total']

    def index_replace(self, index, shards):
        """Creates a new index with another number of shards
        :param index: index to reindex
        :param shards: new number of shards"""

        # collect the params for the new index
        # new index name
        new_index = str(uuid.uuid4())

        idx_cfg = self._connection.index_get_config(index)
        idx_cfg_set = idx_cfg[index]['settings']['index']

        # copy mappings for new index
        c_mapping = idx_cfg[index]['mappings']

        # set the shards for new index
        c_setting = {'number_of_shards': shards}

        # optionally set analysis for new index
        if 'analysis' in idx_cfg_set:
            c_setting['analysis'] = idx_cfg_set['analysis']

        # set replicas for new index
        replicas = int(idx_cfg_set['number_of_replicas'])
        if replicas > self._datanodes:
            replicas = self._datanodes
        c_setting['number_of_replicas'] = replicas

        # set timeout relative to index count
        c_timeout = str(1 + round(self._indices / self._datanodes)) + 'm'
        self._connection.index_create(new_index,
                                      mappings=c_mapping,
                                      settings=c_setting,
                                      timeout=c_timeout)

        # set slicing based on total shards of old index
        index_stats = self._connection.index_get_stats(index)
        slices = index_stats['_shards']['total']

        # set timeout to 500MB of primary per hour
        primary_size = index_stats['_all']['primaries']['store']['size_in_bytes']
        r_timeout = str(1 + round(primary_size / 500000000)) + 'h'
        self._connection.index_reindex(index, new_index,
                                       slices=slices,
                                       timeout=r_timeout)

        # get health verification may not be necessary
        # self._connection.cluster_get_health({
        #     'wait_for_status': 'green', 'timeout': '1h'})

        # replace old index with new index
        self._connection.index_delete(index)
        self._connection.alias_update(new_index, index)

        # minimize segments for new index
        self._connection.index_forcemerge(new_index)

    def index_optimize_shards(self, index):
        """Optimizes an index with the right amount of shards
        :param index: index to optimize"""
        index_config = self._connection.index_get_config(index)
        index_shards = int(
            index_config[index]['settings']['index']['number_of_shards'])

        index_stats = self._connection.index_get_stats(index)
        index_primary_size = int(
            index_stats['_all']['primaries']['store']['size_in_bytes'])

        size_2gb = 2147483648
        size_30gb = 32212254720
        size_45gb = 48318382080

        # Reduce shards to 1 when primary shards are less than 2GB
        if index_primary_size < size_2gb and index_shards > 1:
            LOG.info("Reducing index {} to 1 shard".format(index))

            self.index_replace(index, 1)

        # Increase shards when larger than 45GB
        elif index_primary_size / index_shards > size_45gb:

            number_of_shards = round(index_primary_size / size_30gb)

            if index_shards < number_of_shards:
                LOG.info(
                    "Increasing index {} to {} shards".format(
                        index, number_of_shards))

                self.index_replace(index, number_of_shards)


if __name__ == '__main__':
    # pylint: disable=invalid-name
    parser = ArgumentParser(
        description="Optimize the sharding of an Elasticsearch index")
    parser.add_argument('-i', '--index', dest='index')
    parser.add_argument('-u', '--url', dest='url')
    #
    # args = parser.parse_args()
    #
    # esop = IndexOptimizer(args.url)
    # esop.index_optimize_shards(args.index)

    # con = Connection('http://127.0.0.1:9200')
    # cset = {'index':{'number_of_shards': 1, 'number_of_replicas': 0}}
    # con.index_create('test_reindex', settings=cset, timeout='1m')
    #
    # con.index_reindex('test', 'test_reindex', slices=5, timeout='1h')
    #
    # con.index_delete('test_reindex')

    # io = IndexOptimizer('http://127.0.0.1:9200')
    # io.index_optimize_shards('test')
