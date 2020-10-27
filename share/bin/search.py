import logging
import threading

from project.celery import app as celery_app

from django.conf import settings

from share.bin.util import command
from share.search.daemon import SearchIndexerDaemon
from share.search.elastic_manager import ElasticManager


@command('Manage Elasticsearch')
def search(args, argv):
    """
    Usage:
        {0} search <command> [<args>...]

    Options:
        -h, --help           Show this screen.

    Commands:
    {1.subcommand_list}

    See '{0} search <command> --help' for more information on a specific command.
    """
    pass


@search.subcommand('Drop the Elasticsearch index')
def purge(args, argv):
    """
    Usage: {0} search purge [options]

    Options:
        -i, --index=INDEX    The name of the Elasticsearch index to use.
    """
    ElasticManager().delete_index(args.get('--index'))


@search.subcommand('Create indicies and apply mappings')
def setup(args, argv):
    """
    Usage: {0} search setup [options]

    Options:
        -i, --index=INDEX    The name of the Elasticsearch index to use.
    """
    ElasticManager().create_index(args.get('--index'))


@search.subcommand('Start the search indexing daemon')
def daemon(args, argv):
    """
    Usage: {0} search daemon [options]

    Options:
        -l, --log-level=LOGLEVEL  Set the log level [default: INFO]
    """
    # TODO consolidate loggers? or at least make sure this is catching all the relevant ones
    logging.getLogger('share.search.daemon').setLevel(args['--log-level'])
    logging.getLogger('share.search.messages').setLevel(args['--log-level'])

    elastic_manager = ElasticManager()
    stop_event = threading.Event()
    for index_name in settings.ELASTICSEARCH['ACTIVE_INDEXES']:
        SearchIndexerDaemon.start_indexer_in_thread(celery_app, stop_event, elastic_manager, index_name)

    try:
        stop_event.wait()
    except KeyboardInterrupt:
        pass  # let the finally block stop all threads
    finally:
        if not stop_event.is_set():
            stop_event.set()
