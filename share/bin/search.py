import json
import logging

from project import celery_app

from django.conf import settings

from share.bin.util import command
from share.search.daemon import SearchIndexerDaemon

from bots.elasticsearch import tasks
from bots.elasticsearch.bot import ElasticSearchBot


@command('Manage Elasticsearch')
def search(args, argv):
    """
    Usage:
        {0} search <command> [<args>...]
        {0} search [--help | --filter=FILTER | --all] [--async | --to-daemon] [options]

    Options:
        -h, --help           Show this screen.
        -f, --filter=FILTER  Filter the queryset to be index using this filter. Must be valid JSON.
        -a, --all            Index everything. Equivalent to --filter '{{"id__isnull": false}}'.
        -u, --url=URL        The URL of Elasticsearch.
        -i, --index=INDEX    The name of the Elasticsearch index to use.
        -a, --async          Send an update_elasticsearch task to Celery.
        -t, --to-daemon      Index records by adding them to the indexer daemon's queue.
        -q, --queue=QUEUE    Queue name to send to indexing tasks to.

    Commands:
    {1.subcommand_list}

    See '{0} search <command> --help' for more information on a specific command.
    """

    if args['--filter']:
        args['--filter'] = json.loads(args['--filter'])

    if args['--all']:
        args['--filter'] = {'id__isnull': False}

    kwargs = {
        'filter': args.get('--filter'),
        'index': args.get('--index'),
        'url': args.get('--url'),
        'to_daemon': bool(args.get('--to-daemon')),
        'queue': args.get('--queue'),
        # 'models': args.get('--models'),
    }

    if args['--async']:
        tasks.update_elasticsearch.apply_async((), kwargs)
    else:
        tasks.update_elasticsearch(**kwargs)


@search.subcommand('Drop the Elasticsearch index')
def purge(args, argv):
    """
    Usage: {0} search purge

    NOT YET IMPLEMENTED
    """
    raise NotImplementedError()


@search.subcommand('Synchronize the Elasticsearch index and database')
def janitor(args, argv):
    """
    Usage: {0} search janitor [--dry | --async] [options]

    Options:
        -u, --url=URL        The URL of Elasticsearch.
        -i, --index=INDEX    The name of the Elasticsearch index to use.
        -d, --dry            Dry run the janitor task.
        -t, --to-daemon      Index records by adding them to the indexer daemon's queue.
    """
    kwargs = {
        'es_url': args.get('--url'),
        'es_index': args.get('--index'),
        'dry': bool(args['--dry']),
        'to_daemon': bool(args['--to-daemon']),
    }

    if args['--async']:
        tasks.elasticsearch_janitor.apply_async((), kwargs)
    else:
        tasks.elasticsearch_janitor(**kwargs)


@search.subcommand('Create indicies and apply mappings')
def setup(args, argv):
    """
    Usage: {0} search setup [options]

    Options:
        -u, --url=URL        The URL of Elasticsearch.
        -i, --index=INDEX    The name of the Elasticsearch index to use.
    """
    ElasticSearchBot(es_url=args.get('--url'), es_index=args.get('--index')).setup()


@search.subcommand('Start the search indexing daemon')
def daemon(args, argv):
    """
    Usage: {0} search daemon [options]

    Options:
        -l, --log-level=LOGLEVEL  Set the log level [default: INFO]
    """
    logging.getLogger('share.search.daemon').setLevel(args['--log-level'])
    logging.getLogger('share.search.indexing').setLevel(args['--log-level'])

    queues = {}
    for index, config in settings.ELASTICSEARCH['INDEXES'].items():
        for queue in config['QUEUES']:
            queues.setdefault(queue, []).append(index)

    daemons = []
    for queue, indexes in queues.items():
        daemon = SearchIndexerDaemon(celery_app, queue, indexes)
        daemon.start()
        daemons.append(daemon)

    try:
        for daemon in daemons:
            daemon.join()
    except KeyboardInterrupt:
        for daemon in daemons:
            daemon.stop()

        for daemon in daemons:
            daemon.join()