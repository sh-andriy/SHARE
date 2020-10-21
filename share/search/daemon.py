from concurrent.futures import ThreadPoolExecutor
import logging
import queue as local_queue
import threading
import time

from django.conf import settings

from kombu import Queue as KombuQueue
from kombu.mixins import ConsumerMixin

from elasticsearch import Elasticsearch
from elasticsearch import helpers

from raven.contrib.django.raven_compat.models import client

from share.search.exceptions import DaemonSetupError, DaemonMessageError, DaemonIndexingError
from share.search.messages import IndexableMessage


logger = logging.getLogger(__name__)

LOOP_TIMEOUT = 5


class SearchIndexerDaemon(ConsumerMixin):

    MAX_LOCAL_QUEUE_SIZE = 5000
    PREFETCH_COUNT = 7500

    @classmethod
    def start_indexer_in_thread(cls, celery_app, stop_event, elastic_manager, index_name):
        indexer = cls(
            celery_connection=celery_app.pool.acquire(block=True),
            index_name=index_name,
            index_setup=elastic_manager.get_index_setup(index_name),
            stop_event=stop_event,
        )

        threading.Thread(target=indexer.run).start()

        return indexer

    def __init__(self, celery_connection, index_name, index_setup, stop_event):
        self.connection = celery_connection  # needed by ConsumerMixin

        self.__stop_event = stop_event
        self.__index_name = index_name
        self.__index_setup = index_setup

        self.__thread_pool = None
        self.__incoming_message_queues = {}
        self.__outgoing_action_queue = None

    # overrides ConsumerMixin.run
    def run(self):
        try:
            logger.info('%r: Starting', self)

            self.__start_loops_and_queues()

            logger.debug('%r: Delegating to Kombu.run', self)
            return super().run()
        finally:
            self.stop()

    # for ConsumerMixin -- specifies rabbit queues to consume, registers __on_message callback
    def get_consumers(self, Consumer, channel):
        kombu_queue_settings = settings.ELASTICSEARCH['KOMBU_QUEUE_SETTINGS']
        index_settings = settings.ELASTICSEARCH['INDEXES'][self.__index_name]
        return [
            Consumer(
                [
                    KombuQueue(index_settings['URGENT_QUEUE'], **kombu_queue_settings),
                    KombuQueue(index_settings['DEFAULT_QUEUE'], **kombu_queue_settings),
                ],
                callbacks=[self.__on_message],
                accept=['json'],
                prefetch_count=self.PREFETCH_COUNT,
            )
        ]

    def stop(self):
        # should_stop is defined in ConsumerMixin
        if not self.should_stop:
            logger.warning('%r (%s): Shutting down', self, id(self))
            self.should_stop = True
            self.__thread_pool.shutdown(wait=False)
            self.__stop_event.set()

    @property
    def all_queues_empty(self):
        return self.__outgoing_action_queue.empty() and all(
            message_queue.empty()
            for message_queue in self.__incoming_message_queues.values()
        )

    def __wait_on_stop_event(self):
        self.__stop_event.wait()
        self.stop()

    def __start_loops_and_queues(self):
        if self.__thread_pool:
            raise DaemonSetupError('SearchIndexerDaemon already set up!')

        supported_message_types = self.__index_setup.supported_message_types

        self.__thread_pool = ThreadPoolExecutor(max_workers=len(supported_message_types) + 2)
        self.__thread_pool.submit(self.__wait_on_stop_event)

        # one outgoing action queue and one thread that sends those actions to elastic
        self.__outgoing_action_queue = local_queue.Queue(maxsize=self.MAX_LOCAL_QUEUE_SIZE)
        self.__thread_pool.submit(self.__outgoing_action_loop)

        # for each type of message handler, one queue for incoming messages and a
        # __incoming_message_loop thread that pulls from the queue, builds actions, and
        # pushes those actions to the outgoing action queue
        for message_type in supported_message_types:
            message_queue = local_queue.Queue(maxsize=self.MAX_LOCAL_QUEUE_SIZE)
            self.__incoming_message_queues[message_type] = message_queue
            self.__thread_pool.submit(self.__incoming_message_loop, message_type)

    def __on_message(self, body, message):
        wrapped_message = IndexableMessage.wrap(message)

        message_queue = self.__incoming_message_queues.get(wrapped_message.message_type)
        if message_queue is None:
            logger.warn('%r: unknown message type "%s"', self, wrapped_message.message_type)
            raise DaemonMessageError(f'Received message with unexpected type "{wrapped_message.message_type}" (message: {message})')

        message_queue.put(wrapped_message)

    def __incoming_message_loop(self, message_type):
        try:
            log_prefix = f'{repr(self)} IncomingMessageLoop({message_type}): '
            loop = IncomingMessageLoop(
                message_queue=self.__incoming_message_queues[message_type],
                outgoing_action_queue=self.__outgoing_action_queue,
                action_generator=self.__index_setup.build_action_generator(self.__index_name, message_type),
                log_prefix=log_prefix,
            )

            while not self.should_stop:
                loop.iterate_once(self.__stop_event)

        except Exception as e:
            client.captureException()
            logger.exception('%sEncountered an unexpected error (%s)', log_prefix, e)
        finally:
            self.stop()

    def __outgoing_action_loop(self):
        try:
            log_prefix = f'{repr(self)} OutgoingActionLoop: '
            loop = OutgoingActionLoop(
                action_queue=self.__outgoing_action_queue,
                log_prefix=log_prefix,
            )

            while not self.should_stop:
                loop.iterate_once()

        except Exception as e:
            client.captureException()
            logger.exception('%sEncountered an unexpected error (%s)', log_prefix, e)
        finally:
            self.stop()

    def __repr__(self):
        return '<{}({})>'.format(self.__class__.__name__, self.__index_name)


class IncomingMessageLoop:
    def __init__(self, message_queue, outgoing_action_queue, action_generator, log_prefix):
        self.message_queue = message_queue
        self.outgoing_action_queue = outgoing_action_queue
        self.action_generator = action_generator
        self.log_prefix = log_prefix

        self.chunk_size = settings.ELASTICSEARCH['CHUNK_SIZE']

        logger.info('%sStarted', self.log_prefix)

    def iterate_once(self, stop_event):
        message_chunk = []
        target_ids = set()  # for avoiding duplicate messages within one chunk
        while len(message_chunk) < self.chunk_size:
            try:
                # If we have any messages queued up, push them through ASAP
                message = self.message_queue.get(timeout=.1 if message_chunk else LOOP_TIMEOUT)

                if message.target_id in target_ids:
                    # skip processing duplicate messages in one chunk
                    message.ack()
                else:
                    target_ids.add(message.target_id)
                    message_chunk.append(message)
            except local_queue.Empty:
                break

        if not message_chunk:
            logger.debug('%sRecieved no messages to queue up', self.log_prefix)
            return

        start = time.time()
        logger.debug('%sPreparing %d docs to be indexed', self.log_prefix, len(message_chunk))

        success_count = 0
        # at this point, we have a chunk of messages, each with exactly one pk
        # each message should turn into one elastic action/doc
        for message, action in self.action_generator(message_chunk):
            # Keep blocking on put() until there's space in the queue or it's time to stop
            while not stop_event.is_set():
                try:
                    self.outgoing_action_queue.put((message, action), timeout=LOOP_TIMEOUT)
                    success_count += 1
                    break
                except local_queue.Full:
                    continue

        logger.info('%sPrepared %d docs to be indexed in %.02fs', self.log_prefix, success_count, time.time() - start)


class OutgoingActionLoop:
    # ok the thing here is that we want to wait to ack the message until
    # its action is successfully sent to elastic -- this keeps the message
    # in the rabbit queue so if something explodes, the message will be retried
    # once things recover

    # how this works now is self.action_chunk_iter makes a generator that yields actions
    # and *also* side-effects the corresponding messages into self.messages_awaiting_elastic
    # keyed by the message's target_id -- this is nice because we can use elastic's
    # streaming_bulk helper to avoid too much chunking in memory (...tho it may not make much
    # difference, depending how much the elastic helpers hold in memory) and use the _id on
    # each successful response to find and ack the respective message

    MAX_CHUNK_BYTES = 10 * 1024 ** 2  # 10 megs

    def __init__(self, action_queue, log_prefix):
        self.action_queue = action_queue
        self.log_prefix = log_prefix

        self.chunk_size = settings.ELASTICSEARCH['CHUNK_SIZE']
        self.messages_awaiting_elastic = {}

        self.es_client = Elasticsearch(
            settings.ELASTICSEARCH['URL'],
            retry_on_timeout=True,
            timeout=settings.ELASTICSEARCH['TIMEOUT'],
            # sniff before doing anything
            sniff_on_start=settings.ELASTICSEARCH['SNIFF'],
            # refresh nodes after a node fails to respond
            sniff_on_connection_fail=settings.ELASTICSEARCH['SNIFF'],
            # and also every 60 seconds
            sniffer_timeout=60 if settings.ELASTICSEARCH['SNIFF'] else None,
        )

        logger.info('%sStarted', self.log_prefix)

    def iterate_once(self):
        action_chunk = self.action_chunk_iter()

        elastic_stream = helpers.streaming_bulk(
            self.es_client,
            action_chunk,
            max_chunk_bytes=self.MAX_CHUNK_BYTES,
            raise_on_error=False,
        )

        doc_count = 0
        start = time.time()
        for (ok, resp) in elastic_stream:
            op_type, resp_body = next(iter(resp.items()))

            if not ok and not (resp.get('delete') and resp['delete']['status'] == 404):
                raise DaemonIndexingError(ok, resp)

            message = self.messages_awaiting_elastic.pop(resp_body['_id'])
            message.ack()

        if doc_count:
            logger.info('%sIndexed %d documents in %.02fs', self.log_prefix, doc_count, time.time() - start)
        else:
            logger.debug('%sRecieved no messages for %.02fs', self.log_prefix, time.time() - start)

        if self.messages_awaiting_elastic:
            raise DaemonIndexingError(f'Messages left awaiting elastic! ¿something happened? {self.messages_awaiting_elastic}')

    def action_chunk_iter(self):
        for _ in range(self.chunk_size):
            try:
                message, action = self.action_queue.get(timeout=LOOP_TIMEOUT)
                if action is None or '_id' not in action:
                    message.ack()
                    continue
                if action['_id'] in self.messages_awaiting_elastic:
                    # TODO dedupe earlier (or just ack it?)
                    raise DaemonMessageError('duplicate messages in one chunk -- handle this i guess')
                self.messages_awaiting_elastic[action['_id']] = message
                yield action
            except local_queue.Empty:
                raise StopIteration
