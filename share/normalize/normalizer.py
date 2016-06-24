import abc
import json

from lxml import etree

from share.normalize.links import Context


# NOTE: Context is a thread local singleton
# It is asigned to ctx here just to keep a family interface
ctx = Context()


class Normalizer(metaclass=abc.ABCMeta):

    root_parser = None

    def __init__(self, app_config):
        self.config = app_config

    def do_normalize(self, raw_data):
        if raw_data.data.startswith(b'<'):
            parsed = etree.fromstring(raw_data.data.decode())
        else:
            parsed = json.loads(raw_data.data.decode())

        if self.root_parser:
            parser = self.root_parser
        else:
            try:
                module = __import__(self.config.name + '.normalizer', fromlist=('Manuscript', ))
            except ImportError:
                raise ImportError('Unable to find parser definitions at {}'.format(self.config.name + '.normalizer'))

            from share.models import AbstractCreativeWork
            root_levels = [
                getattr(module, klass.__name__)
                for klass in
                AbstractCreativeWork.__subclasses__()
                if hasattr(module, klass.__name__)
            ]

            if not root_levels:
                raise ImportError('No root level parsers found. You may have to create one or manually specifiy a parser with the root_parser attribute')

            if len(root_levels) > 1:
                raise ImportError('Found root level parsers {!r}. If more than one is found a single parser must be specified via the root_parser attribute')

            parser = root_levels[0]

        return parser(parsed).parse()

    def normalize(self, raw_data):
        ctx.clear()  # Just incase
        # Parsed data will be loaded into ctx
        self.do_normalize(raw_data)
        jsonld = ctx.jsonld
        ctx.clear()  # Clean up

        return jsonld
