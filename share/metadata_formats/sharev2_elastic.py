from share.util.graph import MutableGraph

from .base import MetadataFormatter

class ShareV2ElasticFormatter(MetadataFormatter):
    def format(self, normalized_data):
        mgraph = MutableGraph.from_jsonld(normalized_data.data)
        central_work = mgraph.get_central_node(guess=True)

        if central_work.concrete_type != 'abstractcreativework':
            return

        source_doc = {
            'affiliations': None,
            'contributors': None,
            'date': None,
            'date_created': None,  # TODO another query for first_normd?
            'date_modified': normalized_data.created_at,  # TODO check date format
            'date_published': central_work['date_published'],
            'date_updated': central_work['date_updated'],
            'description': central_work['description'],
            'funders': None,
            'hosts': None,
            'id': None,
            'identifiers': None,
            'justification': None,
            'language': central_work['language'],
            'publishers': None,
            'registration_type': central_work['registration_type'],
            'retracted': None,
            'sources': None,
            'subjects': None,
            'subject_synonyms': None,
            'tags': [tag_node['name'] for tag_node in central_work['tags']],
            'title': central_work['title'],
            'type': central_work.type,
            'types': None,
            'withdrawn': central_work['withdrawn'],
            'lists': None,
        }
