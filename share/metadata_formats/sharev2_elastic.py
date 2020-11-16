from share.util.graph import MutableGraph

from .base import MetadataFormatter

class ShareV2ElasticFormatter(MetadataFormatter):
    def format(self, normalized_datum):
        mgraph = MutableGraph.from_jsonld(normalized_datum.data)
        central_work = mgraph.get_central_node(guess=True)

        if central_work.concrete_type != 'abstractcreativework':
            return

        source_doc = {
            # relationships:
            # 'affiliations': None,
            # 'contributors': None,
            #
            # post-process:
            #   'date': None,
            'date_created': normalized_datum.created_at.isoformat(),  # TODO do another query to get the first normd under the same suid -- unsure how important
            'date_modified': normalized_datum.created_at.isoformat(),
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
