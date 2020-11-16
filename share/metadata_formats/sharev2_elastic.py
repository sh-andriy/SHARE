from share.util.graph import MutableGraph

from .base import MetadataFormatter


class ShareV2ElasticFormatter(MetadataFormatter):
    def format(self, normalized_datum):
        mgraph = MutableGraph.from_jsonld(normalized_datum.data)
        central_work = mgraph.get_central_node(guess=True)

        if central_work.concrete_type != 'abstractcreativework':
            return

        source_doc = {
            'sources': None,

            # attrs:
            'date_created': normalized_datum.created_at.isoformat(),  # TODO do another query to get the first normd under the same suid -- unsure how important
            'date_modified': normalized_datum.created_at.isoformat(),
            'date_published': central_work['date_published'],
            'date_updated': central_work['date_updated'],
            'description': central_work['description'],
            'id': None,
            'identifiers': [identifier['uri'] for identifier in central_work['identifiers']],
            'justification': central_work['justification'],
            'language': central_work['language'],
            'registration_type': central_work['registration_type'],
            'retracted': None,
            'title': central_work['title'],
            'type': central_work.type,
            'withdrawn': central_work['withdrawn'],

            # relations:
            'affiliations': None,
            'contributors': None,
            'funders': self._get_related_agent_names(central_work, 'funder'),
            'publishers': self._get_related_agent_names(central_work, 'publisher'),
            'hosts': self._get_related_agent_names(central_work, 'host'),

            'tags': [tag_node['name'] for tag_node in central_work['tags']],
            'subjects': None,
            'subject_synonyms': None,

            # post-process:
            #   'date': None,
            #   'types': None,
            #   'lists': None,
        }

    def _get_related_agent_names(self, work_node, relation_type):
        return [
            relation_node['cited_as'] or relation_node['agent']['name']
            for relation_node in work_node['agent_relations']
            if relation_node.type == relation_type
        ]
