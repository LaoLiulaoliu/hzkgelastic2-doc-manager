#!/usr/bin/env python
# -*- coding: utf-8 -*-
# Author: Yuande Liu <miraclecome (at) gmail.com>

from mongo_connector.doc_managers.formatters import DefaultDocumentFormatter

class HzkgDocumentFormatter(DefaultDocumentFormatter):
    """
	original formatter:
	{
		"_id": ObjectId("57edbe3843ece042bb10ca9d"),
		"source": {
			"confidence": "0.6",
			"trackingId": "fd6d245b75096dfcf10a9905c377e28a0e53b103"
		},
		"claims": [
			{
				"p": "name",
				"o": "apple"
			},
			{
				"p": "date",
				"o": "2016-09-29"
			}
		]
	}
	
    tranformed formatter:
    {
        "_id": ObjectId("57edbe3843ece042bb10ca9d"),
        "source.confidence": "0.6",
        "source.trackingId": "fd6d245b75096dfcf10a9905c377e28a0e53b103",
        "name": "apple",
        "date": "2016-09-29"
    }
    """
    def transform_element(self, key, value):
		if isinstance(value, list):
            for podict in value:
                yield podict["p"], podict["o"]
        elif isinstance(value, dict):
            formatted = self.format_document(value)
            for doc_key in formatted:
                yield "%s.%s" % (key, doc_key), formatted[doc_key]
        else:
            # We assume that transform_value will return a 'flat' value,
            # not a list or dict
            yield key, self.transform_value(value)


    def format_document(self, document):
        def flatten(doc, path):
            top_level = (len(path) == 0)
            if not top_level:
                path_string = ".".join(path)
            for k in doc:
                v = doc[k]
                if isinstance(v, dict):
                    path.append(k)
                    for inner_k, inner_v in flatten(v, path):
                        yield inner_k, inner_v
                    path.pop()
                else:
                    transformed = self.transform_element(k, v)
                    for new_k, new_v in transformed:
                        if top_level:
                            yield new_k, new_v
                        else:
                            yield "%s.%s" % (path_string, new_k), new_v
        return dict(flatten(document, []))
