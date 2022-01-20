import json
import logging

logger = logging.getLogger(__name__)


class ReadFiles:

    def read_json(self, path_to_file):
        with open(path_to_file) as json_file:
            json_data = json.load(json_file, encoding='utf-8')
        return json_data