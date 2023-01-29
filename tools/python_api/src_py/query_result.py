from .torch_geometric_result_converter import TorchGeometricResultConverter
from .types import Type


class QueryResult:
    def __init__(self, connection, query_result):
        self.connection = connection
        self._query_result = query_result
        self.is_closed = False

    def __del__(self):
        self.close()

    def check_for_query_result_close(self):
        if self.is_closed:
            raise Exception("Query result is closed")

    def has_next(self):
        self.check_for_query_result_close()
        return self._query_result.hasNext()

    def get_next(self):
        self.check_for_query_result_close()
        return self._query_result.getNext()

    def write_to_csv(self, filename, delimiter=',', escape_character='"', newline='\n'):
        self.check_for_query_result_close()
        self._query_result.writeToCSV(
            filename, delimiter, escape_character, newline)

    def close(self):
        if self.is_closed:
            return
        self._query_result.close()
        # Allows the connection to be garbage collected if the query result
        # is closed manually by the user.
        self.connection = None
        self.is_closed = True

    def get_as_df(self):
        self.check_for_query_result_close()
        return self._query_result.getAsDF()

    def get_as_arrow(self, chunk_size):
        self.check_for_query_result_close()
        return self._query_result.getAsArrow(chunk_size)

    def get_column_data_types(self):
        self.check_for_query_result_close()
        return self._query_result.getColumnDataTypes()

    def get_column_names(self):
        self.check_for_query_result_close()
        return self._query_result.getColumnNames()

    def reset_iterator(self):
        self.check_for_query_result_close()
        self._query_result.resetIterator()

    def get_as_networkx(self, directed=True):
        self.check_for_query_result_close()
        import networkx as nx

        if directed:
            nx_graph = nx.DiGraph()
        else:
            nx_graph = nx.Graph()
        properties_to_extract = self._get_properties_to_extract()

        self.reset_iterator()

        nodes = {}
        rels = {}
        table_to_label_dict = {}

        # De-duplicate nodes and rels
        while self.has_next():
            row = self.get_next()
            for i in properties_to_extract:
                column_type, _ = properties_to_extract[i]
                if column_type == Type.NODE.value:
                    _id = row[i]["_id"]
                    nodes[(_id["table"], _id["offset"])] = row[i]
                    table_to_label_dict[_id["table"]] = row[i]["_label"]

                elif column_type == Type.REL.value:
                    _src = row[i]["_src"]
                    _dst = row[i]["_dst"]
                    rels[(_src["table"], _src["offset"], _dst["table"],
                          _dst["offset"])] = row[i]

        # Add nodes
        for node in nodes.values():
            _id = node["_id"]
            node_id = node['_label'] + "_" + str(_id["offset"])
            node[node['_label']] = True
            nx_graph.add_node(node_id, **node)

        # Add rels
        for rel in rels.values():
            _src = rel["_src"]
            _dst = rel["_dst"]
            src_id = str(
                table_to_label_dict[_src["table"]]) + "_" + str(_src["offset"])
            dst_id = str(
                table_to_label_dict[_dst["table"]]) + "_" + str(_dst["offset"])
            nx_graph.add_edge(src_id, dst_id, **rel)
        return nx_graph

    def _get_properties_to_extract(self):
        column_names = self.get_column_names()
        column_types = self.get_column_data_types()
        properties_to_extract = {}

        # Iterate over columns and extract nodes and rels, ignoring other columns
        for i in range(len(column_names)):
            column_name = column_names[i]
            column_type = column_types[i]
            if column_type in [Type.NODE.value, Type.REL.value]:
                properties_to_extract[i] = (column_type, column_name)
        return properties_to_extract

    def get_as_torch_geometric(self):
        self.check_for_query_result_close()
        # Despite we are not using torch_geometric in this file, we need to
        # import it here to throw an error early if the user does not have
        # torch_geometric or torch installed.

        import torch
        import torch_geometric

        converter = TorchGeometricResultConverter(self)
        return converter.get_as_torch_geometric()
