# Query generator class
class QueryGenerator:

    # accept table_name as string, table_params as dict
    @staticmethod
    def aggregate(table_name, table_params):
        try:
            features = []
            feature_list = table_params["feature_list"]
            if feature_list != "":
                for (key, val) in feature_list.items():
                    features.append("{} as {}".format(val, key))

            # if don't want to use where clause then put empty string "" in query_parameters.yaml
            where_clause = table_params["where_clause"]

            # if features are not listed we can assume it to be *
            # or can raise a exception
            projection = ','.join(features) if len(features) != 0 else "*"

            # if don't want to use group by then put empty string "" in query_parameters.yaml
            granularity = table_params["granularity"] if projection != "*" else ""

            query = "Select {},{} from {} {} group by {};".format(granularity, projection, table_name, where_clause, granularity)
            return query

        except Exception as e:
            print(str(e))
            print("Table parameters are missing.")