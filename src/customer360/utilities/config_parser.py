
from pyspark.sql import SparkSession, DataFrame


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

            granularity = table_params["granularity"]

            if granularity!="":
                query = "Select {},{} from {} {} group by {}".format(granularity, projection, table_name, where_clause, granularity)
            else:
                query = "Select {} from {} {}".format(projection, table_name, where_clause)

            return query

        except Exception as e:
            print(str(e))
            print("Table parameters are missing.")

    # accept table_name as string, table_params as dict
    @staticmethod
    def feature_expansion(table_name, table_params):
        try:
            features = []
            feature_list = table_params["feature_list"]
            level = table_params["type"]
            if feature_list != "":
                for (key, val) in feature_list.items():
                    for col in val:
                        features.append("{}({}) as {}".format(key,col,col+"_"+key+"_"+level))

            # if don't want to use where clause then put empty string "" in query_parameters.yaml
            where_clause = table_params["where_clause"]

            # if features are not listed we can assume it to be *
            # or can raise a exception
            projection = ','.join(features) if len(features) != 0 else "*"

            # if don't want to use group by then put empty string "" in query_parameters.yaml

            granularity = table_params["granularity"]

            if granularity!="":
                query = "Select {},{} from {} {} group by {}".format(granularity, projection, table_name, where_clause, granularity)
            else:
                query = "Select {} from {} {}".format(projection, table_name, where_clause)

            return query

        except Exception as e:
            print(str(e))
            print("Table parameters are missing.")


def node_from_config(input_df, config) -> DataFrame:
    table_name = "input_table"
    input_df.createOrReplaceTempView(table_name)
    sql_stmt = QueryGenerator.aggregate(table_name, config)

    spark = SparkSession.builder.getOrCreate()

    df = spark.sql(sql_stmt)
    return df


def expansion(input_df, config) -> DataFrame:
    """
    This function will expand the base feature based on parameters
    :param input_df:
    :param config:
    :return:
    """
    table_name = "input_table"
    input_df.createOrReplaceTempView(table_name)
    sql_stmt = QueryGenerator.feature_expansion(table_name, config)

    spark = SparkSession.builder.getOrCreate()

    df = spark.sql(sql_stmt)
    return df