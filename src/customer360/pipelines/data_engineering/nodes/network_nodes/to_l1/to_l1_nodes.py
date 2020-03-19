from pyspark.sql import DataFrame

from src.customer360.utilities.spark_util import get_spark_session


def get_good_and_bad_cells_for_each_customer(
        im_df,
        streaming_df,
        web_df,
        voip_df,
        volte_df,
        voice_df
) -> DataFrame:
    spark = get_spark_session()

    voice_df.createOrReplaceTempView("voice_df")
    im_df.createOrReplaceTempView("im_df")
    streaming_df.createOrReplaceTempView("streaming_df")
    web_df.createOrReplaceTempView("web_df")
    voip_df.createOrReplaceTempView("voip_df")
    volte_df.createOrReplaceTempView("volte_df")

    result_df = spark.sql("""
        with unioned_df as (
            select msisdn, cs_cgi as cell_id, cei_voice_qoe as qoe, partition_date
            from voice_df
            union all
            
            select msisdn, cgisai as cell_id, cei_im_qoe as qoe, partition_date
            from im_df
            union all
            
            select msisdn, cgisai as cell_id, cei_stream_qoe as qoe, partition_date
            from streaming_df
            union all
            
            select msisdn, cgisai as cell_id, cei_web_qoe as qoe, partition_date
            from web_df
            union all
            
            select msisdn, cgisai as cell_id, cei_voip_qoe as qoe, partition_date
            from voip_df
            union all
            
            select msisdn, cgisai as cell_id, cei_volte_qoe as qoe, partition_date
            from volte_df
        ),
        grouped_cells as (
            select msisdn, cell_id, avg(qoe) as avg_qoe, partition_date
            from unioned_df
            group by msisdn, cell_id, partition_date
        )
        select *,
                boolean(avg_qoe >= 0.75*max(avg_qoe) over (partition by msisdn, partition_date)) as good_cells,
                boolean(avg_qoe <= 0.25*max(avg_qoe) over (partition by msisdn, partition_date)) as bad_cells,
                count(*) over (partition by msisdn, partition_date) as cell_id_count                                        
        from grouped_cells  
    """)

    return result_df


def get_transaction_on_good_and_bad_cells(
    ranked_cells_df,
    cell_master_plan_df,
    sum_voice_daily_location_df
) -> DataFrame:
    spark = get_spark_session()

    ranked_cells_df.createOrReplaceTempView("ranked_cells_df")
    cell_master_plan_df.createOrReplaceTempView("cell_master_plan_df")
    sum_voice_daily_location_df.createOrReplaceTempView("sum_voice_daily_location_df")

    result = spark.sql("""
        with geo as (
            select *, 
                   cast(substr(cgi,6) as integer) as trunc_cgi
            from geo
        ),
        with voice_tx_location as (
            select *,
                cast(case when char_length(ci) > 5 then ci else concat(lac, ci) end as integer) as trunc_cgi
            from sum_voice_daily_location_df
        ),
        with enriched_voice_tx_location as (
            select /*+ BROADCAST(geo) */
                t1.mobile_no as mobile_no,
                t1.partition_date as partition_date
                t1.service_type as service_type
                t1.no_of_call + t1.no_of_inc as total_transaction
                
                t2.cgi as cgi
                t2.soc_cgi_hex as soc_cgi_hex                
            from sum_voice_daily_location_df t1
            inner join geo t1
            on t1.trunc_cgi = t2.trunc_cgi
        ),
        with joined_ranked_cells_df as (
            select 
                t1.msisdn as access_method_num,
                t1.partition_date as partition_date,
                sum(case when t1.good_cells is true then 1 else 0 end) as sum_of_good_cells,
                sum(case when t1.bad_cells is true then 1 else 0 end) as sum_of_bad_cells,
                max(cell_id_count) as cell_id_count,
                sum(case when t1.good_cells is true then t2.total_transaction else 0 end) as total_transaction_good_cells,
                sum(case when t1.bad_cells is true then t2.total_transaction else 0 end) as total_transaction_bad_cells
            from ranked_cells_df t1
            inner join enriched_voice_tx_location t2
                on t1.msisdn = t2.mobile_no
                and t1.cell_id = t2.soc_cgi_hex
                and t1.partition_date = t2.partition_date
            group by t1.msisdn, t1.partition_date
        )
        select *,
            sum_of_good_cells/cell_id_count as share_of_good_cells
            sum_of_bad_cells/cell_id_count as share_of_bad_cells
        from joined_ranked_cells_df
    """)

    return result
