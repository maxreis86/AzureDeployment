import sys
import typing
from datetime import datetime

import pyspark.sql.functions as F
from pyspark.sql import SparkSession
from pyspark.sql.dataframe import DataFrame
from dateutil.relativedelta import relativedelta

def processa_tbciar_re_arv_cilo(
    du: "DataUtils",    
    data_min: typing.Optional[str] = None,
    data_max: typing.Optional[str] = None,
) -> DataFrame:
    """
    Processa a tabela tbciar_re_arv_cilo.
    Sao filtradas somente as transacoes que foram enviadas para pagamento (("dc_nrc_stco_cotr") == "Enviada para pagamento")).
    Apos isso, sao feitos joins com as tabelas EC (para pegar o nu_cnpj_cpf_recr)
    e ARV_BALCAO para obter a abertura do valor financiado por nm_bandeira e dt_vecto.
    A tabela ARV_CIELO e salva com os dados de vl_arv_cilo e vl_arv_mrcd agrupados por "nu_cnpj_cpf_recr", "dt_vecto" e "nm_bandeira"
    
    :param du: instancia do data utils para ser usada para processar a base
    :param data_min: data mínima de processamento (se nulo, considera a última partição de ARV_CIELO)
    :param data_max: data máxima de processamento (se nulo, considera a última partição de ARV)
    :return: Um DataFrame contendo os valores de vl_arv_cilo e vl_arv_mrcd.
    """
    
    from utils.renames import renomeia_para_padrao
    
    # obtém a data mínima e data máxima
    if data_min is None:
        if du.is_table_available("ARV_CIELO", "internal"):
            data_min = max([v["dt_cotr_prtc"] for v in du.get_partition_values("ARV_CIELO", "internal")])
            data_min = (datetime.strptime(data_min, "%Y%m%d") + relativedelta(days=1)).strftime("%Y%m%d")
        else:
            data_min = min([v["dt_cotr_particao"] for v in du.get_partition_values("ARV", "external")])
    data_min = max(data_min, "20220101")
    if data_max is None:        
        data_max = max([v["dt_cotr_particao"] for v in du.get_partition_values("ARV", "external") if v["dt_cotr_particao"] != '__HIVE_DEFAULT_PARTITION__'])
    data_min = min(data_min, data_max) # garante que data mínima nunca será maior que data máxima

    df = (
        du.get_by_name("ARV", "external")
        .filter(
            (F.col("dc_nrc_stco_cotr") == "Enviada para pagamento")
            & (F.col("dt_cotr_particao") >= data_min)
            & (F.col("dt_cotr_particao") <= data_max)
        )
        .select(
            "cd_sqnl_cotr", 
            "dc_idtr_fnct", 
            "dt_cotr_particao",
            "vl_totl_cotr",
            "pc_cdi_clnt",
        )
        .join(
            du.get_by_name("ARV_BALCAO", "external")
            .filter(
                (F.col("dt_cotr_particao") >= data_min)
                & (F.col("dt_cotr_particao") <= data_max)
            )
            .select(
                "cpf_cnpj_recr",
                "cd_sqnl_cotr",
                "dc_arnj_pgmn",
                "dt_prvs_lqdc",
                "vl_totl_cotr_unde_rcbv",
                "dt_cotr_particao",
            )
            .withColumnRenamed("cpf_cnpj_recr", "nu_cnpj_cpf_recr")
            .withColumn("nm_bandeira", F.trim(F.upper(F.split(F.col("dc_arnj_pgmn"), " ")[0])))
            .withColumn("dt_vecto", F.to_date(F.col("dt_prvs_lqdc"), "yyyy-MM-dd"))
            .groupBy(
                "nu_cnpj_cpf_recr",
                "cd_sqnl_cotr",
                "nm_bandeira",
                "dt_vecto",
                "dt_cotr_particao"
            )
            .agg(F.sum("vl_totl_cotr_unde_rcbv").alias("vl_totl_cotr_unde_rcbv")),
            on=["cd_sqnl_cotr", "dt_cotr_particao"],
            how="inner"
        )
        .groupBy("nu_cnpj_cpf_recr", "dt_vecto", "nm_bandeira", "dt_cotr_particao")
        .agg(
            F.sum(F.when(F.trim(F.col("dc_idtr_fnct")) == "Agenda Cielo", F.col("vl_totl_cotr_unde_rcbv")).otherwise(0)).alias("vl_arv_cielo"),
            F.sum(F.when(F.trim(F.col("dc_idtr_fnct")) == "Agenda Mercado", F.col("vl_totl_cotr_unde_rcbv")).otherwise(0)).alias("vl_arv_mercado"),
            F.sum(F.col("vl_totl_cotr")).alias("vl_totl_cotr"),
            F.sum(F.col("vl_totl_cotr") * F.col("pc_cdi_clnt") / 100).alias("vl_cdi_clnt"),
        )
        .withColumn("pc_cdi_clnt", F.col("vl_cdi_clnt") / F.col("vl_totl_cotr"))
        .drop("vl_cdi_clnt")
    )

    df = renomeia_para_padrao(df)
    
    return df


def executa_arv_cilo(params: list) -> None:
    """
    Função wrapper para executar a base de ARV Cielo

    :param params: parâmetros de execução
    """
    # pega os parâmetros de execução
    env = params[0] if len(params) > 0 else "PROD"
    data_min = params[1] if len(params) > 1 else None
    data_max = params[2] if len(params) > 2 else None

    # corrige nulos
    env = "PROD" if env == "" else env
    data_min = None if data_min == "" else data_min
    data_max = None if data_max == "" else data_max

    # instância o data utils
    from utils.data_utils import DataUtils
    du = DataUtils(env)

    # executa o processamento
    df = processa_tbciar_re_arv_cilo(du, data_min, data_max)

    # escreve o resultado para o disco
    du.write_by_name(df, "ARV_CIELO", "internal", partitions=["dt_cotr_prtc"], mode="overwrite")


if __name__ == "__main__":
    # cria a sessão do spark
    spark = (
        SparkSession.builder
        .appName("SPARK_prod_prazo_tbciar_re_arv_cilo")
        .enableHiveSupport()
        .getOrCreate()
    )
    sc = spark.sparkContext

    # adiciona pacotes de execução
    env = sys.argv[1] if len(sys.argv) > 0 else "PROD"
    if env == "PROD":
        sc.addFile("s3://cielo-sdlf-prd-sa-east-1-866812290529-artifactory/prod-prazo/py/balcao20/utils", recursive=True)
    elif env.startswith("USER_"):
        env, e_from, user = env.split("_")
        user = user.lower()
        sc.addFile(f"s3://cielo-sandbox-sa-east-1-107976507529-prod-prazo-sandbox/balcao2.0/{user}/scripts/utils", recursive=True)
    else:
        sc.addFile("s3://cielo-sandbox-sa-east-1-107976507529-prod-prazo-sandbox/balcao2.0/deploy/utils", recursive=True)

    # chama a execução da base
    executa_arv_cilo(sys.argv[1:])