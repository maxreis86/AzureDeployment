import sys
import typing
from datetime import datetime
from dateutil.relativedelta import relativedelta

import pyspark.sql.functions as F
from pyspark.sql import SparkSession
from pyspark.sql.dataframe import DataFrame
from pyspark.sql.types import DoubleType


def processa_tbciar_re_trns_agrg(
    du: "DataUtils",
    data_min: typing.Optional[str] = None,
    data_max: typing.Optional[str] = None,
)  -> DataFrame:
    """
    Esta funcao visa criar um base de transacao agregada no nivel de "nu_cnpj_cpf", "nu_ec", "dt_atzc_trns", "cd_dh_incl_rgst", "in_ctao_presente", "cd_bndr", "cd_tipo_lncm"
    com a soma dos campos vl_brto_agnd_trns e vl_lqdo_agnd_trns e contagem do campo nu_trns.
    As informacoes sao lidas do base sie.tbdwr_trns e o filtro usado é cd_tipo_lncm in isin(1, 2, 3). 1=DEBITO | 2=CREDITO A VISTA | 3=CREDITO PARCELADO

    :param du: instancia do data utils para ser usada para processar a base
    :param data_min: data mínima de processamento (se nulo, considera a última data processada, ou a primeira partição da TRNS ou 20210101)
    :param data_max: data máxima de processamento (se nulo, considera a última partição de TRNS)
    :return: data frame processado
    """
    from utils.renames import renomeia_para_padrao
    
    # obtém a data mínima e data máxima
    if data_min is None:
        if du.is_table_available("TRNS_AG", "internal"):
            data_min = max([v["cd_dh_incl_rgst"] for v in du.get_partition_values("TRNS_AG", "internal")])
            data_min = (datetime.strptime(data_min, "%Y%m%d") + relativedelta(days=1)).strftime("%Y%m%d")
        else:
            data_min = min([v["cd_dh_incl_rgst"] for v in du.get_partition_values("TRNS", "external")])
        #Esse limite de data minima e para o processamento do Control-M, quando a data_min nao e informada
        data_min = max(data_min, "20240411")
    # Esse limite da data minina e para limitar o historico quando o parameto data_min for informado
    data_min = max(data_min, "20220101")
    
    if data_max is None:
        data_max = max([v["cd_dh_incl_rgst"] for v in du.get_partition_values("TRNS", "external")])
    
    # Antes de iniciar o loop para processar todas as particoes, vamos deixar em cache 
    # as tabelas sie.tbdwr_bandeira e sie.tbdwr_ec que serao usadas em cada iteracao
    tbdwr_bandeira = (
        du.get_by_name("BANDEIRA")
        .select(F.col("cd_bandeira").alias("cd_bndr"), F.col("nm_bandeira").alias("nm_bndr"))
        .distinct()
        .persist()
    )
    tbdwr_ec = (
        du.get_by_name("EC", "internal")
        .select(
            "nu_ec", 
            "nu_cnpj_cpf_recr",
            "nu_cnpj9_cpf", 
            "nm_unde_ngco_fpna",
            "nm_sgmt_cmrl",
            "cd_so_ramo_atvd",
            "nm_cdfr",
        )
        .distinct()
        .persist()
    )

    # lista as partições a processar
    particoes_a_processar = sorted([
        v["cd_dh_incl_rgst"] 
        for v in du.get_partition_values("TRNS", "external") 
        if data_min <= v["cd_dh_incl_rgst"] <= data_max
    ], reverse=True)

    if len(particoes_a_processar) == 0:
        return None
    
    # usar um loop para processar uma particao de cada vez, pois isso vai evitar estourar a memoria do cluster,
    # se existir transacoes de diferentes datas (dt_atzc_trns) em uma mesma particao as dadas de dt_atzc_trns serao agregadas em particoes separadas,
    # por isso sera necessario um novo agrupamento nos processos que irao ler essa tabela
    for particao in particoes_a_processar:        
        # apurar periodo
        df = (
            du.get_by_name("TRNS", "external")
            .filter(
                (F.col("cd_dh_incl_rgst") == int(particao)) 
                & (F.col("cd_tipo_lncm").isin([1, 2, 3]))
                & (F.col("dt_atzc_trns") >= "2022-01-01")
            )            
            .select(
                "nu_ec",
                F.to_date(F.col("dt_atzc_trns")).alias("dt_trns"),
                "cd_dh_incl_rgst",
                "cd_bndr",
                "cd_tipo_lncm",
                F.col("qt_prcl_trns").alias("qt_prcl"),
                F.when(F.col("nu_idtr_trns").isNull(), 1).otherwise(0).alias("in_crto_psen"),
                F.when(F.col("in_rbrp") == "S", 1).otherwise(0).alias("in_rbrp"),
                "vl_brto_agnd_trns",
                "vl_lqdo_agnd_trns"
            )
        )

        # agregar os dados         
        df = (
            df.groupBy(
                "nu_ec", 
                "dt_trns", 
                "cd_dh_incl_rgst", 
                "in_crto_psen", 
                "cd_bndr", 
                "cd_tipo_lncm", 
                "qt_prcl",
                "in_rbrp",
            )
            .agg(
                F.sum("vl_brto_agnd_trns").cast(DoubleType()).alias("vl_brto_agnd_trns"),
                F.sum("vl_lqdo_agnd_trns").cast(DoubleType()).alias("vl_lqdo_agnd_trns"),
                F.count("*").alias("qt_trns")
            )
            .withColumn(
                "nm_tipo_trns",
                F.when(F.col("cd_tipo_lncm") == 1, "DÉBITO")
                .when(F.col("cd_tipo_lncm") == 2, "CRÉDITO À VISTA")
                .when(F.col("cd_tipo_lncm") == 3, "CRÉDITO PARCELADO")
                .otherwise("OUTROS")
            )
            .drop("cd_tipo_lncm")
        )

        # join com outras infos
        df = renomeia_para_padrao(
            df.join(F.broadcast(tbdwr_bandeira), on="cd_bndr", how="left")
            .fillna("NULO")
            .join(tbdwr_ec, on="nu_ec", how="left")
            .drop("cd_bndr")
        )
        
        # escreve o resultado para o disco
        # usar mode("overwrite") para sobreescrever a particao em caso de reprocessamento
        du.write_by_name(df, "TRNS_AG", "internal", partitions=["cd_dh_incl_rgst"], mode="overwrite")

        # Apos gravar a particao no disc, usar unpersist explicito
        # para liberar recursos do cluster para a proxima particao
        df.unpersist()
    
    tbdwr_bandeira.unpersist()
    tbdwr_ec.unpersist()

    return df


def executa_trns_agrg(params: list) -> None:
    """
    Função wrapper para executar a base de TRNS agregada

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
    processa_tbciar_re_trns_agrg(du, data_min, data_max)


if __name__ == "__main__":
    # cria a sessão do spark
    spark = (
        SparkSession.builder
        .appName("SPARK_prod_prazo_tbciar_re_trns_agrg")
        .config("spark.driver.maxResultSize", "2G") # Precisamos usar essa configuracao especifica para as tabelas que leem mais dados (chargeback, cancelamento e tbciar_re_trns_agrg)
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
    executa_trns_agrg(sys.argv[1:])

    
