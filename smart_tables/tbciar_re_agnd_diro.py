import sys
import typing
from datetime import datetime
from dateutil.relativedelta import relativedelta

import pyspark.sql.functions as F
from pyspark.sql import SparkSession
from pyspark.sql.dataframe import DataFrame


def processa_tbciar_re_agnd_diro(
    du: "DataUtils",
    data_min: typing.Optional[str] = None,
    data_max: typing.Optional[str] = None,
) -> DataFrame:
    """
    Processa a base de agenda e cria uma visão diária de agenda livre, gravamada por CNPJ e data de vencimento

    :param du: instancia do data utils para ser usada para processar a base
    :param data_min: data mínima de processamento (se nulo, considera a última data processada, ou a primeira partição da AGENDA ou 20220201)
    :param data_max: data máxima de processamento (se nulo, considera a última partição de AGENDA)
    :return: data frame processado
    """
    from utils.renames import renomeia_para_padrao

    # obtém a data mínima e data máxima
    if data_min is None:
        if du.is_table_available("AGENDA_DIARIA", "internal"):
            data_min = max([v["cd_dia_crga"] for v in du.get_partition_values("AGENDA_DIARIA", "internal")])
            data_min = (datetime.strptime(data_min, "%Y%m%d") + relativedelta(days=1)).strftime("%Y%m%d")
        else:
            data_min = min([v["dt_prtc"] for v in du.get_partition_values("AGENDA", "external")])
        #Esse limite de data minima e para o processamento do Control-M, quando a data_min nao e informada
        data_min = max(data_min, "20240215")
    # Esse limite da data minina e para limitar o historico quando o parameto data_min for informado
    data_min = max(data_min, "20220101")

    if data_max is None:
        data_max = max([v["dt_prtc"] for v in du.get_partition_values("AGENDA", "external")])

    # lista as partições a processar
    particoes_a_processar = sorted([
        v["dt_prtc"]
        for v in du.get_partition_values("AGENDA", "external")
        if data_min <= v["dt_prtc"] <= data_max
    ], reverse=True)
    if len(particoes_a_processar) == 0:
        return None
    
    # usar um loop para processar uma particao de cada vez, pois isso vai evitar estourar a memoria do cluster,
    for particao in particoes_a_processar:        
        # TODO: temos que trocar o valor da reserva quando isto estiver disponível
        # TODO: devemos trocar o valor gravamado na base de origem
        df = (
            du.get_by_name("AGENDA", "external")
            .filter(
                (F.col("dt_prtc") == int(particao)) 
                & (F.col("dt_vecto") >= F.col("dt_carga"))
            )            
            .select(
                F.col("dt_prtc").alias("cd_dia_crga"),
                F.col("cnpj_receb").alias("nu_cnpj_cpf_recr"),
                F.to_date(F.col("dt_carga")).alias("dt_dia"),
                F.to_date(F.col("dt_vecto")).alias("dt_vecto"),
                "vlr_tot_ur",
                "vlr_livre",
                "vlr_tot_finan",
                (F.col("vlr_tot_finan") - F.col("vlr_cedido") - F.col("vlr_penhorado")).alias("vlr_gravamado"),
                F.lit(0).alias("vlr_reserva"),
            )
        )

        # agregar os dados
        df = renomeia_para_padrao(
            df.groupBy(["cd_dia_crga", "nu_cnpj_cpf_recr", "dt_dia", "dt_vecto"])
            .agg(
                F.count("*").alias("qt_ur"),
                F.sum("vlr_tot_ur").alias("vl_total"),
                F.sum("vlr_livre").alias("vl_livre"),
                F.sum("vlr_tot_finan").alias("vl_financiado"),
                F.sum("vlr_gravamado").alias("vl_gravamado"),
                F.sum("vlr_reserva").alias("vl_reserva"),
            )
        )
        
        # escreve o resultado para o disco
        # usar mode("overwrite") para sobreescrever a particao em caso de reprocessamento
        du.write_by_name(df, "AGENDA_DIARIA", "internal", partitions=["cd_dia_crga"], mode="overwrite")

        # Apos gravar a particao no disc, usar unpersist explicito
        # para liberar recursos do cluster para a proxima particao
        df.unpersist()

    return df


def executa_agnd_diro(params: list) -> None:
    """
    Função wrapper para executar a base de Agenda Diária

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
    processa_tbciar_re_agnd_diro(du, data_min, data_max)


if __name__ == "__main__":
    # cria a sessão do spark
    spark = (
        SparkSession.builder
        .appName("SPARK_prod_prazo_tbciar_re_agnd_diro")
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
    executa_agnd_diro(sys.argv[1:])