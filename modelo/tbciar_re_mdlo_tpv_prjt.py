import sys
import typing
from datetime import datetime
from datetime import timezone

import pyspark.sql.functions as F
import pyspark.sql.types as T
from pyspark.sql import SparkSession
from pyspark.sql.dataframe import DataFrame


def processa_tbciar_re_mdlo_tpv_prjt(du: "DataUtils", data_max: typing.Union[int, None], dias_proj_tpv: int) -> DataFrame:
    """
    Processa dados de transações diarias para gerar a diminuicao do TPV em caso de churn.

    Esta função realiza o processamento de dados de transações agregadas, calcula a media de TPV para a JANELA_APURACAO_TPV,
    e projeta o TPV para os proximos DIAS_PROJECAO_TPV dias usando a funcao func_proj_tpv.    

    :param du: instancia do data utils para ser usada para processar a base
    :param data_max: data de corte a ser utilizado
    :param dias_proj_tpv (int): Quantidade de dias a serem projetados
    :return: Um DataFrame contendo as curvas de chargeback processadas.
    """
    from modelo.comuns import obtem_ultima_data_e_hora

    # obtém os valores de datas mínimas e máximas que deveriam ser utilizados
    if data_max is None:
        data_max = max([v["cd_dia_crte"] for v in du.get_partition_values("GRUPO_CNPJ", "internal")])
    else:
        data_max = max([
            v["cd_dia_crte"] 
            for v in du.get_partition_values("GRUPO_CNPJ", "internal") 
            if v["cd_dia_crte"] <= data_max
        ])

    # classifica a base de EC
    id_cols = ["cd_grpo_cnpj", "cd_clsr", "nm_tipo_trns", "qt_prcl"]

    # Projetar o run-off to TPV ate a data do churn usando a funcao de projecao
    df_tpv_proj = (
        obtem_ultima_data_e_hora(du, int(data_max), "TPV_MEDIO")
        .select(id_cols + ["vl_sfra_medo", "cd_dia_crte"])
        .withColumn(
            "nu_dia_prjt",
            F.explode(F.array([F.lit(i) for i in range(1, dias_proj_tpv + 1)]))
        )
        .withColumn(
            "vl_sfra_prjt",
            F.col("vl_sfra_medo") * (1 - (F.col("nu_dia_prjt") / float(dias_proj_tpv)))
        )
        .select(id_cols + ["nu_dia_prjt", "vl_sfra_prjt", "cd_dia_crte"])
        .withColumn("dh_crga", F.lit(datetime.now(timezone.utc).strftime("%Y%m%d%H%M%S")))
        .withColumn("dc_prmt_exec", F.lit(f"dias_proj_tpv={dias_proj_tpv}"))
    )

    return df_tpv_proj


def executa_mdlo_tpv_prjt(params: list) -> None:
    """
    Função wrapper para executar a base tbciar_re_mdlo_tpv_prjt

    :param params: parâmetros de execução
    """
    import modelo.configs as confs

    # pega os parâmetros de execução
    env = params[0] if len(params) > 0 else "PROD"
    data_max = params[1] if len(params) > 1 else None
    dias_proj_tpv = params[2] if len(params) > 2 else str(confs.DIAS_PROJECAO_TPV)

    # corrige nulos
    env = "PROD" if env == "" else env
    data_max = None if data_max == "" else data_max
    dias_proj_tpv = confs.DIAS_PROJECAO_TPV if dias_proj_tpv == "" else dias_proj_tpv

    # instância o data utils
    from utils.data_utils import DataUtils
    du = DataUtils(env)
    
    # executa o processamento CANCELAMENTO
    df = processa_tbciar_re_mdlo_tpv_prjt(du, data_max, int(dias_proj_tpv))
    du.write_by_name(df, "TPV_PROJ", "internal", partitions=["cd_dia_crte", "dh_crga"], mode="overwrite")


if __name__ == "__main__":
    # cria a sessão do spark
    spark = (
        SparkSession.builder
        .appName("SPARK_prod_prazo_tbciar_re_mdlo_tpv_prjt")
        .enableHiveSupport()
        .getOrCreate()
    )
    sc = spark.sparkContext

    # adiciona pacotes de execução
    env = sys.argv[1] if len(sys.argv) > 0 else "PROD"
    if env == "PROD":
        sc.addFile("s3://cielo-sdlf-prd-sa-east-1-866812290529-artifactory/prod-prazo/py/balcao20/utils", recursive=True)
        sc.addFile("s3://cielo-sdlf-prd-sa-east-1-866812290529-artifactory/prod-prazo/py/balcao20/modelo", recursive=True)
    elif env.startswith("USER_"):
        env, e_from, user = env.split("_")
        user = user.lower()
        sc.addFile(f"s3://cielo-sandbox-sa-east-1-107976507529-prod-prazo-sandbox/balcao2.0/{user}/scripts/utils", recursive=True)
        sc.addFile(f"s3://cielo-sandbox-sa-east-1-107976507529-prod-prazo-sandbox/balcao2.0/{user}/scripts/modelo", recursive=True)
    else:
        sc.addFile("s3://cielo-sandbox-sa-east-1-107976507529-prod-prazo-sandbox/balcao2.0/deploy/utils", recursive=True)
        sc.addFile("s3://cielo-sandbox-sa-east-1-107976507529-prod-prazo-sandbox/balcao2.0/deploy/modelo", recursive=True)

    # chama a execução da base
    executa_mdlo_tpv_prjt(sys.argv[1:])
