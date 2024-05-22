import sys
import typing
from datetime import datetime, timezone
from dateutil.relativedelta import relativedelta

import pyspark.sql.functions as F
from pyspark.sql import SparkSession
from pyspark.sql.dataframe import DataFrame


def processa_tbciar_re_mdlo_perd(
    du: "DataUtils", 
    data_max: typing.Optional[str] = None, 
    incluir_canc: bool = True
) -> DataFrame:
    """
    A partir dos dados calculados de reserva seleciona qual reserva deverá ser aplicada
    de acordo com o risco aceitável e o perfil de financiamento do cliente

    :param du: instância do data utils
    :param incluir_canc: True se queremos incluir cancelamentos nos dados de curva
    :return: data frame com o valor de reserva aplicado e o cenário de churn a ser considerado
    """
    from modelo.perda import realiza_calculo_perda
    from modelo.comuns import obtem_ultima_data_e_hora

    # Obter o valor da data máxima que deveria ser utilizada
    if data_max is None:
        data_max = max([v["cd_dia_crte"] for v in du.get_partition_values("GRUPO_CNPJ", "internal")])
    else:
        data_max = max([
            v["cd_dia_crte"]
            for v in du.get_partition_values("GRUPO_CNPJ", "internal")
            if v["cd_dia_crte"] <= data_max
        ])
    
    # cria as versões inteiras dos valores
    data_max_int = int(data_max)

    # obtém os dados de reserva ótima
    df_rsva = (
        obtem_ultima_data_e_hora(du, data_max_int, "RESERVA_OTIMA")
        .select("cd_grpo_cnpj", "dc_tipo_churn", "dc_mtvo_rsrv", "vl_rsrv")
    )

    # obtém os dados de probabilidade de churn
    df_churn = (
        obtem_ultima_data_e_hora(du, data_max_int, "PROB_CHURN")
        .select("cd_grpo_cnpj", "pc_prbl_churn")
    )

    # cruza com os dados de parametros
    df_calc = (
        obtem_ultima_data_e_hora(du, data_max_int, "PARAMS_PROJ")
        .drop("cd_dia_crte")
    )

    # calcula os valores de chargeback de ARV
    if incluir_canc:
        df_proj = (
            df_calc.withColumn("vl_chrg_prjt", F.col("vl_chrg_prjt") + F.col("vl_cncl_prjt"))
            .withColumn("vl_chrg_liqd", F.col("vl_chrg_liqd") + F.col("vl_cncl_liqd"))
            .withColumn("vl_chrg_rbrp", F.col("vl_chrg_rbrp") + F.col("vl_cncl_rbrp"))
            .withColumn(
                "vl_chrg_arv", 
                F.col("vl_chrg_arv_cilo") 
                + F.col("vl_chrg_arv_mrcd") 
                + F.col("vl_cncl_arv_cilo") 
                + F.col("vl_cncl_arv_mrcd")
            )
            .withColumn("vl_chrg_prjt_icmo", F.col("vl_chrg_prjt_icmo") + F.col("vl_cncl_prjt_icmo"))
            .withColumn("vl_chrg_liqd_icmo", F.col("vl_chrg_liqd_icmo") + F.col("vl_cncl_liqd_icmo"))
            .withColumn("vl_chrg_rbrp_icmo", F.col("vl_chrg_rbrp_icmo") + F.col("vl_cncl_rbrp_icmo"))
            .withColumn(
                "vl_chrg_arv_icmo", 
                F.col("vl_chrg_arv_cilo_icmo") 
                + F.col("vl_chrg_arv_mrcd_icmo") 
                + F.col("vl_cncl_arv_cilo_icmo") 
                + F.col("vl_cncl_arv_mrcd_icmo")
            )
        )
    else:
        df_proj = (
            df_calc.withColumn("vl_chrg_arv", F.col("vl_chrg_arv_cilo") + F.col("vl_chrg_arv_mrcd"))
            .withColumn("vl_chrg_arv_icmo", F.col("vl_chrg_arv_cilo_icmo") + F.col("vl_chrg_arv_mrcd_icmo"))
        )

    # realiza a projeção de perda
    df_perda_agg = realiza_calculo_perda(
        du=du,
        df_proj=df_proj,
        df_rsva=df_rsva,
        df_churn=df_churn,
        dt_cnpj=data_max,
    )

    return (
        df_perda_agg
        .withColumn("cd_dia_crte", F.lit(data_max_int))
        .withColumn("dh_crga", F.lit(datetime.now(timezone.utc).strftime("%Y%m%d%H%M%S")))
        .withColumn("dc_prmt_exec", F.lit(f"incluir_canc={incluir_canc}"))
    )


def executa_mdlo_perd(params: list) -> None:
    """
    Função wrapper para executar a base tbciar_re_mdlo_perd

    :param params: parâmetros de execução
    """
    import modelo.configs as confs

    # pega os parâmetros de execução
    env = params[0] if len(params) > 0 else "PROD"
    data_max = params[1] if len(params) > 1 else None
    incluir_canc = params[2] if len(params) > 2 else "1"    
    
    # corrige nulos
    env = "PROD" if env == "" else env
    data_max = None if data_max == "" else data_max
    incluir_canc = "1" if incluir_canc == "" else incluir_canc
    incluir_canc = True if incluir_canc == "1" else False
    
    # instância o data utils
    from utils.data_utils import DataUtils
    du = DataUtils(env)

    # chama a função
    df = processa_tbciar_re_mdlo_perd(du, data_max, incluir_canc)

    # escreve o resultado para o disco
    du.write_by_name(df, "PERDA", "internal", partitions=["cd_dia_crte", "dh_crga"], mode="overwrite")

if __name__ == "__main__":
    # cria a sessão do spark
    spark = (
        SparkSession.builder
        .appName("SPARK_prod_prazo_tbciar_re_mdlo_perd")
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
    executa_mdlo_perd(sys.argv[1:])