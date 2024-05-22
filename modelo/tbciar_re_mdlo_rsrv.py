import sys
import typing
from datetime import datetime
from datetime import timezone

import pyspark.sql.functions as F
from pyspark.sql import SparkSession
from pyspark.sql.dataframe import DataFrame
from pyspark.sql.window import Window


def processa_tbciar_re_mdlo_rsrv(
    du: "DataUtils", data_max: typing.Union[str, None], incluir_canc: bool = True,
) -> DataFrame:
    """
    Calcula valores de reserva a serem considerados no caso de churn do cliente
    considerando churn daqui a 60 dias ou churn no dia seguinte

    :param du: instancia do data utils para ser usada para processar a base
    :param data_max: data de corte utilizada
    :param incluir_canc: True se queremos incluir cancelamentos nos dados de curva
    :return: Um DataFrame contendo as projeções de perda e churn por reserva
    """
    import modelo.configs as confs
    from modelo.comuns import obtem_ultima_data_e_hora
    from modelo.perda import calcula_reservas_udf

    # Apurar a cd_dia_crte máxima das tabelas
    if data_max is None:
        data_max = max([v["cd_dia_crte"] for v in du.get_partition_values("GRUPO_CNPJ", "internal")])

    data_max = int(data_max)

    # seleciona as tabelas que serão utilizadas
    df_prjt = obtem_ultima_data_e_hora(du, data_max, "PARAMS_PROJ")

    # ajusta os dados de gravame, fncdiamento e TPV para estimativas do calculo de reserva
    df_rsrv = (
        df_prjt.withColumn("vl_grvm_liq", F.lit(0))
        .withColumn("vl_lvre_liq", F.lit(0))
        .withColumn("pc_fncd_base", F.col("pc_fncd"))
        .withColumn("pc_fncd", F.lit(1))
        .withColumn("pc_fncd_cilo", F.lit(1))
        .withColumn("pc_grvm", F.lit(0))
    )

    # ajusta os valores de chargeback e cancelamento do cliente
    df_rsrv = (
        df_rsrv.withColumn("razao", F.when(F.col("pc_fncd") > 0, 1 /  F.col("pc_fncd_base")).otherwise(None))
        .withColumn("vl_chrg_arv_cilo", F.col("vl_chrg_prjt") - F.col("vl_chrg_rbrp") - F.col("vl_chrg_liqd"))
        .withColumn("vl_chrg_arv_cilo_icmo", F.col("vl_chrg_prjt_icmo") - F.col("vl_chrg_rbrp_icmo") - F.col("vl_chrg_liqd_icmo"))
        .withColumn(
            "vl_chrg_arv_mrcd", 
            F.when(F.col("razao").isNull(), F.lit(0))
            .otherwise(F.col("vl_chrg_arv_mrcd") * F.col("razao"))
        )
        .withColumn(
            "vl_chrg_arv_mrcd_icmo", 
            F.when(F.col("razao").isNull(), F.lit(0))
            .otherwise(F.col("vl_chrg_arv_mrcd_icmo") * F.col("razao"))
        )
        .withColumn("vl_cncl_arv_cilo", F.col("vl_cncl_prjt") - F.col("vl_cncl_rbrp") - F.col("vl_cncl_liqd"))
        .withColumn("vl_cncl_arv_cilo_icmo", F.col("vl_cncl_prjt_icmo") - F.col("vl_cncl_rbrp_icmo") - F.col("vl_cncl_liqd_icmo"))
        .withColumn(
            "vl_cncl_arv_mrcd",
            F.when(F.col("razao").isNull(), F.lit(0))
            .otherwise(F.col("vl_cncl_arv_mrcd") * F.col("razao"))
        )
        .withColumn(
            "vl_cncl_arv_mrcd_icmo", 
            F.when(F.col("razao").isNull(), F.lit(0))
            .otherwise(F.col("vl_cncl_arv_mrcd_icmo") * F.col("razao"))
        )
        .persist()
    )

    # calcula os valores de chargeback de ARV
    if incluir_canc:
        df_calc = (
            df_rsrv.withColumn("vl_chrg_prjt", F.col("vl_chrg_prjt") + F.col("vl_cncl_prjt"))
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
        df_calc = (
            df_rsrv.withColumn("vl_chrg_arv", F.col("vl_chrg_arv_cilo") + F.col("vl_chrg_arv_mrcd"))
            .withColumn("vl_chrg_arv_icmo", F.col("vl_chrg_arv_cilo_icmo") + F.col("vl_chrg_arv_mrcd_icmo"))
        )

    # consolida os dados de projeção em um array para cada CNPJ considerando os cenários de churn 60 e churn 100
    df_calc = (
        df_calc.withColumn("tpv_icmo", F.sum("vl_sfra_prjt").over(Window.partitionBy("cd_grpo_cnpj")))
        .filter(F.col("tpv_icmo") > 1e-15)
        .groupBy("cd_grpo_cnpj")
        .agg(
            F.sort_array(F.collect_list(F.struct(confs.COLS_PROJ)))
            .alias("collected_list")
        )
        .withColumn("dc_tipo_churn", F.lit(60))
    ).unionByName(
        df_calc.withColumn("vl_sfra_prjt", F.lit(0))
        .withColumn("pc_liq_1d", F.lit(0))
        .withColumn("pc_liq_90d", F.lit(0))
        .withColumn("vl_chrg_prjt_icmo", F.lit(0))
        .withColumn("vl_chrg_liqd_icmo", F.lit(0))
        .withColumn("vl_chrg_rbrp_icmo", F.lit(0))
        .withColumn("vl_chrg_arv_icmo", F.lit(0))
        .groupBy("cd_grpo_cnpj")
        .agg(
            F.sort_array(F.collect_list(F.struct(confs.COLS_PROJ)))
            .alias("collected_list")
        )
        .withColumn("dc_tipo_churn", F.lit(100))
    )

    # realiza os calculos de reserva 
    df_prjt_rsva = (
        df_calc.select(
            "cd_grpo_cnpj",
            "dc_tipo_churn",
            F.explode(calcula_reservas_udf(F.col("collected_list"))).alias("projecao")
        )
        .select(
            "cd_grpo_cnpj",
            "dc_tipo_churn",
            F.col("projecao")["vl_rsrv"].alias("vl_rsrv"),
            F.col("projecao")["vl_perd"].alias("vl_perd"),
        )
        .withColumn("dc_prmt_exec", F.lit(f"incluir_canc={incluir_canc}"))
        .withColumn("cd_dia_crte", F.lit(int(data_max)))
        .withColumn("dh_crga", F.lit(datetime.now(timezone.utc).strftime("%Y%m%d%H%M%S")))
    )
    
    return df_prjt_rsva


def executa_mdlo_rsrv(params: list) -> None:
    """
    Função wrapper para executar a base tbciar_re_mdlo_rsrv

    :param params: parâmetros de execução
    """
    import modelo.configs as confs

    # pega os parâmetros de execução
    env = params[0] if len(params) > 0 else "PROD"
    data_max = params[1] if len(params) > 1 else None
    incluir_canc = params[2] if len(params) > 2 else str(confs.INCLUIR_CANC)

    # corrige nulos
    env = "PROD" if env == "" else env    
    data_max = None if data_max == "" else data_max
    incluir_canc = confs.INCLUIR_CANC if incluir_canc == "" else int(incluir_canc)

    # instância o data utils
    from utils.data_utils import DataUtils
    du = DataUtils(env)

    # chama a função
    df = processa_tbciar_re_mdlo_rsrv(du, data_max, incluir_canc)

    # escreve o resultado para o disco
    du.write_by_name(df, "RESERVA", "internal", partitions=["cd_dia_crte", "dh_crga"], mode="overwrite")

if __name__ == "__main__":
    # cria a sessão do spark
    spark = (
        SparkSession.builder
        .appName("SPARK_prod_prazo_tbciar_re_mdlo_rsrv")
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
    executa_mdlo_rsrv(sys.argv[1:])