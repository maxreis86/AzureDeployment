import sys
import typing
from datetime import datetime
from datetime import timezone

import pyspark.sql.functions as F
from pyspark.sql import SparkSession
from pyspark.sql.dataframe import DataFrame
from pyspark.sql.window import Window


def processa_tbciar_re_mdlo_tpv_liq(du: "DataUtils", data_max: typing.Union[str, None]) -> DataFrame:
    """
    Processa a tabela tbciar_re_mdlo_tpv_liq.
    
    O processamento le a tabela TPV_PROJ com a projecao do TPV nao performado    
    
    Cada data de transacao projetada na tabela TPV_PROJ recebe uma curva de liquidacao
    Esse liquidacao e calculada com base nm_tipo_trns e na quantidade de parcelas
    Para transacoes de debito, a liquidação e feita 2 dias após a transacao
    Para transacoes de credito a vista, a liquidação e feita 1 mes após a transacao 
    Para transacoes de credito parcelado, a liquidação e feita a cada mes, de acordo com o numero de parcelas
    
    :param du: instancia do data utils para ser usada para processar a base
    :param data_max: data de corte utilizada no modelo
    :return: Um DataFrame contendo as curvas de chargeback processadas.
    """
    from modelo.comuns import obtem_ultima_data_e_hora

    # Apurar a cd_dt_carga máxima da tabela TPV_PROJ para pegar sempre a utlima particao
    data_max_int = (
        int(max([v["cd_dia_crte"] for v in du.get_partition_values("TPV_PROJ", "internal")]))
        if data_max is None
        else data_max
    )

    # calcula o tpv total na granularidade de interesse
    safra_proj = (
        obtem_ultima_data_e_hora(du, data_max_int, "TPV_PROJ")
        .groupBy("cd_grpo_cnpj", "nm_tipo_trns", "qt_prcl", "nu_dia_prjt")
        .agg(F.sum(F.col("vl_sfra_prjt")).alias("vl_sfra_prjt"))
    )

    # calcula o percentual de valor a liquidar a ser calculado por safra
    df_pct_liq = (
        safra_proj.filter(F.col("nu_dia_prjt") == 1)
        .withColumn("qt_prcl", F.when(F.col("qt_prcl") == 0, 1).otherwise(F.col("qt_prcl")))
        .select("*", F.explode(F.expr("sequence(0, qt_prcl)")).alias("nu_prcl"))
        .withColumn(
            "vl_prcl", 
            F.when(F.col("nu_prcl") > 0, F.col("vl_sfra_prjt") / F.col("qt_prcl"))
            .otherwise(0)
        )
        .withColumn("nu_dia_liq", F.when(F.col("nm_tipo_trns") == "DÉBITO", 2).otherwise(30 * F.col("nu_prcl")))
        .groupBy("cd_grpo_cnpj", "nu_dia_liq")
        .agg(F.sum(F.col("vl_prcl")).alias("vl_prcl"))
        .withColumn("vl_liq", F.sum("vl_prcl").over(Window.partitionBy("cd_grpo_cnpj").orderBy("nu_dia_liq")))
        .withColumn("vl_totl", F.max("vl_liq").over(Window.partitionBy("cd_grpo_cnpj")))
        .withColumn("vl_liq", F.col("vl_totl") - F.col("vl_liq"))
        .withColumn(
            "nu_dia_liq_lag", F.coalesce(
                F.lag(F.col("nu_dia_liq"), -1).over(Window.partitionBy("cd_grpo_cnpj").orderBy("nu_dia_liq")),
                F.col("nu_dia_liq") + 1
            )
        )
        .withColumn("nu_dia_liq", F.when(F.col("nu_dia_liq") == 0, 0).otherwise(F.col("nu_dia_liq") + 1))
        .select(
            "cd_grpo_cnpj", 
            (F.col("vl_liq") / F.col("vl_totl")).alias("pc_liq"),
            F.explode(F.expr("sequence(nu_dia_liq, nu_dia_liq_lag)")).alias("nu_dia_liq")
        )
        .withColumn(
            "pc_liq", 
            F.when((F.col("pc_liq") <= 1e-10) & (F.col("pc_liq") >= -1e-10), 0)
            .otherwise(F.col("pc_liq"))
        )
    ) 

    # calcula o valor a liquidar associado ao TPV projetado
    vl_liq_tpv_proj = (
        safra_proj.groupBy("cd_grpo_cnpj", "nu_dia_prjt")
        .agg(F.sum(F.col("vl_sfra_prjt")).alias("vl_sfra_prjt"))
        .join(df_pct_liq, on="cd_grpo_cnpj", how="left")
        .withColumn("vl_liq", F.col("vl_sfra_prjt") * F.col("pc_liq"))
        .withColumn(
            "vl_liq_90d",
            F.col("vl_liq") - F.coalesce(
                F.lag(F.col("vl_liq"), -90).over(
                    Window.partitionBy("cd_grpo_cnpj", "nu_dia_prjt")
                    .orderBy("nu_dia_liq")
                ),
                F.lit(0)
            )
        )
        .withColumn(
            "vl_liq_1d",
            F.col("vl_liq") - F.coalesce(
                F.lag(F.col("vl_liq"), -1).over(
                    Window.partitionBy("cd_grpo_cnpj", "nu_dia_prjt")
                    .orderBy("nu_dia_liq")
                ),
                F.lit(0)
            )
        )
        .withColumn("nu_dia_prjt", F.col("nu_dia_prjt") + F.col("nu_dia_liq"))
        .groupBy("cd_grpo_cnpj", "nu_dia_prjt")
        .agg(
            F.sum("vl_liq").alias("vl_liq"), 
            F.sum("vl_liq_1d").alias("vl_liq_1d"), 
            (F.sum("vl_liq_90d") / F.sum("vl_liq")).alias("pc_liq_90d")
        )
        .withColumn("vl_liq_eod", F.col("vl_liq") - F.col("vl_liq_1d"))
        .withColumn(
            "pc_liq_1d", 
            1 - F.col("vl_liq_1d") / F.lag(F.col("vl_liq_eod"), 1).over(
                Window.partitionBy("cd_grpo_cnpj").orderBy("nu_dia_prjt")
            )
        )
        .drop("vl_liq_1d", "vl_liq_eod")
        .join(
            safra_proj.groupBy("cd_grpo_cnpj", "nu_dia_prjt")
            .agg(F.sum(F.col("vl_sfra_prjt")).alias("vl_sfra_prjt")),
            on=["cd_grpo_cnpj", "nu_dia_prjt"],
            how="left"
        )
        .fillna({"vl_sfra_prjt": 0, "pc_liq_1d": 1})
        .withColumn("cd_dia_crte", F.lit(data_max_int))
        .withColumn("dh_crga", F.lit(datetime.now(timezone.utc).strftime("%Y%m%d%H%M%S")))
    )
    
    return vl_liq_tpv_proj


def executa_mdlo_tpv_liq(params: list) -> None:
    """
    Função wrapper para executar a base tbciar_re_mdlo_tpv_liq

    :param params: parâmetros de execução
    """
    import modelo.configs as confs

    # pega os parâmetros de execução
    env = params[0] if len(params) > 0 else "PROD"
    data_max = params[1] if len(params) > 1 else None
    
    # corrige nulos
    env = "PROD" if env == "" else env
    data_max = None if data_max == "" else data_max

    # instância o data utils
    from utils.data_utils import DataUtils
    du = DataUtils(env)

    # executa o processamento
    df = processa_tbciar_re_mdlo_tpv_liq(du=du, data_max=data_max)

    # escreve o resultado para o disco
    du.write_by_name(df, "TPV_LIQ", "internal", partitions=["cd_dia_crte", "dh_crga"], mode="overwrite")


if __name__ == "__main__":
    # cria a sessão do spark
    spark = (
        SparkSession.builder
        .appName("SPARK_prod_prazo_tbciar_re_mdlo_tpv_medo")
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
    executa_mdlo_tpv_liq(sys.argv[1:])