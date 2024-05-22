import sys
import typing
from datetime import datetime, timezone

import pyspark.sql.functions as F
from pyspark.sql import SparkSession
from pyspark.sql.dataframe import DataFrame


def processa_tbciar_re_mdlo_prmt_prjt(du: "DataUtils", data_max: typing.Union[str, None]) -> DataFrame:
    """
    Processa a tabela tbciar_re_mdlo_prmt_prjt.
    
    Junta todas as bases de dados que contém informações necessárias para o calculo de reserva
    e projeção de perda
    - CGB_PROJ: Projeção dos valores de chargeback para o TPV legado
    - AGENDA_LIQ: Liquidação dos volumes de agenda em aberto na data de corte
    - TPV_CGB_PROJ: Projeção dos valores de chargeback para o TPV incremental
    - TPV_VR_LIQ: Projeção dos valores de TPV e TPV a liquidar
    - TPV_MEDIO: Valor de TPV médio nos últimos 30 dias usado para extrair dados de RR
    
    :param du: instancia do data utils para ser usada para processar a base
    :return: Um DataFrame contendo as curvas de chargeback processadas.
    """
    from modelo.comuns import obtem_ultima_data_e_hora

    # Apurar a cd_dia_crte máxima das tabelas
    if data_max is None:
        data_max = max([v["cd_dia_crte"] for v in du.get_partition_values("GRUPO_CNPJ", "internal")])

    data_max = int(data_max)

    # Carrega as tabelas com as projecoes
    drop_cols = ["cd_dia_crte", "dh_crga", "dc_prmt_exec"]
    df_arv_medio = obtem_ultima_data_e_hora(du, data_max, "ARV_MEDIO").drop(*drop_cols)
    df_chrg_leg = obtem_ultima_data_e_hora(du, data_max, "CURVA_LEG_CGB").drop(*drop_cols)
    df_canc_leg = obtem_ultima_data_e_hora(du, data_max, "CURVA_LEG_CANC").drop(*drop_cols)
    df_agenda_liq = obtem_ultima_data_e_hora(du, data_max, "AGENDA_LIQ").drop(*drop_cols)
    df_tpv_medio = obtem_ultima_data_e_hora(du, data_max, "TPV_MEDIO").drop(*drop_cols)
    df_chrg_inc = obtem_ultima_data_e_hora(du, data_max, "CURVA_INC_CGB").drop(*drop_cols)
    df_canc_inc = obtem_ultima_data_e_hora(du, data_max, "CURVA_INC_CANC").drop(*drop_cols)
    df_tpv_vr_liq = obtem_ultima_data_e_hora(du, data_max, "TPV_LIQ").drop(*drop_cols)

    # obtém os dados da projeção de chargeback do TPV legado
    df_1 = df_chrg_leg.join(df_canc_leg, on=["cd_grpo_cnpj", "nu_dia_prjt"], how="outer").fillna(0)

    # adiciona os dados de agenda a liquidar
    df_2 = df_1.join(
        df_agenda_liq.withColumnRenamed("nu_dia_vncm", "nu_dia_prjt")
        .select(
            "cd_grpo_cnpj",
            "nu_dia_prjt",
            "vl_totl_liq",
            "vl_lvre_liq",
            "vl_fncd_liq",
            "vl_grvm_liq",
        ),
        on=["cd_grpo_cnpj", "nu_dia_prjt"],
        how="outer"
    )

    # adiciona os volumes de chargeback e cancelamento projetados
    df_3 = df_2.join(
        df_chrg_inc.withColumnRenamed("vl_chrg_prjt", "vl_chrg_prjt_icmo")
        .withColumnRenamed("vl_chrg_liqd", "vl_chrg_liqd_icmo")
        .withColumnRenamed("vl_chrg_rbrp", "vl_chrg_rbrp_icmo")
        .withColumnRenamed("vl_chrg_arv_cilo", "vl_chrg_arv_cilo_icmo")
        .withColumnRenamed("vl_chrg_arv_mrcd", "vl_chrg_arv_mrcd_icmo"),
        on=["cd_grpo_cnpj", "nu_dia_prjt"], 
        how="left"
    ).join(
        df_canc_inc.withColumnRenamed("vl_cncl_prjt", "vl_cncl_prjt_icmo")
        .withColumnRenamed("vl_cncl_liqd", "vl_cncl_liqd_icmo")
        .withColumnRenamed("vl_cncl_rbrp", "vl_cncl_rbrp_icmo")
        .withColumnRenamed("vl_cncl_arv_cilo", "vl_cncl_arv_cilo_icmo")
        .withColumnRenamed("vl_cncl_arv_mrcd", "vl_cncl_arv_mrcd_icmo"),
        on=["cd_grpo_cnpj", "nu_dia_prjt"], 
        how="left"
    )

    # adiciona os dados de TPV incremental
    df_4 = df_3.join(df_tpv_vr_liq, on=["cd_grpo_cnpj", "nu_dia_prjt"], how="left")

    # adiciona o volume de RR esperado
    df_5 = df_4.join(
        df_tpv_medio.groupBy("cd_grpo_cnpj")
        .agg(
            (
                F.sum(F.col("pc_rbrp") * F.col("vl_sfra_medo")) 
                / F.sum("vl_sfra_medo")
            )
            .alias("pc_rbrp")
        ),
        on=["cd_grpo_cnpj"],
        how="left"
    )

    # adiciona os dados de percentual financiado
    df_6 = df_5.join(
        df_arv_medio.groupBy("cd_grpo_cnpj")
        .agg(
            (
                F.sum(F.col("pc_fncd") * F.col("vl_totl"))
                / F.sum(F.col("vl_totl"))
            ).alias("pc_fncd"),
            (
                F.sum(F.col("pc_fncd") * F.col("pc_fncd_cilo") * F.col("vl_totl"))
                / F.sum(F.col("vl_totl"))
            ).alias("pc_fncd_cilo"),
            (
                F.sum(F.col("pc_grvm") * F.col("vl_totl"))
                / F.sum(F.col("vl_totl"))
            ).alias("pc_grvm"),
            (
                F.sum(F.col("razo_arv_mrcd") * F.col("vl_totl"))
                / F.sum(F.col("vl_totl"))
            ).alias("razo_arv_mrcd")
        ).fillna(0),
        on=["cd_grpo_cnpj"],
        how="left"
    )

    # preenche nulos
    df_proj = (
        df_6.fillna(0)
        .withColumn("cd_dia_crte", F.lit(int(data_max)))
        .withColumn("dh_crga", F.lit(datetime.now(timezone.utc).strftime("%Y%m%d%H%M%S")))
    )

    return df_proj


def executa_mdlo_prmt_prjt(params: list) -> None:
    """
    Função wrapper para executar a base tbciar_re_mdlo_prmt_prjt

    :param params: parâmetros de execução
    """
    # pega os parâmetros de execução
    env = params[0] if len(params) > 0 else "PROD"
    data_max = params[1] if len(params) > 1 else None

    # corrige nulos
    env = "PROD" if env == "" else env    
    data_max = None if data_max == "" else data_max

    # instância o data utils
    from utils.data_utils import DataUtils
    du = DataUtils(env)

    # chama a função
    df = processa_tbciar_re_mdlo_prmt_prjt(du, data_max)

    # escreve o resultado para o disco
    du.write_by_name(df, "PARAMS_PROJ", "internal", partitions=["cd_dia_crte", "dh_crga"], mode="overwrite")

if __name__ == "__main__":
    # cria a sessão do spark
    spark = (
        SparkSession.builder
        .appName("SPARK_prod_prazo_tbciar_re_mdlo_prmt_prjt")
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
    executa_mdlo_prmt_prjt(sys.argv[1:])