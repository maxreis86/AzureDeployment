import sys
import typing
from datetime import datetime
from datetime import timezone

import pyspark.sql.functions as F
from pyspark.sql.window import Window
from pyspark.sql import SparkSession
from pyspark.sql.dataframe import DataFrame


def add_agenda_liquidar(df: DataFrame, coluna_agenda: str) -> DataFrame:
    """
    Adiciona colunas com agenda livre e gravamada a liquidar em valor e em pergentual agrupado por CNPJ.

    :param df (DataFrame): O DataFrame de entrada.
    :param coluna_agenda (str): O nome da coluna a ser processada.
    :return: O DataFrame com colunas adicionadas.
    """
    # Define as especificações da janela
    partition_window = Window.partitionBy("cd_grpo_cnpj")
    ordered_window = Window.partitionBy("cd_grpo_cnpj").orderBy("dt_vncm").rowsBetween(Window.unboundedPreceding, 0)
    
    # gera os nomes de coluns
    acum_col_name = f"{coluna_agenda}_grpo_acmo"
    total_col_name = f"{coluna_agenda}_grpo"
    liquidar_col_name = f"{coluna_agenda}_liq"
    liquidar_col_name_pct = f"{coluna_agenda}_liq_prcn"
    
    # calcula os valores acumulados de agenda, o valor total, a quantidade a liquidar
    df = (
        df.withColumn(acum_col_name, F.sum(F.col(coluna_agenda)).over(ordered_window))
        .withColumn(total_col_name, F.sum(F.col(coluna_agenda)).over(partition_window))
        .withColumn(liquidar_col_name, F.col(total_col_name) - F.col(acum_col_name))
        .withColumn(
            liquidar_col_name_pct,
            F.when(F.col(total_col_name) > 0, F.col(liquidar_col_name) / F.col(total_col_name))
            .otherwise(F.lit(0.0))
        )
        .drop(acum_col_name, total_col_name)
    )
    
    return df


def processa_tbciar_mdlo_agnd_liq(
    du: "DataUtils", data_max: typing.Optional[str] = None,
) -> DataFrame:
    """
    Processa a base de agenda e cria uma visão diária de agenda livre e gravamada a receber 
    por grupo de CNPJ para os próximos 540 dias

    :param du: instancia do data utils para ser usada para processar a base
    :param data_max: data de corte a ser processada
    :return: data frame processado
    """
    # obtém a data máxima
    if data_max is None:
        data_max = max([v["cd_dia_crga"] for v in du.get_partition_values("AGENDA_DIARIA", "internal")])

    # pega a classificação de grupo por CNPJ
    dt_cnpj = max([
        v["cd_dia_crte"] 
        for v in du.get_partition_values("GRUPO_CNPJ", "internal")
        if v["cd_dia_crte"] <= data_max
    ])
    df_ec = du.get_by_name("GRUPO_CNPJ", "internal").filter(F.col("cd_dia_crte") == dt_cnpj)
    
    # agrega os dados de agenda para a data selecionada
    df_temp1 = (
        du.get_by_name("AGENDA_DIARIA", "internal")
        .filter(F.col("cd_dia_crga") == int(data_max))
        .join(df_ec, on="nu_cnpj_cpf_recr", how="inner")
        .groupBy("cd_grpo_cnpj", "dt_dia", "dt_vncm")
        .agg(
            F.sum(F.col("vl_totl") * F.col("vl_peso_cnpj")).alias("vl_totl"),
            F.sum(F.col("vl_lvre") * F.col("vl_peso_cnpj")).alias("vl_lvre"),
            F.sum(F.col("vl_fncd") * F.col("vl_peso_cnpj")).alias("vl_fncd"),
            F.sum(F.col("vl_grvm") * F.col("vl_peso_cnpj")).alias("vl_grvm"),
        )
        .cache()
    )

    # extende todas as linhas por 540 dias
    df = (
        df_temp1.select("cd_grpo_cnpj")
        .distinct()
        .crossJoin(
            du.spark.range(0, (541))
            .toDF("nu_dia_vncm")
            .withColumn("dt_dia", F.to_date(F.lit(str(data_max)), "yyyyMMdd"))
            .withColumn("dt_vncm", F.expr("date_add(dt_dia, cast(nu_dia_vncm as integer))"))
        )
        .join(df_temp1, on=["cd_grpo_cnpj", "dt_dia", "dt_vncm"], how="left")
        .fillna(0)
        .withColumn("dh_crga", F.lit(datetime.now(timezone.utc).strftime("%Y%m%d%H%M%S")))
        .withColumn("cd_dia_crte", F.lit(int(data_max)))
    )
    
    # realiza a projeção de valores a liquidar
    df = add_agenda_liquidar(df, "vl_totl")
    df = add_agenda_liquidar(df, "vl_lvre")
    df = add_agenda_liquidar(df, "vl_fncd")
    df = add_agenda_liquidar(df, "vl_grvm")

    return df.drop("vl_totl", "vl_lvre", "vl_fncd", "vl_grvm")


def executa_mdlo_agnd_liq(params: list) -> None:
    """
    Função wrapper para executar a base tbciar_re_mdlo_agnd_liq

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

    import modelo.configs as confs

    # executa o processamento
    df = processa_tbciar_mdlo_agnd_liq(du=du, data_max=data_max)

    # escreve o resultado para o disco
    du.write_by_name(df, "AGENDA_LIQ", "internal", partitions=["cd_dia_crte", "dh_crga"], mode="overwrite")


if __name__ == "__main__":
    # cria a sessão do spark
    spark = (
        SparkSession.builder
        .appName("SPARK_prod_prazo_tbciar_re_mdlo_agnd_liq")
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
    executa_mdlo_agnd_liq(sys.argv[1:])