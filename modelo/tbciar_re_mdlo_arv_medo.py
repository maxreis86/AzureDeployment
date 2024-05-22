import sys
import typing
from datetime import datetime, timezone
from dateutil.relativedelta import relativedelta

import pyspark.sql.functions as F
from pyspark.sql import SparkSession
from pyspark.sql.dataframe import DataFrame


def processa_tbciar_re_mdlo_arv_medo(
    du: "DataUtils",
    janela_apuracao_arv: int,
    data_max: typing.Optional[str] = None,
) -> DataFrame:
    """
    Processa dados de transações diarias para gerar o volume transacionado médio numa janela escolhida

    Esta função realiza o processamento de dados de transações agregadas, calcula a media de TPV para a JANELA_APURACAO_TPV,
    e calcula o % do TPV que é antecipado por RR   

    :param du: instancia do data utils para ser usada para processar a base
    :param data_max: data máxima de chargeback
    :param janela_apuracao_arv (int): Quantidade de meses a serem considerados para calcular a media de ARV
    :return: Um DataFrame contendo as curvas de chargeback processadas.
    """

    # obtém os valores de datas mínimas e máximas que deveriam ser utilizados
    if data_max is None:
        data_max = max([v["cd_dia_crte"] for v in du.get_partition_values("GRUPO_CNPJ", "internal")])
    else:
        data_max = max([
            v["cd_dia_crte"] 
            for v in du.get_partition_values("GRUPO_CNPJ", "internal") 
            if v["cd_dia_crte"] <= data_max
        ])

    data_max_dt = datetime(int(data_max[:4]), int(data_max[4:6]), int(data_max[6:]))
    # calcula a data mínima a ser utilizada (data_max_dt - janela_apuracao_arv)
    data_min_dt = data_max_dt - relativedelta(months=janela_apuracao_arv)

    # cria as versões inteiras dos valores
    data_min_int = int(data_min_dt.strftime("%Y%m%d"))
    data_max_int = int(data_max_dt.strftime("%Y%m%d"))

    # classifica a base de EC
    df_ec = du.get_by_name("GRUPO_CNPJ", "internal").filter(F.col("cd_dia_crte") == data_max_int)

    # obtém a base de agenda e calcula o percentual médio de antecipação por bandeira
    df_agenda_bndr = (
        du.get_by_name("AGENDA_FIN", "internal")
        .filter(
            (F.col("cd_dia_crga") >= data_min_int)
            & (F.col("cd_dia_crga") <= data_max_int)
        )
        .groupBy("nu_cnpj_cpf_recr", "cd_bndr")
        .agg(
            F.sum("vl_totl").alias("vl_totl"),
            F.sum("vl_fncd").alias("vl_fncd"),
            F.sum("vl_arv_cilo").alias("vl_fncd_cilo"),
            F.sum("vl_arv_mrcd").alias("vl_fncd_mrcd"),
            F.sum("vl_grvm").alias("vl_grvm"),
        )         
    )

    # agrega os dados no grupo de cnpj
    df_arv_medio = (
        df_agenda_bndr.join(df_ec, on="nu_cnpj_cpf_recr", how="inner")
        .groupBy("cd_grpo_cnpj", "cd_bndr")
        .agg(
            F.sum(F.col("vl_totl") * F.col("vl_peso_cnpj")).alias("vl_totl"),
            F.sum(F.col("vl_fncd") * F.col("vl_peso_cnpj")).alias("vl_fncd"),
            F.sum(F.col("vl_fncd_cilo") * F.col("vl_peso_cnpj")).alias("vl_fncd_cilo"),
            F.sum(F.col("vl_fncd_mrcd") * F.col("vl_peso_cnpj")).alias("vl_fncd_mrcd"),
            F.sum(F.col("vl_grvm") * F.col("vl_peso_cnpj")).alias("vl_grvm"),
        )
        .withColumn("pc_fncd", F.col("vl_fncd") / F.col("vl_totl"))
        .withColumn("pc_fncd_cilo", F.least(F.col("vl_fncd_cilo") / F.col("vl_fncd"), F.lit(1)))
        .withColumn("pc_grvm", F.col("vl_grvm") / F.col("vl_fncd"))
        .withColumn("pc_fncd_cilo", F.least(F.col("pc_fncd_cilo"), 1 - F.col("pc_grvm")))
        .withColumn("razo_arv_mrcd", F.col("vl_fncd_mrcd") / F.col("vl_totl"))
        .drop("vl_fncd", "vl_fncd_cilo", "vl_fncd_mrcd", "vl_grvm")
        .fillna(0, subset=["pc_fncd_cilo", "razo_arv_mrcd", "pc_grvm"])    
        .withColumn("cd_dia_crte", F.lit(data_max_int))
        .withColumn("dh_crga", F.lit(datetime.now(timezone.utc).strftime("%Y%m%d%H%M%S")))
        .withColumn(
            "dc_prmt_exec", F.concat(
                F.lit("janela_apuracao_arv="), F.lit(janela_apuracao_arv)
            )
        )
    )

    return df_arv_medio


def executa_mdlo_arv_medo(params: list) -> None:
    """
    Função wrapper para executar a base tbciar_re_mdlo_arv_medo

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
    df = processa_tbciar_re_mdlo_arv_medo(
        du=du,
        janela_apuracao_arv=confs.JANELA_APURACAO_ARV,
        data_max=data_max,
    )

    # escreve o resultado para o disco    
    du.write_by_name(df, "ARV_MEDIO", "internal", partitions=["cd_dia_crte", "dh_crga"], mode="overwrite")


if __name__ == "__main__":
    # cria a sessão do spark
    spark = (
        SparkSession.builder
        .appName("SPARK_prod_prazo_tbciar_re_mdlo_arv_medo")
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
    executa_mdlo_arv_medo(sys.argv[1:])