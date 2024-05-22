import sys
import typing
from datetime import datetime, timezone
from dateutil.relativedelta import relativedelta

import pyspark.sql.functions as F
import pyspark.sql.types as T
from pyspark.sql import SparkSession
from pyspark.sql.dataframe import DataFrame


def processa_tbciar_re_mdlo_trns_clsr(
    du: "DataUtils",
    tipo_valor: str,
    data_max: typing.Optional[str] = None,
) -> DataFrame:
    """
    Gera uma versão da base de transação agrupada por cluster e data para os últimos 540 dias

    :param du: instancia do data utils para ser usada para processar a base
    :param tipo_valor (str): Tipo de valor para fazer o calculo
    :param data_max: data máxima de chargeback
    :return: Um DataFrame contendo os valores transacionados por data e safra
    """
    from modelo.comuns import classifica_em_grupos, obtem_ultima_data_e_hora

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
    data_min_dt = data_max_dt - relativedelta(months=540)

    # cria as versões inteiras dos valores
    data_min_int = int(data_min_dt.strftime("%Y%m%d"))
    data_max_int = int(data_max_dt.strftime("%Y%m%d"))

    # obtém a lista de ECs a serem abertos por cadeia forçada
    lista_cf = [
        r[0]
        for r in (
            obtem_ultima_data_e_hora(du, data_max_int, "CF_CLUSTER")
            .select("nm_grpo")
            .distinct()
            .collect()
        )
    ]

    # classifica a base de EC
    dt_cnpj = max([
        v["cd_dia_crte"] 
        for v in du.get_partition_values("GRUPO_CNPJ", "internal")
        if int(v["cd_dia_crte"]) <= data_max_int
    ])
    df_gc = du.get_by_name("GRUPO_CNPJ", "internal").filter(F.col("cd_dia_crte") == dt_cnpj).drop("cd_dia_crte")

    # obtém os dados de cadeia forçada por EC
    df_ec = (
        du.get_by_name("EC", "internal")
        .select("nu_ec", "nu_cnpj9_cpf", "nm_cdfr")
        .withColumn(
            "nm_grpo",
            F.when(
                F.col("nm_cdfr") == "NAO SE APLICA",
                F.col("nu_cnpj9_cpf").cast("string")
            )
            .otherwise(F.col("nm_cdfr"))
        )
        .drop("nu_cnpj9_cpf", "nm_cdfr")
    )

    # obtém os valores de safra de volume transacionado por cluster para
    # os últimos 540 dias
    df_trns_agregada = (
        classifica_em_grupos(
            du.get_by_name("TRNS_AG", "internal")
            .filter(
                (F.col("cd_dh_incl_rgst") >= data_min_int)
                & (F.col("cd_dh_incl_rgst") <= data_max_int)
                & (F.col("dt_trns") >= data_min_dt)
                & (F.col("dt_trns") <= data_max_dt)
            )
            .select(
                "nu_ec", 
                "nu_cnpj_cpf_recr",
                "nm_bndr", 
                "nm_tipo_trns",
                "qt_prcl", 
                "dt_trns",
                "in_crto_psen",
                "nm_unde_ngco_fpna",
                "nm_sgmt_cmrl",
                "cd_so_ramo_atvd",
                "in_rbrp",
                f"vl_{tipo_valor}_agnd_trns",
            )
            .join(df_ec, on="nu_ec", how="inner"),
            lista_cf,
        )
        .groupBy("nu_cnpj_cpf_recr", "cd_clsr", "nm_tipo_trns", "qt_prcl", "dt_trns")
        .agg(
            F.sum(F.col(f"vl_{tipo_valor}_agnd_trns"))
            .cast(T.DoubleType())
            .alias("vl_sfra"),
            F.sum(F.col("in_rbrp").cast(T.IntegerType()) * F.col(f"vl_{tipo_valor}_agnd_trns"))
            .cast(T.DoubleType())
            .alias("vl_rbrp"),
        )
    )

    # adiona os dados de grupo e agrega o resultado
    df_trns_agregada = (
        df_trns_agregada.join(df_gc, on=["nu_cnpj_cpf_recr"], how="inner")
        .groupBy("cd_grpo_cnpj", "cd_clsr", "nm_tipo_trns", "qt_prcl", "dt_trns")
        .agg(
            F.sum("vl_sfra").alias("vl_sfra"),
            F.sum("vl_rbrp").alias("vl_rbrp"),
            F.sum(F.col("vl_sfra") * F.col("vl_peso_cnpj")).alias("vl_sfra_pndr"),
            F.sum(F.col("vl_rbrp") * F.col("vl_peso_cnpj")).alias("vl_rbrp_pndr"),
        )
        .withColumn("cd_dia_crte", F.lit(data_max_int))
        .withColumn("dh_crga", F.lit(datetime.now(timezone.utc).strftime("%Y%m%d%H%M%S")))
        .withColumn("dc_prmt_exec", F.concat(F.lit("tipo_valor="), F.lit(tipo_valor)))
    )

    return df_trns_agregada


def executa_mdlo_trns_clsr(params: list) -> None:
    """
    Função wrapper para executar a base tbciar_re_mdlo_trns_clsr

    :param params: parâmetros de execução
    """
    import modelo.configs as confs

    # pega os parâmetros de execução
    env = params[0] if len(params) > 0 else "PROD"
    data_max = params[1] if len(params) > 1 else None
    tipo_valor = params[2] if len(params) > 2 else confs.TIPO_VALOR

    # corrige nulos
    env = "PROD" if env == "" else env    
    data_max = None if data_max == "" else data_max
    tipo_valor = confs.TIPO_VALOR if tipo_valor == "" else tipo_valor

    # instância o data utils
    from utils.data_utils import DataUtils
    du = DataUtils(env)

    # executa o processamento
    df = processa_tbciar_re_mdlo_trns_clsr(du=du, tipo_valor=tipo_valor, data_max=data_max)

    # escreve o resultado para o disco
    du.write_by_name(df, "TRNS_CLUSTER", "internal", partitions=["cd_dia_crte", "dh_crga"], mode="overwrite")


if __name__ == "__main__":
    # cria a sessão do spark
    spark = (
        SparkSession.builder
        .appName("SPARK_prod_prazo_tbciar_re_mdlo_trns_clsr")
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
    executa_mdlo_trns_clsr(sys.argv[1:])