import sys
import typing
from datetime import datetime, timezone
from dateutil.relativedelta import relativedelta

import pyspark.sql.functions as F
from pyspark.sql import SparkSession
from pyspark.sql.dataframe import DataFrame
from pyspark.sql.window import Window

def processa_tbciar_re_mdlo_cdfr(
    du: "DataUtils",
    janela_apuracao: int,
    tipo_valor: str,
    basis_point: int, 
    data_max: typing.Optional[str] = None,
) -> DataFrame:
    """
    Baseado nos chargebacks dos últimos 6 meses, calcula quais são as cadeia forçadas que 
    devem ser modeladas separadamente

    :param du: instancia do data utils para ser usada para processar a base
    :param data_max: data máxima de chargeback
    :param janela_apuracao (int): Número de meses para a janela de apuração das curvas.
    :param basis_point: threshold in basis point para incluir na lista
    :param tipo_valor (str): Tipo de valor para fazer o calculo
    :return: Um DataFrame contendo a lista de clientes que serão modelados a parte
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
    data_min_dt = data_max_dt - relativedelta(months=janela_apuracao + 1) + relativedelta(days=1)

    # cria as versões inteiras dos valores
    data_min_int = int(data_min_dt.strftime("%Y%m%d"))
    data_max_int = int(data_max_dt.strftime("%Y%m%d"))
    
    # calcula a lista de clientes que ficará aberta a nivel cadeia forçada devido
    # ao alto volume de chargeback
    grupos_cluster = (
        du.get_by_name("CHARGEBACK", "internal")
        .filter(
            (F.col("cd_dh_incl_rgst") >= data_min_int)
            & (F.col("cd_dh_incl_rgst") <= data_max_int)
            & (F.col("dt_chrg") >= data_min_dt)
            & (F.col("nm_unde_ngco_fpna") == "GRANDES CONTAS")
        )
        .withColumn(
            "nm_grpo",
            F.when(
                F.col("nm_cdfr") == "NAO SE APLICA", 
                F.col("nu_cnpj9_cpf").cast("string")
            )
            .otherwise(F.col("nm_cdfr"))
        )
        .groupBy("nm_grpo")
        .agg(F.abs(F.sum(F.col(f"vl_{tipo_valor}_chrg"))).alias("vl_chrg"))
        .withColumn("pc_chrg_totl", F.col("vl_chrg") / F.sum("vl_chrg").over(Window.partitionBy(F.lit(1))))
        .filter(F.col("pc_chrg_totl") > basis_point / 10000)
        .select(
            "nm_grpo",
            "vl_chrg",
            "pc_chrg_totl",
            F.lit(data_max_int).alias("cd_dia_crte")
        )
        .withColumn("dh_crga", F.lit(datetime.now(timezone.utc).strftime("%Y%m%d%H%M%S")))
        .withColumn(
            "dc_prmt_exec", F.concat(
                F.lit("janela_apuracao="), F.lit(janela_apuracao),
                F.lit(";tipo_valor="), F.lit(tipo_valor),
                F.lit(";basis_point="), F.lit(basis_point)
            )
        )
    )
    
    return grupos_cluster

def executa_mdlo_cdfr(params: list) -> None:
    """
    Função wrapper para executar a base tbciar_re_mdlo_cdfr

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

    import modelo.configs as confs

    # executa o processamento
    df = processa_tbciar_re_mdlo_cdfr(
        du=du,
        janela_apuracao=confs.MESES_CGB_CF,
        tipo_valor=confs.TIPO_VALOR,
        basis_point=confs.BASIS_POINTS_CF,
        data_max=data_max,
    )

    # escreve o resultado para o disco
    du.write_by_name(df, "CF_CLUSTER", "internal", partitions=["cd_dia_crte", "dh_crga"], mode="overwrite")


if __name__ == "__main__":
    # cria a sessão do spark
    spark = (
        SparkSession.builder
        .appName("SPARK_prod_prazo_tbciar_re_mdlo_cdfr")
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
    executa_mdlo_cdfr(sys.argv[1:])