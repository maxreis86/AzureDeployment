import sys
from datetime import datetime, timezone
import typing
from dateutil.relativedelta import relativedelta

import pyspark.sql.functions as F
from pyspark.sql import SparkSession
from pyspark.sql.dataframe import DataFrame
from pyspark.sql.window import Window


def peso_por_ec(
    du: "DataUtils", data_max: typing.Union[str, None], tipo_valor: str, tipo_peso: str
)  -> DataFrame:
    """
    Calcula um peso associado a cada EC dado o faturamento e a data do mesmo

    :param du: instancia do data utils para ser usada para processar a base
    :param data_max: data de corte para o calculo
    :param tipo_valor: tipo de valor para utilizar no calculo (brto, lqdo)
    :param tipo_peso: tipo de peso a ser aplicado sobre o faturamento
    :return: data frame processado
    """
    from utils.math import aplica_peso_a_dif_datas

    data_max_int = int(data_max)
    data_max_dt = datetime.strptime(data_max, "%Y%m%d")
    data_min_dt = data_max_dt - relativedelta(days=540)
    data_min_int = int(data_min_dt.strftime("%Y%m%d"))

    return (
        aplica_peso_a_dif_datas(
            df=du.get_by_name("TRNS_AG", "internal")
            .filter(
                (F.col("cd_dh_incl_rgst") >= data_min_int)
                & (F.col("cd_dh_incl_rgst") <= data_max_int)
                & (F.col("dt_trns") > data_min_dt)
            )
            .groupBy("nu_cnpj_cpf_recr", "nu_ec", "dt_trns")
            .agg(F.sum(f"vl_{tipo_valor}_agnd_trns").alias("vl_trns"))
            .withColumn("dt_min", F.lit(data_min_dt)),
            dt_col_1="dt_min",
            dt_col_2="dt_trns",
            tipo_peso=tipo_peso,
        )
        .withColumn("vl_trns", F.col("vl_trns") * F.col("peso"))
        .groupBy("nu_cnpj_cpf_recr", "nu_ec")
        .agg(F.sum("vl_trns").alias("vl_peso_ec"))
        .withColumn("vl_peso_total_cnpj", F.sum("vl_peso_ec").over(Window.partitionBy("nu_cnpj_cpf_recr")))
        .withColumn("pct_peso_ec", F.col("vl_peso_ec") / F.col("vl_peso_total_cnpj"))
        .orderBy("nu_ec", "pct_peso_ec", ascending=False)
        .dropDuplicates(subset=["nu_ec"])
        .drop('nu_cnpj_cpf_recr', 'vl_peso_total_cnpj', 'vl_peso_ec')
    )


def processa_tbciar_re_mdlo_prbl_churn(du: "DataUtils", data_max: typing.Optional[str] = None) -> DataFrame:
    """
    Calcula um valor médio de probabilidade de churn por grupo de CNPJ

    :param du: instância do data utils
    :param data_max: data com o ultimo dia de processamento da tabela GRUPO_CNPJ
    :return: data frame com o valor de reserva aplicado e o cenário de churn a ser considerado
    """
    # Apurar a cd_dt_carga máxima da tabela
    dt_churn  = max([v["cd_mes"] for v in du.get_partition_values("BASE_CHURN", "internal")])
    dt_churn_gc  = max([v["cd_mes"] for v in du.get_partition_values("CHURN_GC", "internal")])

    # obtém os valores de datas mínimas e máximas que deveriam ser utilizados
    if data_max is None:
        data_max = max([v["cd_dia_crte"] for v in du.get_partition_values("GRUPO_CNPJ", "internal")])
    else:
        data_max = max([
            v["cd_dia_crte"] 
            for v in du.get_partition_values("GRUPO_CNPJ", "internal") 
            if v["cd_dia_crte"] <= data_max
        ])
    
    # Calcula o peso de cada EC para ser usado no calculo do churn do grupo_cnpj
    from modelo import configs as confs
    df_peso_ec = peso_por_ec(du, data_max, confs.TIPO_VALOR, confs.TIPO_PESO)

    # monta uma base única com a probabilidade de churn por EC
    prob_churn = (
        du.get_by_name("BASE_CHURN")
        .filter(F.col("cd_mes") == dt_churn)
        .select("nu_so_ec", F.col("pc_prbl_churn").alias("pc_prbl_churn"))
        .unionByName(
            du.get_by_name("CHURN_GC")
            .filter(F.col("cd_mes") == dt_churn_gc)
            .select("nu_so_ec", "pc_prbl_churn")
        )
        .groupBy("nu_so_ec")
        .agg(F.max("pc_prbl_churn").alias("pc_prbl_churn"))
    )

    # adiciona informações relevantes para avaliação da probabilidade de churn
    # 1. Peso do CNPJ usado para ponderar a probabilidade
    # 2. Unidade de negócio / Matriz Segmento, para identificar gaps de modelagem
    # 3. Flag de EC novo, para identificar clientes sem modelo
    # 4. Base Ativa, para identificar clientes com e sem faturamento
    churn = (
        du.get_by_name("GRUPO_CNPJ")
        .filter(F.col("cd_dia_crte") == data_max)
        .select("nu_cnpj_cpf_recr", "cd_grpo_cnpj", "vl_peso_cnpj")
        .join(
            du.get_by_name("EC", "internal")
            .select(
                "nu_so_ec", 
                "nu_ec",
                "nu_cnpj_cpf_recr", 
                (F.date_format(F.col("dt_aflc_ec"), "yyyyMM") > dt_churn_gc).cast("int").alias("ec_novo"),
                "nm_unde_ngco_fpna", 
                "mn_mtrz_sgmt"
            )
            .distinct(),
            on="nu_cnpj_cpf_recr",
            how="inner",
        )
        .join(prob_churn, on=["nu_so_ec"], how="left")
        .join(
            du.get_by_name("BASE_ATIVA", "internal")
            .filter(F.col("cd_mes") == dt_churn_gc)
            .select("nu_so_ec", "dc_clsf_30d", "dc_clsf_60d"),
            on="nu_so_ec",
            how="left"
        )
        .join(
            df_peso_ec,
            on="nu_ec",
            how="left"
        )
        .fillna(0, subset=["pct_peso_ec"])
    )

    # ajusta a probabilidade de churn de acordo com cenários
    # 1. se o EC não é novo e ele não aparece na base de faturamento, ele já churneou meses atrás
    # 2. se está inativo a 60 dias, então ele é um churn hoje
    # 3. se há uma probabilidade de churn estimada, a mantem
    # 4. se está inativo a 30 dias assumimos probabilidade de churn = 0.5
    # 5. se está faltando valores de churn calcula a média do grupo
    # 6. se o grupo não tiver média, assume o valor a probabilidade média da base
    # e agrupa os dados a nível grupo de CNPJ
    churn = (
        churn.withColumn(
            "pc_prbl_churn",
            F.when((F.col("dc_clsf_60d").isNull()) & (F.col("ec_novo") == 0), F.lit(1))
            .when(F.col("dc_clsf_60d") == "INATIVO", F.lit(1))
            .when(F.col("pc_prbl_churn").isNotNull(), F.col("pc_prbl_churn"))
            .when((F.col("dc_clsf_60d") != "INATIVO") & (F.col("dc_clsf_30d") == "INATIVO"), F.lit(0.5))
            .when(
                (F.col("dc_clsf_30d") != "INATIVO")
                & (F.col("nm_unde_ngco_fpna") != "GRANDES CONTAS"),
                F.mean("pc_prbl_churn").over(Window.partitionBy("cd_grpo_cnpj"))
            )
            .otherwise(F.mean("pc_prbl_churn").over(Window.partitionBy(F.lit(1))))
        )
        .withColumn("pc_prbl_churn", F.col("vl_peso_cnpj") * F.col("pct_peso_ec") * F.col("pc_prbl_churn"))
        .groupBy("cd_grpo_cnpj")
        .agg(
            (F.sum("pc_prbl_churn") / F.sum(F.col("vl_peso_cnpj") * F.col("pct_peso_ec"))).alias("pc_prbl_churn"),
            F.countDistinct(F.col("nu_cnpj_cpf_recr")).alias("nu_cnpj")
        )
        .withColumn("cd_dia_crte", F.lit(int(data_max)))
        .withColumn("dh_crga", F.lit(datetime.now(timezone.utc).strftime("%Y%m%d%H%M%S")))
        .withColumn(
            "dc_prmt_exec", F.lit(
                f"tipo_peso={confs.TIPO_PESO};"
                + f"tipo_valor={confs.TIPO_VALOR}"
            )
        )
    )

    return churn


def executa_mdlo_prbl_churn(params: list) -> None:
    """
    Função wrapper para executar a base tbciar_re_mdlo_prbl_churn

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
    df = processa_tbciar_re_mdlo_prbl_churn(du, data_max=data_max)

    # escreve o resultado para o disco
    du.write_by_name(df, "PROB_CHURN", "internal", partitions=["cd_dia_crte", "dh_crga"], mode="overwrite")


if __name__ == "__main__":
    # cria a sessão do spark
    spark = (
        SparkSession.builder
        .appName("SPARK_prod_prazo_tbciar_re_mdlo_prbl_churn")
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
    executa_mdlo_prbl_churn(sys.argv[1:])