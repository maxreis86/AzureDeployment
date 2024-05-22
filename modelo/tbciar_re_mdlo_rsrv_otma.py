import sys
import typing
from datetime import datetime
from datetime import timezone

import pyspark.sql.functions as F
from pyspark.sql import SparkSession
from pyspark.sql.dataframe import DataFrame
from pyspark.sql.window import Window


def processa_tbciar_re_mdlo_rsrv_otma(
    du: "DataUtils", 
    data_max: typing.Union[str, None],
    perda_max_varejo: float = 10, 
    perda_max_gc: float = 1000, 
    margem_financ: float = 0.01, 
    delta_rsrv_otma: float = 0.3,
    var_min_delta_ot: float = 0.2, 
) -> DataFrame:
    """
    A partir dos dados calculados de reserva seleciona qual reserva deverá ser aplicada
    de acordo com o risco aceitável e o perfil de financiamento do cliente

    :param du: instância do data utils
    :param data_max: data de corte dos calculos
    :param perda_max_varejo: valor máximos de perda aceitável para varejo
    :param perda_max_gc: valor máximos de perda aceitável para grandes contas
    :param margem_financ: Máxima diferença considerável aceitável entre o percentual financiado 
                          normalmente pelo cliente e o valor de agenda livre após a reserva
    :param delta_rsrv_otma: Máxima diferença considerável aceitável entre o percentual financiado 
                          normalmente pelo cliente e o valor de agenda livre para poder aplicar
                          a reserva ótima, quando a mesma é maior que a reserva de perda limite
    :return: data frame com o valor de reserva aplicado e o cenário de churn a ser considerado
    """
    from modelo.comuns import obtem_ultima_data_e_hora
    window = Window.partitionBy("cd_grpo_cnpj", "dc_tipo_churn").orderBy("vl_rsrv")

    # Apurar a cd_dia_crte máxima das tabelas
    if data_max is None:
        data_max = max([v["cd_dia_crte"] for v in du.get_partition_values("GRUPO_CNPJ", "internal")])

    data_max = int(data_max)

    # Apurar a cd_dt_carga máxima da tabela
    drop_cols = ["cd_dia_crte", "dh_crga", "dc_prmt_exec"]
    df_rsrv = obtem_ultima_data_e_hora(du, data_max, "RESERVA").drop(*drop_cols)
    df_arv = obtem_ultima_data_e_hora(du, data_max, "ARV_MEDIO").drop(*drop_cols)

    # calcula o valor percentual financiado
    pc_fncd = (
        df_arv.select("cd_grpo_cnpj", "vl_totl", "pc_fncd")
        .groupBy("cd_grpo_cnpj")
        .agg((F.sum(F.col("pc_fncd") * F.col("vl_totl")) / F.sum("vl_totl")).alias("pc_fncd"))
    )

    # calcula o valor de mínima variação de reserva dada uma variação de perda
    # considerando um valor mínimo de variação de reserva
    reserva_otima = (
        df_rsrv.withColumn(
            "delta", (F.col("vl_perd") / F.lag("vl_perd").over(window))
            / (F.lag("vl_perd").over(window) / F.lag("vl_perd", 2).over(window))
        )
        .withColumn("delta_abs", F.lag("vl_perd").over(window) - F.col("vl_perd"))
        .withColumn(
            "delta", 
            F.when(
                (
                    (F.col("cd_grpo_cnpj").contains(" | ")) 
                    & (F.col("delta_abs") >= var_min_delta_ot * perda_max_varejo)
                ) | (
                    (~F.col("cd_grpo_cnpj").contains(" | ")) 
                    & (F.col("delta_abs") >= var_min_delta_ot * perda_max_gc)
                ), 
                F.col("delta")
            ).otherwise(F.lit(None))
        )
        .withColumn("min_delta", F.min("delta").over(Window.partitionBy("cd_grpo_cnpj", "dc_tipo_churn")))
    )

    # obtém a reserva ótima como o ponto de menor variação de perda como o aumento de reserva
    reserva_otima = (
        reserva_otima.withColumn(
            "vl_rsrv_otma",
            F.when(
                (F.col("min_delta") == F.col("delta")) & (F.col("min_delta").isNotNull()),
                F.col("vl_rsrv")
            )
            .otherwise(F.lit(None))
        )
        .withColumn(
            "vl_perd_rsrv_otma",
            F.when(F.col("vl_rsrv") == F.col("vl_rsrv_otma"), F.col("vl_perd"))
            .otherwise(F.lit(None))
        )
    )

    # obtém a reserva de perda limite como a minima reserva que atinge a perda considerada
    reserva_otima = (
        reserva_otima.withColumn(
            "vl_rsrv_lmte", 
            F.when(
                (
                    (F.col("cd_grpo_cnpj").contains(" | ")) & (F.col("vl_perd") < perda_max_varejo)
                ) | (
                    (~F.col("cd_grpo_cnpj").contains(" | ")) & (F.col("vl_perd") < perda_max_gc)
                ), 
                F.col("vl_rsrv")
            ).otherwise(F.lit(None))
        )
        .withColumn("vl_rsrv_lmte", F.min("vl_rsrv_lmte").over(Window.partitionBy("cd_grpo_cnpj", "dc_tipo_churn")))
        .withColumn(
            "vl_perd_rsrv_lmte",
            F.when(F.col("vl_rsrv") == F.col("vl_rsrv_lmte"), F.col("vl_perd"))
            .otherwise(F.lit(None))
        )
    )

    # obtém os valores de reserva em cada cenário
    reserva_otima = (
        reserva_otima.withColumn("dc_tipo_churn", F.concat(F.lit("vl_c"), F.lpad(F.col("dc_tipo_churn"), 3, "0")))
        .groupBy("cd_grpo_cnpj")
        .pivot("dc_tipo_churn")
        .agg(
            F.sum(F.lit(1)).alias("cntg"),
            F.min("vl_rsrv_otma").alias("rsrv_otma"),
            F.max("vl_perd_rsrv_otma").alias("perd_otma"),
            F.min("vl_rsrv_lmte").alias("rsrv_lmte"),
            F.max("vl_perd_rsrv_lmte").alias("perd_lmte"),
        )
        .join(pc_fncd, on=["cd_grpo_cnpj"], how="left")
    )

    # calcula a reserva a ser utilizada
    # 1. Existe uma perda que permita chegar ao threshold definido como perda aceitável?
    cond_1 = (
        (F.col("vl_c060_rsrv_lmte").isNull()) 
        & (F.col("vl_c060_rsrv_otma").isNotNull())
        & (1 - F.col("vl_c060_rsrv_otma") - F.coalesce(F.col("pc_fncd"), F.lit(0)) > margem_financ)
    )
    # 2. se a reserva ótima for maior que a reserva para perda máxima
    # e o valor de reserva ótima não ultrapassa o limite de financiamento mais uma margem
    # então iremos utilizar a reserva ótima
    cond_2 = (
        (F.col("vl_c060_rsrv_otma") > F.col("vl_c060_rsrv_lmte"))
        & (1 - F.col("vl_c060_rsrv_otma") - F.coalesce(F.col("pc_fncd"), F.lit(0)) > delta_rsrv_otma)
    )
    # 3. se a reserva ótima for maior que a reserva para perda máxima
    # e o valor de reserva ótima ultrapassa o limite de financiamento
    # vamos verificar se o valor da reserva de perda máxima ultrapassa uma margem mínima
    # se não ultrapassar vamos utiliza-la
    cond_3 = (1 - F.col("vl_c060_rsrv_lmte") - F.coalesce(F.col("pc_fncd"), F.lit(0)) > margem_financ)
    # todos os outros casos (i.e. utiliza reserva de perda limite, mas ela ultrapassa o valor de financiamento)
    # então nós vamos considerar o maior valor de reserva estipulado e assumir churn 100%
    reserva_otima = (
        reserva_otima.withColumn(
            "vl_rsrv", 
            F.when(cond_1, F.col("vl_c060_rsrv_otma"))
            .when(cond_2, F.col("vl_c060_rsrv_otma"))
            .when(cond_3, F.col("vl_c060_rsrv_lmte"))
            .otherwise(
                F.greatest(
                    F.col("vl_c060_rsrv_otma"), 
                    F.col("vl_c060_rsrv_lmte"), 
                    F.col("vl_c100_rsrv_otma"), 
                    F.col("vl_c100_rsrv_lmte")
                )
            )
        )
        .withColumn("dc_tipo_churn", F.when(cond_1, 60).when(cond_2, 60).when(cond_3, 60).otherwise(100))
        .withColumn(
            "dc_mtvo_rsrv",
            F.when(
                (F.col("vl_c060_cntg") == 0) | (F.col("vl_c060_cntg").isNull()), 
                "Cliente não faturou nos últimos 30 dias, usando churn 100"
            )
            .when(
                (F.col("vl_c060_rsrv_lmte").isNull()) & (F.col("vl_c060_rsrv_otma").isNull()), 
                "Não é possível atribuir perda limite ou perda ótima no churn 60"
            )
            .when(cond_1, "Não há reserva que atinja a perda limite, vamos usar a reserva ótima")
            .when(cond_2, "Reserva ótima não ultrapassa agenda financiada + margem")
            .when(cond_3, "Reserva de perda limite não ultrapassa agenda financiada")
            .otherwise("Reserva ultrapassa agenda financiada")
        )
        .drop("vl_c060_cntg", "vl_c100_cntg")
    )

    # adiciona dados sobre a perda associadas a reserva
    reserva_otima = (
        reserva_otima.join(
            df_rsrv,
            on=["cd_grpo_cnpj", "dc_tipo_churn", "vl_rsrv"],
            how="left"
        )
        .join(
            df_rsrv.filter(F.col("dc_tipo_churn") == 100)
            .groupBy("cd_grpo_cnpj", "dc_tipo_churn")
            .agg(F.min("vl_perd").alias("vl_perd_min")),
            on=["cd_grpo_cnpj", "dc_tipo_churn"],
            how="left"
        )
        .withColumn(
            "vl_perd", 
            F.when(F.col("vl_perd").isNull(), F.col("vl_perd_min")).otherwise(F.col("vl_perd"))
        )
        .drop("vl_perd_min")
    )

    # adiciona informação de qual seria a perda do cliente em caso de reserva 0
    reserva_otima = reserva_otima.join(
        df_rsrv.filter(F.col("vl_rsrv") == 0)
        .select("cd_grpo_cnpj", "dc_tipo_churn", "vl_perd")
        .withColumnRenamed("vl_perd", "vl_perd_rsrv0"),
        on=["cd_grpo_cnpj", "dc_tipo_churn"],
        how="left"
    )

    # adiciona a informação da perda do cliente ao aplicar a máxima reserva do churn 100
    reserva_otima = (
        reserva_otima.join(
            df_rsrv.filter(F.col("dc_tipo_churn") == 100)
            .withColumn("max_rsrv", F.max("vl_rsrv").over(Window.partitionBy("cd_grpo_cnpj")))
            .filter(F.col("vl_rsrv") == F.col("max_rsrv"))
            .select("cd_grpo_cnpj", "dc_tipo_churn", "vl_rsrv", "vl_perd")
            .withColumnRenamed("vl_rsrv", "vl_max_rsrv_c100")
            .withColumnRenamed("vl_perd", "vl_perd_max_rsrv_c100"),
            on=["cd_grpo_cnpj", "dc_tipo_churn"],
            how="left"
        )
        .withColumn("vl_rsrv", F.coalesce(F.col("vl_rsrv"), F.col("vl_max_rsrv_c100")))
        .withColumn("vl_perd", F.coalesce(F.col("vl_perd"), F.col("vl_perd_max_rsrv_c100")))
        .persist()
    )

    return (
        reserva_otima.withColumn(
            "dc_prmt_exec", F.lit(
                f"perda_max_varejo={perda_max_varejo};"
                + f"perda_max_gc={perda_max_gc};"
                + f"margem_financ={margem_financ};"
                + f"delta_rsrv_otma={delta_rsrv_otma};"
                + f"var_min_delta_ot={var_min_delta_ot};"
            )
        )
        .withColumn("cd_dia_crte", F.lit(data_max))
        .withColumn("dh_crga", F.lit(datetime.now(timezone.utc).strftime("%Y%m%d%H%M%S")))
    )


def executa_mdlo_rsrv_otma(params: list) -> None:
    """
    Função wrapper para executar a base tbciar_re_mdlo_rsrv_otma

    :param params: parâmetros de execução
    """
    import modelo.configs as confs

    # pega os parâmetros de execução
    env = params[0] if len(params) > 0 else "PROD"
    data_max = params[1] if len(params) > 1 else None
    perda_max_varejo = params[2] if len(params) > 2 else str(confs.PERDA_MAX_VAREJO)
    perda_max_gc = params[3] if len(params) > 3 else str(confs.PERDA_MAX_GC)
    margem_financ = params[4] if len(params) > 4 else str(confs.MARGEM_FINANC)
    delta_rsrv_otma = params[5] if len(params) > 5 else str(confs.DELTA_RSVA_OT)
    var_min_delta_ot = params[6] if len(params) > 6 else str(confs.VAR_MIN_DELTA_OT)
    
    # corrige nulos
    env = "PROD" if env == "" else env    
    data_max = None if data_max == "" else data_max
    perda_max_varejo = confs.PERDA_MAX_VAREJO if perda_max_varejo == "" else float(perda_max_varejo)
    perda_max_gc = confs.PERDA_MAX_GC if perda_max_gc == "" else float(perda_max_gc)
    margem_financ = confs.MARGEM_FINANC if margem_financ == "" else float(margem_financ)
    delta_rsrv_otma = confs.DELTA_RSVA_OT if delta_rsrv_otma == "" else float(delta_rsrv_otma)
    var_min_delta_ot = confs.VAR_MIN_DELTA_OT if var_min_delta_ot == "" else float(var_min_delta_ot)
    
    # instância o data utils
    from utils.data_utils import DataUtils
    du = DataUtils(env)

    # chama a função
    df = processa_tbciar_re_mdlo_rsrv_otma(
        du=du, 
        data_max=data_max,
        perda_max_varejo=perda_max_varejo, 
        perda_max_gc=perda_max_gc, 
        margem_financ=margem_financ,
        delta_rsrv_otma=delta_rsrv_otma,
        var_min_delta_ot=var_min_delta_ot,
    )

    # escreve o resultado para o disco
    du.write_by_name(df, "RESERVA_OTIMA", "internal", partitions=["cd_dia_crte", "dh_crga"], mode="overwrite")

if __name__ == "__main__":
    # cria a sessão do spark
    spark = (
        SparkSession.builder
        .appName("SPARK_prod_prazo_tbciar_re_mdlo_rsrv_otma")
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
    executa_mdlo_rsrv_otma(sys.argv[1:])