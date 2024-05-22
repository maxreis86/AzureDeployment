import sys
import typing
from datetime import datetime
from dateutil.relativedelta import relativedelta

import pyspark.sql.functions as F
from pyspark.sql import SparkSession
from pyspark.sql.dataframe import DataFrame
from pyspark.sql.types import DoubleType


def processa_tbciar_re_mcaj(
    du: "DataUtils",
    data_min: typing.Optional[str] = None,
    data_max: typing.Optional[str] = None,
)  -> DataFrame:
    """
    Calcula os valores de MCAj por mês, separando entre os grupos de Aquisição, ARV/RR e Outros

    :param du: instancia do data utils para ser usada para processar a base
    :param data_min: data mínima de processamento (se nulo, considera a última data processada ou 202201)
    :param data_max: data máxima de processamento (se nulo, considera a última partição de RENTABILIDADE)
    :return: data frame processado
    """
    # obtém a data mínima e data máxima
    if data_min is None:
        if du.is_table_available("MCAJ", "internal"):
            data_min = max([v["cd_mes"] for v in du.get_partition_values("MCAJ", "internal")])
            data_min = (datetime.strptime(data_min, "%Y%m") + relativedelta(months=1)).strftime("%Y%m")
        else:
            data_min = "202201"
    data_min = max(data_min, "202201")
    if data_max is None:
        data_max = du.get_by_name("RENTABILIDADE").agg(F.max("cd_mes")).collect()[0][0]
        if du.is_table_available("MCAJ", "internal"):
            ult_data = int(max([v["cd_mes"] for v in du.get_partition_values("MCAJ", "internal")]))
            if data_max == ult_data:
                return

    # apurar periodo
    df = (
        du.get_by_name("RENTABILIDADE")
        .filter(F.col("cd_mes").between(int(data_min), int(data_max)))
        .select(
            "cd_mes",
            "nu_ec", 
            "nu_so_ec",
            "cd_grp_produto_servico",
            "cd_indica_trans_prz_flexi",
            F.coalesce(F.col("vl_fat_contabil"), F.lit(0)).alias("vl_fat_contabil"),
            F.coalesce(F.col("receita_comissao_rr"), F.lit(0)).alias("receita_comissao_rr"),
            F.coalesce(F.col("receita_comissao_rr_excd"), F.lit(0)).alias("receita_comissao_rr_excd"),
            F.coalesce(F.col("receita_comissao_rr_desc"), F.lit(0)).alias("receita_comissao_rr_desc"),
            F.coalesce(F.col("impostos"), F.lit(0)).alias("impostos"),
            F.coalesce(F.col("custo_var_lib_parc"), F.lit(0)).alias("custo_var_lib_parc"),
            F.coalesce(F.col("custo_var_arv"), F.lit(0)).alias("custo_var_arv"),
            F.coalesce(F.col("float_financ"), F.lit(0)).alias("float_financ"),
            F.coalesce(F.col("imposto_rcta_financ"), F.lit(0)).alias("imposto_rcta_financ"),
            F.coalesce(F.col("custo_cptc_rr"), F.lit(0)).alias("custo_cptc_rr"),
            F.coalesce(F.col("margem_contrib"), F.lit(0)).alias("margem_contrib"),
            F.coalesce(F.col("margem_contrib_ajustada"), F.lit(0)).alias("margem_contrib_ajustada")
        )
        .persist()
    )

    # calculo RR     
    df_calc_rr_isolado = (
        df.filter(F.col("cd_indica_trans_prz_flexi") == 3)
        .groupBy(["cd_mes", "nu_ec", "nu_so_ec", "cd_grp_produto_servico", "cd_indica_trans_prz_flexi"])
        .agg(
            F.sum(F.col("vl_fat_contabil")).alias("volume_capturado_total_com_rr"),
            F.sum(
                F.col("receita_comissao_rr") 
                + F.col("receita_comissao_rr_excd") 
                + F.col("receita_comissao_rr_desc") 
                + F.col("impostos") 
                + F.col("custo_var_lib_parc") 
                + F.col("custo_var_arv")
            )
            .cast(DoubleType())
            .alias("margem_contrib_rr"),
            F.sum(
                F.col("receita_comissao_rr") 
                + F.col("receita_comissao_rr_excd") 
                + F.col("receita_comissao_rr_desc") 
                + F.col("impostos") 
                + F.col("custo_var_lib_parc") 
                + F.col("custo_var_arv") 
                + F.col("float_financ") 
                + F.col("imposto_rcta_financ") 
                + F.col("custo_cptc_rr")
            )
            .cast(DoubleType())
            .alias("margem_contrib_ajustada_rr"),
            F.sum(
                F.col("margem_contrib") - (
                    F.col("receita_comissao_rr") 
                    + F.col("receita_comissao_rr_excd") 
                    + F.col("receita_comissao_rr_desc") 
                    + F.col("impostos") 
                    + F.col("custo_var_lib_parc") 
                    + F.col("custo_var_arv")
                )
            )
            .cast(DoubleType())
            .alias("margem_contrib_produto_isolada"),
            F.sum(
                F.col("margem_contrib_ajustada") - (
                    F.col("receita_comissao_rr") 
                    + F.col("receita_comissao_rr_excd") 
                    + F.col("receita_comissao_rr_desc") 
                    + F.col("impostos") 
                    + F.col("custo_var_lib_parc") 
                    + F.col("custo_var_arv") 
                    + F.col("float_financ") 
                    + F.col("imposto_rcta_financ") 
                    + F.col("custo_cptc_rr")
                )
            )
            .cast(DoubleType())
            .alias("margem_contrib_ajustada_produto_isolada")
        )
    )
        
    # consulta todos os valores de rentabilidade
    df_estrut_rent = (
        df.select(
            "cd_mes", 
            "nu_ec", 
            "nu_so_ec", 
            "cd_grp_produto_servico",
            F.when(F.col("cd_indica_trans_prz_flexi") == 3, 3)
            .otherwise(1)
            .alias("cd_indica_trans_prz_flexi"),
            "vl_fat_contabil",
            "margem_contrib",
            "margem_contrib_ajustada"
        )
        .groupBy("cd_mes", "nu_ec", "nu_so_ec", "cd_grp_produto_servico", "cd_indica_trans_prz_flexi")
        .agg(
            F.sum(F.col("vl_fat_contabil")).cast(DoubleType()).alias("volume_capturado"),
            F.sum(F.col("margem_contrib")).cast(DoubleType()).alias("margem_contrib"),
            F.sum(F.col("margem_contrib_ajustada")).cast(DoubleType()).alias("margem_contrib_ajustada")
        )
    )

    # cria colunas dummy para clientes que apenas utilizam RR
    df_dummy = (
        df_estrut_rent
        .union(
            df_estrut_rent
            .join(
                df_calc_rr_isolado, 
                on=["cd_mes", "nu_ec", "nu_so_ec", "cd_grp_produto_servico", "cd_indica_trans_prz_flexi"], 
                how="left"
            )
            .select(
                df_estrut_rent.cd_mes,
                df_estrut_rent.nu_ec,
                df_estrut_rent.nu_so_ec,
                df_estrut_rent.cd_grp_produto_servico,
                F.lit(1).alias("cd_indica_trans_prz_flexi"),
                F.lit(0).alias("volume_capturado"),
                F.lit(0).alias("margem_contrib"),
                F.lit(0).alias("margem_contrib_ajustada")
            )
            .where(df_calc_rr_isolado.nu_ec.isNotNull())
        )
        .groupBy("cd_mes", "nu_ec", "nu_so_ec", "cd_grp_produto_servico", "cd_indica_trans_prz_flexi")
        .agg(
            F.sum("volume_capturado").cast(DoubleType()).alias("volume_capturado"),
            F.sum("margem_contrib").cast(DoubleType()).alias("margem_contrib"),
            F.sum("margem_contrib_ajustada").cast(DoubleType()).alias("margem_contrib_ajustada")
        )
    )

    # base estruturante para o calculo final
    df_calc_estrut = (
        df_dummy.join(
            df_calc_rr_isolado, 
            on=["cd_mes", "nu_ec", "nu_so_ec", "cd_grp_produto_servico"], 
            how="left"
        )
        .select(
            df_dummy.cd_mes, 
            df_dummy.nu_ec, 
            df_dummy.nu_so_ec, 
            df_dummy.cd_grp_produto_servico, 
            df_dummy.cd_indica_trans_prz_flexi,
            df_dummy.volume_capturado, 
            df_dummy.margem_contrib, 
            df_dummy.margem_contrib_ajustada, 
            df_calc_rr_isolado.volume_capturado_total_com_rr, 
            df_calc_rr_isolado.margem_contrib_rr, 
            df_calc_rr_isolado.margem_contrib_ajustada_rr,
            df_calc_rr_isolado.margem_contrib_produto_isolada, 
            df_calc_rr_isolado.margem_contrib_ajustada_produto_isolada,
        )
    )

    # base final denominando os grupos
    df_mcaj = (
        df_calc_estrut
        .withColumn(
            "margem_contrib_calculated",
            F.when(F.col("cd_indica_trans_prz_flexi") == 3, F.col("margem_contrib_rr"))
            .when(
                F.col("margem_contrib_produto_isolada").isNotNull(), 
                F.col("margem_contrib") + F.col("margem_contrib_produto_isolada")
            )
            .otherwise(F.col("margem_contrib"))
        )
        .withColumn(
            "margem_contrib_ajustada_calculated",
            F.when(F.col("cd_indica_trans_prz_flexi") == 3, F.col("margem_contrib_ajustada_rr"))
            .when(
                F.col("margem_contrib_ajustada_produto_isolada").isNotNull(), 
                F.col("margem_contrib_ajustada") + F.col("margem_contrib_ajustada_produto_isolada")
            )
            .otherwise(F.col("margem_contrib_ajustada"))
        )
        .withColumn(
            "dc_grpo_prdt",
            F.when(F.col("cd_indica_trans_prz_flexi") == 3, "MCaj ARV/RR") # RR
            .when(F.col("cd_grp_produto_servico").isin([3, 15, 24, 25, 26, 41, 45, 51, 54, 88]), "MCaj Aquisição") # CREDITO
            .when(F.col("cd_grp_produto_servico").isin([4, 5, 28, 29, 31, 32, 35, 42, 53]), "MCaj Aquisição") # DEBITO
            .when(F.col("cd_grp_produto_servico") == 10, "MCaj Aquisição") # VOUCHER AUTOMOTIVO
            .when(F.col("cd_grp_produto_servico") == 6, "MCaj ARV/RR") # ARV
            .otherwise("MCaj Outros")
        )
        .groupBy(["cd_mes", "nu_ec", "nu_so_ec", "dc_grpo_prdt"])
        .agg(
            F.sum(F.col("volume_capturado")).cast(DoubleType()).alias("vl_capr"),
            F.sum(F.col("margem_contrib_calculated")).cast(DoubleType()).alias("vl_mrgm_crbc"),
            F.sum(F.col("margem_contrib_ajustada_calculated")).cast(DoubleType()).alias("vl_mrgm_crbc_ajsd")
        )
        .orderBy("cd_mes", "nu_ec", "nu_so_ec", "dc_grpo_prdt")
    )

    # escreve o resultado para o disco
    du.write_by_name(df_mcaj, "MCAJ", "internal", partitions=["cd_mes"], mode="overwrite")

    return df_mcaj


def executa_mcaj(params: list) -> None:
    """
    Função wrapper para executar a base de Mcaj
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

    # executa o processamento
    processa_tbciar_re_mcaj(du, data_min, data_max)


if __name__ == "__main__":
    # cria a sessão do spark
    spark = (
        SparkSession.builder
        .appName("SPARK_prod_prazo_tbciar_re_mcaj")
        .enableHiveSupport()
        .getOrCreate()
    )
    sc = spark.sparkContext

    # adiciona pacotes de execução
    env = sys.argv[1] if len(sys.argv) > 0 else "PROD"
    if env == "PROD":
        sc.addFile("s3://cielo-sdlf-prd-sa-east-1-866812290529-artifactory/prod-prazo/py/balcao20/utils", recursive=True)
    elif env.startswith("USER_"):
        env, e_from, user = env.split("_")
        user = user.lower()
        sc.addFile(f"s3://cielo-sandbox-sa-east-1-107976507529-prod-prazo-sandbox/balcao2.0/{user}/scripts/utils", recursive=True)
    else:
        sc.addFile("s3://cielo-sandbox-sa-east-1-107976507529-prod-prazo-sandbox/balcao2.0/deploy/utils", recursive=True)

    # chama a execução da base
    executa_mcaj(sys.argv[1:])
