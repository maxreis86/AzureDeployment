import sys
import typing
from datetime import datetime, timezone

import numpy as np
import pyspark.sql.functions as F
import pyspark.sql.types as T
from pyspark.sql import SparkSession
from pyspark.sql.dataframe import DataFrame
from pyspark.sql.window import Window
from scipy.optimize import curve_fit


def arctan_func(x, a, b):
    return a * np.arctan(b * x)


def curve_fit_udf(xdata, ydata):
    """
    Logica para realizar o fit para estimar todos os parametros da funcao de projecao para cada segmento
    """
    # Converter para numpy arrays
    xdata = np.array(xdata)
    ydata = np.array(ydata)
    
    popt, _ = curve_fit(arctan_func, xdata, ydata, maxfev=5000)

    return popt.tolist()
curve_fit_udf_spark = F.udf(curve_fit_udf, T.ArrayType(T.DoubleType()))


def calcular_projecoes(params, proj_dias: int = 540):
    x_projection = np.arange(0, proj_dias + 1)
    y_projection = arctan_func(x_projection, *params)
    return [float(y) for y in y_projection]
calcular_projecoes_udf = F.udf(calcular_projecoes)


def processa_tbciar_re_mdlo_crva_ajst(
    du: "DataUtils",
    data_max: typing.Union[str, None],
    dias_corte: int,
    proj_dias: int,
    fator_ext: float
) -> DataFrame:
    """
    Processa uma tabela de curvas de chargeback para gerar projeções futuras.

    Esta função lê uma tabela de entrada contendo curvas de chargeback por cluster pegando sempre a ultima partição,
    aplica uma função de ajuste de curva para obter parâmetros de projeção e,
    com base nesses parâmetros, gera projeções de chargeback para períodos futuros.
    As projeções são então armazenadas em uma tabela de saída especificada e também
    no caminho do S3 fornecido.

    :param du: instancia do data utils para ser usada para processar a base
    :param dias_corte: dias em que se considera que não há chargeback no começo da curva
    :param proj_dias: número de dias para projeção da curva arctangente
    :param fator_ext: fator a ser aplicado para extender a cauda da curva
    :return: Um DataFrame contendo as projeções de chargeback, incluindo o cluster, o percentual projetado de chargeback
    e o incremento diário, para cada dia desde a venda.
    """
    from modelo.comuns import obtem_ultima_data_e_hora

    # obtém os valores de datas mínimas e máximas que deveriam ser utilizados
    if data_max is None:
        data_max = max([v["cd_dia_crte"] for v in du.get_partition_values("GRUPO_CNPJ", "internal")])
    else:
        data_max = max([
            v["cd_dia_crte"] 
            for v in du.get_partition_values("GRUPO_CNPJ", "internal") 
            if v["cd_dia_crte"] <= data_max
        ])

    # carrega as curvas que foram construídas de chargeback e cancelamento
    df_curvas = (
        obtem_ultima_data_e_hora(du, int(data_max), "CURVA_FIT")
        .withColumn(
            "max_dvenda",
            F.max("nu_dia_vnda").over(Window.partitionBy("cd_clsr"))
        )
        .drop("cd_dia_crte")
    )

    outputs = dict()
    windowSpec = Window.partitionBy("cd_clsr").orderBy("cd_clsr")
    window_spec_2 = Window.partitionBy("cd_clsr").orderBy("nu_dia_vnda")
    for s in ["chrg", "cncl"]:
        # Ler base com curvas de chargback por segmento observada
        # Estimar todos os parametros da funcao para cada segmento
        df_params_segmento = (
            df_curvas.filter(
                (F.col("nu_dia_vnda") >= dias_corte)
                & (
                    (F.col("cd_clsr").contains(" | "))
                    | (F.col("max_dvenda") < 540)
                )
            )
            .groupBy("cd_clsr")
            .agg(
                curve_fit_udf_spark(
                    F.collect_list("nu_dia_vnda"),
                    F.collect_list(f"pc_{s}")
                ).alias(f"params_{s}"),
            )
        )

        # Calcular projecoes para {proj_dias} dias
        df_projecoes_segmento = (
            df_params_segmento.withColumn(
                f"proj_{s}", calcular_projecoes_udf(F.col(f"params_{s}"), F.lit(proj_dias))
            )
            .withColumn(f"proj_{s}", F.from_json(F.col(f"proj_{s}"), T.ArrayType(T.DoubleType())))
            .select("cd_clsr", F.explode(F.col(f"proj_{s}")))
            .withColumnRenamed("col", f"pc_{s}_prjt")
            .withColumn("nu_dia_vnda", (F.row_number().over(windowSpec) - 1) + F.lit(dias_corte))
        )

        # calcula o valor incremental dia a dia
        df_projecoes_segmento = (
            df_projecoes_segmento.withColumn(
                f"pc_{s}_prjt", 
                F.when(
                    (F.col(f"pc_{s}_prjt") > -1e-19) & (F.col(f"pc_{s}_prjt") < 1e-19),
                    0
                ).otherwise(F.col(f"pc_{s}_prjt"))
            )
            .withColumn(
                f"icmo_{s}",
                F.col(f"pc_{s}_prjt") - 
                F.lag(F.col(f"pc_{s}_prjt"), 1).over(window_spec_2)
            )
            .fillna(0)
        )

        # adiciona a cauda da curva
        outputs[s] = (
            df_params_segmento.select("cd_clsr").distinct() 
            .select("*", F.explode(F.array([F.lit(i) for i in range(541)])).alias("nu_dia_vnda"))
            .join(df_projecoes_segmento, on=["cd_clsr", "nu_dia_vnda"], how="left")
            .withColumn(
                f"icmo_{s}", 
                F.last(F.col(f"icmo_{s}"), ignorenulls=True)
                .over(window_spec_2.rowsBetween(Window.unboundedPreceding, Window.currentRow))
            )
            .withColumn(
                f"icmo_{s}", 
                F.when(F.col("nu_dia_vnda") <= dias_corte, F.lit(0))
                .when(F.col("nu_dia_vnda") <= proj_dias, F.col(f"icmo_{s}"))
                .otherwise(
                    F.col(f"icmo_{s}") 
                    * F.pow(F.lit(fator_ext), F.col("nu_dia_vnda") - F.lit(proj_dias))
                )
            )
            .withColumn(f"pc_{s}_prjt", F.sum(f"icmo_{s}").over(window_spec_2))
        )

    # calcula o incremento diário nos clientes projetados de forma individual
    df_gc = (
        df_curvas.filter(
            (~F.col("cd_clsr").contains(" | "))
            & (F.col("max_dvenda") == 540)
        )
        .withColumn(
            "icmo_chrg", 
            F.col("pc_chrg") - 
            F.lag(F.col("pc_chrg"), 1).over(window_spec_2)
        )
        .withColumn(
            "icmo_cncl", 
            F.col("pc_cncl") - 
            F.lag(F.col("pc_cncl"), 1).over(window_spec_2)
        )
        .withColumnRenamed("pc_chrg", "pc_chrg_prjt")
        .withColumnRenamed("pc_cncl", "pc_cncl_prjt")
        .fillna(0)
        .drop("max_dvenda", "dc_prmt_exec", "dh_crga")
    )
    
    # junta os dados numa base única
    df_curva_proj = (
        outputs["chrg"].withColumn("pc_cncl_prjt", F.lit(0))
        .withColumn("icmo_cncl", F.lit(0))
        .unionByName(
            outputs["cncl"].withColumn("pc_chrg_prjt", F.lit(0))
            .withColumn("icmo_chrg", F.lit(0))
        )
        .unionByName(df_gc)
        .groupBy("cd_clsr", "nu_dia_vnda")
        .agg(
            F.sum("pc_chrg_prjt").alias("pc_chrg_prjt"),
            F.sum("icmo_chrg").alias("icmo_chrg"),
            F.sum("pc_cncl_prjt").alias("pc_cncl_prjt"),
            F.sum("icmo_cncl").alias("icmo_cncl"),
        )        
        .withColumn("cd_dia_crte", F.lit(int(data_max)))
        .withColumn("dh_crga", F.lit(datetime.now(timezone.utc).strftime("%Y%m%d%H%M%S")))
        .withColumn(
            "dc_prmt_exec", F.concat(
                F.lit("dias_corte="), F.lit(dias_corte),
                F.lit(";proj_dias="), F.lit(proj_dias),
                F.lit(";fator_ext="), F.lit(fator_ext)
            )
        )
    )
    
    return df_curva_proj

def executa_mdlo_crva_ajst(params: list) -> None:
    """
    Função wrapper para executar a base tbciar_re_mdlo_crva_ajst

    :param params: parâmetros de execução
    """
    import modelo.configs as confs

    # pega os parâmetros de execução
    env = params[0] if len(params) > 0 else "PROD"
    data_max = params[1] if len(params) > 1 else None
    dias_scgb = params[2] if len(params) > 2 else str(confs.DIAS_SEM_CGB)
    dias_psmooth = params[3] if len(params) > 3 else str(confs.DIAS_PROJ_SMOOTH)
    fator_dsmooth = params[4] if len(params) > 4 else str(confs.FATOR_DECAI_SMOOTH)

    # corrige nulos
    env = "PROD" if env == "" else env
    data_max = None if data_max == "" else data_max
    dias_scgb = confs.DIAS_SEM_CGB if dias_scgb == "" else int(dias_scgb)
    dias_psmooth = confs.DIAS_PROJ_SMOOTH if dias_psmooth == "" else int(dias_psmooth)
    fator_dsmooth = confs.FATOR_DECAI_SMOOTH if fator_dsmooth == "" else float(fator_dsmooth)

    # instância o data utils
    from utils.data_utils import DataUtils
    du = DataUtils(env)

    # executa o processamento
    df = processa_tbciar_re_mdlo_crva_ajst(
        du=du,
        data_max=data_max,
        dias_corte=confs.DIAS_SEM_CGB,
        proj_dias=confs.DIAS_PROJ_SMOOTH,
        fator_ext=confs.FATOR_DECAI_SMOOTH,
    )

    # escreve o resultado para o disco
    du.write_by_name(df, "CURVA_SMOOTH", "internal", partitions=["cd_dia_crte", "dh_crga"], mode="overwrite")


if __name__ == "__main__":
    # instancia a sessão do spark
    spark = (
        SparkSession.builder.appName("SPARK_prod_prazo_tbciar_re_mdlo_crva_ajst")
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
    executa_mdlo_crva_ajst(sys.argv[1:])