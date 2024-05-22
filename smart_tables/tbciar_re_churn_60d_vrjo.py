import sys
import typing

from datetime import date, datetime
import pyspark.sql.functions as F
from pyspark.sql.dataframe import DataFrame
from pyspark.sql import SparkSession
from dateutil.relativedelta import relativedelta


def processa_tbciar_re_churn_60d_vrjo(
    du: "DataUtils", data_min: typing.Optional[str] = None, data_max: typing.Optional[str] = None
) -> DataFrame:
    """
    Esta funcao visa apurar o incremento desde a ultima carga de dados da base de churn.

    :param du: instancia do data utils para ser usada para processar a base
    :param data_min: parametro opcional da data de inicio a ser considerado, Usado para carga historica
    :param data_max: parametro opcional a ser usado para definir a maxima data a ser considerada neste processo
    :return: Este processo retorna um dataframe com os dados apurados (diferencao entre os dados ja gravados e os dados brutos para o periodo)
    """
    if data_min is None:
        # buscar o ultimo mes de pauracao e calcular qual o proximo mes.
        if du.is_table_available("BASE_CHURN", "internal"):
            mes_base_churn = int(max([v["cd_mes"] for v in du.get_partition_values("BASE_CHURN", "internal")]))
        else:
            mes_base_churn = 202312
        
        # calcula o mês mínimo para conseguir fazer os cálculos
        data_hist = date(mes_base_churn // 100, mes_base_churn % 100, 1)
        mes_base = data_hist - relativedelta(months=1)
        mes_base_calc = 100 * mes_base.year + mes_base.month
    else:
        # se o parametro foi informado, estamos fazendo carga historica e devemos buscar os dados a partir do mes e ano informado
        mes_base = datetime.strptime(data_min, "%Y%m%d") - relativedelta(months=1)
        mes_base_calc = 100 * mes_base.year + mes_base.month
        
    # adiciona um mês máximo baseado no fechamento do mês anterior
    if data_max is None:
        data_max = date.today() - relativedelta(months=1)
    else:
        data_max = datetime.strptime(data_max, "%Y%m%d")
    mes_max_int = 100 * data_max.year + data_max.month

    # pega as tabelas de referencia
    base_ec = du.get_by_name("EC", "external")

    df_new_raw_score_churn60 = (
        du.get_by_name("CHURN_60", "external")
        .select("nu_so_ec", "prob", "cd_mes")
        .filter((F.col("cd_mes") >= mes_base_calc) & (F.col("cd_mes") <= mes_max_int))
        .join(base_ec.select("nu_so_ec").distinct(), on="nu_so_ec", how="inner")
    )

    # Adiciona dois meses a data guardada em CD_MES, gerando a data prevista de churning
    df_tmp_score_churn60 = df_new_raw_score_churn60.withColumn(
        "tmp_data_de_churn", F.add_months(F.to_date(F.concat(F.col("cd_mes"), F.lit("01")), "yyyyMMdd"), 2)
    )
    df_tmp_score_churn60 = df_tmp_score_churn60.withColumn(
        "dt_churn", F.year(F.col("tmp_data_de_churn"))*100 + F.month(F.col("tmp_data_de_churn"))
    )

    # montar o dataframe final com a nova coluna formatada
    df_apurado_score_churn60 = df_tmp_score_churn60.select("nu_so_ec", "dt_churn", "prob", "cd_mes").withColumnRenamed("prob", "pc_prbl_churn")

    return df_apurado_score_churn60


def executa_churn_60d_vrjo(params: list) -> None:
    """
    Função wrapper para executar a base de Churn 60 dias Varejo

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
    df = processa_tbciar_re_churn_60d_vrjo(du, data_min, data_max)

    # escreve o resultado para o disco
    du.write_by_name(df, "BASE_CHURN", "internal", partitions="cd_mes", mode="overwrite")


if __name__ == "__main__":
    # cria a sessão do spark
    spark = (
        SparkSession.builder
        .appName("SPARK_prod_prazo_tbciar_re_churn_60d_vrjo")
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
    executa_churn_60d_vrjo(sys.argv[1:])