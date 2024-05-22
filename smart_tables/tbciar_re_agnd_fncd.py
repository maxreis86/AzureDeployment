from itertools import chain
import sys
import typing
from datetime import datetime
from dateutil.relativedelta import relativedelta

import pyspark.sql.functions as F
from pyspark.sql import SparkSession
from pyspark.sql.dataframe import DataFrame


def processa_tbciar_re_agnd_fncd(
    du: "DataUtils",
    data_min: typing.Optional[str] = None,
    data_max: typing.Optional[str] = None,
) -> DataFrame:    
    """
    Processa e gera um DataFrame da agenda financiada, considerando um intervalo de datas.

    A função ajusta automaticamente as datas de início e fim de processamento baseando-se nas
    disponibilidades das tabelas 'AGENDA_FIN' e 'AGENDA'. Caso as datas não sejam fornecidas,
    a função utiliza a última data processada, a primeira partição da tabela 'AGENDA' ou uma
    data padrão (20230224) como mínimo, e a última partição da tabela 'AGENDA' como máximo.
    
    Durante o processamento, a função realiza uma série de operações para consolidar informações
    referentes ao valor total e financiado por CNPJ, bandeira, e data de vencimento, além de
    calcular métricas de financiamento e participação de mercado relacionadas à 'Cielo'.

    :param du: Instância de DataUtils utilizada para processar a base de dados.
    :param data_min: Data mínima de processamento, no formato 'aaaammdd'. Se nulo, a função calcula
                     automaticamente com base nas disponibilidades das tabelas.
    :param data_max: Data máxima de processamento, no formato 'aaaammdd'. Se nulo, utiliza a última
                     partição disponível na tabela 'AGENDA'.
    :return: Um DataFrame contendo as informações processadas, com detalhamento de valores totais e
             financiados, além de métricas específicas para análise.
    """

    from utils.renames import renomeia_para_padrao    
    from utils.dates import is_valid_date        

    # de-para dos códigos para nomes de bandeira dos dados de agenda
    ARRANJOS = {
        3: "MASTERCARD",
        4: "VISA",
        5: "DINERS",
        6: "AMEX",
        8: "ELO",
        10: "CABAL",
        11: "AGIPLAN",
        13: "BANESCARD",
        15: "CREDSYSTEM",
        19: "SOROCRED",
        21: "HIPERCARD",
        22: "AVISTA",
        23: "CREDZ",
        29: "HIPER",
        32: "ELO",
        33: "SODEXO",
        34: "VR BENEFICIOS",
        35: "POLICARD",
        36: "VALECARD",
        37: "GOOD CARD",
        38: "GREENCARD",
        40: "VEROCHEQUE",
    }
    mapping_bandeira = F.create_map([F.lit(x) for x in chain(*ARRANJOS.items())])
    
    # obtém a data mínima e data máxima
    if data_min is None:
        if du.is_table_available("AGENDA_FIN", "internal"):
            data_min = max([v["cd_dia_crga"] for v in du.get_partition_values("AGENDA_FIN", "internal")])
            data_min = (datetime.strptime(data_min, "%Y%m%d") + relativedelta(days=1)).strftime("%Y%m%d")
        else:
            data_min = min([v["dt_prtc"] for v in du.get_partition_values("AGENDA", "external")])
        #Esse limite de data minima e para o processamento do Control-M, quando a data_min nao e informada
        data_min = max(data_min, "20240424")
    # Esse limite da data minina e para limitar o historico quando o parameto data_min for informado
    data_min = max(data_min, "20230224")

    if data_max is None:
        data_max = max([v["dt_prtc"] for v in du.get_partition_values("AGENDA", "external")])

    # lista as partições a processar
    particoes_a_processar = sorted([
        v["dt_prtc"]
        for v in du.get_partition_values("AGENDA", "external")
        if (data_min <= v["dt_prtc"] <= data_max) and is_valid_date(v["dt_prtc"])
    ], reverse=True)
    if len(particoes_a_processar) == 0:
        return None
    
    # usar um loop para processar uma particao de cada vez, pois isso vai evitar estourar a memoria do cluster,
    for particao in particoes_a_processar:
        # calcula a data mínima para extrair os dados da tabela ARV_CIELO
        # Contar um ano antes da particao que sera processada
        particao_dt = datetime(int(particao[:4]), int(particao[4:6]), int(particao[6:]))
        particao_min = (particao_dt - relativedelta(years=1)).strftime("%Y%m%d")

        df_arv_cielo = (
            du.get_by_name("ARV_CIELO", "internal")
            .filter(
                (F.col("dt_cotr_prtc") >= particao_min)
                & (F.col("dt_cotr_prtc") < particao)
            )
        )

        # obtém os dados de agenda
        df = (
            du.get_by_name("AGENDA", "external")
            .filter(
                (F.col("dt_prtc") == int(particao))
                & (F.col("dt_vecto") >= F.col("dt_carga"))
            )            
            .select(
                F.col("dt_prtc").alias("cd_dia_crga"),
                F.col("cnpj_receb").alias("nu_cnpj_cpf_recr"),
                F.to_date(F.col("dt_vecto")).alias("dt_vecto"),
                "vlr_tot_ur",
                "vlr_tot_finan",
                "arranjo",
                (F.col("vlr_tot_finan") - F.col("vlr_cedido") - F.col("vlr_penhorado")).alias("vlr_gravamado"),
                mapping_bandeira[F.col("arranjo")].alias("nm_bandeira") 
            )
            .withColumn(
                "cd_bndr",
                F.when(F.col("arranjo").isin(3, 4), "MASTER/VISA")
                .when(F.col("arranjo") == 6, "AMEX")
                .when(F.col("arranjo") == 8, "ELO")        
                .otherwise("OUTRAS")
            )
        )

        # agrega os dados no nível de interesse
        df = (
            df.groupBy("nu_cnpj_cpf_recr", "cd_bndr", "nm_bandeira", "dt_vecto", "cd_dia_crga")
            .agg(
                F.sum("vlr_tot_ur").alias("vl_total"),
                F.sum("vlr_tot_finan").alias("vl_fncd"),
                F.sum("vlr_gravamado").alias("vlr_gravamado")
            )
        )

        # adiciona as informações de ARV cielo
        df = (
            renomeia_para_padrao(df).join(
                df_arv_cielo.groupBy("nu_cnpj_cpf_recr", "dt_vncm", "nm_bndr")
                .agg(F.sum("vl_arv_cilo").alias("vl_arv_cilo")),
                on=["nu_cnpj_cpf_recr", "dt_vncm", "nm_bndr"],
                how="left"
            )
            .fillna(subset=["vl_arv_cilo"], value=0)
            .groupBy("nu_cnpj_cpf_recr", "cd_bndr", "nm_bndr", "cd_dia_crga")
            .agg(
                F.sum("vl_totl").alias("vl_totl"),
                F.sum("vl_fncd").alias("vl_fncd"),
                F.sum("vlr_gravamado").alias("vl_grvm"),
                F.sum("vl_arv_cilo").alias("vl_arv_cilo")
            )
        )

        # adiciona as informações de ARV mercado
        df = (
            df.join(
                df_arv_cielo.filter(F.col("dt_vncm") >= particao_dt)
                .groupBy("nu_cnpj_cpf_recr", "nm_bndr")
                .agg(F.sum("vl_arv_mrcd").alias("vl_arv_mrcd")),
                on=["nu_cnpj_cpf_recr", "nm_bndr"],
                how="left"
            )
            .fillna(subset=["vl_arv_mrcd"], value=0)
        )

        # adiciona as métricas de interesse
        df = (
            df
            .groupBy("nu_cnpj_cpf_recr", "cd_bndr", "cd_dia_crga")
            .agg(
                F.sum("vl_totl").alias("vl_totl"),
                F.sum("vl_fncd").alias("vl_fncd"),
                F.sum("vl_grvm").alias("vl_grvm"),
                F.sum("vl_arv_cilo").alias("vl_arv_cilo"),
                F.sum("vl_arv_mrcd").alias("vl_arv_mrcd"),
            )
            .withColumn("pct_fncd", F.col("vl_fncd") / F.col("vl_totl"))
            .withColumn("pct_arv_cilo", F.when(F.col("vl_arv_cilo") > 0, F.col("vl_arv_cilo") / F.col("vl_totl")).otherwise(0))
            .withColumn(
                "razo_mrcd_cilo",
                F.when((F.col("vl_arv_cilo") == 0) & (F.col("vl_arv_mrcd") > 0), 1)
                .when(F.col("vl_arv_cilo") > 0, F.col("vl_arv_mrcd") / F.col("vl_arv_cilo")
                ).otherwise(0)
            )
            .withColumn("vl_fncd_cilo", F.col("vl_fncd") * F.col("pct_arv_cilo"))
            .withColumn("vl_fncd_mrcd", F.col("vl_fncd") * (F.lit(1) - F.col("pct_arv_cilo")))
        )
        
        # escreve o resultado para o disco
        # usar mode("overwrite") para sobreescrever a particao em caso de reprocessamento
        du.write_by_name(df, "AGENDA_FIN", "internal", partitions=["cd_dia_crga"], mode="overwrite")

        # Apos gravar a particao no disc, usar unpersist explicito
        # para liberar recursos do cluster para a proxima particao
        df.unpersist()

    return df


def executa_agnd_fncd(params: list) -> None:
    """
    Função wrapper para executar a base de agenda financiada

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
    processa_tbciar_re_agnd_fncd(du, data_min, data_max)


if __name__ == "__main__":
    # cria a sessão do spark
    spark = (
        SparkSession.builder
        .appName("SPARK_prod_prazo_tbciar_re_agnd_fncd")
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
    executa_agnd_fncd(sys.argv[1:])