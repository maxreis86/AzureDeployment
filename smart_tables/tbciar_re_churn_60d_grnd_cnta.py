import sys
import typing
from datetime import datetime
from dateutil.relativedelta import relativedelta

import pyspark.sql.functions as F
from pyspark.sql import SparkSession
from pyspark.sql.dataframe import DataFrame

def processa_tbciar_re_churn_60d_grnd_cnta(
    du: "DataUtils",
    cd_mes_max: typing.Optional[str] = None,
    id_grupo: typing.Optional[str] = "nu_so_ec",
    meses_churn: typing.Optional[int] = 4,
    meses_historico_metricas: typing.Optional[int] = 18,
    var_churn: typing.Optional[str] = "dc_clsf_60d",
)  -> DataFrame:
    """
    Essa funcao processa a tabela tbciar_churn_60d_gc.
    Essa tabela tem como entrada a base ativa e a base de metricas de churn.
    Para cada grupo de dc_faxa_ftrm, dc_faxa_vrco_ftrm e dc_faxa_mes_atvo, sera calculada a probabilidade de churn.
    Essa probabilidade sera calculada como a media de churn dos ultimos X meses, onde X sera o parametro meses_historico_metricas.
    Cada EC/CNPJ tera uma probabilidade de churn final de acordo com seu grupo de dc_faxa_ftrm, dc_faxa_vrco_ftrm e dc_faxa_mes_atvo.

    :param du: instancia do data utils para ser usada para processar a base
    :param cd_mes_max: data com o mes a ser processado
    :param id_grupo: Variavel que define a granularidade do modelo de churn grandes contas (nu_so_ec ou nu_cnpj_cpf_recr)
    :param meses_churn: quantidade de meses para observar o churn
    :param meses_historico_metricas: quantidade de meses de historico para observar o churn por grupo e calcular uma probabilidade media por grupo
    :param var_churn: nome da variavel com o tipo de churn que sera usado (dc_clsf_30d, dc_clsf_60d ou dc_clsf_90d)
    :return: data frame processado
    """
    
    from utils.renames import renomeia_para_padrao
    
    data_limite = "202202" # Data minima de corte para processamento

    # Se data maxima nao for informada, pegar a data maxima disponivel na base MET_CHURN
    if cd_mes_max is None:
        cd_mes_max = max([v["cd_mes"] for v in du.get_partition_values("MET_CHURN", "internal")])
    else:
        cd_mes_max = cd_mes_max[0:6] # Mesmo se for informado uma data com dia, vamos pegar somente o mes e ano

    # Precisamos reduzir X (meses_churn) meses da data maxima. Para ter o periodo minimo de obervacao do churn
    # Por exemplo, se a base de metricas tem a ultima informacao de 202312, entao so podemos observar o churn ate 202308 se meses_churn=4
    cd_mes_max_prob = (datetime.strptime(cd_mes_max, "%Y%m") - relativedelta(months=meses_churn)).strftime("%Y%m")

    # A cd_mes_max sera x meses antes da id_grupo, de acordo com o valor de meses_historico_metricas
    cd_mes_min = (datetime.strptime(cd_mes_max, "%Y%m") - relativedelta(months=meses_historico_metricas)).strftime("%Y%m")

    if int(cd_mes_min) < int(data_limite):
        raise ValueError(
            f"Nao ha dados suficientes para processar o mes {cd_mes_min}, "
            f"A data limite e {data_limite} e sao necessarios {meses_historico_metricas} meses. " 
            f"Abortando o processo."
        )
        
    # obtém os dados da base ativa
    df_base_ativa = (
        du.get_by_name("BASE_ATIVA", "internal")
        .select(id_grupo, var_churn, "cd_mes",)
    )
    
    # obtém os dados de métricas de churn
    df_metricas_churn = (
        du.get_by_name("MET_CHURN", "internal")    
        .select(
            id_grupo,
            "dc_faxa_ftrm",
            "dc_faxa_vrco_ftrm",
            "dc_faxa_mes_atvo",
            "cd_mes"
        )
    )
    
    df_prob_churn = (
        df_metricas_churn.filter(
            (F.col("cd_mes") >= cd_mes_min)
            & (F.col("cd_mes") <= cd_mes_max_prob)
        )
        # Vamos considerar o periodo X meses de historico para observar o churn por grupo
        # e calcular uma probabilidade media por grupo
        .join(
            df_base_ativa.filter(F.col(var_churn) != "INATIVO")
            .withColumnRenamed(var_churn, var_churn+"_atual"),
            on=[id_grupo, "cd_mes"],
            how="inner"
        ) 
        # Cruzar com a base ativa pelo mes atual
        # para definir se o cliente estava ativo no mes para o qual queremos calcular a probabilidade de churn
        .join(
            df_base_ativa.withColumnRenamed("cd_mes", "cd_mes_churn")
            # Subrtraimos a quantidade de meses que queremos observar o churn
            .withColumn("cd_mes", F.expr(f"date_format(add_months(to_date(cast(cd_mes_churn as string), 'yyyyMM'), - {meses_churn}), 'yyyyMM')")),
            on=[id_grupo, "cd_mes"],
            how="left"
        ) 
        # Cruzar novamente com a base ativa, mas agora para o mes que queremos observar o churn
        # Entao, realizamos o join do cd_mes com o cd_mes_churn 
        # (que foi subtraido a quantidade de meses que queremos observar o churn)
        .withColumn("flag_churn", F.when(F.col(var_churn) == "INATIVO", 1).otherwise(0))
        .groupBy(
            "dc_faxa_ftrm", 
            "dc_faxa_vrco_ftrm", 
            "dc_faxa_mes_atvo"
        )
        .agg(F.mean("flag_churn").alias("pc_prbl_churn"))
        .select("dc_faxa_ftrm", "dc_faxa_vrco_ftrm", "dc_faxa_mes_atvo", "pc_prbl_churn")
    )
    
    df_base_ativa_churn = (
        df_base_ativa.filter(
            (F.col(var_churn) != "INATIVO")
            & (F.col("cd_mes") == cd_mes_max)
        ) 
        # Pegar da base ativa apenas os clientes ativos no mes que queremos calcular a probabilidade de churn
        .join(
            df_metricas_churn,
            on=[id_grupo, "cd_mes"],
            how="inner"
        ) 
        # Cruzar com a base de metricas de churn para pegar os grupos de dc_faxa_ftrm, dc_faxa_vrco_ftrm e dc_faxa_mes_atvo, 
        # que serao utilizados para calcular a probabilidade de churn
        .join(
            df_prob_churn,
            on=["dc_faxa_ftrm", "dc_faxa_vrco_ftrm", "dc_faxa_mes_atvo"],
            how="inner"
        )
        .select(id_grupo, "cd_mes", "pc_prbl_churn")
    )
    
    df_base_ativa_churn = renomeia_para_padrao(df_base_ativa_churn)
    
    return df_base_ativa_churn


def executa_churn_60d_grnd_cnta(params: list) -> None:
    """
    Função wrapper para executar a base de Churn 60 dias

    :param params: parâmetros de execução
    """
    # pega os parâmetros de execução
    env = params[0] if len(params) > 0 else "PROD"
    cd_mes_max = params[1] if len(params) > 1 else None
    id_grupo = params[2] if len(params) > 2 else "nu_so_ec"
    meses_churn = params[3] if len(params) > 3 else 4
    meses_historico_metricas = params[4] if len(params) > 4 else 18
    var_churn = params[5] if len(params) > 5 else "dc_clsf_60d"
    
    # corrige nulos
    env = "PROD" if env == "" else env
    cd_mes_max = None if cd_mes_max == "" else cd_mes_max
    id_grupo = "nu_so_ec" if id_grupo == "" else id_grupo
    meses_churn = 4 if meses_churn == "" else meses_churn
    meses_historico_metricas = 18 if meses_historico_metricas == "" else meses_historico_metricas
    var_churn = "dc_clsf_60d" if var_churn == "" else var_churn
    
    # instância o data utils
    from utils.data_utils import DataUtils
    du = DataUtils(env)

    # executa o processamento
    df = processa_tbciar_re_churn_60d_grnd_cnta(du, cd_mes_max, id_grupo, meses_churn, meses_historico_metricas, var_churn)

    # escreve o resultado para o disco
    du.write_by_name(df, "CHURN_GC", "internal", partitions=["cd_mes"], mode="overwrite")


if __name__ == "__main__":
    # cria a sessão do spark
    spark = (
        SparkSession.builder
        .appName("SPARK_prod_prazo_tbciar_re_churn_60d_grnd_cnta")
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
    executa_churn_60d_grnd_cnta(sys.argv[1:])