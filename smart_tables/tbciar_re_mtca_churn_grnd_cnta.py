import sys
import typing
from datetime import datetime
from dateutil.relativedelta import relativedelta

import pyspark.sql.functions as F
from pyspark.sql import SparkSession
from pyspark.sql.dataframe import DataFrame
from pyspark.sql.window import Window


def processa_tbciar_re_mtca_churn_grnd_cnta(
    du: "DataUtils",
    data_min: typing.Optional[str] = None,
    data_max: typing.Optional[str] = None,    
    id_grupo: str = "nu_so_ec",
    num_percentiles: int = 4,
    tipo_valor: str = "brto",
)  -> DataFrame:
    """
    Essa funcao processa a tabela de metricas de churn grandes contas.
    Essa tabela é composta pelas seguintes variaveis que serao usadas como entrada no modelo de churn grandes contas:
        -nm_tipo_ec_ecom
        -vl_ftrm
        -nu_mes_atvo
        -vl_ftrm_m1
        -vl_vrco_ftrm
        -dc_faxa_ftrm
        -dc_faxa_vrco_ftrm
        -dc_faxa_mes_atvo


    E possivel informar a data maxima e data minima para forcar a execucao de um periodo especifico.
    Pode-se tambem informar apenas a data maxima, e serao processados todos os meses desde a ultima data processada (ou data limite "20220201") ate a data maxima informada.
    E possivel tambem informar apenas a data minima e a data maxima sera o ultimo mes disponivel para processamento.
    Se nenhuma data for informada, o processamento sera feito para todos os meses disponiveis contanto a partir do ultimo mes processado ou da data minima de corte.

    :param du: instancia do data utils para ser usada para processar a base
    :param data_min: data com o primeiro mes a ser processado
    :param data_max: data com o ultimo mes a ser processado
    :param id_grupo: Variavel que define a granularidade do modelo de churn grandes contas (nu_so_ec ou nu_cnpj_cpf_recr)
    :param num_percentiles: numero de agrupamentos dos percentis das variaveis continuos para o modelo de churn grandes contas
    :param tipo_valor: tipo de valor para utilizar no calculo (brto, lqdo)    
    :return: data frame processado
    """

    from utils.renames import renomeia_para_padrao

    data_limite = "20220201" # Data minima de corte para processamento

    # Se data maxima nao for informada, pegar a data maxima disponivel na base TRNS_AG
    if data_max is None:
        data_max = max([v["cd_dh_incl_rgst"] for v in du.get_partition_values("TRNS_AG", "internal")])
    # Se a data minina nao foi informada, vamos verificar primeiro qual foi o ultimo processamento da base MET_CHURN e somar +1 mes
    if data_min is None:
        if du.is_table_available("MET_CHURN", "internal"):
            data_min = max([v["cd_mes"] for v in du.get_partition_values("MET_CHURN", "internal")])
            data_min = (datetime.strptime(data_min, "%Y%m") + relativedelta(months=1)).strftime("%Y%m%d")
        # Se a tabela MET_CHURN ainda nao existe, vamos pegar a data minima de corte, desde que existam dados disponiveis nessa data
        else:
            data_min = min([v["cd_dh_incl_rgst"] for v in du.get_partition_values("TRNS_AG", "internal")])
        data_min = max(data_min, data_limite)

    # Vamos calcular qual o ultimo mes fechado disponivel para processamento
    # Por exemplo, se a data_max for 20220225, o ultimo mes fechado disponivel para processamento sera 202201
    # Mas se data_max for 20220430, entao o ultimo mes fechado disponivel para processamento sera 202204
    cd_mes_max = (datetime.strptime(data_max, "%Y%m%d") + relativedelta(days=1) - relativedelta(months=1)).strftime("%Y%m")

    # Precisamos calcular tambem qual o primeiro mes fechado disponivel para processamento (um mes com todos os dias disponiveis para processamento)
    # Por exemplo, se a data_min for 20220202, o primeiro mes fechado disponivel para processamento sera 202203
    # Porem, se a data_min for 20220201, entao o primeiro mes fechado disponivel para processamento sera 202202
    cd_mes_min = (datetime.strptime(data_min, "%Y%m%d") + relativedelta(months=1) - relativedelta(days=1)).strftime("%Y%m")

    # A data cd_mes_min representa o primeiro mes fechado que sera processado, mas para processar esse mes, nos precisamos de dois meses fechados disponiveis
    # Entao precisamos criar uma variavel cd_mes_min_corte que sera a data de inicio dos dados que serao lidos para processamento
    cd_mes_min_corte = (datetime.strptime(data_min, "%Y%m%d") + relativedelta(months=0) - relativedelta(days=1)).strftime("%Y%m")

    # Data minima e maxima em formato datetime com primeiro e ultimo dia do mes
    data_min_corte_dt = datetime.strptime(cd_mes_min_corte, "%Y%m")
    data_min_dt = datetime.strptime(cd_mes_min, "%Y%m")
    data_max_dt = datetime.strptime(cd_mes_max, "%Y%m") + relativedelta(months=1) - relativedelta(days=1)

    # cria as versões inteiras dos valores
    data_min_corte_int = int(data_min_corte_dt.strftime("%Y%m%d"))
    data_min_int = int(data_min_dt.strftime("%Y%m%d"))
    data_max_int = int(data_max_dt.strftime("%Y%m%d"))

    if data_min_int < int(data_limite):
        raise ValueError(f"Nao ha dados suficientes para processar o mes {cd_mes_min}, A data limite e {data_limite} e sao necessarios dois meses. Abortando o processo.")

    if cd_mes_min > cd_mes_max:
        raise ValueError(f"O mes de inicio ({cd_mes_min}) precisa ser menor ou igual ao mes de fim ({cd_mes_max}). Abortando o processo.")

    # Selecionar o vl_faturamento da base TRNS_AG para pegar somente transacoes de venda
    df_fat_mensal = (
        du.get_by_name("TRNS_AG", "internal")
        .filter(
            (F.col("cd_dh_incl_rgst") >= data_min_corte_int)
            & (F.col("cd_dh_incl_rgst") <= data_max_int)
            & (F.col("dt_trns") > data_min_corte_dt)
        )
        .join(du.get_by_name("EC", "internal").select("nu_ec", "nu_so_ec"), on="nu_ec", how="inner")
        .select(id_grupo, "dt_trns", f"vl_{tipo_valor}_agnd_trns")
        .withColumn("cd_mes", F.date_format("dt_trns", "yyyyMM"))
        .groupBy(id_grupo, "cd_mes")
        .agg(F.sum(f"vl_{tipo_valor}_agnd_trns").alias("vl_faturamento"))
    )
        
    df = (
        df_fat_mensal.join(
            du.get_by_name("EC", "internal")
            .filter(F.col("nm_unde_ngco_fpna") == "GRANDES CONTAS")
            .select(
                "nu_ec",
                "nu_so_ec",
                "nu_cnpj_cpf_recr",
                "dt_aflc_ec",
                "in_ecommerce"
            ),
            on="nu_so_ec",
            how="inner"
        )
        .withColumn("nm_tipo_ec_ecom", F.when(F.col("in_ecommerce") == 1, F.lit("E-COMMERCE")).otherwise("NÃO E-COMMERCE"))
        .withColumn("dt_mes", F.last_day(F.to_date(F.col("cd_mes").cast("string"), "yyyyMM")))
        .withColumn("meses_ativo", F.months_between(F.col("dt_mes"), F.col("dt_aflc_ec")))
        .groupBy(id_grupo, "cd_mes", "nm_tipo_ec_ecom")
        .agg(
            F.sum("vl_faturamento").alias("vl_faturamento"),
            F.max("meses_ativo").alias("meses_ativo") # Se o agrupamento for por CNPJ, meses ativos sera do EC mais antigo: max(meses_ativo)
        )
        .withColumn("vl_faturamento_M1", F.lag("vl_faturamento").over(Window.partitionBy(id_grupo).orderBy("cd_mes")))
        .withColumn(
            "vl_faturamento_M1",
            F.when(
                F.col("vl_faturamento_M1").isNull(),
                float(
                    df_fat_mensal
                    .filter(
                        (F.col("cd_mes") >= int(cd_mes_min))
                    )
                    .select(F.mean(F.col("vl_faturamento")).alias('mean'))
                    .collect()[0]["mean"]
                    or 0.0
                ) # Se o EC/CNPJ nao teve faturamento no mes anteior (M-1), substituir por media do faturamento da base, ou zero se nao for possivel calcular a media
            ).otherwise(F.col("vl_faturamento_M1"))
        )
        .withColumn(
            "variacao_fat",
            F.when(F.col("vl_faturamento_M1") == 0, 0.0).otherwise(
                (F.col("vl_faturamento") - F.col("vl_faturamento_M1")) / F.col("vl_faturamento_M1")
            )
        )
        .filter(F.col("cd_mes") >= int(cd_mes_min)) # Filtrar somente os dados que devem ser gravados na base final, excluindo o mes que foi usado apara para calcular a variacao_fat
        .withColumn("faixa_faturamento", F.ntile(num_percentiles).over(Window.partitionBy("cd_mes").orderBy("vl_faturamento")))
        .withColumn("faixa_var_fat", F.ntile(num_percentiles).over(Window.partitionBy("cd_mes").orderBy("variacao_fat")))
        .withColumn("faixa_meses_ativo", F.ntile(num_percentiles).over(Window.partitionBy("cd_mes").orderBy("meses_ativo")))
    )

    return renomeia_para_padrao(df)

def executa_mtca_churn_grnd_cnta(params: list) -> None:
    """
    Função wrapper para executar a base de Metricas Churn Grandes Contas

    :param params: parâmetros de execução
    """
    # pega os parâmetros de execução
    env = params[0] if len(params) > 0 else "PROD"
    data_min = params[1] if len(params) > 1 else None
    data_max = params[2] if len(params) > 2 else None
    id_grupo = params[3] if len(params) > 3 else "nu_so_ec"
    num_percentiles = params[4] if len(params) > 4 else 4
    tipo_valor = params[5] if len(params) > 5 else "brto"

    # corrige nulos
    env = "PROD" if env == "" else env
    data_min = None if data_min == "" else data_min
    data_max = None if data_max == "" else data_max

    # instância o data utils
    from utils.data_utils import DataUtils
    du = DataUtils(env)

    # executa o processamento
    df = processa_tbciar_re_mtca_churn_grnd_cnta(
        du=du,
        data_min=data_min,
        data_max=data_max,
        id_grupo=id_grupo,
        num_percentiles=num_percentiles,
        tipo_valor=tipo_valor,
    )

    # escreve o resultado para o disco
    du.write_by_name(df, "MET_CHURN", "internal", partitions=["cd_mes"], mode="overwrite")


if __name__ == "__main__":
    # cria a sessão do spark
    spark = (
        SparkSession.builder
        .appName("SPARK_prod_prazo_tbciar_re_mtca_churn_grnd_cnta")
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
    executa_mtca_churn_grnd_cnta(sys.argv[1:])