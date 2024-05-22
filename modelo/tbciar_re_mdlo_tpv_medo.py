import sys
import typing
from datetime import datetime
from datetime import timezone
from dateutil.relativedelta import relativedelta

import pyspark.sql.functions as F
import pyspark.sql.types as T
from pyspark.sql import SparkSession
from pyspark.sql.dataframe import DataFrame


def processa_tbciar_re_mdlo_tpv_medo(
    du: "DataUtils",
    tipo_peso: str,
    janela_apuracao_tpv: int,
    data_max: typing.Optional[str] = None,
) -> DataFrame:
    """
    Processa dados de transações diarias para gerar o volume transacionado médio numa janela escolhida

    Esta função realiza o processamento de dados de transações agregadas, calcula a media de TPV para a JANELA_APURACAO_TPV,
    e calcula o % do TPV que é antecipado por RR   

    :param du: instancia do data utils para ser usada para processar a base
    :param data_max: data máxima de chargeback
    :param tipo_peso (str): Métrica de ponderamento a ser aplicada em cada ponto da curva de TPV
    :param janela_apuracao_tpv (int): Quantidade de meses a serem considerados para calcular a media de TPV
    :return: Um DataFrame contendo as curvas de chargeback processadas.
    """
    from modelo.comuns import obtem_ultima_data_e_hora
    from utils.math import aplica_peso_a_dif_datas

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

    # calcula a data mínima a ser utilizada (data_max_dt - janela_apuracao)    
    dt_min_dt = data_max_dt - relativedelta(months=janela_apuracao_tpv)

    # cria as versões inteiras dos valores
    dt_min_int = int(dt_min_dt.strftime("%Y%m%d"))
    data_max_int = int(data_max_dt.strftime("%Y%m%d"))

    # seleciona a ultima data disponível da base de cluster
    id_cols = ["cd_grpo_cnpj", "cd_clsr", "nm_tipo_trns", "qt_prcl"]

    # Le os dados da TRNS_AG dos últimos X dias e calcula o valor médio transacionado
    # por grupo de CNPJ, além dos valores de RR realizados no período
    df_temp1_trns_media = (
        obtem_ultima_data_e_hora(du, data_max_int, "TRNS_CLUSTER")
        .filter((F.col("dt_trns") > dt_min_dt))
        .select(
            *(
                id_cols
                + [
                    "dt_trns", 
                    F.col("vl_sfra_pndr").alias("vl_sfra"), 
                    F.col("vl_rbrp_pndr").alias("vl_sfra_rbrp")
                ]
            )
        )
        .persist()
    )

    # calcula o valor médio transacionado por dia dado um peso para cada safra transacionada
    df_trns_media = (
        df_temp1_trns_media.select(id_cols)
        .distinct()
        .crossJoin(
            aplica_peso_a_dif_datas(
                du.spark.range(0, (int((data_max_dt - dt_min_dt).days)))
                .toDF("dias")
                .withColumn("dt_min", F.to_date(F.lit(str(dt_min_int)), "yyyyMMdd"))
                .withColumn("dt_trns", F.expr("date_add(dt_min, cast(dias as integer))")),
                "dt_min",
                "dt_trns",
                tipo_peso
            )
        )    
        .join(df_temp1_trns_media, on=id_cols + ["dt_trns"], how="left")
        .fillna(0)
        .withColumn("vl_sfra_pndr", F.col("vl_sfra") * F.col("peso"))
        .withColumn("vl_sfra_rbrp_pndr", F.col("vl_sfra_rbrp") * F.col("peso"))
        .groupBy(id_cols)
        .agg(
            F.sum("vl_sfra_pndr").cast(T.DoubleType()).alias("vl_sfra_pndr"),
            F.sum("vl_sfra_rbrp_pndr").cast(T.DoubleType()).alias("vl_sfra_rbrp_pndr"),
            F.sum(F.col("peso")).alias("peso"),
            F.date_add(F.max("dt_trns"), 1).alias("dt_trns")
        )
        .withColumn("vl_sfra_medo", F.col("vl_sfra_pndr") / F.col("peso"))
        .withColumn("vl_rbrp_medio", F.col("vl_sfra_rbrp_pndr") / F.col("peso"))
        .withColumn("pc_rbrp", F.col("vl_rbrp_medio") / F.col("vl_sfra_medo"))
        .select(id_cols + ["vl_sfra_medo", "pc_rbrp"])
        .withColumn("cd_dia_crte", F.lit(data_max_int))
        .withColumn("dh_crga", F.lit(datetime.now(timezone.utc).strftime("%Y%m%d%H%M%S")))
        .withColumn(
            "dc_prmt_exec", 
            F.lit(f"tipo_peso={tipo_peso};janela_apuracao_tpv={janela_apuracao_tpv}")
        )
    )

    return df_trns_media



def executa_mdlo_tpv_medo(params: list) -> None:
    """
    Função wrapper para executar a base tbciar_re_mdlo_tpv_medo

    :param params: parâmetros de execução
    """
    import modelo.configs as confs

    # pega os parâmetros de execução
    env = params[0] if len(params) > 0 else "PROD"
    data_max = params[1] if len(params) > 1 else None
    tipo_peso = params[2] if len(params) > 2 else str(confs.TIPO_PESO_TPV_MED)
    janela = params[3] if len(params) > 3 else str(confs.JANELA_APURACAO_TPV)

    # corrige nulos
    env = "PROD" if env == "" else env
    data_max = None if data_max == "" else data_max
    tipo_peso = confs.TIPO_PESO_TPV_MED if tipo_peso == "" else tipo_peso
    janela = confs.JANELA_APURACAO_TPV if janela == "" else int(janela)

    # instância o data utils
    from utils.data_utils import DataUtils
    du = DataUtils(env)

    # executa o processamento
    df = processa_tbciar_re_mdlo_tpv_medo(
        du=du, tipo_peso=tipo_peso, janela_apuracao_tpv=janela, data_max=data_max,
    )

    # escreve o resultado para o disco
    du.write_by_name(df, "TPV_MEDIO", "internal", partitions=["cd_dia_crte", "dh_crga"], mode="overwrite")


if __name__ == "__main__":
    # cria a sessão do spark
    spark = (
        SparkSession.builder
        .appName("SPARK_prod_prazo_tbciar_re_mdlo_tpv_medo")
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
    executa_mdlo_tpv_medo(sys.argv[1:])