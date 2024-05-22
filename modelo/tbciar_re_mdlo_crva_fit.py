import sys
import typing
from datetime import datetime, timezone
from dateutil.relativedelta import relativedelta

import pyspark.sql.functions as F
from pyspark.sql import SparkSession
from pyspark.sql.types import DoubleType
from pyspark.sql.dataframe import DataFrame
from pyspark.sql.window import Window


def gera_curva_evento(
    du: "DataUtils",
    df_trns: DataFrame,
    lista_cf: list,
    datas: typing.Tuple[int, int, datetime, datetime],
    dias_cruzamento: int,
    tipo_valor: str,
    tipo_peso: str,
    base: str = "CHARGEBACK",    
    suffixo: str = "chrg"
) -> DataFrame:
    """
    Dada uma base (CHARGEBACK / CANCELAMENTO) obtém as curvas cumulativas do evento
    por safra de transação associado aos parâmetros de geração das curvas

    :param du: instancia do data utils para ser usada para processar a base
    :param lista_cf: lista de clientes que deve ser considerada como seu próprio cluster
    :param df_trns: base de safras transacionadas por cluster
    :param datas: lista de data minima e maxima como inteiros e datetimes
    :param dias_cruzamento: Número de dias para o qual cruzamos dados de chargeback/cancelamento
    :param tipo_valor: Tipo de valor para fazer o calculo
    :param tipo_peso: Métrica de ponderamento a ser aplicada em cada ponto de curva de chargeback/cancelamento
    :param base: nome da base de dados a ser carregada    
    :param suffixo: suffixo a ser aplicado nos nomes (chrg/cncl)
    :return: Um DataFrame contendo as curvas de chargeback/cancelamento processadas.
    """
    from modelo.comuns import classifica_em_grupos
    from utils.math import aplica_peso_a_dif_datas

    # separa as datas
    data_min_int, data_max_int, data_min_dt, data_max_dt = datas

    # filtra a base de TRNS para o intervalo de data adequado
    df_trns = df_trns.filter(
        (
            (F.col("cd_clsr").contains(" | "))
            & (F.col("nu_dia_vnda") <= dias_cruzamento)
        ) | (~F.col("cd_clsr").contains(" | "))
    )

    # Calcula o volume de eventos que ocorreram desde a venda
    df_vlr = (
        classifica_em_grupos(
            df=du.get_by_name(base, "internal")
            .filter(
                (F.col("cd_dh_incl_rgst") >= data_min_int)
                & (F.col("cd_dh_incl_rgst") <= data_max_int)
                & (F.col(f"dt_{suffixo}") >= data_min_dt)
                & (
                    (
                        ~(
                            (F.col("nm_cdfr").isin(lista_cf))
                            | (F.col("nu_cnpj9_cpf").cast("string").isin(lista_cf))
                        )
                        & (F.col("nu_dia_vnda") <= dias_cruzamento)
                    ) | (
                        (F.col("nm_cdfr").isin(lista_cf))
                        | (F.col("nu_cnpj9_cpf").cast("string").isin(lista_cf))
                    )
                )
            )
            .withColumn(
                "nm_grpo", 
                F.when(
                    F.col("nm_cdfr") == "NAO SE APLICA", 
                    F.col("nu_cnpj9_cpf").cast("string")
                )
                .otherwise(F.col("nm_cdfr"))
            ),
            lista_cf=lista_cf,
        )
        .groupBy("cd_clsr", "dt_trns", "nu_dia_vnda")
        .agg(F.abs(F.sum(f"vl_{tipo_valor}_{suffixo}")).cast(DoubleType()).alias(f"vl_{suffixo}"))
        .withColumn(f"vl_{suffixo}", F.abs(F.col(f"vl_{suffixo}")))
    )

    # calcula o valor das curvas ponderando por dias da transação
    data_min_dt_all = data_min_dt - relativedelta(days=dias_cruzamento)
    data_min_dt_corp = data_min_dt - relativedelta(days=540)
    df_safra = (
        aplica_peso_a_dif_datas(
            df=df_trns.join(df_vlr, on=["cd_clsr", "dt_trns", "nu_dia_vnda"], how="left")
            .fillna(0)
            .withColumn(
                "dt_min", 
                F.when(F.col("cd_clsr").contains(" | "), F.lit(data_min_dt_all))
                .otherwise(F.lit(data_min_dt_corp))
            ),
            dt_col_1="dt_min",
            dt_col_2="dt_trns",
            tipo_peso=tipo_peso
        )
        .drop("dt_min")
        .groupBy("cd_clsr", "nu_dia_vnda")
        .agg(
            (
                F.sum(F.col(f"vl_{suffixo}") * F.col("peso")) / F.sum(F.col("vl_sfra") * F.col("peso"))
            ).alias(f"pc_{suffixo}")
        )
        .fillna(0, subset=[f"pc_{suffixo}"])
    )

    # converte em valores percentuais e calcula o valor acumulados
    return df_safra.select(
        "cd_clsr",
        "nu_dia_vnda",
        F.sum(F.col(f"pc_{suffixo}"))
        .over(Window.partitionBy(["cd_clsr"])
        .orderBy("nu_dia_vnda")
        .rowsBetween(Window.unboundedPreceding, 0))
        .alias(f"pc_{suffixo}")
    )


def processa_tbciar_re_mdlo_crva_fit(
    du: "DataUtils",
    janela_apuracao: int,
    dias_cruzamento: typing.Dict[str, int],
    tipo_valor: str,
    tipo_peso: str,
    data_min: typing.Optional[str] = None,
    data_max: typing.Optional[str] = None,
) -> DataFrame:
    """
    Processa dados de transações e chargeback/cancelamento para gerar 
    curvas de chargeback/cancelamento.

    Esta função realiza o processamento de dados de transações agregadas e dados de chargeback/cancelamento
    para produzir curvas de chargeback/cancelamento agregados por segmento.
    Essas curvas são utilizadas para análise de risco e comportamento
    de chargeback ao longo do tempo. E para projeção das curvas de chargeback/cancelamento até 540 dias
    
    A função foi desenhada inicialmente para ser processada uma vez por mês, 
    lendo dados de 24 meses, desconsiderandos os últimos 6 meses

    :param du: instancia do data utils para ser usada para processar a base
    :param data_min: data mínima de chargeback/cancelamento
    :param data_max: data máxima de chargeback/cancelamento
    :param janela_apuracao: Número de meses para a janela de apuração das curvas.
    :param dias_cruzamento: Número de dias para o qual cruzamos dados de chargeback/cancelamento
    :param tipo_valor: Tipo de valor para fazer o calculo
    :param tipo_peso: Métrica de ponderamento a ser aplicada em cada ponto de curva de chargeback/cancelamento
    :return: Um DataFrame contendo as curvas de chargeback/cancelamento processadas.
    """
    from modelo.comuns import obtem_ultima_data_e_hora

    # obtém os valores de datas mínimas e máximas que deveriam ser utilizados
    if data_min is not None and data_max is not None:
        # se ambos os valores forem dados os utiliza diretamente
        data_min_dt = datetime(int(data_min[:4]), int(data_min[4:6]), int(data_min[6:]))
        data_max_dt = datetime(int(data_max[:4]), int(data_max[4:6]), int(data_max[6:]))
    elif data_min is not None and data_max is None:
        # se só o valor da data minima for dado, aplica a jenala a partir da data minima
        data_min_dt = datetime(int(data_min[:4]), int(data_min[4:6]), int(data_min[6:]))
        data_max_dt = data_min_dt + relativedelta(months=janela_apuracao)
    elif data_min is None and data_max is not None:
        # se só o valor da data maxima for dado, aplica a jenala a partir da data maxima
        data_max_dt = datetime(int(data_max[:4]), int(data_max[4:6]), int(data_max[6:]))
        data_min_dt = data_max_dt - relativedelta(months=janela_apuracao)
    else:
        # se nenhum for dado, usa o último dia da base de chargeback/cancelamento com a janela aplicada
        data_max = min(
            max([v["cd_dia_crte"] for v in du.get_partition_values("TRNS_CLUSTER", "internal")]),
            max([v["cd_dh_incl_rgst"] for v in du.get_partition_values("CHARGEBACK", "internal")]),
            max([v["cd_dh_incl_rgst"] for v in du.get_partition_values("CANCELAMENTO", "internal")]),
        )
        data_max_dt = datetime(int(data_max[:4]), int(data_max[4:6]), int(data_max[6:]))
        data_min_dt = data_max_dt - relativedelta(months=janela_apuracao + 1) + relativedelta(days=1)

    # cria as versões inteiras dos valores
    data_min_int = int(data_min_dt.strftime("%Y%m%d"))
    data_max_int = int(data_max_dt.strftime("%Y%m%d"))

    # obtém a lista de ECs a serem abertos por cadeia forçada
    lista_cf = [
        r[0]
        for r in (
            obtem_ultima_data_e_hora(du, data_max_int, "CF_CLUSTER")
            .select("nm_grpo")
            .distinct()
            .collect()
        )
    ]

    # carrega os dados da TRNS
    df_trns = (
        obtem_ultima_data_e_hora(du, data_max_int, "TRNS_CLUSTER")
        .groupBy("cd_clsr", "dt_trns")
        .agg(F.sum("vl_sfra").alias("vl_sfra"))
    )

    # explode a base de trns por meses
    df_trns = (
        df_trns.withColumn(
            "delta_dt_max", 
            F.least(
                F.datediff(F.lit(data_max_dt), F.col("dt_trns")), 
                F.when(
                    F.col("cd_clsr").contains(" | "),
                    F.lit(max(dias_cruzamento.values()))
                ).otherwise(F.lit(540))
            )
        )
        .select("*", F.explode(F.expr("sequence(0, delta_dt_max)")).alias("nu_dia_vnda"))
        .persist()
    )

    # gera a curva de chargebacks
    df_chrg = gera_curva_evento(
        du=du,
        df_trns=df_trns,
        lista_cf=lista_cf,
        datas=(data_min_int, data_max_int, data_min_dt, data_max_dt),
        dias_cruzamento=dias_cruzamento["chrg"], 
        tipo_valor=tipo_valor,
        tipo_peso=tipo_peso,
        base="CHARGEBACK",
        suffixo="chrg"
    )

    # gera a curva de cancelamentos
    df_cncl = gera_curva_evento(
        du=du,
        df_trns=df_trns,
        lista_cf=lista_cf,
        datas=(data_min_int, data_max_int, data_min_dt, data_max_dt),
        dias_cruzamento=dias_cruzamento["cncl"], 
        tipo_valor=tipo_valor,
        tipo_peso=tipo_peso,
        base="CANCELAMENTO",
        suffixo="cncl"
    )

    # combinas as curvas
    df_curvas = (
        df_chrg.withColumn("pc_cncl", F.lit(0))
        .unionByName(df_cncl.withColumn("pc_chrg", F.lit(0)))
        .groupBy("cd_clsr", "nu_dia_vnda")
        .agg(
            F.sum(F.col("pc_chrg")).alias("pc_chrg"),
            F.sum(F.col("pc_cncl")).alias("pc_cncl"),
        )
        .withColumn("cd_dia_crte", F.lit(data_max_int))
        .withColumn("dh_crga", F.lit(datetime.now(timezone.utc).strftime("%Y%m%d%H%M%S")))
        .withColumn(
            "dc_prmt_exec", F.lit(
                f"janela_apuracao={janela_apuracao};"
                + f"dias_chrg={dias_cruzamento['chrg']};"
                + f"dias_cncl={dias_cruzamento['cncl']};"
                + f"tipo_peso={tipo_peso};"
                + f"tipo_valor={tipo_valor};"
            )
        )
    )
    
    return df_curvas

def executa_mdlo_crva_fit(params: list) -> None:
    """
    Função wrapper para executar a base tbciar_re_mdlo_crva_fit

    :param params: parâmetros de execução
    """
    import modelo.configs as confs

    # pega os parâmetros de execução
    env = params[0] if len(params) > 0 else "PROD"
    data_min = params[1] if len(params) > 1 else None
    data_max = params[2] if len(params) > 2 else None
    janela = params[3] if len(params) > 3 else str(confs.JANELA_APURACAO)
    dias_chrg = params[4] if len(params) > 4 else str(confs.DIAS_CRUZAMENTO["chrg"])
    dias_cncl = params[5] if len(params) > 5 else str(confs.DIAS_CRUZAMENTO["cncl"])
    tipo_valor = params[6] if len(params) > 6 else confs.TIPO_VALOR
    tipo_peso = params[7] if len(params) > 7 else confs.TIPO_PESO

    # corrige nulos
    env = "PROD" if env == "" else env
    data_min = None if data_min == "" else data_min
    data_max = None if data_max == "" else data_max
    janela = confs.JANELA_APURACAO if janela == "" else int(janela)
    dias_chrg = confs.DIAS_CRUZAMENTO["chrg"] if dias_chrg == "" else int(dias_chrg)
    dias_cncl = confs.DIAS_CRUZAMENTO["cncl"] if dias_cncl == "" else int(dias_cncl)
    tipo_valor = confs.TIPO_VALOR if tipo_valor == "" else tipo_valor
    tipo_peso = confs.TIPO_PESO if tipo_peso == "" else tipo_peso

    # instância o data utils
    from utils.data_utils import DataUtils
    du = DataUtils(env)

    # executa o processamento
    df = processa_tbciar_re_mdlo_crva_fit(
        du=du,
        janela_apuracao=janela,
        dias_cruzamento={"chrg": dias_chrg, "cncl": dias_cncl},
        tipo_valor=tipo_valor,
        tipo_peso=tipo_peso,
        data_min=data_min,
        data_max=data_max,
    )

    # escreve o resultado para o disco
    du.write_by_name(df, "CURVA_FIT", "internal", partitions=["cd_dia_crte", "dh_crga"], mode="overwrite")


if __name__ == "__main__":
    # instancia a sessão do spark
    spark = (
        SparkSession.builder.appName("SPARK_prod_prazo_tbciar_re_mdlo_crva_fit")
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
    executa_mdlo_crva_fit(sys.argv[1:])