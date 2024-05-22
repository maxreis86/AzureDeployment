import typing
from itertools import chain
from datetime import datetime
from datetime import timezone
from dateutil.relativedelta import relativedelta

import pyspark.sql.functions as F
import pyspark.sql.types as T
from pyspark.sql import DataFrame
from pyspark.sql.window import Window


def obtem_ultima_data_e_hora(du: "DataUtils", data: typing.Union[int, None], tabela: str) -> DataFrame:
    """
    Obtém a tabela para a última data e hora antes da data selecionada

    :param du: instância do data utils
    :param data: data de corte considerada
    :param tabela: tabela a ser carregada
    :return: data frame filtrado para data e hora selecionada
    """
    if data is None:
        data = 20991231

    # obtém as partições da tabela
    parts = du.get_partition_values(tabela, "internal")

    # obtém o corte
    dt_corte = max([
        v["cd_dia_crte"] for v in parts if int(v["cd_dia_crte"]) <= data
    ])

    # obtém a ultima carga da data de corte selecionada
    dh_carga = max([
        v["dh_crga"] for v in parts if int(v["cd_dia_crte"]) == int(dt_corte)
    ])

    # carrega a tabela com o filtro
    return du.get_by_name(tabela, "internal").filter(
        (F.col("cd_dia_crte") == dt_corte)
        & (F.col("dh_crga") == dh_carga)
    )


def adiciona_grupo_bandeira(df: DataFrame) -> DataFrame:
    """
    Adiciona uma coluna de grupo de bandeira dada uma coluna de bandeira

    :param df: data frame com coluna nm_bndr
    :return: data frame com coluna cd_bndr
    """
    return df.withColumn(
        "cd_bndr",
        F.when(F.col("nm_bndr").isin(["MASTERCARD", "VISA"]), "MASTER/VISA")
        .when(F.col("nm_bndr").isin(["AMEX", "ELO"]), F.col("nm_bndr"))
        .otherwise("OUTRAS")
    )


def extrai_bandeira_do_cluster(df: DataFrame) -> DataFrame:
    """
    Extraí o código de bandeira a partir dos dados de cluster

    :param df: data frame com campo cd_clsr
    :return: data frame com coluna cd_bndr
    """
    return df.withColumn(
        "cd_bndr",
        F.when(F.col("cd_clsr").contains("MASTER/VISA |"), "MASTER/VISA")
        .when(F.col("cd_clsr").contains("AMEX |"), "AMEX")
        .when(F.col("cd_clsr").contains("ELO |"), "ELO")
        .when(F.col("cd_clsr").contains("| OUTRAS"), "OUTRAS")
        .otherwise("N/A")
    )


def classifica_em_grupos(
    df: DataFrame, lista_cf: list = list(), alternativas: bool = False,
) -> DataFrame:
    """
    Dado um data frame classifica cada linha de dado em um grupo de acordo
    com a clusterização a ser aplicada no modelo

    :param df: df a ser classificado
    :param lista_cf: lista de clientes que deve ser considerada como seu próprio cluster
    :param alternativas: True se devemos retornar versões alternativas, menos granulares
        cluster para lidar com problemas como insuficiencia de dados
    :return: df com a coluna cd_clsr
    """
    from modelo.comuns import adiciona_grupo_bandeira
    from modelo.lookups import TENDENCIA_DIFERIMENTO

    assert {
        "in_crto_psen",
        "nm_unde_ngco_fpna",
        "nm_sgmt_cmrl",
        "nm_grpo",
        "nm_tipo_trns",
        "cd_so_ramo_atvd",
        "nm_bndr",
    }.issubset(set(df.columns))

    # cria um mapeamento de tendencia de diferimento com o diciionario TENDENCIA_DIFERIMENTO importado de lookups
    mapping_diferimento = F.create_map([F.lit(x) for x in chain(*TENDENCIA_DIFERIMENTO.items())])
    
    # cria campos de códigos alternativas
    df = (
        adiciona_grupo_bandeira(df)
        .withColumn("cd_cp", F.when(F.col("in_crto_psen") == 1, "CP").otherwise("CNP"))
        .withColumn(
            "cd_uneg", 
            F.when(F.col("nm_unde_ngco_fpna") == "GRANDES CONTAS", "GRANDES CONTAS")
            .otherwise("VAREJO/EMPR")
        )
        .withColumn(
            "cd_sgm", 
            F.when(
                (F.col("nm_sgmt_cmrl").isin(["NAO SE APLICA", "INEXISTENTE", "NAO IDENTIFICADO"]))
                | (F.col("nm_sgmt_cmrl").isNull()), 
                "N/A"
            )
            .otherwise(F.col("nm_sgmt_cmrl"))
        )
        .withColumn(
            "cd_cf", 
            F.when(
                F.col("nm_grpo").isin(lista_cf), 
                F.col("nm_grpo")
            ).otherwise(F.lit(None))
        )
        .withColumn("cd_tipo", F.col("nm_tipo_trns"))
        .withColumn("cd_td", mapping_diferimento[F.col("cd_so_ramo_atvd")])
        .withColumn(
            "cd_td", 
            F.when(F.col("cd_td").isNull(), "média")
            .otherwise(F.col("cd_td"))
        )
    )
    
    # configura os calculos de cluster
    # param 1: nome da coluna a ser criada
    # param 2: condição prévia a ser checada para criar uma intervenção manual
    # param 3: lista de colunas concatenadas para formar um cluster
    cond_1 = (
        F.when(F.col("cd_cf").isNotNull(), F.col("cd_cf"))
        .when(F.col("cd_tipo") == "DÉBITO", F.concat(F.col("cd_cp"), F.lit(" | "), F.col("cd_tipo")))
        .when(F.col("cd_bndr") == "OUTRAS", F.concat(F.col("cd_cp"), F.lit(" | "), F.col("cd_bndr")))
    )  
    cond_2 = (
        F.when(F.col("cd_tipo") == "DÉBITO", F.concat(F.col("cd_cp"), F.lit(" | "), F.col("cd_tipo")))
        .when(F.col("cd_bndr") == "OUTRAS", F.concat(F.col("cd_cp"), F.lit(" | "), F.col("cd_bndr")))
    )        
    cond_n = F.when(F.col("cd_tipo") == "XXXXX", F.lit(""))
    configs_cols = [
        ("cd_clsr", cond_1, ["cd_bndr", "cd_tipo", "cd_cp", "cd_td"]),
    ]
        
    if alternativas:
        configs_cols += [
            ("cd_alt_1", cond_2, ["cd_bndr", "cd_tipo", "cd_cp", "cd_td"]),
            ("cd_alt_2", cond_2, ["cd_bndr", "cd_tipo", "cd_cp"]),
            ("cd_alt_3", cond_n, ["cd_tipo", "cd_cp"]),
            ("cd_alt_4", cond_n, ["cd_cp"])
        ]
    
    # aplica os calculos de cluster
    return df.select(
        "*", *[
            cond.otherwise(
                F.concat(
                    *[F.col(c) if s is None else F.lit(s) for c in cols for s in [None, " | "]][:-1]
                )
            ).alias(col_name)
            for col_name, cond, cols in configs_cols
        ]
    ).drop("cd_cp", "cd_bndr", "cd_uneg", "cd_sgm", "cd_tipo", "cd_cf", "cd_td")


def projeta_curva_legada(
    du: "DataUtils",
    tipo_valor: str,
    base: str,
    suffixo: str,
    data_max: typing.Optional[str] = None,
) -> DataFrame:
    """
    Processa dados de transações e chargeback para gerar curvas de chargeback.

    Esta função realiza o processamento de dados de transações agregadas e dados de chargeback
    para produzir curvas de chargeback agregados por segmento.
    Essas curvas são utilizadas para análise de risco e comportamento
    de chargeback ao longo do tempo. E para projeção das curvas de chargeback até 540 dias
    
    A função foi desenhada inicialmente para ser processada uma vez por mês, lendo dados de 24 meses, desconsiderandos os últimos 6 meses

    :param du: instancia do data utils para ser usada para processar a base
    :param data_max: data máxima de chargeback
    :param tipo_valor (str): Tipo de valor para fazer o calculo
    :param base: nome da base de dados a ser carregada
    :param suffixo: suffixo a ser aplicado nos nomes (cgb/canc)
    :return: Um DataFrame contendo as curvas de chargeback processadas.
    """
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

    # calcula a data mínima a ser utilizada que será sempre 540 dias antes da data máxima
    data_min_dt = data_max_dt - relativedelta(days=540)

    # cria as versões inteiras dos valores
    data_min_int = int(data_min_dt.strftime("%Y%m%d"))
    data_max_int = int(data_max_dt.strftime("%Y%m%d"))

    # seleciona as datas das bases a serem carregas
    dt_cnpj = max([
        v["cd_dia_crte"] 
        for v in du.get_partition_values("GRUPO_CNPJ", "internal")
        if int(v["cd_dia_crte"]) <= data_max_int
    ])

    # carrega os dados de grupo CNPJ
    df_gc = (
        du.get_by_name("GRUPO_CNPJ", "internal")
        .filter(F.col("cd_dia_crte") == dt_cnpj)
        .drop("cd_dia_crte")
        .persist()
    )

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
    
    # seleciona os dados de agrupamento
    id_cols = ["cd_grpo_cnpj", "dt_trns", "cd_clsr", "nm_tipo_trns", "qt_prcl"]
    window = Window.partitionBy(id_cols).orderBy("nu_dia_vnda")

    # adquire os dados da base de eventos alvo
    eventos_df = classifica_em_grupos(
        du.get_by_name(base, "internal")
        .filter(
            (F.col("cd_dh_incl_rgst") >= data_min_int)
            & (F.col("cd_dh_incl_rgst") <= data_max_int)
            & (F.col(f"dt_{suffixo}") >= data_min_dt)
        )
        .withColumn(
            "nm_grpo",
            F.when(
                F.col("nm_cdfr") == "NAO SE APLICA",
                F.col("nu_cnpj9_cpf").cast("string")
            )
            .otherwise(F.col("nm_cdfr"))
        ),
        lista_cf
    )

    # agrupa os dados nível grupo CNPJ
    evnt_grupo = (
        eventos_df.groupBy(id_cols[1:] + ["nu_cnpj_cpf_recr"])
        .agg(F.abs(F.sum(F.col(f"vl_{tipo_valor}_{suffixo}")).cast(T.DoubleType())).alias(f"vl_{suffixo}"))
        .join(df_gc, on=["nu_cnpj_cpf_recr"], how="inner")
        .groupBy(id_cols)
        .agg(F.sum(F.col(f"vl_{suffixo}") * F.col("vl_peso_cnpj")).alias(f"vl_{suffixo}"))
        .drop("vl_peso_cnpj")
    )

    # obtém os dados de TRNS cluster
    trns_cluster = (
        obtem_ultima_data_e_hora(du, data_max_int, "TRNS_CLUSTER")
        .filter((F.col("dt_trns") > data_min_dt))
        .withColumn("pc_rbrp", F.col("vl_rbrp_pndr") /  F.col("vl_sfra_pndr"))
        .select(*(id_cols + [F.col("vl_sfra_pndr").alias("vl_sfra"), "pc_rbrp"]))
    )

    # adiciona os dados de evento
    trns_evento = (
        trns_cluster.join(evnt_grupo, on=id_cols, how="left")
        .fillna(0)
        .withColumn(f"pc_{suffixo}", F.least(F.col(f"vl_{suffixo}") / F.col("vl_sfra"), F.lit(1)))
        .withColumn(
            "nu_dia_vnda", F.datediff(F.lit(data_max_dt), F.col("dt_trns"))
        )
    )

    # expande os dados por cluster
    trns_curva = extrai_bandeira_do_cluster(
        trns_evento.alias("a")
        .join(
            obtem_ultima_data_e_hora(du, data_max_int, "CURVA_SMOOTH").alias("b"),
            on=F.expr("a.cd_clsr = b.cd_clsr AND b.nu_dia_vnda > a.nu_dia_vnda"),
            how="left"
        )
        .select(
            [f"a.{i}" for i in id_cols] + [
                "a.vl_sfra",
                f"a.pc_{suffixo}",
                "a.pc_rbrp",
                "b.nu_dia_vnda",
                f"b.icmo_{suffixo}",
            ]
        )
    )

    # adiciona os dados de VR LIQ
    trns_vr = (
        trns_curva.withColumn(
            "nu_dia_prjt",
            F.datediff(F.expr("date_add(dt_trns, nu_dia_vnda)"), F.lit(data_max_dt))
        )
        .join(
            obtem_ultima_data_e_hora(du, data_max_int, "VR_LIQ")
            .select(
                "cd_clsr",
                "nm_tipo_trns",
                "qt_prcl",
                "nu_dia_vnda",
                f"vl_{suffixo}_liq",
            ),
            on=["cd_clsr", "nm_tipo_trns", "qt_prcl", "nu_dia_vnda"],
            how="left"
        )
    )

    # adiciona os dados de ARV
    trns_arv = trns_vr.join(
        obtem_ultima_data_e_hora(du, data_max_int, "ARV_MEDIO")
        .select(
            "cd_grpo_cnpj",
            "cd_bndr",
            (F.col("pc_fncd") * F.col("pc_fncd_cilo")).alias("pc_arv"),
            "razo_arv_mrcd",
        ),
        on=["cd_grpo_cnpj", "cd_bndr"],
        how="left"
    )

    # adiciona os dados de ARV agrupados
    trns_arv = trns_arv.join(
        obtem_ultima_data_e_hora(du, data_max_int, "ARV_MEDIO")
        .groupBy("cd_grpo_cnpj")
        .agg(
            (
                F.sum(F.col("pc_fncd") * F.col("pc_fncd_cilo") * F.col("vl_totl"))
                / F.sum(F.col("vl_totl"))
            ).alias("pc_arv_medo"),
            (
                F.sum(F.col("razo_arv_mrcd") * F.col("vl_totl"))
                / F.sum(F.col("vl_totl"))
            ).alias("razo_arv_mrcd_medo")
        ),
        on=["cd_grpo_cnpj"],
        how="left"
    )

    # realiza os calculos de projeções de evento
    proj_evento = (
        trns_arv.withColumn("pc_arv", F.when(F.col("pc_arv").isNull(), F.col("pc_arv_medo")).otherwise(F.col("pc_arv")))
        .withColumn(
            "razo_arv_mrcd",
            F.when(F.col("razo_arv_mrcd").isNull(), F.col("razo_arv_mrcd_medo")).otherwise(F.col("razo_arv_mrcd"))
        )
        .fillna(0, subset=["pc_arv", "razo_arv_mrcd"])
        .drop("pc_arv_med", "razo_arv_mrcd_medo")
        .withColumn(f"icmo_{suffixo}", F.sum(F.col(f"icmo_{suffixo}")).over(window.rowsBetween(Window.unboundedPreceding, 0)))
        .withColumn(f"pc_{suffixo}_prjt", F.col(f"pc_{suffixo}") + F.col(f"icmo_{suffixo}"))
        .withColumn(f"pc_{suffixo}_prjt", F.when(F.col(f"pc_{suffixo}_prjt") > 1, 1).otherwise(F.col(f"pc_{suffixo}_prjt")))
        .withColumn(
            f"pc_{suffixo}_prjt",
            F.col(f"pc_{suffixo}_prjt") - F.coalesce(F.lag(F.col(f"pc_{suffixo}_prjt"), 1).over(window), F.col(f"pc_{suffixo}"))
        )
        .withColumn(f"vl_{suffixo}_prjt", F.col("vl_sfra") * F.col(f"pc_{suffixo}_prjt"))
        .drop(f"pc_{suffixo}", f"icmo_{suffixo}")
    )

    # adiciona os valores nulos de valores a liquidar
    proj_vr = proj_evento.withColumn(
        f"vl_{suffixo}_liq",
        F.when(
            F.col(f"vl_{suffixo}_liq").isNull(),
            F.when(F.col("nu_dia_vnda") <= 2, 1)
            .when(F.col("nm_tipo_trns") == "DÉBITO", 0)
            .when(
                (F.col("nm_tipo_trns") == "CRÉDITO À VISTA") & (F.col("qt_prcl") == 0),
                F.when(F.col("nu_dia_vnda") < 30, 1).otherwise(0)
            )
            .otherwise(
                1 - F.least(
                    F.floor(F.col("nu_dia_vnda") / 30) / F.col("qt_prcl"),
                    F.lit(1)
                )
            )
        ).otherwise(F.least(F.col(f"vl_{suffixo}_liq"), F.lit(1)))
    )

    # adiciona os valores do evento RR, liquidado, ARV Cielo e ARV mercado
    df_prjt = (
        proj_vr.withColumn(f"vl_{suffixo}_rbrp", F.col(f"vl_{suffixo}_prjt") * F.col("pc_rbrp"))
        .withColumn(
            f"vl_{suffixo}_liqd",
            F.col(f"vl_{suffixo}_prjt") * (1 - F.col("pc_rbrp")) * (1 - F.col(f"vl_{suffixo}_liq"))
        )
        .withColumn(
            f"vl_{suffixo}_arv_cilo",
            F.col(f"vl_{suffixo}_prjt") * (1 - F.col("pc_rbrp")) * F.col(f"vl_{suffixo}_liq") * F.col("pc_arv")
        )
        .withColumn(
            f"vl_{suffixo}_arv_mrcd",
            F.col(f"vl_{suffixo}_prjt") * F.col(f"vl_{suffixo}_liq") * F.col("razo_arv_mrcd")
        )
        .fillna(0)
    )

    # agrupa os dados por grupos de cnpj e dia de projeção
    df_prjt = (
        df_prjt.groupBy("cd_grpo_cnpj", "nu_dia_prjt")
        .agg(
            F.sum(f"vl_{suffixo}_prjt").alias(f"vl_{suffixo}_prjt"),
            F.sum(f"vl_{suffixo}_liqd").alias(f"vl_{suffixo}_liqd"),
            F.sum(f"vl_{suffixo}_rbrp").alias(f"vl_{suffixo}_rbrp"),
            F.sum(f"vl_{suffixo}_arv_cilo").alias(f"vl_{suffixo}_arv_cilo"),
            F.sum(f"vl_{suffixo}_arv_mrcd").alias(f"vl_{suffixo}_arv_mrcd"),
        )
        .withColumn("cd_dia_crte", F.lit(int(data_max)))
        .withColumn("dh_crga", F.lit(datetime.now(timezone.utc).strftime("%Y%m%d%H%M%S")))
        .withColumn("dc_prmt_exec", F.lit(f"tipo_valor={tipo_valor}"))
    )

    return df_prjt


def projeta_curva_incremental(du: "DataUtils", data_max: typing.Union[str, None], suffixo: str) -> DataFrame:
    """
    Realiza a projeção de chargeback/cancelamento para volumes incrementais.
    
    O processamento le a tabela TPV_PROJ com a projecao do TPV nao performado  e a tabela CURVA_SMOOTH com as curvas de chargeback
    projetadas ate 540 dias, pegando sempre a ultima carga de cada tabela.
    
    Cada data de transacao projetada na tabela TPV_PROJ recebe uma projecao de chargeback
    Esse chargeback é calculado pela soma do incremento diario de chargeback da tabela CURVA_SMOOTH
    Entao, se por exemplo para cada grupo ha 60 dias de projecao de TPV, cada um dos 60 dias recebera 540 dias de projecao de chargeback

    :param du: instancia do data utils para ser usada para processar a base
    :param data_max: data de corte a ser considerada
    :param suffixo: suffixo a ser aplicado nos nomes (cgb/canc)
    :return: Um DataFrame contendo as curvas de chargeback processadas.
    """
    # cria as versões inteiras dos valores
    if data_max is None:
        data_max = max([v["cd_dia_crte"] for v in du.get_partition_values("GRUPO_CNPJ", "internal")])
    else:
        data_max = max([
            v["cd_dia_crte"]
            for v in du.get_partition_values("GRUPO_CNPJ", "internal")
            if v["cd_dia_crte"] <= data_max
        ])
        
    data_max_dt = datetime(int(data_max[:4]), int(data_max[4:6]), int(data_max[6:]))

    # cria as versões inteiras dos valores    
    data_max_int = int(data_max_dt.strftime("%Y%m%d"))

    # obtém o evento projetado
    evento_prjt = (
        obtem_ultima_data_e_hora(du, data_max_int, "TPV_PROJ")
        .withColumnRenamed("vl_sfra_prjt", "vl_sfra")
        .alias("a")
        .join(
            obtem_ultima_data_e_hora(du, data_max_int, "CURVA_SMOOTH").alias("b"),
            on=["cd_clsr"],
            how="left"
        )
        .select("a.*", "b.nu_dia_vnda", f"b.icmo_{suffixo}")
        .withColumn(f"vl_{suffixo}_prjt", F.col("vl_sfra") * F.col(f"icmo_{suffixo}"))
    )

    # adiciona o valor a liquidar
    evento_vr = (
        evento_prjt.join(
            obtem_ultima_data_e_hora(du, data_max_int, "VR_LIQ")
            .select("cd_clsr", "nm_tipo_trns", "qt_prcl", "nu_dia_vnda", f"vl_{suffixo}_liq"),
            on=["cd_clsr", "nm_tipo_trns", "qt_prcl", "nu_dia_vnda"],
            how="left"
        )
        .withColumn("nu_dia_prjt", F.col("nu_dia_prjt") + F.col("nu_dia_vnda"))
    )

    # adiciona os dados de RR
    evento_rr = extrai_bandeira_do_cluster(
         evento_vr.join(
            obtem_ultima_data_e_hora(du, data_max_int, "TPV_MEDIO")
            .select("cd_grpo_cnpj", "cd_clsr", "nm_tipo_trns", "qt_prcl", "pc_rbrp"),
            on=["cd_grpo_cnpj", "cd_clsr", "nm_tipo_trns", "qt_prcl"],
            how="left"
        )
        .fillna(subset=["pc_rbrp"], value=0.0)
    )

    # adiciona os valores de ARV
    evento_arv = evento_rr.join(
        obtem_ultima_data_e_hora(du, data_max_int, "ARV_MEDIO")
        .select(
            "cd_grpo_cnpj",
            "cd_bndr",
            (F.col("pc_fncd") * F.col("pc_fncd_cilo")).alias("pc_arv"),
            "razo_arv_mrcd",
        ),
        on=["cd_grpo_cnpj", "cd_bndr"],
        how="left"
    )

    # adiciona os dados de ARV médio
    evento_arv = evento_arv.join(
        obtem_ultima_data_e_hora(du, data_max_int, "ARV_MEDIO")
        .groupBy("cd_grpo_cnpj")
        .agg(
            (
                F.sum(F.col("pc_fncd") * F.col("pc_fncd_cilo") * F.col("vl_totl"))
                / F.sum(F.col("vl_totl"))
            ).alias("pc_arv_medo"),
            (
                F.sum(F.col("razo_arv_mrcd") * F.col("vl_totl"))
                / F.sum(F.col("vl_totl"))
            ).alias("razo_arv_mrcd_medo")
        ),
        on=["cd_grpo_cnpj"],
        how="left"
    )

    # preenche os valores de ARV
    evento_arv = (
        evento_arv.withColumn("pc_arv", F.when(F.col("pc_arv").isNull(), F.col("pc_arv_medo")).otherwise(F.col("pc_arv")))
        .withColumn(
            "razo_arv_mrcd",
            F.when(F.col("razo_arv_mrcd").isNull(), F.col("razo_arv_mrcd_medo")).otherwise(F.col("razo_arv_mrcd"))
        )
        .fillna(0, subset=["pc_arv", "razo_arv_mrcd"])
        .drop("pc_arv_med", "razo_arv_mrcd_medo")
    )

    # preenche nulos no valor a liquidar
    evento_fvr = evento_arv.withColumn(
        f"vl_{suffixo}_liq",
        F.when(
            F.col(f"vl_{suffixo}_liq").isNull(),
            F.when(F.col("nu_dia_vnda") <= 2, 1)
            .when(F.col("nm_tipo_trns") == "DÉBITO", 0)
            .when(
                (F.col("nm_tipo_trns") == "CRÉDITO À VISTA") & (F.col("qt_prcl") == 0),
                F.when(F.col("nu_dia_vnda") < 30, 1).otherwise(0)
            )
            .otherwise(
                1 - F.least(
                    F.floor(F.col("nu_dia_vnda") / 30) / F.col("qt_prcl"),
                    F.lit(1)
                )
            )
        ).otherwise(F.least(F.col(f"vl_{suffixo}_liq"), F.lit(1)))
    )

    # adiciona os valores projetados
    evento_prjt = (
        evento_fvr.withColumn(f"vl_{suffixo}_rbrp", F.col(f"vl_{suffixo}_prjt") * F.col("pc_rbrp"))
        .withColumn(
            f"vl_{suffixo}_liqd",
            F.col(f"vl_{suffixo}_prjt") * (1 - F.col("pc_rbrp")) * (1 - F.col(f"vl_{suffixo}_liq"))
        )
        .withColumn(
            f"vl_{suffixo}_arv_cilo",
            F.col(f"vl_{suffixo}_prjt") * (1 - F.col("pc_rbrp")) * F.col(f"vl_{suffixo}_liq") * F.col("pc_arv")
        )
        .withColumn(
            f"vl_{suffixo}_arv_mrcd",
            F.col(f"vl_{suffixo}_prjt") * F.col(f"vl_{suffixo}_liq") * F.col("razo_arv_mrcd")
        )
    )

    # agrupa os dados no nivel de interesse
    df = (
        evento_prjt.groupBy("cd_grpo_cnpj", "nu_dia_prjt")
        .agg(
            F.sum(f"vl_{suffixo}_prjt").alias(f"vl_{suffixo}_prjt"),
            F.sum(f"vl_{suffixo}_liqd").alias(f"vl_{suffixo}_liqd"),
            F.sum(f"vl_{suffixo}_rbrp").alias(f"vl_{suffixo}_rbrp"),
            F.sum(f"vl_{suffixo}_arv_cilo").alias(f"vl_{suffixo}_arv_cilo"),
            F.sum(f"vl_{suffixo}_arv_mrcd").alias(f"vl_{suffixo}_arv_mrcd"),
        )
        .withColumn("cd_dia_crte", F.lit(data_max_int))
        .withColumn("dh_crga", F.lit(datetime.now(timezone.utc).strftime("%Y%m%d%H%M%S")))
    )
    
    return df