import sys
import typing
from dataclasses import dataclass
from datetime import datetime
from datetime import timezone

import pyspark.sql.functions as F
from pyspark.sql import SparkSession
from pyspark.sql.dataframe import DataFrame


@dataclass
class ParametrosStressed:
    """
    Lista de parâmetros a serem utilizados no modelo stressed

    :param cgb: multiplicador dos valores de chargeback
    :param canc: multiplicador dos valores de cancelamento
    :param rr_rel: percentual de crescimento dos valores de RR 
    :param rr_abs: valor mínimo de RR (como percentual do TPV)
    :param arv_rel: percentual de crescimento dos valores de ARV
    :param arv_abs: valor mínimo de agenda financiada
    :param gravame_rel: percentual de variação da agenda gravamada
    :param gravame_abs: valor máximo de agenda gravamada (como percentual de agenda financiada)
    :param arv_cilo_rel: percentual de variação da agenda Cielo
    :param arv_cilo_abs: valor mínimo de financiamento Cielo (como percentual de agenda financiada)
    :param arv_mrcd_rel: percentual de variação do ARV mercado
    :param arv_mrcd_abs: valor mínimo de ARV mercado (como percentual da agenda total)
    :param churn_rel: percentual de variação da probabilidade de churn
    :param churn_abs: valor mínimo da probabilidade de churn
    """
    cgb: float = 2.0
    canc: float = 2.0
    rr_rel: float = 1.0
    rr_abs: float = 0.0
    arv_rel: float = 2.5
    arv_abs: float = 1.0
    gravame_rel: float = 0.0
    gravame_abs: float = 0.0
    arv_cilo_rel: float = 3.0
    arv_cilo_abs: float = 1.0
    arv_mrcd_rel: float = 1.0
    arv_mrcd_abs: float = 0.0
    churn_rel: float = 3.0
    churn_abs: float = 1.0


def _stress_cgb(df_calc: DataFrame, params: ParametrosStressed) -> DataFrame:
    """
    Multiplica os valores de chargeback pelo multiplicador esperado

    :param df_calc: dados que serão usados como input nos calculos
    :param params: dataclass com parâmetros stressed a serem aplicados
    :return: data frame com parâmetros estressados
    """
    # TODO: Precisamos modificar esse código para colocar um teto de chargeback que não ultrapasse 100% do TPV
    return (
        df_calc.withColumn("vl_chrg_prjt", F.col("vl_chrg_prjt") * params.cgb)
        .withColumn("vl_chrg_liqd", F.col("vl_chrg_liqd") * params.cgb)
        .withColumn("vl_chrg_rbrp", F.col("vl_chrg_rbrp") * params.cgb)
        .withColumn("vl_chrg_arv_cilo", F.col("vl_chrg_arv_cilo") * params.cgb)
        .withColumn("vl_chrg_arv_mrcd", F.col("vl_chrg_arv_mrcd") * params.cgb)
    )


def _stress_canc(df_calc: DataFrame, params: ParametrosStressed) -> DataFrame:
    """
    Multiplica os valores de cancelamento pelo multiplicador esperado

    :param df_calc: dados que serão usados como input nos calculos
    :param params: dataclass com parâmetros stressed a serem aplicados
    :return: data frame com parâmetros estressados
    """
    # TODO: Precisamos modificar esse código para colocar um teto de cancelamento que não ultrapasse 100% do TPV
    return (
        df_calc.withColumn("vl_cncl_prjt", F.col("vl_cncl_prjt") * params.canc)
        .withColumn("vl_cncl_liqd", F.col("vl_cncl_liqd") * params.canc)
        .withColumn("vl_cncl_rbrp", F.col("vl_cncl_rbrp") * params.canc)
        .withColumn("vl_cncl_arv_cilo", F.col("vl_cncl_arv_cilo") * params.canc)
        .withColumn("vl_cncl_arv_mrcd", F.col("vl_cncl_arv_mrcd") * params.canc)
    )


def _stress_rbrp(df_calc: DataFrame, params: ParametrosStressed) -> DataFrame:
    """
    Calcula o percentual de RR a ser considerado e aplica o RR como um percentual do chargeback esperado
    e então desconta o RR dos demais valores

    :param df_calc: dados que serão usados como input nos calculos
    :param params: dataclass com parâmetros stressed a serem aplicados
    :return: data frame com parâmetros estressados
    """
    # calcula o novo parâmetro de rrr
    df_calc = df_calc.withColumn(
        "pc_rbrp", 
        F.least(
            F.greatest(F.col("pc_rbrp") * params.rr_rel, F.lit(params.rr_abs)),
            F.lit(1)
        )
    )

    # calcula multiplicadores do volume de chargeback
    df_calc = (
        df_calc.withColumn(
            "mult_rbrp", 
            F.when(
                (F.col("pc_rbrp_base") > 0) & (F.col("vl_chrg_prjt") > 0), 
                (F.col("vl_chrg_rbrp") / F.col("vl_chrg_prjt"))
                * (F.col("pc_rbrp") / F.col("pc_rbrp_base"))
            )
            .otherwise(F.col("pc_rbrp"))
        )
        .withColumn(
            "mult_rbrp_icmo", 
            F.when(
                (F.col("pc_rbrp_base") > 0) & (F.col("vl_chrg_prjt_icmo") > 0), 
                (F.col("vl_chrg_rbrp_icmo") / F.col("vl_chrg_prjt_icmo"))
                * (F.col("pc_rbrp") / F.col("pc_rbrp_base"))
            )
            .otherwise(F.col("pc_rbrp"))
        )
    )

    # calcula os valores ajustados de chargeback
    df_calc = (
        df_calc.withColumn("vl_chrg_rbrp_", F.col("vl_chrg_prjt") * F.col("mult_rbrp"))
        .withColumn("vl_chrg_rbrp_icmo_", F.col("vl_chrg_prjt_icmo") * F.col("mult_rbrp_icmo"))
        .withColumn(
            "vl_chrg_liqd", 
            F.coalesce(
                F.col("vl_chrg_liqd") 
                * (F.col("vl_chrg_prjt") - F.col("vl_chrg_rbrp_")) 
                / (F.col("vl_chrg_prjt") - F.col("vl_chrg_rbrp")),
                F.lit(0)
            )
        )
        .withColumn(
            "vl_chrg_liqd_icmo", 
            F.coalesce(
                F.col("vl_chrg_liqd_icmo") 
                * (F.col("vl_chrg_prjt_icmo") - F.col("vl_chrg_rbrp_icmo_")) 
                / (F.col("vl_chrg_prjt_icmo") - F.col("vl_chrg_rbrp_icmo")),
                F.lit(0)
            )
        )
        .withColumn("vl_chrg_rbrp", F.col("vl_chrg_rbrp_"))
        .withColumn("vl_chrg_rbrp_icmo", F.col("vl_chrg_rbrp_icmo_"))
    )

    # remove colunas criadas
    return df_calc.drop("pc_rbrp_base", "mult_rbrp", "mult_rbrp_icmo", "vl_chrg_rbrp_", "vl_chrg_rbrp_icmo_")


def _stress_arv(df_calc: DataFrame, params: ParametrosStressed) -> DataFrame:
    """
    Calcula a razão entre o chargeback de ARV e os valores totais, e considera a razão de 
    crescimento do financiamento para avaliar o valor total de chargeback em ARV.

    Além disso, recalcula o valor de agenda legada baseado no parâmetro de financiamento
    ajustado e considerando a liquidação da agenda total

    :param df_calc: dados que serão usados como input nos calculos
    :param params: dataclass com parâmetros stressed a serem aplicados
    :return: data frame com parâmetros estressados
    """
    # calcula os novos valores de parâmetros
    df_calc = (
        df_calc.withColumn(
            "pc_fncd", 
            F.least(
                F.greatest(F.col("pc_fncd") * params.arv_rel, F.lit(params.arv_abs)),
                F.lit(1)
            )
        )
        .withColumn(
            "pc_fncd_cilo", 
            F.least(
                F.greatest(F.col("pc_fncd_cilo") * params.arv_cilo_rel, F.lit(params.arv_cilo_abs)),
                F.lit(1)
            )
        )
    )

    # recalcula os montantes de agenda no tempo
    df_calc = (
        df_calc.withColumn("vl_lvre_liq", F.col("vl_totl_liq") * (1 - F.col("pc_fncd")))
        .withColumn("vl_fncd_liq", F.col("vl_totl_liq") * F.col("pc_fncd"))
    )

    # calcula os multiplicadores a serem aplicados
    df_calc = (
        df_calc.withColumn(
            "multi", 
            F.when(
                (F.col("pc_fncd_cilo_base") * F.col("pc_fncd_base")) > 0,
                F.col("razo_arv_cilo")
                * (F.col("pc_fncd") * F.col("pc_fncd_cilo")) 
                / (F.col("pc_fncd_cilo_base") * F.col("pc_fncd_base"))
            )
            .otherwise(F.col("pc_fncd") * F.col("pc_fncd_cilo"))
        )
        .withColumn(
            "multi_icmo", 
            F.when(
                (F.col("pc_fncd_cilo_base") * F.col("pc_fncd_base")) > 0,
                F.col("razo_arv_cilo_icmo")
                * (F.col("pc_fncd") * F.col("pc_fncd_cilo")) 
                / (F.col("pc_fncd_cilo_base") * F.col("pc_fncd_base"))
            )
            .otherwise(F.col("pc_fncd") * F.col("pc_fncd_cilo"))
        )
    )

    # calcula os novos valores de chargeback
    return (
        df_calc.withColumn(
            "vl_chrg_arv_cilo", 
            (F.col("vl_chrg_prjt") - F.col("vl_chrg_rbrp") - F.col("vl_chrg_liqd"))
            * F.col("multi")
        )
        .withColumn(
            "vl_chrg_arv_cilo_icmo", 
            (F.col("vl_chrg_prjt_icmo") - F.col("vl_chrg_rbrp_icmo") - F.col("vl_chrg_liqd_icmo"))
            * F.col("multi_icmo")
        )
        .drop("pc_fncd_base", "pc_fncd_cilo_base", "multi", "multi_icmo")
    )


def _stress_gravame(df_calc: DataFrame, params: ParametrosStressed) -> DataFrame:
    """
    Calcula o percentual de RR a ser considerado e aplica o RR como um percentual do chargeback esperado
    e então desconta o RR dos demais valores

    :param df_calc: dados que serão usados como input nos calculos
    :param params: dataclass com parâmetros stressed a serem aplicados
    :return: data frame com parâmetros estressados
    """
    return (
        df_calc.withColumn(
            "pc_grvm", 
            F.least(
                F.col("pc_grvm") * params.gravame_rel, 
                F.lit(params.gravame_abs),
                F.lit(1),
                1 - F.col("pc_fncd_cilo")
            ),
        )
        .withColumn("vl_grvm_liq", F.col("vl_fncd_liq") * F.col("pc_grvm"))
    )


def _stress_arv_mrcd(df_calc: DataFrame, params: ParametrosStressed) -> DataFrame:
    """
    Calcula a razão entre o chargeback de ARV e os valores totais, e considera a razão de 
    crescimento do financiamento para avaliar o valor total de chargeback em ARV.

    Além disso, recalcula o valor de agenda legada baseado no parâmetro de financiamento
    ajustado e considerando a liquidação da agenda total

    :param df_calc: dados que serão usados como input nos calculos
    :param params: dataclass com parâmetros stressed a serem aplicados
    :return: data frame com parâmetros estressados
    """
    # calcula os novos valores de parâmetros
    df_calc = df_calc.withColumn(
        "pc_arv_mrcd", 
        F.least(
            F.greatest(F.col("pc_arv_mrcd") * params.arv_mrcd_rel, F.lit(params.arv_mrcd_abs)),
            F.lit(1)
        )
    )

    # calcula os multiplicadores a serem aplicados
    df_calc = (
        df_calc.withColumn(
            "multi", 
            F.when(
                F.col("pc_arv_mrcd_base") > 0,
                F.col("razo_arv_mrcd") * F.col("pc_arv_mrcd") / F.col("pc_arv_mrcd_base")
            )
            .otherwise(F.col("pc_arv_mrcd"))
        )
        .withColumn(
            "multi_icmo", 
            F.when(
                F.col("pc_arv_mrcd_base") > 0,
                F.col("razo_arv_mrcd_icmo") * F.col("pc_arv_mrcd") / F.col("pc_arv_mrcd_base")
            )
            .otherwise(F.col("pc_arv_mrcd"))
        )
    )

    # calcula os novos valores de chargeback
    return (
        df_calc.withColumn("vl_chrg_arv_mrcd", F.col("vl_chrg_prjt") * F.col("multi"))
        .withColumn("vl_chrg_arv_mrcd_icmo", F.col("vl_chrg_prjt_icmo") * F.col("multi_icmo"))
        .drop("pc_arv_mrcd_base", "multi", "multi_icmo")
    )
    

def processa_tbciar_re_mdlo_stsd(
    du: "DataUtils", 
    params: ParametrosStressed, 
    incluir_canc: bool = True, 
    lista_de_grupos: typing.Optional[typing.List[str]] = None,
    data_max: typing.Optional[str] = None,
) -> DataFrame:
    """
    A partir dos dados calculados de reserva seleciona qual reserva deverá ser aplicada
    de acordo com o risco aceitável e o perfil de financiamento do cliente

    :param du: instância do data utils
    :param params: dataclass com parâmetros stressed a serem aplicados
    :param incluir_canc: True se queremos incluir cancelamentos nos dados de curva
    :param lista_de_grupos: lista de grupos de cnpj para aplicar a modelagem
    :param data_max: data a ser considerada para calculo do multiplicador de grupo
    :return: data frame com o valor de reserva aplicado e o cenário de churn a ser considerado
    """
    from modelo.comuns import obtem_ultima_data_e_hora
    from modelo.perda import realiza_calculo_perda

    # Obter o valor da data máxima que deveria ser utilizada
    if data_max is None:
        data_max = max([v["cd_dia_crte"] for v in du.get_partition_values("GRUPO_CNPJ", "internal")])
    else:
        data_max = max([
            v["cd_dia_crte"]
            for v in du.get_partition_values("GRUPO_CNPJ", "internal")
            if v["cd_dia_crte"] <= data_max
        ])
    data_max_int = int(data_max)

    # carrega as bases de dados
    drop_cols = ["cd_dia_crte", "dh_crga", "dc_prmt_exec"]
    df_calc = obtem_ultima_data_e_hora(du, data_max_int, "PARAMS_PROJ").drop(*drop_cols)
    df_rsrv = obtem_ultima_data_e_hora(du, data_max_int, "RESERVA_OTIMA").select("cd_grpo_cnpj", "dc_tipo_churn", "vl_rsrv")
    df_churn = obtem_ultima_data_e_hora(du, data_max_int, "PROB_CHURN").drop(*drop_cols)

    # filtra para a lista de grupos
    if lista_de_grupos:
        df_calc = df_calc.filter(F.col("cd_grpo_cnpj").isin(lista_de_grupos))

    # cria as razões inicias do modelo que serão necessárias para as aplicações dos valores stressed
    df_calc = (
        df_calc.withColumn("pc_rbrp_base", F.col("pc_rbrp"))
        .withColumn("pc_fncd_base", F.col("pc_fncd"))
        .withColumn("pc_fncd_cilo_base", F.col("pc_fncd_cilo"))
        .withColumn("pc_grvm_base", F.col("pc_grvm"))
        .withColumn("pc_arv_mrcd_base", F.col("razo_arv_mrcd"))
        .withColumnRenamed("razo_arv_mrcd", "pc_arv_mrcd")
        .withColumn(
            "razo_arv_cilo", 
            F.coalesce(
                F.col("vl_chrg_arv_cilo") / (F.col("vl_chrg_prjt") - F.col("vl_chrg_rbrp") - F.col("vl_chrg_liqd")),
                F.lit(0)
            )
        )
        .withColumn(
            "razo_arv_cilo_icmo", 
            F.coalesce(
                F.col("vl_chrg_arv_cilo_icmo") 
                / (F.col("vl_chrg_prjt_icmo") - F.col("vl_chrg_rbrp_icmo") - F.col("vl_chrg_liqd_icmo")),
                F.lit(0)
            )
        )
        .withColumn("razo_arv_mrcd", F.coalesce(F.col("vl_chrg_arv_mrcd") / F.col("vl_chrg_prjt"), F.lit(0)))
        .withColumn("razo_arv_mrcd_icmo", F.coalesce(F.col("vl_chrg_arv_mrcd_icmo") / F.col("vl_chrg_prjt_icmo"), F.lit(0)))
    )

    # calcula os valores de chargeback
    if incluir_canc:
        df_calc = (
            df_calc.withColumn("vl_chrg_prjt", F.col("vl_chrg_prjt") + F.col("vl_cncl_prjt"))
            .withColumn("vl_chrg_liqd", F.col("vl_chrg_liqd") + F.col("vl_cncl_liqd"))
            .withColumn("vl_chrg_rbrp", F.col("vl_chrg_rbrp") + F.col("vl_cncl_rbrp"))
            .withColumn("vl_chrg_arv_cilo", F.col("vl_chrg_arv_cilo") + F.col("vl_cncl_arv_cilo"))
            .withColumn("vl_chrg_arv_mrcd", F.col("vl_chrg_arv_mrcd") + F.col("vl_cncl_arv_mrcd"))
            .withColumn("vl_chrg_prjt_icmo", F.col("vl_chrg_prjt_icmo") + F.col("vl_cncl_prjt_icmo"))
            .withColumn("vl_chrg_liqd_icmo", F.col("vl_chrg_liqd_icmo") + F.col("vl_cncl_liqd_icmo"))
            .withColumn("vl_chrg_rbrp_icmo", F.col("vl_chrg_rbrp_icmo") + F.col("vl_cncl_rbrp_icmo"))
            .withColumn("vl_chrg_arv_cilo_icmo", F.col("vl_chrg_arv_cilo_icmo") + F.col("vl_cncl_arv_cilo_icmo"))
            .withColumn("vl_chrg_arv_mrcd_icmo", F.col("vl_chrg_arv_mrcd_icmo") + F.col("vl_cncl_arv_mrcd_icmo"))
        )

    # aplica os valores de stress
    df_calc = _stress_cgb(df_calc, params)
    df_calc = _stress_canc(df_calc, params)
    df_calc = _stress_rbrp(df_calc, params)
    df_calc = _stress_arv(df_calc, params)
    df_calc = _stress_gravame(df_calc, params)
    df_calc = _stress_arv_mrcd(df_calc, params)

    # carrega os dados de churn
    df_churn = (
        df_churn.select("cd_grpo_cnpj", "pc_prbl_churn")
        .withColumn(
            "pc_prbl_churn", 
            F.least(
                F.greatest(F.col("pc_prbl_churn") * params.churn_rel, F.lit(params.churn_abs)),
                F.lit(1)
            )
        )
    )

    # consolida os dados de projeção em um array para cada CNPJ considerando os cenários de churn 60 e churn 100
    df_prjt = (
        df_calc.withColumn("vl_chrg_arv", F.col("vl_chrg_arv_cilo") + F.col("vl_chrg_arv_mrcd"))
        .withColumn("vl_chrg_arv_icmo", F.col("vl_chrg_arv_cilo_icmo") + F.col("vl_chrg_arv_mrcd_icmo"))
    )

    # gera o calculo de perda stressed
    df_perda = realiza_calculo_perda(
        du=du, 
        df_proj=df_prjt, 
        df_rsva=df_rsrv,
        df_churn=df_churn,
        dt_cnpj=data_max,
    )

    return (
        df_perda.withColumn("dc_prmt_exec", F.lit(f"incluir_canc={incluir_canc}"))
        .withColumn("cd_dia_crte", F.lit(int(data_max)))
        .withColumn("dh_crga", F.lit(datetime.now(timezone.utc).strftime("%Y%m%d%H%M%S")))
    )


def executa_mdlo_stsd(params: list) -> None:
    """
    Função wrapper para executar a base tbciar_re_mdlo_stsd

    :param params: parâmetros de execução
    """
    import modelo.configs as confs

    # pega os parâmetros de execução
    env = params[0] if len(params) > 0 else "PROD"
    data_max = params[1] if len(params) > 1 else None
    incluir_canc = params[2] if len(params) > 2 else str(confs.INCLUIR_CANC)

    # corrige nulos
    env = "PROD" if env == "" else env    
    data_max = None if data_max == "" else data_max
    incluir_canc = confs.INCLUIR_CANC if incluir_canc == "" else int(incluir_canc)

    # instância o data utils
    from utils.data_utils import DataUtils
    du = DataUtils(env)

    # chama a função
    df = processa_tbciar_re_mdlo_stsd(du, ParametrosStressed(), incluir_canc, data_max=data_max)

    # escreve o resultado para o disco
    du.write_by_name(df, "STRESSED", "internal", partitions=["cd_dia_crte", "dh_crga"], mode="overwrite")

if __name__ == "__main__":
    # cria a sessão do spark
    spark = (
        SparkSession.builder
        .appName("SPARK_prod_prazo_tbciar_re_mdlo_stsd")
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
    executa_mdlo_stsd(sys.argv[1:])