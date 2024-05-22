import sys
import typing

from datetime import datetime
from datetime import timezone
from dateutil.relativedelta import relativedelta
from functools import reduce
from operator import mul

import pyspark.sql.functions as F
import pyspark.sql.types as T
from pyspark.sql import DataFrame
from pyspark.sql import SparkSession
from pyspark.sql.window import Window


class VRLiquidar:
    def __init__(
        self, 
        du: "DataUtils",
        janela: int,
        dias_cruzamento: typing.Dict[str, int],
        tipo_peso: str,
        tipo_valor: str,
    ) -> None:
        self.du = du
        self.janela = janela
        self.dias_cruzamento = dias_cruzamento
        self.tipo_peso = tipo_peso
        self.tipo_valor = tipo_valor
        self.raw_chrg = self.du.get_by_name("CHARGEBACK", "internal")
        self.raw_cncl = self.du.get_by_name("CANCELAMENTO", "internal")

    def _salva_dados(self, df: DataFrame) -> None:
        """
        Dado um dataframe com dados de valores a liquidar
    
        :param df (DataFrame): Dados novos que serao salvos na tabela
        """
        self.du.write_by_name(df, "VR_LIQ", "internal", partitions=["cd_dia_crte", "dh_crga"], mode="overwrite")

    def _prepara_dados(
        self,        
        df: DataFrame, 
        evento: str,
        sigla: str,
        data_min_int: int, 
        data_max_int: int, 
        data_min_dt: datetime
    ) -> DataFrame:
        """
        Prepara os dados de chargeback/cancelamento para uso no calculo de valor recuperável
        
        :param df: data frame com os dados brutos de chargeback/cancelamento
        :param evento: nome do evento (chargeback/cancelamento)
        :param sigla: sigla a ser usada para o evento (chrg/cncl)
        :param data_min_int: data minima para considerar chargeback/cancelamento como inteiro YYYYMMDD
        :param data_max_int: data maxima para considerar chargeback/cancelamento como inteiro YYYYMMDD
        :param data_min_int: data minima para considerar chargeback/cancelamento como datetime
        :return: base de chargeback/cancelamento pré-processada
        """
        from modelo.comuns import classifica_em_grupos, obtem_ultima_data_e_hora
        from utils.math import aplica_peso_a_dif_datas

        # obtém a lista de ECs a serem abertos por cadeia forçada
        lista_cf = [
            r[0]
            for r in (
                obtem_ultima_data_e_hora(self.du, data_max_int, "CF_CLUSTER")
                .select("nm_grpo")
                .distinct()
                .collect()
            )
        ]

        # filtra a base de chargeback/cancelamento para as datas selecionadas
        # NOTA: para RR nós assumimos a curva de liquidação normal dado que os dados de
        # data de vencimento não condizem com o esperado
        df_evento = (
            df.filter(
                (F.col("cd_dh_incl_rgst") >= data_min_int) 
                & ((F.col(f"dt_{sigla}") >= data_min_dt))
                & (F.col("cd_dh_incl_rgst") <= data_max_int)
                & (F.col("nu_dia_vnda") <= self.dias_cruzamento[sigla])
                & (F.col("nu_dia_vnda") >= 2)
                & (F.col("nm_tipo_trns").isNotNull())
            )
            .withColumn(
                "nm_grpo", 
                F.when(
                    F.col("nm_cdfr") == "NAO SE APLICA", 
                    F.col("nu_cnpj9_cpf").cast("string")
                )
                .otherwise(F.col("nm_cdfr"))
            )
            .withColumn(
                f"vl_{self.tipo_valor}_liq",
                F.when(F.col("in_rbrp") == 0, F.col(f"vl_{self.tipo_valor}_liq"))
                .when(F.col("nu_dia_vnda") <= 2, F.col(f"vl_{self.tipo_valor}_trns"))
                .when(F.col("nm_tipo_trns") == "DÉBITO", F.lit(0))
                .when(
                    F.col("nm_tipo_trns") == "CRÉDITO À VISTA",
                    F.when(F.col("nu_dia_vnda") <= 30, F.col(f"vl_{self.tipo_valor}_trns"))
                    .otherwise(F.lit(0))
                )
                .otherwise(
                    F.col(f"vl_{self.tipo_valor}_trns") * (
                        1
                        - F.least(F.floor(F.col("nu_dia_vnda") / 30), F.col("qt_prcl"))
                        / F.col("qt_prcl")
                    )
                )
            )
        )

        # aplica os dados de cluster
        df_evento = classifica_em_grupos(df_evento, lista_cf, alternativas=False)

        # calcula um peso a ser considerado para cada data point de acordo
        # com a distancia até a data mínima
        df_evento = (
            aplica_peso_a_dif_datas(
                df=df_evento.withColumn("dt_min", F.lit(data_min_dt)),
                dt_col_1="dt_min",
                dt_col_2=f"dt_{sigla}",
                tipo_peso=self.tipo_peso
            )
            .drop("dt_min")
            .select(
                "*", 
                (F.col(f"vl_{self.tipo_valor}_liq") * F.col("peso")).alias("vl_liq_pndr"),
                (F.col(f"vl_{self.tipo_valor}_{sigla}") * F.col("peso")).alias(f"vl_{sigla}_pndr"),
            )
            .persist()
        )

        return df_evento
    
    def _gera_curvas_de_vr(self, df_evento: DataFrame, sigla: str, id_cols: typing.List[str]) -> DataFrame:
        """
        Gera as curvas de valor recuperável dado o cluster selecionado

        :param df_evento: base de chargeback/cancelamento pré-processadas
        :param sigla: sigla a ser usada para o evento (chrg/cncl)
        :param id_cols: lista de colunas para agregar dados
        :return: base de valore recuperável por dias e cluster
        """
        from utils.math import fill_linear_interpolation
        
        # agrega os dados de chargeback
        df_vr = (
            df_evento.groupBy(id_cols + ["nu_dia_vnda"])
            .agg(
                F.sum(F.col("vl_liq_pndr")).alias("vl_liq_pndr"),
                F.abs(F.sum(F.col(f"vl_{sigla}_pndr"))).alias(f"vl_{sigla}_pndr"),
                F.countDistinct(F.col(f"nu_{sigla}")).alias("qt_trns"),
            )
        )

        # expande a curva de valor recuperável 
        curva_dias = (
            df_vr.select(*id_cols).distinct() 
            .select("*", F.explode(F.array([F.lit(i) for i in range(541)])).alias("nu_dia_vnda"))
        )
        df_vr_exp = curva_dias.join(
            df_vr, on=id_cols + ["nu_dia_vnda"], how="left"
        )

        # vamos tentar fazer o preenchimento de maneira inteligente
        # há três fatores que contribuem para que o valor calculado de recuperação
        # de chargeback não seja exatamento 1, 0.8, 0.6 ... seguindo o padrão de liquidação:
        # 1. dias úteis - transações só são liquidas em dias úteis fazendo com que os intervalos não sejam exatamente 30 dias
        # 2. cancelamentos - um cancelamento pode ocorrer entre datas fazendo com que o valor recuperável caia
        # 3. negociações - alguns ECs possuem negociações para fazer liquidações em datas especiais
        # lenvando isso em conta, vamos calcular para cada curva um fator de desvio da norma
        # para projetar os valores faltantes e preencher nulos
        # computa o valor esperado
        df_vr_exp = (
            df_vr_exp.withColumn("cd_dia", F.floor(F.col("nu_dia_vnda") / 30))
            .withColumn(
                "vl_esperado",
                F.when(F.col("nu_dia_vnda") <= 2, F.lit(1))
                .when(F.col("nm_tipo_trns") == "DÉBITO", F.lit(0))
                .when(
                    F.col("nm_tipo_trns") == "CRÉDITO À VISTA",
                    F.when(F.col("nu_dia_vnda") <= 25, F.lit(1))
                    .when(F.col("nu_dia_vnda") >= 35, F.lit(0))
                    .otherwise(F.lit(None))
                )
                .when(
                    (F.col("nu_dia_vnda") > (30 * F.col("cd_dia")) + 5)
                    & (F.col("cd_dia") >= F.col("qt_prcl")),
                    F.lit(0)
                )
                .when(
                    (F.col("nu_dia_vnda") >= (30 * F.col("cd_dia")) + 5)
                    & (F.col("nu_dia_vnda") < (30 * (F.col("cd_dia") + 1)) - 5),
                    F.greatest(
                        F.col("qt_prcl") - F.col("cd_dia"),
                        F.lit(0)
                    ) / F.col("qt_prcl")
                )
                .when(F.col("nu_dia_vnda").isin([3, 4]), F.lit(1))
                .otherwise(F.lit(None))
            )
            .withColumn("vl_pc_vr_liq", F.col("vl_liq_pndr") / F.col(f"vl_{sigla}_pndr"))
        )
        df_ag = (
            df_vr_exp.filter((F.col(f"vl_{sigla}_pndr").isNotNull()) & (F.col("vl_esperado").isNotNull()))
            .groupBy(id_cols + ["cd_dia"])
            .agg(
                F.sum(F.col("vl_esperado") * F.col(f"vl_{sigla}_pndr")).alias("vl_esperado"),
                F.sum(F.col("vl_liq_pndr")).alias("vl_liq_pndr")
            )
            .withColumn("razao_vl_esp", F.col("vl_liq_pndr") / F.col("vl_esperado"))
            .drop("vl_esperado", "vl_liq_pndr")
            .cache()
        )

        # é importante ressaltar que os cancelamentos em compras parceladas
        # são aplicados primeiro nas parcelas futuras, portanto, quanto menor
        # a razão dentro dos X dias, menor ela será acima de X dias 
        mul_udf = F.udf(lambda x: reduce(mul, x), T.FloatType())
        df_proj = (
            df_ag.withColumn(
                "var_razao", 
                F.col("razao_vl_esp") 
                / F.lag(F.col("razao_vl_esp")).over(
                    Window.partitionBy(id_cols).orderBy("cd_dia")
                )
            )
            .withColumn(
                "var_razao", 
                F.when(F.col("var_razao") == 1, F.lit(None)).otherwise(F.col("var_razao"))
            )
            .withColumn(
                "var_razao",
                F.last(F.col("var_razao"), True).over(
                    Window.partitionBy(id_cols)
                    .orderBy("cd_dia")
                    .rowsBetween(Window.unboundedPreceding, Window.currentRow)
                )
            )
            .withColumn(
                "razao_vl_esp",
                F.last(F.col("razao_vl_esp"), True).over(
                    Window.partitionBy(id_cols)
                    .orderBy("cd_dia")
                    .rowsBetween(Window.unboundedPreceding, Window.currentRow)
                )
            )
            .withColumn("var_razao", F.least(F.col("var_razao"), F.lit(1)))
            .fillna(1, subset=["var_razao"])
            .filter(F.col("cd_dia") >= self.dias_cruzamento[sigla] // 30)
            .withColumn(
                "var_cp", mul_udf(
                    F.collect_list(F.col("var_razao"))
                    .over(Window.partitionBy(id_cols).orderBy("cd_dia"))
                )
            )
            .withColumn("razao_vl_esp_2", F.col("var_cp") * F.col("razao_vl_esp"))
            .drop("var_razao", "var_cp", "razao_vl_esp")
        )

        # adiciona os dados de razão e interpola os resultados
        df_vr_exp = (
            fill_linear_interpolation(
                df_vr_exp.join(df_ag, on=id_cols + ["cd_dia"], how="left")
                .join(df_proj, on=id_cols + ["cd_dia"], how="left")
                .withColumn(
                    "vl_vr_liq", 
                    F.when(
                        F.col("vl_pc_vr_liq").isNull(),
                        F.when(F.col("nu_dia_vnda") <= 2, F.lit(1))
                        .when(
                            F.col("nu_dia_vnda") <= self.dias_cruzamento[sigla], 
                            F.col("vl_esperado") * F.col("razao_vl_esp")
                        )
                        .otherwise(F.col("vl_esperado") * F.col("razao_vl_esp_2"))
                    ).otherwise(F.col("vl_pc_vr_liq"))
                ),
                id_cols=id_cols,
                order_col="nu_dia_vnda",
                value_col="vl_vr_liq",
            )
            .fillna(0, subset=["vl_vr_liq", "qt_trns"])
            .withColumn(f"vl_{sigla}_liq", F.least(F.col("vl_vr_liq"), F.lit(1)))
            .select(*id_cols + ["nu_dia_vnda", "qt_trns", f"vl_{sigla}_liq"])
            .cache()
        )

        return df_vr_exp

    def _calcula_valor_a_liq(
        self,        
        data_min: typing.Optional[str] = None, 
        data_max: typing.Optional[str] = None,
    ) -> DataFrame:   
        """
        A partir da base de chargeback com os valores a liquidar pré-calculados 
        gera uma curva de valores a liquidar para cada cluster e quantidade de parcelas
        
        :param data_min: data mínima para fazer o calculo das curvas de valor a liquidar
        :param data_max: data máxima para fazer o calculo das curvas de valor a liquidar
        :return DataFrame: Retorna novos dados a serem salvos na tabela de cruzamento
        """ 
        # obtém os valores de datas mínimas e máximas que deveriam ser utilizados
        if data_min is not None and data_max is not None:
            # se ambos os valores forem dados os utiliza diretamente
            data_min_dt = datetime(int(data_min[:4]), int(data_min[4:6]), int(data_min[6:]))
            data_max_dt = datetime(int(data_max[:4]), int(data_max[4:6]), int(data_max[6:]))
        elif data_min is not None and data_max is None:
            # se só o valor da data minima for dado, aplica a jenala a partir da data minima
            data_min_dt = datetime(int(data_min[:4]), int(data_min[4:6]), int(data_min[6:]))
            data_max_dt = data_min_dt + relativedelta(months=self.janela)
        elif data_min is None and data_max is not None:
            # se só o valor da data maxima for dado, aplica a jenala a partir da data maxima
            data_max_dt = datetime(int(data_max[:4]), int(data_max[4:6]), int(data_max[6:]))
            data_min_dt = data_max_dt - relativedelta(months=self.janela)
        else:
            # se nenhum for dado, usa o último dia da base de chargeback com a janela aplicada
            data_max = data_max = min(
                max([v["cd_dia_crte"] for v in self.du.get_partition_values("TRNS_CLUSTER", "internal")]),
                max([v["cd_dh_incl_rgst"] for v in self.du.get_partition_values("CHARGEBACK", "internal")]),
                max([v["cd_dh_incl_rgst"] for v in self.du.get_partition_values("CANCELAMENTO", "internal")]),
            )
            data_max_dt = datetime(int(data_max[:4]), int(data_max[4:6]), int(data_max[6:]))
            data_min_dt = data_max_dt - relativedelta(months=self.janela)

        # cria as versões inteiras dos valores
        data_min_int = int(data_min_dt.strftime("%Y%m%d"))
        data_max_int = int(data_max_dt.strftime("%Y%m%d"))

        # gera a base de chargeback e cancelamentos
        df_chargebacks = self._prepara_dados(            
            df=self.raw_chrg, 
            evento="chargeback",
            sigla="chrg",
            data_min_int=data_min_int, 
            data_max_int=data_max_int, 
            data_min_dt=data_min_dt,
        )
        df_cancelamento = self._prepara_dados(            
            df=self.raw_cncl, 
            evento="cancelamento",
            sigla="cncl",
            data_min_int=data_min_int, 
            data_max_int=data_max_int, 
            data_min_dt=data_min_dt,
        )

        # gera a base de valor recuperável para diferentes clusters
        clusters = ["cd_clsr"] + [c for c in df_chargebacks.columns if c.startswith("cd_alt_")]
        id_cols = ["nm_tipo_trns", "qt_prcl"]
        outputs = list()
        for df, s, col in [(df_chargebacks, "chrg", "cncl"), (df_cancelamento, "cncl", "chrg")]:
            for c in clusters:
                outputs.append(
                    self._gera_curvas_de_vr(df, s, [c] + id_cols)
                    .withColumnRenamed(c, "cd_clsr")
                    .withColumnRenamed("qt_trns", f"qt_{s}")
                    .withColumn(f"vl_{col}_liq", F.lit(0))
                    .withColumn(f"qt_{col}", F.lit(0))
                )
        df_vr = reduce(DataFrame.unionByName, outputs)

        # retorna o resultado com os parâmetros utilizados
        id_curva = (
            f"data_min={data_min_int};" 
            + f"data_max={data_max_int};"
            + f"dias_chrg={self.dias_cruzamento['chrg']};"
            + f"dias_cncl={self.dias_cruzamento['cncl']};"
            + f"tipo_peso={self.tipo_peso};"
            + f"tipo_valor={self.tipo_valor};"
        )
        return (
            df_vr.groupBy(
                "cd_clsr", 
                "nm_tipo_trns",
                "qt_prcl", 
                "nu_dia_vnda", 
            )
            .agg(
                F.sum("qt_chrg").alias("qt_chrg"),
                F.sum("vl_chrg_liq").alias("vl_chrg_liq"),
                F.sum("qt_cncl").alias("qt_cncl"),
                F.sum("vl_cncl_liq").alias("vl_cncl_liq"),
            )
            .withColumn("cd_dia_crte", F.lit(data_max_int))
            .withColumn("dh_crga", F.lit(datetime.now(timezone.utc).strftime("%Y%m%d%H%M%S")))
            .withColumn("dc_prmt_exec", F.lit(id_curva))
        )
        

    def executar(
        self, data_min: typing.Optional[str] = None, data_max: typing.Optional[str] = None,
    ) -> None:
        """
        Metodo inicializador da classe e responsavel pela chamados das outras funcoes.

        :param data_min: data mínima para fazer o calculo das curvas de valor a liquidar
        :param data_max: data máxima para fazer o calculo das curvas de valor a liquidar
        """
        df = self._calcula_valor_a_liq(data_min, data_max)
        self._salva_dados(df)


def executa_mdlo_vlrcpv_liq(params: list) -> None:
    """
    Função wrapper para executar a base tbciar_re_mdlo_vlrcpv_liq

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
    proc = VRLiquidar(
        du=du,
        janela=janela,
        dias_cruzamento={"chrg": dias_chrg, "cncl": dias_cncl},
        tipo_valor=tipo_valor,
        tipo_peso=tipo_peso,
    )
    proc.executar(data_min, data_max)


if __name__ == "__main__":
    # instancia a sessão do spark
    spark = (
        SparkSession.builder.appName("SPARK_prod_prazo_tbciar_re_mdlo_vlrcpv_liq")
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
    executa_mdlo_vlrcpv_liq(sys.argv[1:])


