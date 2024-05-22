import sys
import typing

from datetime import datetime
from dateutil.relativedelta import relativedelta
from functools import reduce
from operator import add

from pyspark.sql import DataFrame
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.window import Window

class CruzamentoCancelamentos:
    DATA_LIMITE: str = "20220101"
    DIAS_DE_CALCULO: int = 360

    def __init__(self, du: "DataUtils"):
        self.du = du
        
        self.raw_trns = self.du.get_by_name("TRNS")
        self.raw_bndr = self.du.get_by_name("BANDEIRA")
        self.raw_ec = self.du.get_by_name("EC", "internal")
        
        # cria bases que devem ser pré-processadas
        self.bndr_ = None
        self.ec_ = None
        
    @property
    def bndr(self) -> DataFrame:
        """
        Pré-processa a base de bandeira para potencialmente ser utilizada
        em múltiplos dias da base
        
        :return: base de bandeira pré-processada
        """
        if self.bndr_ is None:
            self.bndr_ = self.raw_bndr.select(
                F.col("cd_bandeira").alias("cd_bndr"), F.col("nm_bandeira").alias("nm_bndr")
            ).distinct().persist()
        return self.bndr_
    
    @property
    def ec(self) -> DataFrame:
        """
        Pré-processa a base de bandeira para potencialmente ser utilizada
        em múltiplos dias da base
        
        :return: base de bandeira pré-processada
        """
        if self.ec_ is None:
            self.ec_ = (
                self.raw_ec.select(
                    "nu_ec",
                    "nu_cnpj9_cpf",
                    "nm_unde_ngco_fpna",
                    "nm_sgmt_cmrl",
                    "cd_so_ramo_atvd",
                    "nm_cdfr",
                )
                .distinct()
                .persist()
            )
        return self.ec_

    def _busca_maior_data(
        self, 
        data_min: typing.Optional[str] = None,
        data_max: typing.Optional[str] = None,
    ) -> typing.Tuple[str, str]:
        """
        Busca o ultimo dado inserido na tabela de cruzamento de cancelamento 
        para usa-lo como filtro de particao para dados novos.

        :param data_min: data minima de carga histórica selecionada
        :param data_max: data maxima de carga histórica selecionada
        :return: data minima e data maxima 
        """
        # obtém a data mínima e data máxima
        if data_min is None:
            if self.du.is_table_available("CANCELAMENTO", "internal"):
                data_min = max([v["cd_dh_incl_rgst"] for v in self.du.get_partition_values("CANCELAMENTO", "internal")])
                data_min = (datetime.strptime(data_min, "%Y%m%d") + relativedelta(days=1)).strftime("%Y%m%d")
            else:
                data_min = min([v["cd_dh_incl_rgst"] for v in self.du.get_partition_values("TRNS", "external")])
        data_min = max(data_min, self.DATA_LIMITE)
        if data_max is None:
            data_max = max([v["cd_dh_incl_rgst"] for v in self.du.get_partition_values("TRNS", "external")])
        
        return data_min, data_max

    def _salva_dados(self, df: DataFrame) -> None:
        """
        Dado um dataframe com dados de cruzamento de cancelamentos salva na tabela.
    
        :param df (DataFrame): Dados novos que serao salvos na tabela
        """
        from utils.renames import renomeia_para_padrao
        self.du.write_by_name(renomeia_para_padrao(df), "CANCELAMENTO", "internal", partitions="cd_dh_incl_rgst", mode="overwrite")

    def _busca_novos_cancelamentos(self, data_min: str, data_max: str) -> DataFrame:   
        """
        Dados a entrada da uma particao inserida na tabela de cruzamento 
        busca por novos cancelamentos na tabela de transacao, cruza dados 
        com transacoes e forma a saida nova a ser salva.
        
        :param data_min: data minima de carga histórica selecionada
        :param data_max: data maxima de carga histórica selecionada
        :return DataFrame: Retorna novos dados a serem salvos na tabela de cruzamento
        """ 
        data_min_dt = datetime.strptime(data_min, "%Y%m%d")
        data_limite = datetime.strptime(self.DATA_LIMITE, "%Y%m%d")
        min_trns = max(data_min_dt - relativedelta(days=self.DIAS_DE_CALCULO + 1), data_limite)
        
        # lista os dados de cancelamento
        canc = (
            self.raw_trns.filter(
                (F.col("cd_dh_incl_rgst") >= int(data_min))
                & (F.col("cd_dh_incl_rgst") <= int(data_max))
                & (F.col("cd_tipo_lncm") == 6)
                & (F.col("dt_cptr_trns") >= min_trns)
            )
            .select(
                "nu_sqnl_agnd_trns",
                "cd_dh_incl_rgst",
                F.trim(F.col("nu_trns")).alias("nu_cncl"),
                F.trim(F.col("cd_idtf_trns_orig")).alias("nu_trns_orig"),
                F.to_date(F.col("dt_atzc_trns")).alias("dt_trns"),
                F.to_date(F.col("dt_cptr_trns")).alias("dt_cncl"),
                F.col("cd_bndr").alias("cd_bndr"),
                "dc_mtvo_ajst",
                "nu_ec",
                "nu_cnpj_cpf_recr",
                F.col("cd_moda").alias("cd_moda"),
                F.col("vl_brto_trns_recb").alias("vl_brto_cncl"),
                F.col("vl_lqdo_trns").alias("vl_lqdo_cncl"),
                "nu_idtr_trns",
                "in_rbrp"
            )
        )

        # se existir uma base de cancelamento, pega os últimos 540 dias antes da data minima
        # e remove os cancelamentos já processados anteriorment
        if self.du.is_table_available("CANCELAMENTO", "internal"):
            df_pre = (
                self.du.get_by_name("CANCELAMENTO")
                .filter(
                    (F.col("cd_dh_incl_rgst") >= int((data_min_dt - relativedelta(days=540)).strftime("%Y%m%d")))
                    & (F.col("cd_dh_incl_rgst") < int(data_min_dt.strftime("%Y%m%d")))
                )
                .select("nu_cncl", F.lit(1).alias("in_pp"))
                .distinct()
                .checkpoint()
            )
            canc = canc.join(df_pre, on="nu_cncl", how="left").filter(F.col("in_pp").isNull()).drop("in_pp")

        # remove duplicatas da base restante
        windowSpec = Window.partitionBy("nu_cncl", "cd_dh_incl_rgst")
        df_cancelamentos = (
            canc.withColumn("nu_max", F.max("nu_sqnl_agnd_trns").over(windowSpec))
            .withColumn("nu_min", F.min("nu_sqnl_agnd_trns").over(windowSpec))
            .filter(
                F.when(F.col("nu_max") >= 999, F.col("nu_sqnl_agnd_trns") == F.col("nu_max"))
                .otherwise(F.col("nu_sqnl_agnd_trns") == F.col("nu_min"))
            )
            .withColumn("min_partition", F.min("cd_dh_incl_rgst").over(Window.partitionBy("nu_cncl")))
            .filter(F.col("cd_dh_incl_rgst") == F.col("min_partition"))
            .drop("min_partition", "nu_sqnl_agnd_trns", "nu_max", "nu_min")
            .distinct()
            .persist()
        )

        # cria uma base da qual extraíremos todos os dados
        trns = (
            self.raw_trns.filter(
                (F.col("cd_dh_incl_rgst") >= int(min_trns.strftime("%Y%m%d")))
                & (F.col("cd_dh_incl_rgst") <= int(data_max))
                & (F.col("nu_sqnl_agnd_trns") != 999999999)
                & (F.col("nu_sqnl_agnd_trns") != 999)
                & (F.col("cd_tipo_lncm").isin([1, 2, 3]))
            )
            .select(
                "cd_tipo_lncm",
                F.trim(F.col("nu_trns")).alias("nu_trns_orig"),
                F.to_date(F.col("dt_atzc_trns")).alias("dt_trns"),
                "dt_vncm_unde_rcbv",
                F.col("qt_prcl_trns").alias("qt_prcl"),
                "nu_sqnl_agnd_trns",
                "in_rbrp",
                "nu_idtr_trns",
                F.col("vl_brto_trns_recb").alias("vl_brto_trns"),
                "vl_lqdo_trns",
                F.col("vl_brto_agnd_trns").alias("vl_brto_agnd"),
                F.col("vl_lqdo_agnd_trns").alias("vl_lqdo_agnd"),
                "cd_bndr",
            )
            .join(
                df_cancelamentos.select("nu_trns_orig").distinct(),
                on=["nu_trns_orig"],
                how="inner",
            )
            .persist()
        )

        # obtém os dados de vendas a serem cruzados
        df_valor = (
            trns.groupBy("nu_trns_orig", "dt_vncm_unde_rcbv")
            .agg(
                F.sum(F.col("vl_brto_agnd")).alias("vl_brto_agnd"), 
                F.sum(F.col("vl_lqdo_agnd")).alias("vl_lqdo_agnd"),
            )
            .filter(F.col("vl_brto_agnd") > 0)
        )
        df_info = (
            trns.filter(
                ((F.col("cd_tipo_lncm") == 3) & (F.col("nu_sqnl_agnd_trns") == 1))
                | ((F.col("cd_tipo_lncm").isin([1, 2])) & (F.col("nu_sqnl_agnd_trns") == 0))
            )
            .select(
                "nu_trns_orig", 
                "cd_tipo_lncm", 
                "dt_trns",
                "nu_idtr_trns", 
                "qt_prcl", 
                "in_rbrp",
                "vl_brto_trns", 
                "vl_lqdo_trns",
                "cd_bndr",
            )
        )

        # adiciona as informações das transações
        df_cancelamentos = (
            df_cancelamentos.alias("a")
            .join(df_info.alias("b"), on="nu_trns_orig", how="left")
            .select(
                "a.cd_dh_incl_rgst",
                "a.nu_cncl",
                "a.nu_trns_orig",
                "a.nu_ec",
                "a.nu_cnpj_cpf_recr", 
                F.coalesce(F.col("b.dt_trns"), F.col("a.dt_trns")).alias("dt_trns"),
                "a.dt_cncl",
                F.when(F.col("b.cd_tipo_lncm") == 1, "DÉBITO")
                .when(F.col("b.cd_tipo_lncm") == 2, "CRÉDITO À VISTA")
                .when(F.col("b.cd_tipo_lncm") == 3, "CRÉDITO PARCELADO")
                .otherwise(F.lit(None))
                .alias("nm_tipo_trns"),
                "b.qt_prcl",
                F.when(F.col("b.cd_tipo_lncm").isNull(), F.lit(None))
                .when(
                    F.coalesce(F.col("b.nu_idtr_trns"), F.col("a.nu_idtr_trns"))
                    .isNull(),
                    1
                )
                .otherwise(0)
                .alias("in_crto_psen"),
                F.coalesce("b.cd_bndr", "a.cd_bndr").alias("cd_bndr"),
                "a.cd_moda",
                "a.dc_mtvo_ajst",
                F.when(F.col("b.cd_tipo_lncm").isNull(), F.lit(None))
                .when(
                    F.coalesce(F.col("b.in_rbrp"), F.col("a.in_rbrp")) == "S",
                    1
                )
                .otherwise(0)
                .alias("in_rbrp"),
                "a.vl_brto_cncl",
                "a.vl_lqdo_cncl",
                "b.vl_brto_trns",
                "b.vl_lqdo_trns",
            )
            .filter(F.col("dt_trns") >= min_trns)
            .select("*", F.datediff(F.col("dt_cncl"), F.col("dt_trns")).alias("nu_dia_vnda"))
        )

        # calcula o valor a liquidar para cada cancelamento
        df_liquidar = (
            df_cancelamentos.select("nu_cncl", "dt_cncl", "nu_trns_orig").alias("a")
            .join(
                df_valor.alias("b"),
                on=(
                    (F.col("a.nu_trns_orig") == F.col("b.nu_trns_orig"))
                    & (F.col("a.dt_cncl") <= F.col("b.dt_vncm_unde_rcbv"))
                ),
                how="left"
            )
            .groupBy("nu_cncl")
            .agg(
                F.sum(F.col("vl_brto_agnd")).alias("vl_brto_liq"),
                F.sum(F.col("vl_lqdo_agnd")).alias("vl_lqdo_liq"),
            )
        )
        
        # adiciona o valor a liquidar
        df_cancelamentos = (
            df_cancelamentos.join(df_liquidar, on="nu_cncl", how="left")
            .select(
                list(df_cancelamentos.columns)
                + [
                    F.when(F.col("nm_tipo_trns").isNull(), F.lit(None))
                    .when(F.col(c).isNull(), F.lit(0))
                    .otherwise(F.col(c))
                    .alias(c)
                    for c in ["vl_brto_liq", "vl_lqdo_liq"]
                ] 
            )
        )

        # adiciona os dados de bandeira e ec
        df_cancelamentos = df_cancelamentos.join(self.bndr, on="cd_bndr", how="left").drop("cd_bndr")
        df_cancelamentos = df_cancelamentos.join(self.ec, on="nu_ec", how="left")

        return df_cancelamentos

    def executar(
        self, 
        data_min: typing.Optional[str] = None,
        data_max: typing.Optional[str] = None,
    ) -> None:
        """
        Metodo inicializador da classe e responsavel pela chamados das outras funcoes.

        :param data_min: data minima de carga histórica selecionada
        :param data_max: data maxima de carga histórica selecionada
        """
        # obtém a ultima data de atualização
        data_min, data_max = self._busca_maior_data(data_min, data_max)

        # transforma os dados
        df = self._busca_novos_cancelamentos(data_min, data_max)

        # salva os resultados
        self._salva_dados(df)


def executa_cncl(params: list) -> None:
    """
    Função wrapper para executar a base de Cancelamentos

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
    dados_cancelamento = CruzamentoCancelamentos(du)
    dados_cancelamento.executar(data_min, data_max)


if __name__ == "__main__":
    # cria a sessão do spark
    spark = (
        SparkSession.builder
        .appName("SPARK_prod_prazo_tbciar_re_cncl")
        .config("spark.driver.maxResultSize", "2G") # Precisamos usar essa configuracao especifica para as tabelas que leem mais dados (chargeback e cancelamento)
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
    executa_cncl(sys.argv[1:])

