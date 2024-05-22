import sys
import typing
from datetime import datetime
from dateutil.relativedelta import relativedelta

import pyspark.sql.functions as F
from pyspark.sql import SparkSession
from pyspark.sql.dataframe import DataFrame
from pyspark.sql.window import Window

class GrupoCNPJ:
    # lista de CNPJs a serem tratados como modelos de grandes contas
    CNPJS_EXCECAO = [
        638254000130,
        13021765000150,
        10421140000278,
        28506798000167,
        28686887000132,
        26962065000101,
        18127208000131,
        14112683000183,
        16417975000400,
        4302474000653,
        29094711000154,
        4302474000300,
        30760969000121,
        41157137000131,
        10545650000176,
        46849920000180,
        4638058000116,
        3471411000234,
        53205084000166,
        26962065000101,
        10421140000197,
        41774248000197,
        4302455000112,
        4514120000168,
        5109255000100,
        50972157000157,
        16417975000230,
        43164987000192,
        59149492000124,
        26962065000373,
        2310074000150,
        4302474000491,
        4302474000572,
        34697770000157,
        69349710000128,
        16417975000150,
        56734197000147,
        14544564000108,
        26962065000454,
        46172262000134,
        4302474000734,
        74270224000150,
        30002664000150,
        8415682000105,
        3471411000153,
        26962065000292,
        28506798000329,
        33638265000179,
        17931998000140,
        65164501000159,
        5109255000291,
        4302474000220,
        26386919000159,
        8839364000171,
        4302474000149,
        4302474000149,
        26962065000535,
        7095770000104,
    ]

    def __init__(
        self,
        du: "DataUtils",
        data_max_dt: datetime,
        tipo_valor: str,
        tipo_peso: str,
        dias_grupo_cnpj: int,
    ) -> None:
        """
        Instancia a classe de grupo de CNPJ

        :param du: instancia do data utils para ser usada para processar a base
        :param data_max_dt: data de corte
        :param tipo_valor: Tipo de valor para fazer o calculo
        :param tipo_peso: tipo de peso a ser aplicado sobre o faturamento
        :param dias_grupo_cnpj: Quantidade de dias para fazer os calculos médios por grupo
        """
        self.du = du
        self.data_max_dt = data_max_dt
        self.tipo_valor = tipo_valor
        self.tipo_peso = tipo_peso
        self.dias_grupo_cnpj = dias_grupo_cnpj

        # cria os campos de data
        self.data_max = self.data_max_dt.strftime("%Y%m%d")
        self.data_max_int = int(self.data_max)
        self.data_min_dt = self.data_max_dt - relativedelta(days=self.dias_grupo_cnpj)
        self.data_min = self.data_min_dt.strftime("%Y%m%d")
        self.data_min_int = int(self.data_min)

        # carrega os dados compartilhados
        self._trns_ag = (
            self.du.get_by_name("TRNS_AG", "internal")
            .filter(
                (F.col("cd_dh_incl_rgst") >= self.data_min_int)
                & (F.col("cd_dh_incl_rgst") <= self.data_max_int)
                & (F.col("dt_trns") > self.data_min_dt) 
            )
            .select("nu_ec", "nu_cnpj_cpf_recr", "in_rbrp", f"vl_{self.tipo_valor}_agnd_trns")
            .persist()
        )
        self._raw_ec = self.du.get_by_name("EC", "internal")
        self._raw_trns = self.du.get_by_name("TRNS", "external")
        self._raw_agnd = self.du.get_by_name("AGENDA_DIARIA", "internal")

    def _classificacao_cnpj(self) -> DataFrame:
        """
        Remove duplicidades nos dados de CNPJ de unidade de negócio, segmento comercial
        e flag de e-commerce dado o EC mais representativo

        :return: data frame com os atributos por CNPJ
        """
        # Ler a base tabela_entrada_trns com dados de transacoes agragados e agregar por cd_cluster
        df_trns = (
            self._trns_ag.groupBy("nu_ec", "nu_cnpj_cpf_recr")
            .agg(F.sum(f"vl_{self.tipo_valor}_agnd_trns").alias("vl_trns"))
        )

        # para cada CNPJ obtém o EC mais representativo
        return (
            self._raw_ec.select(
                "nu_ec", 
                "nu_cnpj_cpf_recr", 
                "nm_unde_ngco_fpna", 
                "nm_sgmt_cmrl", 
                F.when(F.col("in_ecommerce") == 1, F.lit("E-COMMERCE"))
                .otherwise("NÃO E-COMMERCE")
                .alias("nm_ecommerce"),
            )
            .join(df_trns, on=["nu_ec", "nu_cnpj_cpf_recr"], how="left")
            .fillna(0)
            .withColumn(
                "row",
                F.row_number().over(
                    Window.partitionBy("nu_cnpj_cpf_recr")
                    .orderBy(F.col("vl_trns").desc())
                )
            )
            .filter(F.col("row") == 1)
            .drop("nu_ec")
            .dropDuplicates(["nu_cnpj_cpf_recr"])
            .repartition("nu_cnpj_cpf_recr")
            .persist()
        )

    def _peso_por_cnpj(self)  -> DataFrame:
        """
        Calcula um peso associado a cada CNPJ dado o faturamento e a data do mesmo

        :return: data frame processado - nu_cnpj_cpf_recr + vl_peso_cnpj
        """
        from utils.math import aplica_peso_a_dif_datas

        # calcula as datas que serão utilizadas no peso
        data_min_dt = self.data_max_dt - relativedelta(days=540)
        data_min_int = int(data_min_dt.strftime("%Y%m%d"))
        
        # obtém os pesos em cada data
        return (
            aplica_peso_a_dif_datas(
                df=self.du.get_by_name("TRNS_AG", "internal")
                .filter(
                    (F.col("cd_dh_incl_rgst") >= data_min_int)
                    & (F.col("cd_dh_incl_rgst") <= self.data_max_int)
                    & (F.col("dt_trns") > data_min_dt)
                )
                .groupBy("nu_cnpj_cpf_recr", "dt_trns")
                .agg(F.sum(f"vl_{self.tipo_valor}_agnd_trns").alias("vl_trns"))
                .withColumn("dt_min", F.lit(data_min_dt)),
                dt_col_1="dt_min",
                dt_col_2="dt_trns",
                tipo_peso=self.tipo_peso,
            )
            .withColumn("vl_trns", F.col("vl_trns") * F.col("peso"))
            .groupBy("nu_cnpj_cpf_recr")
            .agg(F.sum("vl_trns").alias("vl_peso_cnpj"))
            .repartition("nu_cnpj_cpf_recr")
            .persist()
        )


    def _liquidacao_media(self)  -> DataFrame:
        """
        Esta funcao visa criar um base de transacao agregada no nivel de "nu_cnpj_cpf" com a media ponderada por vl_brto_agnd_trns/vl_lqdo_agnd_trns
        de nu_dias_a_liq (diferenca entre dt_vncm_unde_rcbv e dt_atzc_trns) distribuido em faixas

        As informacoes sao lidas do base sie.tbdwr_trns e o filtro usado é cd_tipo_lncm in isin(1, 2, 3). 1=DEBITO | 2=CREDITO A VISTA | 3=CREDITO PARCELADO

        :return: data frame processado - nu_cnpj_cpf_recr + qt_dias_lqdc + dc_dias_lqdc
        """
        return (
            self._raw_trns.filter(
                (F.col("cd_dh_incl_rgst") >= self.data_min_int)
                & (F.col("cd_dh_incl_rgst") <= self.data_max_int)
                & (F.col("cd_tipo_lncm").isin([1, 2, 3]))
            )
            .select(
                "nu_cnpj_cpf_recr",
                "cd_dh_incl_rgst",
                F.col(f"vl_{self.tipo_valor}_agnd_trns").alias("vl_trns"),
                "dt_vncm_unde_rcbv",
                "dt_atzc_trns"
            )
            .withColumn(
                "qt_dias_lqdc", 
                F.datediff(F.col("dt_vncm_unde_rcbv"), F.col("dt_atzc_trns"))
                * F.col("vl_trns")
            )
            .groupBy("nu_cnpj_cpf_recr")
            .agg((F.sum("qt_dias_lqdc") / F.sum("vl_trns")).alias("qt_dias_lqdc"))
            .withColumn(
                "dc_dias_lqdc",
                F.when(F.col("qt_dias_lqdc") <= 90, "<=90")
                .when(F.col("qt_dias_lqdc") <= 180, "91 a 180")
                .otherwise(">180")
            )
            .repartition("nu_cnpj_cpf_recr")
            .persist()
        )


    def _faixa_antecipacao(self)  -> DataFrame:
        """
        Esta funcao visa criar uma base agregada no nivel de "nu_cnpj_cpf" com a media ponderada por vl_financiado/vl_total distribuido em faixas

        :return: data frame processado - nu_cnpj_cpf_recr + vl_prcn_fncd + dc_faxa_fncd
        """
        return (
           self._raw_agnd.filter(
                (F.col("cd_dia_crga") >= self.data_min_int)
                & (F.col("cd_dia_crga") <= self.data_max_int)
            )
            .groupBy("nu_cnpj_cpf_recr")
            .agg(F.coalesce((F.sum("vl_fncd") / F.sum("vl_totl")), F.lit(0)).alias("vl_prcn_fncd"))
            .withColumn(
                "dc_faxa_fncd",
                F.when(F.col("vl_prcn_fncd") <= 0.2, "0-20%")
                .when((F.col("vl_prcn_fncd") > 0.2) & (F.col("vl_prcn_fncd") <= 0.4), "21-40%")
                .when((F.col("vl_prcn_fncd") > 0.4) & (F.col("vl_prcn_fncd") <= 0.6), "41-60%")
                .when((F.col("vl_prcn_fncd") > 0.6) & (F.col("vl_prcn_fncd") <= 0.8), "61-80%")
                .when((F.col("vl_prcn_fncd") > 0.8) & (F.col("vl_prcn_fncd") <= 0.9), "81-90%")
                .when((F.col("vl_prcn_fncd") > 0.9) & (F.col("vl_prcn_fncd") <= 0.95), "91-95%")
                .when((F.col("vl_prcn_fncd") > 0.95) & (F.col("vl_prcn_fncd") <= 0.98), "95-98%")
                .otherwise("98-100%")
            )
            .repartition("nu_cnpj_cpf_recr")
            .persist()
        )


    def _faixa_de_rr(self)  -> DataFrame:
        """
        Calcula categoria para os CNPJs baseado no % de RR

        :return: data frame processado - nu_cnpj_cpf_recr + vl_prcn_rbrp + dc_faxa_rbrp
        """
        return (
            self._trns_ag.groupBy("nu_cnpj_cpf_recr")
            .agg( 
                (
                    F.sum(
                        F.when(
                            F.col("in_rbrp") == 1, F.col(f"vl_{self.tipo_valor}_agnd_trns")
                        ).otherwise(0)
                    ) 
                    / F.sum(f"vl_{self.tipo_valor}_agnd_trns")
                ).alias("vl_prcn_rbrp")
            )
            .withColumn("dc_faxa_rbrp", F.when(F.col("vl_prcn_rbrp") >= 0.9, ">=90%").otherwise("<90%"))
            .repartition("nu_cnpj_cpf_recr")
            .persist()
        )


    def _processa_dados(self) -> DataFrame:
        """
        Realiza o processamento de dados completo
        
        :return: data frame processado
            nu_cnpj_cpf_recr
            vl_peso_cnpj
            qt_dias_lqdc
            vl_prcn_fncd
            vl_prcn_rbrp
            cd_grpo_cnpj
            cd_dia_crga
        """
        from utils.renames import renomeia_para_padrao
        
        # gera as variáveis de classificação do CNPJ
        df_exposicao = self._peso_por_cnpj()
        df_cnpj = self._classificacao_cnpj()
        df_liq = self._liquidacao_media()
        df_faixa_antecipacao = self._faixa_antecipacao()
        df_faixa_rr = self._faixa_de_rr()
        
        # cruza as bases com as variáveis para classificação
        df = (
            df_exposicao.join(df_cnpj, on=["nu_cnpj_cpf_recr"], how="inner")
            .join(df_liq, on=["nu_cnpj_cpf_recr"], how="left")
            .join(df_faixa_antecipacao, on=["nu_cnpj_cpf_recr"], how="left")
            .join(df_faixa_rr, on=["nu_cnpj_cpf_recr"], how="left")
        )

        # cria o campo de classificação
        df = df.select(
            "nu_cnpj_cpf_recr",
            F.when(
                (F.col("nm_unde_ngco_fpna") == "GRANDES CONTAS")
                | (F.col("nu_cnpj_cpf_recr").isin(self.CNPJS_EXCECAO)), 
                1
            )
            .otherwise(F.col("vl_peso_cnpj"))
            .alias("vl_peso_cnpj"),
            "qt_dias_lqdc",
            "vl_prcn_fncd",
            "vl_prcn_rbrp",
            F.when(
                (F.col("nm_unde_ngco_fpna") == "GRANDES CONTAS")
                | (F.col("nu_cnpj_cpf_recr").isin(self.CNPJS_EXCECAO)), 
                F.col("nu_cnpj_cpf_recr").cast("string")
            )
            .otherwise(
                F.concat(
                    F.coalesce(F.col("nm_sgmt_cmrl"), F.lit("NULO")), 
                    F.lit(" | "),
                    F.coalesce(F.col("nm_ecommerce"), F.lit("NULO")),
                    F.lit(" | "),
                    F.coalesce(F.col("dc_dias_lqdc"), F.lit("NULO")), 
                    F.lit(" | "),
                    F.coalesce(F.col("dc_faxa_fncd"), F.lit("NULO")),
                    F.lit(" | "),
                    F.coalesce(F.col("dc_faxa_rbrp"), F.lit("NULO")),
                )
            ) 
            .alias("cd_grpo_cnpj")
        )

        # ajusta o valor de peso
        df = (
            df.withColumn("peso_tot", F.sum("vl_peso_cnpj").over(Window.partitionBy("cd_grpo_cnpj")))
            .withColumn("vl_peso_cnpj", F.col("vl_peso_cnpj") / F.col("peso_tot"))
            .drop("peso_tot")
            .withColumn(
                "dc_prmt_exec", F.concat(
                    F.lit("tipo_valor="), F.lit(self.tipo_valor),
                    F.lit(";dias_grupo_cnpj="), F.lit(self.dias_grupo_cnpj),
                    F.lit(";tipo_peso="), F.lit(self.tipo_peso)
                )
            )
        )

        # ajusta os nomes de colunas
        return renomeia_para_padrao(df).withColumn("cd_dia_crte", F.lit(self.data_max_int))

    def executar(self) -> None:
        """
        Realiza o processamento e escrita dos dados para o disco
        """
        df = self._processa_dados()
        self.du.write_by_name(df, "GRUPO_CNPJ", "internal", partitions=["cd_dia_crte"], mode="overwrite")


def processa_tbciar_re_grupo_cnpj(
    du: "DataUtils", 
    tipo_valor: str,
    dias_grupo_cnpj: int,
    tipo_peso: str,
    data_min: typing.Optional[str] = None,
    data_max: typing.Optional[str] = None,
) -> None:
    """
    Data uma data de corte, classifica o EC de acordo com suas caracteristicas médias 

    :param du: instancia do data utils para ser usada para processar a base
    :param data_min: data mínima de processamento (se nulo, considera a última data processada, ou a primeira partição da TRNS_AG ou 20220201)
    :param data_max: data de corte
    :param tipo_valor: Tipo de valor para fazer o calculo
    :param tipo_peso: tipo de peso a ser aplicado sobre o faturamento
    :param dias_grupo_cnpj: Quantidade de dias para fazer os calculos médios por grupo
    """
    # obtém a data mínima e data máxima das partições
    if data_min is None:
        if du.is_table_available("GRUPO_CNPJ", "internal"):
            data_min = max([v["cd_dia_crte"] for v in du.get_partition_values("GRUPO_CNPJ", "internal")])
            data_min = (datetime.strptime(data_min, "%Y%m%d") + relativedelta(days=7)).strftime("%Y%m%d")
        else:
            data_min = min([v["cd_dh_incl_rgst"] for v in du.get_partition_values("TRNS_AG", "internal")])
        #Esse limite de data minima e para o processamento do Control-M, quando a data_min nao e informada
        data_min = max(data_min, "20240406")
    # Esse limite da data minina e para limitar o historico quando o parameto data_min for informado
    data_min = max(data_min, "20230524")
    
    if data_max is None:
        data_max = max([v["cd_dh_incl_rgst"] for v in du.get_partition_values("TRNS_AG", "internal")])
    
    # Conversão para o formato data do período observado
    start_date = datetime.strptime(data_min, "%Y%m%d")
    end_date = datetime.strptime(data_max, "%Y%m%d")
    
    # Lista com todos os sabados entre as datas estabelecidas - sabado = 5
    # Porque o modelo ira rodar todos os domingos com os dados de sabado
    sabados = list()
    while start_date <= end_date:
        if start_date.weekday() == 5:
            sabados.append(start_date)
        start_date += relativedelta(days=1)

    # verifica se estamos processando histórico no modelo padrão
    if data_min is None and data_max is None and du.is_table_available("GRUPO_CNPJ", "internal"):
        parts = [v["cd_dia_crte"] for v in du.get_partition_values("GRUPO_CNPJ", "internal")]
        sabados = [
            s for s in sabados
            if s.strftime("%Y%m%d") not in parts
        ]

    # realiza o loop de processamento
    for data_max_dt in sabados:
        grupo = GrupoCNPJ(
            du=du,
            data_max_dt=data_max_dt,
            dias_grupo_cnpj=dias_grupo_cnpj,
            tipo_peso=tipo_peso,
            tipo_valor=tipo_valor,
        )
        grupo.executar()


def executa_grpo_cnpj(params: list) -> None:
    """
    Função wrapper para executar a base de Grupo de CNPJ

    :param params: parâmetros de execução
    """
    # pega os parâmetros de execução
    env = params[0] if len(params) > 0 else "PROD"
    data_min = params[1] if len(params) > 1 else None
    data_max = params[2] if len(params) > 2 else None
    tipo_valor = params[3] if len(params) > 3 else "brto"
    dias_grupo_cnpj = params[4] if len(params) > 4 else "90"
    tipo_peso = params[5] if len(params) > 5 else "linear"

    # corrige nulos
    env = "PROD" if env == "" else env
    data_min = None if data_min == "" else data_min
    data_max = None if data_max == "" else data_max
    tipo_valor = "brto" if tipo_valor == "" else tipo_valor
    dias_grupo_cnpj = "90" if dias_grupo_cnpj == "" else dias_grupo_cnpj
    tipo_peso = "linear" if tipo_peso == "" else tipo_peso

    # converte tipos
    dias_grupo_cnpj = int(dias_grupo_cnpj)

    # instância o data utils
    from utils.data_utils import DataUtils
    du = DataUtils(env)

    # executa o processamento
    processa_tbciar_re_grupo_cnpj(
        du=du,
        tipo_valor=tipo_valor,
        dias_grupo_cnpj=dias_grupo_cnpj,
        tipo_peso=tipo_peso,
        data_min=data_min,
        data_max=data_max,
    )


if __name__ == "__main__":
    # cria a sessão do spark
    spark = (
        SparkSession.builder
        .appName("SPARK_prod_prazo_tbciar_re_grpo_cnpj")
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
    executa_grpo_cnpj(sys.argv[1:])
