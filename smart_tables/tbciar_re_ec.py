import sys
import typing
from datetime import datetime

import pyspark.sql.functions as F
from pyspark.sql import SparkSession
from pyspark.sql.dataframe import DataFrame
from pyspark.sql.window import Window


class BaseEC:
    # lista de nu_so_ec que devem ser tratados como empreendedores
    ECS_EMPREENDEDORES: typing.List[int] = [
        1095237540, 1095237559, 1095237524, 1095237664, 1095237680, 1095966321, 1095966283,
        1103821480, 1095966437, 1095966194, 1095966240, 1085594626, 1058208966, 1091431750,
        1085594529, 2776039047, 2776039055, 1050412157, 2774360328, 1095237400, 1095237575,
        1095237648, 1095237486, 1095237478, 1093593650, 1093593676, 2774108572, 1100151769,
        1119344767, 1119344791, 1119344686, 1119344716, 1119344929, 1119344961, 1119344872,
        1119344902, 1119344457, 1119344422, 1119344414, 1119344503, 1119344643, 1119344597,
        1119344546, 1095491935, 1086099084
    ]
    
    # lista de diretorias consideradas como grandes contas
    GRANDES_CONTAS: typing.List[int] = [6, 13, 14, 15, 18, 21]
    
    def __init__(self, du: "DataUtils"):
        self.du = du

        self.raw_ec = self.du.get_by_name("EC", "external")
        self.raw_repr = self.du.get_by_name("REPRESENTANTES")
        self.raw_gern = self.du.get_by_name("GERENCIA")
        self.raw_cf = self.du.get_by_name("CADEIA", "external")

    def _salva_dados(self, df: DataFrame) -> None:
        """
        Dado um dataframe com dados do processamento salva na tabela.
    
        :param df (DataFrame): Dados novos que serao salvos na tabela
        """
        self.du.write_by_name(df, "EC", "internal", mode="overwrite")
        
    def _processa_representantes(self) -> DataFrame:
        """
        Processa os dados de representante para obter a última informação disponível de 
        representate por código
        
        :return: base de representantes tratada
        """
        return (
            self.raw_repr.select(
                "cd_so_repr_venda", 
                F.when(F.col("dt_exclusao_rved").isNull(), datetime(2099, 12, 31, 23, 59, 59))
                .otherwise(F.col("dt_exclusao_rved"))
                .alias("dt_exclusao_rved"), 
                "cd_gerencia", 
                "nm_repr_venda"
            )
            .withColumn(
                "ultima_data", 
                F.max(F.col("dt_exclusao_rved")).over(
                    Window.partitionBy("cd_so_repr_venda").orderBy("dt_exclusao_rved")
                )
            )
            .filter(F.col("ultima_data") == F.col("dt_exclusao_rved"))
            .dropDuplicates(["cd_so_repr_venda"])
            .drop("dt_exclusao_rved", "ultima_data")
        )
    
    def _processa_ec(self) -> DataFrame:
        """
        Processa os dados de EC
        """
        from utils.renames import renomeia_para_padrao

        # obtém dados de representantes
        representantes = self._processa_representantes()
        
        # atualiza a base de ECs
        return renomeia_para_padrao(
            self.raw_ec.alias("a")
            .join(representantes.alias("b"), on="cd_so_repr_venda", how="left")
            .join(self.raw_gern.alias("c"), on=F.col("b.cd_gerencia") == F.col("c.cd_gerencia"), how="left")
            .join(
                self.raw_cf.withColumn("nu_so_ec", F.col("nu_so_ec").cast("bigint"))
                .alias("d"),
                on=F.col("a.nu_so_ec") == F.col("d.nu_so_ec"), 
                how="left"
            )
            .select(
                "a.nu_ec",
                "a.nu_so_ec",
                "a.nu_so_cpf_cnpj",
                F.when(F.col("a.nu_cnpj") > 0, F.col("nu_cnpj"))
                .otherwise(F.col("a.nu_cpf"))
                .alias("nu_cnpj_cpf_recr"),
                F.when(
                    F.col("a.nu_cnpj") > 0, 
                    F.substring(F.lpad(F.col("nu_cnpj"), 14, "0"), 0, 9)
                    .cast("bigint")
                )
                .otherwise(F.col("a.nu_cpf").cast("bigint"))
                .alias("nu_cnpj9_cpf"),
                "a.nm_ec",
                "a.nm_fantasia",
                "a.nm_marca",
                F.when(
                    F.col("d.nm_cdfr").isNotNull(), 
                    F.col("d.nm_cdfr")
                )
                .otherwise("NAO SE APLICA")
                .alias("nm_cadeia_forcada"),
                F.when(F.col("a.nu_so_ec").isin(self.ECS_EMPREENDEDORES), "EMPREENDEDORES")
                .when(F.col("a.cd_diretoria").isin(self.GRANDES_CONTAS), "GRANDES CONTAS")
                .when(F.col("a.cd_matriz_sgmt") == 10, "EMPREENDEDORES")
                .otherwise("VAREJO")
                .alias("nm_unde_ngco_fpna"),
                F.col("a.dc_unde_ngco").alias("nm_unde_ngco_bdata"),
                "a.mn_matriz_sgmt",
                "a.nm_sgm_comerc",
                "a.cd_so_ramo_atividade",
                "a.nm_ramo_atividade",
                "a.nm_diretoria",
                "c.nm_gerencia",
                F.col("c.nm_resp_gerencia").alias("nm_supex_gerente_relac"),
                F.col("b.nm_repr_venda").alias("nm_gerente_relac"),
                "a.nm_gestor_conta",
                F.to_date("a.dt_afiliacao_ec").alias("dt_afiliacao_ec"),
                F.to_date("a.dt_situ_funcion").alias("dt_situ_funcion"),
                "a.dc_situ_funcion",
                F.when(F.col("a.cd_so_tipo_ec_ecom") == "S", 1) 
                .otherwise(0)
                .alias("in_ecommerce"),
            )
        )

    def executar(self) -> None:
        """
        Metodo inicializador da classe e responsavel pela chamados das outras funcoes.
        """
        df = self._processa_ec()
        self._salva_dados(df)



def executa_ec(params: list) -> None:
    """
    Função wrapper para executar a base de EC

    :param params: parâmetros de execução
    """
    # pega os parâmetros de execução
    env = params[0] if len(params) > 0 else "PROD"

    # instância o data utils
    from utils.data_utils import DataUtils
    du = DataUtils(env)

    # executa o processamento
    dados_ec = BaseEC(du)
    dados_ec.executar()


if __name__ == "__main__":
    # cria a sessão do spark
    spark = (
        SparkSession.builder
        .appName("SPARK_prod_prazo_tbciar_re_ec")
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
    executa_ec(sys.argv[1:])

    