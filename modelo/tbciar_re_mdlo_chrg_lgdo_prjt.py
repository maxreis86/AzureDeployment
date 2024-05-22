import sys

from pyspark.sql import SparkSession


def executa_mdlo_chrg_lgdo_prjt(params: list) -> None:
    """
    Função wrapper para executar a base tbciar_re_mdlo_chrg_lgdo_prjt

    :param params: parâmetros de execução
    """
    from modelo.comuns import projeta_curva_legada
    import modelo.configs as confs

    # pega os parâmetros de execução
    env = params[0] if len(params) > 0 else "PROD"
    data_max = params[1] if len(params) > 1 else None
    tipo_valor = params[2] if len(params) > 2 else confs.TIPO_VALOR

    # corrige nulos
    env = "PROD" if env == "" else env    
    data_max = None if data_max == "" else data_max
    tipo_valor = confs.TIPO_VALOR if tipo_valor == "" else tipo_valor

    # instância o data utils
    from utils.data_utils import DataUtils
    du = DataUtils(env)
    
    # executa o processamento CHARGEBACK
    df = projeta_curva_legada(
        du=du,
        tipo_valor=tipo_valor,
        base="CHARGEBACK",
        suffixo="chrg",
        data_max=data_max
    )
    du.write_by_name(df, "CURVA_LEG_CGB", "internal", partitions=["cd_dia_crte", "dh_crga"], mode="overwrite")
    df.unpersist()


if __name__ == "__main__":
    # cria a sessão do spark
    spark = (
        SparkSession.builder
        .appName("SPARK_prod_prazo_tbciar_re_mdlo_chrg_lgdo_prjt")
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
    executa_mdlo_chrg_lgdo_prjt(sys.argv[1:])
