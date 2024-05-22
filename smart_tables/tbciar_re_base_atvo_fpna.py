import sys
import typing

from datetime import date, datetime
from dateutil.relativedelta import relativedelta

import pyspark.sql.functions as F
from pyspark.sql import SparkSession
from pyspark.sql.dataframe import DataFrame

def processa_base_ativa(
    du: "DataUtils",
    data_hist_min: typing.Optional[str] = None,
    data_mes_max: typing.Optional[str] = None,
) -> DataFrame:
    """
    Gera as classificações de base ativa por EC com base no faturamento mensal
    
    :param du: instancia do data utils para ser usada para processar a base
    :param data_hist_min: parametro usado para indicar ao processo o codigo do mes de inicio da apuracao. 
        - Este parametro deve ser usado para definir o ponto de inicio para cargas historicas
        - Este parametro pode ser mantiodo em seu valor default para que seja executada carga incremental
    :param data_mes_max: máximo mês para processar a base
    :return: base ativa para o período selecionado
    """

    from utils.renames import renomeia_para_padrao

    if data_hist_min is None:
        # buscar o ultimo mes de pauracao e calcular qual o proximo mes.
        if du.is_table_available("BASE_ATIVA", "internal"):
            mes_str = max([v["cd_mes"] for v in du.get_partition_values("BASE_ATIVA", "internal")])
            mes_base_ativa = mes_str
        else:
            mes_base_ativa = "202012"
        # calcula o mês mínimo para conseguir fazer os cálculos
        data_hist = date(int(mes_base_ativa) // 100, int(mes_base_ativa) % 100, 1)
        mes_base = data_hist - relativedelta(months=2)
        mes_base_calc = 100 * mes_base.year + mes_base.month
    else:
        # se o parametro foi informado, estamos fazendo carga historica e devemos buscar os dados a partir do mes e ano informado
        # mes mínimo que queremos extrair da base
        mes_base = datetime.strptime(data_hist_min, "%Y%m%d") + relativedelta(months=1)
        mes_base_ativa = 100 * mes_base.year + mes_base.month
        
        # mês minimo para conseguir fazer os calculos de base ativa
        mes_base = datetime.strptime(data_hist_min, "%Y%m%d") - relativedelta(months=3)
        mes_base_calc = 100 * mes_base.year + mes_base.month
        
    # adiciona um mês máximo baseado no fechamento do mês anterior
    if data_mes_max is None:
        data_mes_max = date.today() - relativedelta(months=1)
    else: 
        data_mes_max = datetime.strptime(data_mes_max, "%Y%m%d")
    data_mes_max_int = 100 * data_mes_max.year + data_mes_max.month
    
    # adiciona o nome de faturamento
    fat_mensal = du.get_by_name("FAT_MENSAL", "external")
    fat_mensal.createOrReplaceTempView("faturamento")

    # cria base a ser carregada
    delta_base_ativa_mensal = renomeia_para_padrao(du.spark.sql(
    f"""
    -- cria uma base com uma flag de 1 para os ECs que tiveram alguma transação registrada
    WITH suporte_base AS (
        SELECT a.cd_mes, a.nu_so_ec, 1 AS volume
        FROM faturamento a 
        WHERE COALESCE(a.qt_venda_fat, 0)>0 AND COALESCE(a.cd_rede_capturadora, 0)<=3 
        AND a.cd_mes >= {mes_base_calc} 
        GROUP BY 1, 2
    ),

    -- adiciona colunas m1 e m2 a base do mês corrente
    suporte_base_m0 AS (
        SELECT 
            a.cd_mes,
            a.nu_so_ec,
            a.volume AS volume_atual,
            0 AS volume_ant_m1,
            0 AS volume_ant_m2,
            0 AS volume_ant_m3
        FROM suporte_base a 
    ),

    -- adiciona uma flag se houve transação no mês anterior
    suporte_base_m1 AS (
        SELECT 
            (CASE WHEN CAST(SUBSTRING(CAST(a.cd_mes as string), -2) AS int)=12 THEN a.cd_mes + 89 ELSE a.cd_mes + 1 END) AS cd_mes,
            a.nu_so_ec,
            0 AS volume_atual,
            a.volume AS volume_ant_m1,
            0 AS volume_ant_m2,
            0 AS volume_ant_m3
        FROM suporte_base a 
    ),

    -- adiciona uma flag se houve transação 2 meses atrás
    suporte_base_m2 AS (
        SELECT 
            (CASE WHEN CAST(SUBSTRING(CAST(a.cd_mes as string), -2) AS int)>=11 THEN a.cd_mes + 90 ELSE a.cd_mes + 2 END) AS cd_mes,
            a.nu_so_ec,
            0 AS volume_atual,
            0 AS volume_ant_m1,
            a.volume AS volume_ant_m2,
            0 AS volume_ant_m3
        FROM suporte_base a 
    ),

    -- adiciona uma flag se houve transação 3 meses atrás
    suporte_base_m3 AS (
        SELECT 
            (CASE WHEN CAST(SUBSTRING(CAST(a.cd_mes as string), -2) AS int)>=10 THEN a.cd_mes + 91 ELSE a.cd_mes + 3 END) AS cd_mes,
            a.nu_so_ec,
            0 AS volume_atual,
            0 AS volume_ant_m1,
            0 AS volume_ant_m2,
            a.volume AS volume_ant_m3
        FROM suporte_base a 
    ),

    -- empilha as bases com as infos dos últimos 90 dias
    empilha_90d AS (
        SELECT * FROM suporte_base_m0 
        UNION ALL 
        SELECT * from suporte_base_m1
        UNION ALL 
        SELECT * from suporte_base_m2
        UNION ALL 
        SELECT * from suporte_base_m3
    ),

    -- cria uma base com flags de volume transacionado por mês
    base_ativa_mensal_90d AS (
        SELECT 
            a.cd_mes,
            a.nu_so_ec,
            SUM(a.volume_atual) AS volume_atual,
            SUM(a.volume_ant_m1) AS volume_ant_m1,
            SUM(a.volume_ant_m2) AS volume_ant_m2,
            SUM(a.volume_ant_m3) AS volume_ant_m3
        FROM empilha_90d a
        GROUP BY 1, 2
    )

    -- cria os campos de classificação de atividade
    SELECT
        CAST(cd_mes AS BIGINT) AS cd_mes,
        nu_so_ec,
        (
            CASE 
                WHEN volume_atual=0 THEN 'INATIVO'
                WHEN volume_atual<>0 AND volume_ant_m1=0 THEN 'REATIVACAO'
                ELSE 'ATIVO'
            END
        ) AS classif_30d,
        (
            CASE 
                WHEN volume_atual=0 AND volume_ant_m1=0 THEN 'INATIVO'
                WHEN volume_atual<>0 AND volume_ant_m1=0 AND volume_ant_m2=0 THEN 'REATIVACAO'
                ELSE 'ATIVO'
            END
        ) AS classif_60d,
        (
            CASE 
                WHEN volume_atual=0 AND volume_ant_m1=0 AND volume_ant_m2=0 THEN 'INATIVO'
                WHEN volume_atual<>0 AND volume_ant_m1=0 AND volume_ant_m2=0 AND volume_ant_m3=0 THEN 'REATIVACAO'
                ELSE 'ATIVO'
            END
        ) AS classif_90d
    FROM base_ativa_mensal_90d
    WHERE cd_mes>{mes_base_ativa} AND cd_mes<={data_mes_max_int}; 
    """
    ))

    # escreve o resultado para o disco
    # usar mode("overwrite") para sobreescrever a particao em caso de reprocessamento
    du.write_by_name(delta_base_ativa_mensal, "BASE_ATIVA", "internal", partitions="cd_mes", mode="overwrite")

    return delta_base_ativa_mensal


def executa_base_atvo_fpna(params: list) -> None:
    """
    Função wrapper para executar a base ativa

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

    # processa a base ativa
    processa_base_ativa(du, data_min, data_max)
 
if __name__ == "__main__":
    # instancia a sessão do spark
    spark = (
        SparkSession.builder.appName("SPARK_prod_prazo_tbciar_re_base_atvo_fpna")
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
    executa_base_atvo_fpna(sys.argv[1:])