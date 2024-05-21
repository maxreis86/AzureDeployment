import pyspark.sql.functions as F
from pyspark.sql.dataframe import DataFrame


RENAME_DICT = {
    "nm_fantasia": "nm_fnts",
    "nm_marca": "nm_marc",
    "nm_cadeia_forcada": "nm_cdfr",
    "mn_matriz_sgmt": "mn_mtrz_sgmt",
    "nm_sgm_comerc": "nm_sgmt_cmrl",
    "cd_so_ramo_atividade": "cd_so_ramo_atvd",
    "nm_ramo_atividade": "nm_ramo_atvd",
    "nm_diretoria": "nm_drta",
    "nm_gerencia": "nm_grnc",
    "nm_supex_gerente_relac": "nm_supex_grte_rlct",
    "nm_gerente_relac": "nm_grte_rlct",
    "nm_gestor_conta": "nm_gstr_cnta",
    "dt_afiliacao_ec": "dt_aflc_ec",
    "dt_situ_funcion": "dt_stco_fncm",
    "dc_situ_funcion": "dc_stco_fncm",
    "in_ecommerce": "in_ecommerce",
    "dt_transacao": "dt_trns",
    "in_rr": "in_rbrp",
    "in_ctao_presente": "in_crto_psen",
    "qt_parcelas": "qt_prcl",
    "nm_tipo_transacao": "nm_tipo_trns",
    "grupo_cnpj": "cd_grpo_cnpj",
    "cd_dt_carga": "cd_data_crga",
    "cd_dia_carga": "cd_dia_crga",
    "dt_vecto": "dt_vncm",
    "qt_ur": "qt_ur",
    "vl_total": "vl_totl",
    "vl_livre": "vl_lvre",
    "vl_financiado": "vl_fncd",
    "vl_gravamado": "vl_grvm",
    "vl_reserva": "vl_rsrv",
    "nm_bandeira": "nm_bndr",
    "dt_cancelamento": "dt_cncl",
    "cd_moeda": "cd_moda",
    "dias_desde_venda": "nu_dia_vnda",
    "vl_brto_liquidar": "vl_brto_liq",
    "vl_lqdo_liquidar": "vl_lqdo_liq",
    "vl_brto_canc": "vl_brto_cncl",
    "vl_lqdo_canc": "vl_lqdo_cncl",
    "nu_canc": "nu_cncl",
    "dt_chargeback": "dt_chrg",
    "vl_brto_cgb": "vl_brto_chrg",
    "vl_lqdo_cgb": "vl_lqdo_chrg",
    "nu_cgb": "nu_chrg",
    "classif_30d": "dc_clsf_30d",
    "classif_60d": "dc_clsf_60d",
    "classif_90d": "dc_clsf_90d",
    "dc_grp_produto": "dc_grpo_prdt",
    "volume_capturado": "vl_capr",
    "margem_contrib": "vl_mrgm_crbc",
    "margem_contrib_ajustada": "vl_mrgm_crbc_ajsd",
    "cd_mes": "cd_mes",
    "cd_cluster": "cd_clsr",
    "vl_financ" : "vl_fncd",
    "vl_gravame" : "vl_grvm",
    "vl_arv_cielo" : "vl_arv_cilo",
    "vl_arv_mercado" : "vl_arv_mrcd",
    "pct_financiado" : "pc_fncd",
    "pct_arv_cielo" : "pc_arv_cilo",
    "ratio_merc_cielo" : "pc_mrcd_cilo",
    "vl_financ_cielo" : "vl_fncd_cilo",
    "vl_financ_merc" : "vl_fncd_mrcd",
    "dt_cotr_particao": "dt_cotr_prtc",
}

def renomeia_para_padrao(df: DataFrame) -> DataFrame:
    """
    Renomeia os campos de um data frame para o padrão a ser utilizdo no ambiente Cielo

    :param df: data frame a ser renomeado
    :return: data frame com os campos renomeados
    """
    global RENAME_DICT
    return df.select([F.col(c).alias(RENAME_DICT.get(c, c)) for c in df.columns])