# associa os schemas a cada um dos ambientes mapeados
ENVIRONMENTS = {    
    "PROD": "re_produtos_prazo",
    "LATEST": "sb" + "_produtos_prazo",
    "TEST": "sb" + "_produtos_prazo",
    "USER": "sb" + "_produtos_prazo",
}

# define os buckets s3 de cada ambiente
bucket_sandbox = "cielo-sandbox-sa-east-1-" + "107976507529-prod-prazo-sandbox"
BUCKETS = {
    "USER": f"s3://{bucket_sandbox}/balcao2.0",
    "TEST": f"s3://{bucket_sandbox}/balcao2.0/test",
    "LATEST": f"s3://{bucket_sandbox}/balcao2.0/latest",
    "PROD": "s3://cielo-sdlf-prd-sa-east-1-866812290529-refined/re_produtos_prazo",    
}

# define prefixos para nomes de tabelas por ambiente
PREFIXOS = {
    "PROD": {"external": "", "internal": ""},
    "LATEST": {"external": "", "internal": ""},
    "TEST": {"external": "amostra_", "internal": "test_balcao20_"},
}

# para ambientes de usuários vamos mapear listas de nomes de usuários que servirão
# de prefixo para criação de schemas particulares
USER_MAP = {
    "DENIS": "dlopes", 
    "FORLI": "pforli",
    "MAX": "mreis",
    "BRUNO": "bcarvalho",
    "VICTOR": "vshimizu",
}

# lista as bases de dados que são geradas pelo ambiente
ENVIRONMENT_TABLES = {
    "TEST": "test_utils",
    "EC": "tbciar_re_ec",
    "TRNS_AG": "tbciar_re_trns_agrg",
    "MCAJ": "tbciar_re_mcaj",
    "AGENDA_DIARIA": "tbciar_re_agnd_diro",
    "CANCELAMENTO": "tbciar_re_cncl",
    "CHARGEBACK": "tbciar_re_chrg",
    "ARV_CIELO": "tbciar_re_arv_cilo",
    "AGENDA_FIN": "tbciar_re_agnd_fncd",
    "BASE_ATIVA": "tbciar_re_base_atvo_fpna",    
    "GRUPO_CNPJ": "tbciar_re_grpo_cnpj",
    "CF_CLUSTER": "tbciar_re_mdlo_cdfr",
    "TRNS_CLUSTER": "tbciar_re_mdlo_trns_clsr",
    "CURVA_FIT": "tbciar_re_mdlo_crva_fit",
    "CURVA_SMOOTH": "tbciar_re_mdlo_crva_ajst",
    "VR_LIQ": "tbciar_re_mdlo_vlrcpv_liq",
    "ARV_MEDIO": "tbciar_re_mdlo_arv_medo",
    "TPV_MEDIO": "tbciar_re_mdlo_tpv_medo",
    "CURVA_LEG_CGB": "tbciar_re_mdlo_chrg_lgdo_prjt",
    "CURVA_LEG_CANC": "tbciar_re_mdlo_cncl_lgdo_prjt",
    "AGENDA_LIQ": "tbciar_re_mdlo_agnd_liq",
    "TPV_PROJ": "tbciar_re_mdlo_tpv_prjt",
    "CURVA_INC_CGB": "tbciar_re_mdlo_chrg_icmo_prjt",
    "CURVA_INC_CANC": "tbciar_re_mdlo_cncl_icmo_prjt",
    "TPV_LIQ": "tbciar_re_mdlo_tpv_liq",
    "PARAMS_PROJ": "tbciar_re_mdlo_prmt_prjt",
    "RESERVA": "tbciar_re_mdlo_rsrv",
    "RESERVA_OTIMA": "tbciar_re_mdlo_rsrv_otma",
    "STRESSED": "tbciar_re_mdlo_stsd",    
    "DELTA_UR": "tbciar_re_dlta_ur",
    "SHARE_CIELO": "tbciar_re_shre_cilo",
    "ARV_CONCO": "tbciar_re_arv_cnra",
    "DIM_GRUPO": "tbciar_re_dmen_grpo",
    "TRNS_GRUPO": "tbciar_re_trns_grpo",
    "PIX_GRUPO": "tbciar_re_pix_grpo",
    "BALCAO_GRUPO": "tbciar_re_balc_grpo",
    "MCAJ_GRUPO": "tbciar_re_mcaj_grpo",
    "CBK_GRUPO": "tbciar_re_chrg_grpo",
    "SHARE_GRUPO": "tbciar_re_shre_grpo",
    "ARV_CONCO_GRUPO": "tbciar_re_arv_cnra_grpo",
    "EVOLUCAO_CONTA": "tbciar_re_evlc_cnta_grpo",
    "AGENDA_DIARIA_GRUPO": "tbciar_re_agnd_diro_grpo",
    "CHURN_GC": "tbciar_re_churn_60d_grnd_cnta",
    "MET_CHURN": "tbciar_re_mtca_churn_grnd_cnta",
    "BASE_CHURN": "tbciar_re_churn_60d_vrjo",
    "PERDA": "tbciar_re_mdlo_perd",
    "PROB_CHURN": "tbciar_re_mdlo_prbl_churn",
    "PERDA_GRUPO": "tbciar_re_perd_grpo",
    "DEBITO_GRUPO": "tbciar_re_dbto_grpo",
}

# lista as bases de dados que não são geradas pelo modelo
EXTERNAL_TABLES = {
    "PROD": {
        "BANDEIRA": "sie.tbdwr_bandeira",
        "CALENDARIO": "sie.tbdwr_agenda",
        "REPRESENTANTES": "sie.tbdwr_repr_venda",
        "GERENCIA": "sie.tbdwr_gerencia",
        "EC": "sie.tbdwr_ec",
        "TRNS": "sie.tbdwr_trns",
        "FAT_DIARIO": "sie.tbdwf_fat_diario",
        "FAT_MENSAL": "sie.tbdwa_fat_mensal5",
        "ARV": "sie.tbdwr_nrc_cotr_ngci",
        "UR_BALCAO": "sie.tbdwr_nrc_unde_rcbv_totl",
        "ARV_BALCAO": "sie.tbdwr_nrc_cotr_unde_rcbv",
        "AGENDA": "gur.tbciar_tr_agnd_pgmn",
        "RENTABILIDADE": "re_rentabilidade.vwdw_smrz_dre",
        "PIX": "sie.tbdwr_transacao_pix",
        "EQUIP": "sie.tbdwr_equipamento_logico",
        "CADEIA": "u_produtos_prazo.tbciar_tr_cdfr",
        "DEBITO": "gd.tbdwr_debito_pendente_gd",
        "CHURN_60": "u_produtos_prazo.tbciar_tr_score_churn_60",        
    },
}
EXTERNAL_TABLES["LATEST"] = EXTERNAL_TABLES["PROD"]
EXTERNAL_TABLES["TEST"] = {t: ENVIRONMENTS["TEST"] + "." + n.split(".")[-1] for t, n in EXTERNAL_TABLES["PROD"].items()}
EXTERNAL_TABLES["TEST"]["CALENDARIO"] = EXTERNAL_TABLES["PROD"]["CALENDARIO"]
EXTERNAL_TABLES["TEST"]["BANDEIRA"] = EXTERNAL_TABLES["PROD"]["BANDEIRA"]
EXTERNAL_TABLES["TEST"]["REPRESENTANTES"] = EXTERNAL_TABLES["PROD"]["REPRESENTANTES"]
EXTERNAL_TABLES["TEST"]["GERENCIA"] = EXTERNAL_TABLES["PROD"]["GERENCIA"]
EXTERNAL_TABLES["TEST"]["DEBITO"] = EXTERNAL_TABLES["PROD"]["DEBITO"]