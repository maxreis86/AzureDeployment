import typing
from itertools import chain
from datetime import datetime
from dateutil.relativedelta import relativedelta

import pyspark.sql.functions as F
import pyspark.sql.types as T
from pyspark.sql import DataFrame
from pyspark.sql.window import Window


def calcula_perdas(
    array, 
    reserva_pct: float, 
    debug: bool = False, 
    transferir_rsva: bool = False, 
    distr_prop: bool = False,
    zerar_agnd: bool = True,
    threshold_fin: float = 0.8,
) -> typing.Dict[str, float]:
    """
    Dado um array de structs que contém em cada dias de projeção as seguintes informações:
    
    Index 00: dia de projeção no futuro
    Index 01: valor projetado de chargeback total
    Index 02: valor projetado de chargeback que foi antecipado via RR
    Index 03: valor projetado de chargeback após recuperação pelos valores a liquidar
    Index 04: valor projetado de chargeback que foram antecipados pela cielo a serem recuperados
    Index 04: valor projetado de chargeback total do tpv incremental
    Index 05: valor projetado de chargeback que foi antecipado via RR do tpv incremental
    Index 06: valor projetado de chargeback após recuperação pelos valores a liquidar do tpv incremental
    Index 07: valor projetado de chargeback que foram antecipados pela cielo a serem recuperados do tpv incremental
    Index 08: valor total da agenda a ser liquidado
    Index 09: valor livre da agenda a ser liquidado
    Index 10: valor financiado da agenda a ser liquidado
    Index 11: valor gravamado a ser liquidado
    Index 12: valor de TPV não performado projetado
    Index 13: percentual de TPV não performado que vai liquidar no próximo dia
    Index 14: percentual de TPV não performado que vai liquidar no próximos 90 dias
    Index 15: percentual de antecipação feito por RR do cnpj
    Index 16: percentual do agenda total que é antecipado
    Index 17: percentual do agenda financiada que é antecipado via cielo
    Index 18: percentual do agenda financiada que é gravamada

    E um percentual de reserva a ser constituído, calcula o valor de perda esperada
    que o cliente terá em caso de um evento de decréscimo de TPV

    :param array: array de projeções de valores
    :param reserva_pct: valor percentual de reserva a ser constituido
    :param debug: True se queremos executar o calculo em modo debug
    :param transferir_rsva: True se queremos incluir a mecânica de transferir reserva legada para incremental
    :param distr_prop: True se queremos distribuir a recomposição de reserva proporcional as agenda livres
    :param zerar_agnd: True se queremos que o cliente zere a agenda livre quando o financiamento é acima de um threshold
    :param threshold_fin: threshold de financiamento para zerar a agenda
    :return: perda ao final do loop e perda total dos 540 dias
    """
    indices = [
        "dia", 
        "cgb_tot_leg", 
        "cgb_rr_leg", 
        "cgb_liq_leg", 
        "cgb_arv_leg",
        "cgb_tot_inc", 
        "cgb_rr_inc", 
        "cgb_liq_inc", 
        "cgb_arv_inc",
        "agnd_tot", 
        "agnd_lvr", 
        "agnd_fin", 
        "agnd_grv", 
        "tpv", 
        "liq_tpv_1d", 
        "liq_tpv_90d", 
        "pct_rr",
        "pct_fin",
        "pct_fin_cielo",
        "pct_grv",
    ]
    dp = {n: i for i, n in enumerate(indices)}

    # calcula o valor percentual de financiamento e gravame
    pct_fin = array[0][dp["pct_fin"]]
    pct_grv = array[0][dp["pct_grv"]]
    pct_rr = array[0][dp["pct_rr"]]
    
    # obtém os valores iniciais de simulação
    agnd_tot_leg = array[0][dp["agnd_tot"]]  # agenda total no período inicial
    rsva_leg = reserva_pct * agnd_tot_leg    # reserva inicial estipulada
    agnd_tot_inc = 0                         # agenda total gerada pelo TPV incremental
    agnd_lvr_inc = 0                         # agenda livre gerada pelo TPV incremental
    agnd_grv_inc = 0                         # agenda gravamada gerada pelo TPV incremental
    rsva_inc = 0                             # reserva inicial do TPV incremental
    perda = 0                                # debit balance inicial
    perda_arv = 0                            # debit balance relativo a ARV inicial
    
    # agenda livre no período inicial
    desc_lvr = rsva_leg * (1 - pct_fin)
    agnd_lvr_leg = agnd_tot_leg * (1 - pct_fin) - desc_lvr  
    
    # agenda antecipada no período inicial
    desc_fin = rsva_leg * pct_fin
    agnd_fin_leg = agnd_tot_leg * pct_fin - desc_fin  
    
    # agenda gravamada no período inicial
    agnd_grv_leg = agnd_fin_leg * pct_grv
    
    if debug:
        outputs = list()
        outputs.append(
            dict(
                pc_rbrp=pct_rr,
                pc_fncd=pct_fin,
                pc_grvm=pct_grv,
                nu_dia_prjt=0,
                vl_chrg_totl_lgdo=0,
                vl_chrg_rbrp_lgdo=0,
                vl_chrg_lqdo_lgdo=0,
                vl_chrg_liq_lgdo=0,
                vl_chrg_arv_lgdo=0,
                vl_chrg_totl_icmo=0,
                vl_chrg_rbrp_icmo=0,
                vl_chrg_lqdo_icmo=0,
                vl_chrg_liq_icmo=0,
                vl_chrg_arv_icmo=0,
                vl_totl_liq=0,
                vl_lvre_liq=0,
                vl_fncd_liq=0,
                vl_grvm_liq=0,
                vl_tpv_icmo=0,
                pc_tpv_liq_1d=0,
                pc_tpv_liq_90d=0,
                vl_perd=perda,
                vl_perd_arv=perda_arv,
                pc_agnd_lvre_90d=0,
                pc_agnd_grvm_90d=0,
                pc_agnd_icmo_90d=0,
                pc_lqdo_agnd_totl=0,
                pc_lqdo_agnd_lvre=0,
                pc_lqdo_agnd_grvm=0,
                pc_lqdo_tpv_icmo=0,
                vl_rcpv_agnd_lvre_lgdo=0,
                vl_rcpv_agnd_lvre_icmo=0,
                vl_rcpv_agnd_grvm_lgdo=0,
                vl_rcpv_agnd_grvm_icmo=0,
                vl_rcpv_lvre_lgdo=0,
                vl_rcpv_lvre_icmo=0,
                vl_rcpv_grvm_lgdo=0,
                vl_rcpv_grvm_icmo=0,
                vl_rcpv_rsrv_lgdo=0,
                vl_rcpv_rsrv_icmo=0,
                vl_rcpv_lvre_icmo_tpv=0,
                vl_rcpv_lvre_icmo_agnd=0,
                vl_rsrv_idel=0,
                vl_recm_tpv_icmo=0,
                vl_recm_agnd_icmo=0,
                vl_recm_agnd_lgdo=0,
                vl_trne_rsrv_icmo=0,
                vl_rsrv_lvre=0,
                vl_rsrv_lvre_icmo=0,
                vl_rsrv_lvre_lgdo=0,
                vl_rsrv_lgdo=rsva_leg,
                vl_rsrv_icmo=rsva_inc,
                vl_antp_rbrp=0,
                vl_antp_arv=0,
                vl_antp_arv_icmo=0,
                vl_antp_arv_lgdo=0,
                vl_antp_grvm=0,
                vl_antp_grvm_icmo=0,
                vl_antp_grvm_lgdo=0,
                vl_agnd_totl_lgdo=agnd_tot_leg, 
                vl_agnd_lvre_lgdo=agnd_lvr_leg,
                vl_agnd_grvm_lgdo=agnd_grv_leg,
                vl_agnd_totl_icmo=agnd_tot_inc,
                vl_agnd_lvre_icmo=agnd_lvr_inc,
                vl_agnd_grvm_icmo=agnd_grv_inc,
            )
        )

    # para cada dia de projeção
    for i in range(1, len(array) - 1):
        # calcula o percentual de agenda que vai liquidar por grupo
        liqd_agnd_tot = array[i + 1][dp["agnd_tot"]] / array[i][dp["agnd_tot"]] if array[i][dp["agnd_tot"]] != 0 else 0
        liqd_agnd_lvr = array[i + 1][dp["agnd_lvr"]] / array[i][dp["agnd_lvr"]] if array[i][dp["agnd_lvr"]] != 0 else 0
        liqd_agnd_grv = array[i + 1][dp["agnd_grv"]] / array[i][dp["agnd_grv"]] if array[i][dp["agnd_grv"]] != 0 else 0
        liqd_tpv_inc = array[i][dp["liq_tpv_1d"]]

        # calcula o percentual da agenda que está por liquidar nos próximos 90 dias
        pct_agnd_l90 = (
            (array[i][dp["agnd_lvr"]] - array[i:i + 91][-1][dp["agnd_lvr"]]) / array[i][dp["agnd_lvr"]] 
            if array[i][dp["agnd_lvr"]] != 0 else 0
        )
        pct_agnd_g90 = (
            (array[i][dp["agnd_grv"]] - array[i:i + 91][-1][dp["agnd_grv"]]) / array[i][dp["agnd_grv"]] 
            if array[i][dp["agnd_grv"]] != 0 else 0
        )
        pct_agnd_i90 = array[i][dp["liq_tpv_90d"]]
        
        # obtém as variáveis da data em questão
        cgb_tot_leg = array[i][dp["cgb_tot_leg"]]
        cgb_rr_leg = array[i][dp["cgb_rr_leg"]]
        cgb_liq_leg = array[i][dp["cgb_liq_leg"]]
        cgb_aliq_leg = cgb_tot_leg - cgb_rr_leg - cgb_liq_leg
        cgb_arv_leg = array[i][dp["cgb_arv_leg"]]

        cgb_tot_inc = array[i][dp["cgb_tot_inc"]]
        cgb_rr_inc = array[i][dp["cgb_rr_inc"]]
        cgb_liq_inc = array[i][dp["cgb_liq_inc"]]
        cgb_aliq_inc = cgb_tot_inc - cgb_rr_inc - cgb_liq_inc
        cgb_arv_inc = array[i][dp["cgb_arv_inc"]]

        tpv_inc = array[i][dp["tpv"]]
        
        # ###################################################################################################### #
        # DESCONTOS DE AGENDA                                                                                    #
        # ###################################################################################################### # 
        # deconta os valores a liquidar recuperados dos valores de agenda livre, gravamada e reserva
        r_agnd_lvr_leg = agnd_lvr_leg / agnd_tot_leg if agnd_tot_leg > 0 else 0
        r_agnd_grv_leg = agnd_grv_leg / agnd_tot_leg if agnd_tot_leg > 0 else 0
        r_rsva_leg = rsva_leg / agnd_tot_leg if agnd_tot_leg > 0 else 0
        
        cgb_aliq_leg_rec = min(cgb_aliq_leg, agnd_tot_leg)
        
        agnd_lvr_leg -= r_agnd_lvr_leg * cgb_aliq_leg_rec
        agnd_grv_leg -= r_agnd_grv_leg * cgb_aliq_leg_rec
        rsva_leg -= r_rsva_leg * cgb_aliq_leg_rec
        agnd_tot_leg -= cgb_aliq_leg_rec
        
        cgb_aliq_leg_exc = cgb_aliq_leg - cgb_aliq_leg_rec

        # deconta os valores a liquidar recuperados dos valores de agenda livre, gravamada e reserva
        r_agnd_lvr_inc = agnd_lvr_inc / agnd_tot_inc if agnd_tot_inc > 0 else 0
        r_agnd_grv_inc = agnd_grv_inc / agnd_tot_inc if agnd_tot_inc > 0 else 0
        r_rsva_inc = rsva_inc / agnd_tot_inc if agnd_tot_inc > 0 else 0
        
        cgb_aliq_inc_rec = min(cgb_aliq_inc, agnd_tot_inc)
        
        agnd_lvr_inc -= r_agnd_lvr_inc * cgb_aliq_inc_rec
        agnd_grv_inc -= r_agnd_grv_inc * cgb_aliq_inc_rec
        rsva_inc -= r_rsva_inc * cgb_aliq_inc_rec
        agnd_tot_inc -= cgb_aliq_inc_rec
        
        cgb_aliq_inc_exc = cgb_aliq_inc - cgb_aliq_inc_rec
        
        # ###################################################################################################### #
        # RECUPERAÇÃO                                                                                            #
        # ###################################################################################################### # 
        # calcula o chargeback que precisa ser compensado
        perda += cgb_rr_leg + cgb_rr_inc + cgb_liq_leg + cgb_liq_inc + cgb_aliq_leg_exc + cgb_aliq_inc_exc
        perda_arv += cgb_arv_leg + cgb_arv_inc

        # RECUPERAÇÃO 1 - Agenda Livre
        vr_agnd_lvr_leg = agnd_lvr_leg * pct_agnd_l90                  # valor de agenda livre legada a liquidar em 90 dias
        vr_agnd_lvr_inc = (agnd_lvr_inc + tpv_inc) * pct_agnd_i90      # valor de agenda livre incremental a liquidar em 90 dias
        vr_agnd_lvr = vr_agnd_lvr_leg + vr_agnd_lvr_inc                # valor recuperável por agenda livre
        perda_lvr = perda + perda_arv                                  # valor de perda a ser recuperado em agenda livre
        if vr_agnd_lvr > 0 and perda_lvr > 0:
            recup_lvr = min(perda_lvr, vr_agnd_lvr)                    # valor recuperado em agenda livre
            recup_lvr_leg = recup_lvr * vr_agnd_lvr_leg / vr_agnd_lvr  # valor recuperado em agenda livre legada
            recup_lvr_inc = recup_lvr * vr_agnd_lvr_inc / vr_agnd_lvr  # valor recuperado em agenda livre incremental
            perda -= recup_lvr * perda / perda_lvr                     # valor de perda a ser recuperado
            perda_arv -= recup_lvr * perda_arv / perda_lvr             # valor de perda a ser recuperado
        else:
            recup_lvr = recup_lvr_leg = recup_lvr_inc = 0

        # RECUPERAÇÃO 2 - Agenda Gravamada
        vr_agnd_grv_leg = agnd_grv_leg * pct_agnd_g90                  # valor de agenda gravamada legada a liquidar em 90 dias
        vr_agnd_grv_inc = agnd_grv_inc * pct_agnd_i90                  # valor de agenda gravamada incremental a liquidar em 90 dias
        vr_agnd_grv = vr_agnd_grv_leg + vr_agnd_grv_inc                # valor recuperável por agenda gravamada       
        if vr_agnd_grv > 0:
            recup_grv = min(perda, vr_agnd_grv)                        # valor recuperado em agenda gravamada
            recup_grv_leg = recup_grv * vr_agnd_grv_leg / vr_agnd_grv  # valor recuperado em agenda livre legada
            recup_grv_inc = recup_grv * vr_agnd_grv_inc / vr_agnd_grv  # valor recuperado em agenda livre incremental
            perda -= recup_grv                                         # valor de perda a ser recuperado
        else:
            recup_grv = recup_grv_leg = recup_grv_inc = 0

        # RECUPERAÇÃO 3 - Reserva
        rsva_tot = rsva_leg + rsva_inc                                 # valor de reserva total
        perda_rsva = perda + perda_arv                                 # valor de perda a ser recuperado em reserva
        if rsva_tot > 0 and perda_rsva > 0:
            recup_rsva = min(perda_rsva, rsva_tot)                     # valor recuperado em reserva
            recup_rsva_leg = recup_rsva * rsva_leg / rsva_tot          # valor de recuperável na reserva legada
            recup_rsva_inc = recup_rsva * rsva_inc / rsva_tot          # valor de recuperável na reserva incremental
            rsva_leg -= recup_rsva_leg                                 # valor da reserva legada pós recuperação
            rsva_inc -= recup_rsva_inc                                 # valor da reserva incremental pós recuperação
            perda -= recup_rsva * perda / perda_rsva                   # valor de perda a ser recuperado
            perda_arv -= recup_rsva * perda_arv / perda_rsva           # valor de perda a ser recuperado
        else:
            recup_rsva = recup_rsva_leg = recup_rsva_inc = 0

        # calcula a razão que o TPV incremental representa sobre o total de montante recuperável
        # de agenda livre incremental e novo TPV incremental
        ratio_tpv_inc = tpv_inc / (tpv_inc + agnd_lvr_inc) if (tpv_inc + agnd_lvr_inc) > 0 else 0
        ratio_agnd_inc = 1 - ratio_tpv_inc

        # calcula o valor recuperado que estará atrelado ao TPV incremental
        recup_lvr_inc_tpv = recup_lvr_inc * ratio_tpv_inc
        recup_lvr_inc_agnd = recup_lvr_inc * ratio_agnd_inc
        
        # ###################################################################################################### #
        # LIQUIDAÇÕES                                                                                            #
        # ###################################################################################################### #          
        # recupera valores da agenda livre e liquida a mesma
        agnd_lvr_inc = agnd_lvr_inc - recup_lvr_inc_agnd
        agnd_lvr_inc *= liqd_tpv_inc
        agnd_lvr_leg = agnd_lvr_leg - recup_lvr_leg
        agnd_lvr_leg *= liqd_agnd_lvr

        # calcula os valor da agenda total, baseado na recuperação
        agnd_tot_leg = agnd_tot_leg - recup_lvr_leg - recup_grv_leg - recup_rsva_leg
        agnd_tot_leg *= liqd_agnd_tot
        
        agnd_tot_inc = agnd_tot_inc - recup_lvr_inc_agnd - recup_grv_inc - recup_rsva_inc
        agnd_tot_inc *= liqd_tpv_inc
        agnd_tot_inc += tpv_inc - recup_lvr_inc_tpv

        # calcula os valores de agenda gravamada, baseado na recuperação
        agnd_grv_inc -= recup_grv_inc
        agnd_grv_inc *= liqd_tpv_inc
        agnd_grv_leg -= recup_grv_leg
        agnd_grv_leg *= liqd_agnd_grv

        # desconta a recuperação do TPV incremental
        tpv_inc -= recup_lvr_inc_tpv
        
        # calcula a liquidação dos valores de reserva
        rsva_leg *= liqd_agnd_tot
        rsva_inc *= liqd_tpv_inc
        
        # ###################################################################################################### #
        # RECOMPOSIÇÃO                                                                                           #
        # ###################################################################################################### #            
        # calcula o valor de reserva ideal
        rsva_ideal = reserva_pct * (agnd_tot_leg + agnd_tot_inc)

        # verifica se há valores que precisam ser recompostos entre a reserva ideal e existente
        gap_rsva = max(rsva_ideal - rsva_leg - rsva_inc, 0)
        if distr_prop:
            # calcula a recomposição pelo valor proporcional
            tot_mont_lvr = (tpv_inc + agnd_lvr_inc + agnd_lvr_leg)
            recomp_rsva = min(gap_rsva, tot_mont_lvr)
            recomp_tpv_inc = recomp_rsva * tpv_inc / tot_mont_lvr if tot_mont_lvr > 0 else 0
            recomp_agnd_inc = recomp_rsva * agnd_lvr_inc / tot_mont_lvr if tot_mont_lvr > 0 else 0
            recomp_agnd_leg = recomp_rsva * agnd_lvr_leg / tot_mont_lvr if tot_mont_lvr > 0 else 0
        else:
            # calcula a recomposição por hierarquia de agendas
            recomp_tpv_inc = min(gap_rsva, tpv_inc)
            recomp_agnd_inc = min(gap_rsva - recomp_tpv_inc, agnd_lvr_inc)
            recomp_agnd_leg = min(gap_rsva - recomp_tpv_inc - recomp_agnd_inc, agnd_lvr_leg)

        # desconta o TPV incremental dos valores recomposts
        tpv_inc -= recomp_tpv_inc

        # calcula o novo valor de reser
        rsva_inc += recomp_tpv_inc + recomp_agnd_inc
        rsva_leg += recomp_agnd_leg

        # calcula a transferência de reserva legada para TPV incremental
        transf_rsva_inc = min(tpv_inc, rsva_leg) if transferir_rsva else 0
        rsva_leg -= transf_rsva_inc
        rsva_inc += transf_rsva_inc

        # desconta o TPV incremental dos valores transferidos
        tpv_inc -= transf_rsva_inc

        # libera reserva para volumes livres
        rsva_livre = max(rsva_leg + rsva_inc - rsva_ideal, 0)
        rsva_livre_inc = rsva_livre * rsva_inc / (rsva_leg + rsva_inc) if rsva_leg + rsva_inc > 0 else 0
        rsva_livre_leg = rsva_livre * rsva_leg / (rsva_leg + rsva_inc) if rsva_leg + rsva_inc > 0 else 0
        rsva_inc -= rsva_livre_inc
        rsva_leg -= rsva_livre_leg

        # ###################################################################################################### #
        # ANTECIPAÇÃO                                                                                            #
        # ###################################################################################################### # 
        # seta o valor incremental livre
        inc_livre_inc = tpv_inc + rsva_livre_inc
        inc_livre_leg = rsva_livre_leg + transf_rsva_inc

        # calcula o valor livre que será antecipado via RR
        antc_rr = tpv_inc * pct_rr
        inc_livre_inc -= antc_rr
        agnd_tot_inc -= antc_rr

        # calcula o restante do valor livre que será antecipado via ARV
        antc_arv = (inc_livre_inc + inc_livre_leg) * pct_fin
        antc_arv_inc = antc_arv * inc_livre_inc / (inc_livre_inc + inc_livre_leg) if inc_livre_inc + inc_livre_leg > 0 else 0
        antc_arv_leg = antc_arv * inc_livre_leg / (inc_livre_inc + inc_livre_leg) if inc_livre_inc + inc_livre_leg > 0 else 0
        inc_livre_inc -= antc_arv_inc
        inc_livre_leg -= antc_arv_leg

        # calcula do valor antecipado quanto vira gravame
        antc_grv_inc = antc_arv_inc * pct_grv
        antc_grv_leg = antc_arv_leg * pct_grv
        antc_grv = antc_grv_inc + antc_grv_leg
        
        # ###################################################################################################### #
        # AGENDAS                                                                                                #
        # ###################################################################################################### #
        # reconstitui a agenda livre incremental
        agnd_lvr_inc = agnd_lvr_inc + inc_livre_inc - recomp_agnd_inc
        
        # reconstitui a agenda livre legado
        agnd_lvr_leg = agnd_lvr_leg + inc_livre_leg - recomp_agnd_leg

        # calcula o TPV incremental projetado
        tpv_inc_proj = max(sum([v[dp["tpv"]] for v in array[i + 1:]]), 0)

        # se o tpv incremental projetado por zero, e o cliente tiver uma antecipação
        # acima de 80%, assume que o mesmo vai antecipar 100% da agenda no evento
        # pós churn
        if zerar_agnd and tpv_inc_proj == 0 and pct_fin > threshold_fin:
            antc_arv_inc += agnd_lvr_inc 
            antc_arv_leg += agnd_lvr_leg
            antc_arv = antc_arv_inc + antc_arv_leg
            agnd_lvr_inc = agnd_lvr_leg = 0
            antc_grv_inc = antc_arv_inc * pct_grv
            antc_grv_leg = antc_arv_leg * pct_grv
            antc_grv = antc_grv_inc + antc_grv_leg
            pct_fin = 1
        
        # reconstitui a agenda gravamada
        agnd_grv_inc += antc_grv_inc
        agnd_grv_leg += antc_grv_leg

        # ###################################################################################################### #
        # PRÓXIMO DIA                                                                                            #
        # ###################################################################################################### # 
        # se a agenda, reserva e tpv incremental forem nulos, interrompe o loop
        if max(agnd_tot_leg + agnd_tot_inc, 0) <= 1e-15 and max(rsva_inc + rsva_leg, 0) <= 1e-15 and tpv_inc_proj <= 1e-15:
            break
        
        if debug:
            outputs.append(
                dict(
                    pc_rbrp=pct_rr,
                    pc_fncd=pct_fin,
                    pc_grvm=pct_grv,
                    nu_dia_prjt=array[i][dp["dia"]],
                    vl_chrg_totl_lgdo=array[i][dp["cgb_tot_leg"]],
                    vl_chrg_rbrp_lgdo=array[i][dp["cgb_rr_leg"]],
                    vl_chrg_lqdo_lgdo=array[i][dp["cgb_liq_leg"]],
                    vl_chrg_liq_lgdo=cgb_aliq_leg,
                    vl_chrg_arv_lgdo=array[i][dp["cgb_arv_leg"]],
                    vl_chrg_totl_icmo=array[i][dp["cgb_tot_inc"]],
                    vl_chrg_rbrp_icmo=array[i][dp["cgb_rr_inc"]],
                    vl_chrg_lqdo_icmo=array[i][dp["cgb_liq_inc"]],
                    vl_chrg_liq_icmo=cgb_aliq_inc,
                    vl_chrg_arv_icmo=array[i][dp["cgb_arv_inc"]],
                    vl_totl_liq=array[i][dp["agnd_tot"]],
                    vl_lvre_liq=array[i][dp["agnd_lvr"]],
                    vl_fncd_liq=array[i][dp["agnd_fin"]],
                    vl_grvm_liq=array[i][dp["agnd_grv"]],
                    vl_tpv_icmo=array[i][dp["tpv"]],
                    pc_tpv_liq_1d=array[i][dp["liq_tpv_1d"]],
                    pc_tpv_liq_90d=array[i][dp["liq_tpv_90d"]],
                    vl_perd=perda,
                    vl_perd_arv=perda_arv,
                    pc_agnd_lvre_90d=pct_agnd_l90,
                    pc_agnd_grvm_90d=pct_agnd_g90,
                    pc_agnd_icmo_90d=pct_agnd_i90,
                    pc_lqdo_agnd_totl=liqd_agnd_tot,
                    pc_lqdo_agnd_lvre=liqd_agnd_lvr,
                    pc_lqdo_agnd_grvm=liqd_agnd_grv,
                    pc_lqdo_tpv_icmo=liqd_tpv_inc,
                    vl_rcpv_agnd_lvre_lgdo=vr_agnd_lvr_leg,
                    vl_rcpv_agnd_lvre_icmo=vr_agnd_lvr_inc,
                    vl_rcpv_agnd_grvm_lgdo=vr_agnd_grv_leg,
                    vl_rcpv_agnd_grvm_icmo=vr_agnd_grv_inc,
                    vl_rcpv_lvre_lgdo=recup_lvr_leg,
                    vl_rcpv_lvre_icmo=recup_lvr_inc,
                    vl_rcpv_grvm_lgdo=recup_grv_leg,
                    vl_rcpv_grvm_icmo=recup_grv_inc,
                    vl_rcpv_rsrv_lgdo=recup_rsva_leg,
                    vl_rcpv_rsrv_icmo=recup_rsva_inc,
                    vl_rcpv_lvre_icmo_tpv=recup_lvr_inc_tpv,
                    vl_rcpv_lvre_icmo_agnd=recup_lvr_inc_agnd,
                    vl_rsrv_idel=rsva_ideal,
                    vl_recm_tpv_icmo=recomp_tpv_inc,
                    vl_recm_agnd_icmo=recomp_agnd_inc,
                    vl_recm_agnd_lgdo=recomp_agnd_leg,
                    vl_trne_rsrv_icmo=transf_rsva_inc,
                    vl_rsrv_lvre=rsva_livre,
                    vl_rsrv_lvre_icmo=rsva_livre_inc,
                    vl_rsrv_lvre_lgdo=rsva_livre_leg,
                    vl_rsrv_lgdo=rsva_leg,
                    vl_rsrv_icmo=rsva_inc,
                    vl_antp_rbrp=antc_rr,
                    vl_antp_arv=antc_arv,
                    vl_antp_arv_icmo=antc_arv_inc,
                    vl_antp_arv_lgdo=antc_arv_leg,
                    vl_antp_grvm=antc_grv,
                    vl_antp_grvm_icmo=antc_grv_inc,
                    vl_antp_grvm_lgdo=antc_grv_leg,
                    vl_agnd_totl_lgdo=agnd_tot_leg,
                    vl_agnd_lvre_lgdo=agnd_lvr_leg,
                    vl_agnd_grvm_lgdo=agnd_grv_leg,
                    vl_agnd_totl_icmo=agnd_tot_inc,
                    vl_agnd_lvre_icmo=agnd_lvr_inc,
                    vl_agnd_grvm_icmo=agnd_grv_inc,
                )
            )

    # calcula o valor de chargeback que não poderá ser recuperado
    if len(array) <= 2:
        add = add_arv = 0
    elif i != len(array) - 1:
        add = sum(
            [
                array[j][dp["cgb_rr_leg"]] 
                + array[j][dp["cgb_liq_leg"]]  
                + array[j][dp["cgb_rr_inc"]] 
                + array[j][dp["cgb_liq_inc"]]  
                for j in range(i + 1, len(array))
            ]
        )
        add_arv = sum(
            [
                array[j][dp["cgb_arv_leg"]] + array[j][dp["cgb_arv_inc"]] 
                for j in range(i + 1, len(array))
            ]
        )
    else:
        add = add_arv = 0
    
    # se o loop foi interrompido no primeiro loop, e não havia agenda 
    # no inicio, significa que não há nenhum mecanismo de recuperção
    # e portanto nada da perda é recuperável
    if len(array) <= 2:
        perda = sum([
            a[dp["cgb_rr_leg"]] 
            + a[dp["cgb_liq_leg"]]  
            + a[dp["cgb_rr_inc"]] 
            + a[dp["cgb_liq_inc"]]  
            for a in array
        ])
        perda_arv = sum([a[dp["cgb_arv_leg"]] + a[dp["cgb_arv_inc"]] for a in array])
    elif i == 1 and array[0][dp["agnd_tot"]] == 0:
        perda = perda_arv = 0
    
    if not debug:
        return {
            "vl_perd_rcpv_crde": perda, 
            "vl_perd_totl_crde": perda + add,
            "vl_perd_rcpv_fdor": perda_arv, 
            "vl_perd_totl_fdor": perda_arv + add_arv,
            "vl_perd_rcpv_finl": perda + perda_arv,
            "vl_perd_totl_finl": perda + add + perda_arv + add_arv,
        }
    else:
        return outputs    


def calcula_reservas(array, early_stop: float = 0.01) -> typing.List[typing.Dict[str, float]]:
    """
    Dado um array de structs que contém em cada dias de projeção as seguintes informações:

    Executa para percentuais de reservas incrementais o calculo de perda esperada
    dado o percentual de reserva.

    Esse calculo pode ser terminado precocemente caso seja constado que o valor de perda
    após a agenda terminar zerada seja 0, de forma que toda perda que podia ser recuperada
    na agenda conseguiu ser compensada

    :param array: array de projeções de valores
    :param early_stop: minima variação do valor de perda recuperável entre reservas para continuar pesquisa
    :return: lista de dicionários com percentual de reserva e perda associada
    """
    # cria uma lista do valor de reserva ao longo do tempo
    reserva = list()
    perda = list()
    
    # percorre a lista de reservas
    reserva_pct_list = [p / 1000 for p in range(6)] + [p / 100 for p in range(1, 101)]
    perda_ant = 1e9
    for reserva_pct in reserva_pct_list:
        # aplica o calculo de reserva
        perdas = calcula_perdas(array, reserva_pct)
        perda_recup = perdas["vl_perd_rcpv_finl"]
        perda_total = perdas["vl_perd_totl_finl"]
        
        # salva os resultados
        reserva.append(reserva_pct)
        perda.append(perda_total)
        
        # verifica se a perda acumulada ficou maior que zero se esse for o caso 
        # significa que há oportunidade para aumentar a reserva e reduzir a perda
        if perda_recup > 0 and perda_ant - perda_recup > early_stop:
            perda_ant = perda_recup
            continue
        else:
            break
    
    return [
        {"vl_rsrv": reserva[i], "vl_perd": perda[i]} 
        for i in range(len(reserva))
    ]
    
    
schema_perdas = T.StructType(
    [
        T.StructField("vl_perd_rcpv_crde", T.DoubleType(), False),
        T.StructField("vl_perd_totl_crde", T.DoubleType(), False),
        T.StructField("vl_perd_rcpv_fdor", T.DoubleType(), False),
        T.StructField("vl_perd_totl_fdor", T.DoubleType(), False),
        T.StructField("vl_perd_rcpv_finl", T.DoubleType(), False),
        T.StructField("vl_perd_totl_finl", T.DoubleType(), False),
    ]
)
schema_reservas = T.ArrayType(
    T.StructType(
        [
            T.StructField("vl_rsrv", T.DoubleType(), False),
            T.StructField("vl_perd", T.DoubleType(), False),
        ]
    )
)
calcula_perdas_udf = F.udf(calcula_perdas, schema_perdas)
calcula_reservas_udf = F.udf(calcula_reservas, schema_reservas)


def realiza_calculo_perda(
    du: "DataUtils", 
    df_proj: DataFrame, 
    df_rsva: DataFrame,
    df_churn: DataFrame,
    dt_cnpj: str,
) -> DataFrame:
    """
    Dado um dataframe com dados que podem ser utilizado para o calculo de perda, realiza
    a projeção de perda esperada em churn 60/100 e com reservas selecionada, 0% e 100%

    :param du: instancia do data utils
    :param df_proj: data frame com os parâmetros do calculo de perda
    :param df_rsva: dados de reserva ótima
    :param df_churn: dados de probabilidade de churn
    :param dt_cnpj: data dos dados de grupo de cnpj
    :return: data frame com as projeções de perda
    """
    from modelo.comuns import obtem_ultima_data_e_hora
    import modelo.configs as confs

    # consolida os dados de projeção em um array para cada CNPJ considerando os cenários de churn 60 e churn 100
    df_calc_perda = (
        df_proj.groupBy("cd_grpo_cnpj")
        .agg(
            F.sort_array(F.collect_list(F.struct(confs.COLS_PROJ)))
            .alias("collected_list")
        )
        .withColumn("dc_tipo_churn", F.lit(60))
    ).unionByName(
        df_proj.withColumn("vl_sfra_proj", F.lit(0))
        .withColumn("pc_liq_1d", F.lit(0))
        .withColumn("pc_liq_90d", F.lit(0))
        .withColumn("vl_chrg_prjt_icmo", F.lit(0))
        .withColumn("vl_chrg_liqd_icmo", F.lit(0))
        .withColumn("vl_chrg_rbrp_icmo", F.lit(0))
        .withColumn("vl_chrg_arv_icmo", F.lit(0))
        .groupBy("cd_grpo_cnpj")
        .agg(
            F.sort_array(F.collect_list(F.struct(confs.COLS_PROJ)))
            .alias("collected_list")
        )
        .withColumn("dc_tipo_churn", F.lit(100))
    )

    # adiciona os dados de reserva
    df_calc_perda = df_calc_perda.join(
        df_rsva.select("cd_grpo_cnpj", "dc_tipo_churn", "vl_rsrv")
        .withColumnRenamed("dc_tipo_churn", "dc_tipo_churn_ofcl"),
        on=["cd_grpo_cnpj"],
        how="left"
    )

    # gera os resultados de perda
    df_perda = (
        df_calc_perda.select(
            "cd_grpo_cnpj",
            "dc_tipo_churn",
            "dc_tipo_churn_ofcl",
            "vl_rsrv",
            calcula_perdas_udf(F.col("collected_list"), F.col("vl_rsrv")).alias("projecao"),
            calcula_perdas_udf(F.col("collected_list"), F.lit(0)).alias("projecao0"),
            calcula_perdas_udf(F.col("collected_list"), F.lit(1)).alias("projecao1"),
        )
        .select(
            ["cd_grpo_cnpj", "dc_tipo_churn", "dc_tipo_churn_ofcl", "vl_rsrv"]
            + [
                F.col(f"projecao{proj}")[f"vl_perd_{perd}_{mod}"].alias(f"vl_perd_{perd}_{mod}{proj}")
                for proj in ["", "0", "1"]
                for perd in ["rcpv", "totl"]
                for mod in ["crde", "fdor", "finl"]
            ]
        )
        .withColumn("vl_perd_rcpv", F.col("vl_perd_totl_finl0") - F.col("vl_perd_totl_finl"))
        .withColumn("vl_perd_max", F.col("vl_perd_totl_finl0") - F.col("vl_perd_totl_finl1"))
    )

    # agrega os dados a nivel cnpj e pelo tipo de churn
    df_perda_agg = (
        df_perda.withColumn("dc_tipo_churn", F.concat(F.lit("vl_c"), F.lpad(F.col("dc_tipo_churn"), 3, "0")))
        .groupBy("cd_grpo_cnpj")
        .pivot("dc_tipo_churn")
        .agg(
            *(
                [
                    F.max(f"vl_perd_{perd}_{mod}{proj}").alias(f"perd_{perd}_{mod}{proj}")
                    for proj in ["", "0", "1"]
                    for perd in ["rcpv", "totl"]
                    for mod in ["crde", "fdor", "finl"]
                ] + [
                    F.max("vl_perd_rcpv").alias("perd_rcpv"),
                    F.max("vl_perd_max").alias("perd_max")
                ]
            )
        )
        .join(
            df_perda.filter(F.col("dc_tipo_churn") == F.col("dc_tipo_churn_ofcl"))
            .drop("dc_tipo_churn_ofcl"),
            on="cd_grpo_cnpj",
            how="inner"
        )
    )

    # calcula o valor do multiplicador a ser aplicado no varejo
    ratio_tpv = (
        obtem_ultima_data_e_hora(du, int(dt_cnpj), "TRNS_CLUSTER")
        .filter(
            F.col("dt_trns") 
            >= datetime.strptime(dt_cnpj, "%Y%m%d") - relativedelta(days=90)
        )
        .groupBy("cd_grpo_cnpj")
        .agg((F.sum("vl_sfra_pndr") / F.sum("vl_sfra")).alias("ratio_tpv"))
    )
    cnpjs = (
        du.get_by_name("GRUPO_CNPJ")
        .filter(F.col("cd_dia_crte") == dt_cnpj)
        .groupBy("cd_grpo_cnpj")
        .agg(F.countDistinct("nu_cnpj_cpf_recr").alias("qt_cnpj"))
    )
    para_expandir = (
        cnpjs.join(ratio_tpv, on="cd_grpo_cnpj", how="left")
        .withColumn("vl_mltr", F.coalesce(1 / F.col("ratio_tpv"), F.col("qt_cnpj")))
        .drop("ratio_tpv")
    )

    # adiciona os dados do multiplicador
    df_perda_agg = (
        df_perda_agg.join(para_expandir, on="cd_grpo_cnpj", how="left")
        .withColumn("qt_cnpj", F.coalesce(F.col("qt_cnpj"), F.lit(1)))
        .withColumn("vl_mltr", F.coalesce(F.col("vl_mltr"), F.lit(1)))
        .select(
            ["cd_grpo_cnpj", "qt_cnpj", "vl_mltr", "dc_tipo_churn", "vl_rsrv"] + [
                (F.col(f"vl_c{chrn}_perd_{perd}_{mod}{proj}") * F.col("vl_mltr"))
                .alias(f"vl_c{chrn}_perd_{perd}_{mod}{proj}")
                for chrn in ["060", "100"]
                for proj in ["", "0", "1"]
                for perd in ["rcpv", "totl"]
                for mod in ["crde", "fdor", "finl"]
            ] + [
                (F.col(f"vl_c{chrn}_perd_{met}") * F.col("vl_mltr")).alias(f"vl_c{chrn}_perd_{met}")
                for chrn in ["060", "100"]
                for met in ["rcpv", "max"]
            ] + [
                (F.col(f"vl_perd_{perd}_{mod}{proj}") * F.col("vl_mltr"))
                .alias(f"vl_perd_{perd}_{mod}{proj}")
                for proj in ["", "0", "1"]
                for perd in ["rcpv", "totl"]
                for mod in ["crde", "fdor", "finl"]
            ] + [
                (F.col(f"vl_perd_{met}") * F.col("vl_mltr")).alias(f"vl_perd_{met}")
                for met in ["rcpv", "max"]
            ]
        )
    )

    # adiciona os dados de probabilidade de churn
    df_perda_agg = (
        df_perda_agg.join(df_churn, on="cd_grpo_cnpj", how="left")
        .select(
            ["*"] + [
                (F.col(f"vl_perd_{perd}_{mod}") * F.col("pc_prbl_churn")).alias(f"vl_perd_{perd}_{mod}_espa")
                for perd in ["rcpv", "totl"]
                for mod in ["crde", "fdor", "finl"]
            ] + [
                (F.col(f"vl_perd_{met}") * F.col("pc_prbl_churn")).alias(f"vl_perd_{met}_espa")
                for met in ["rcpv", "max"]
            ]
        )
    )

    return df_perda_agg

