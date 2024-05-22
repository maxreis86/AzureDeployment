
# Número de meses para considerar o valor de chargeback na determinação
# de quais clientes devemos abrir a nivel cadeia forçada
MESES_CGB_CF = 6

# Mínimo em basis point para incluir na lista de CFs a parte
BASIS_POINTS_CF = 5

# Número de dias que consideramos que não deve existir chargeback
DIAS_SEM_CGB = 2

# Número de dias em que vamos aplicar a projeção de chargeback smooth
DIAS_PROJ_SMOOTH = 150

# Fator de decaimento do chargeback incremental ao atingir um platô no smooth
FATOR_DECAI_SMOOTH = 0.95

# Periodo de tempo em MESES com o historico que sera usado para apurar as curvas de chargeback por segmento
JANELA_APURACAO = 24

# número de dias para o qual faremos o cruzamento do chargeback/cancelamento com as informações da transação original
DIAS_CRUZAMENTO = {"chrg": 180, "cncl": 360}

# Tempo minimo de maturacao das safras de chargeback em MESES. Serao consideradas apenas safras com idade maior ou igual a definida
MATURACAO_SAFRA = {k: v // 30 for k, v in DIAS_CRUZAMENTO.items()}

# seleciona o tipo de valor para usar nos calculos das curvas
# opções -> brto, lqdo
TIPO_VALOR: str = "brto"

# seleciona o tipo de calculo de peso a ser aplicado por dia no calculo curvas
# opções -> none, linear, log, exp
TIPO_PESO: str = "log"

# Para o calculo do TPV medio, nao iremos aplicar ponderacao
# opções -> none, linear, log, exp
TIPO_PESO_TPV_MED: str = "none"

# Periodo de tempo em MESES com o historico que sera usado para calcular a media diaria de TPV
JANELA_APURACAO_TPV = 3

# Período em MESES para apurar dados históricos de ARV
JANELA_APURACAO_ARV = 3

# Periodo de tempo em DIAS para o qual sera projetado o TPV
DIAS_PROJECAO_TPV = 60

# Periodo de tempo em DIAS para o calculo de valores médios de grupo
DIAS_GRUPO_CNPJ = 30

# Tipo de peso a ser aplicado no calculo do peso do CNPJ por data de transação
TIPO_PESO_CNPJ = "linear"

# Flag se desejamos incluir cancelamentos na projeção de perda
INCLUIR_CANC = 1

# Valores máximos de perda aceitável para grandes contas e varejo
PERDA_MAX_VAREJO = 10
PERDA_MAX_GC = 1000

# Máxima diferença considerável aceitável entre o percentual financiado
# normalmente pelo cliente e o valor de agenda livre após a reserva
MARGEM_FINANC = 0.01

# Máxima diferença considerável aceitável entre o percentual financiado 
# normalmente pelo cliente e o valor de agenda livre para poder aplicar
# a reserva ótima, quando a mesma é maior que a reserva de perda limite
DELTA_RSVA_OT = 0.3

# Percentual a ser multiplicado pela perda máxima para que a variação de
# perda seja considerada no cálculo de reserva ótima
VAR_MIN_DELTA_OT = 0.2

# Colunas utilizadas no calculo de perda
COLS_PROJ = [
    "nu_dia_prjt",
    "vl_chrg_prjt", 
    "vl_chrg_rbrp", 
    "vl_chrg_liqd", 
    "vl_chrg_arv",
    "vl_chrg_prjt_icmo", 
    "vl_chrg_rbrp_icmo", 
    "vl_chrg_liqd_icmo", 
    "vl_chrg_arv_icmo",
    "vl_totl_liq", 
    "vl_lvre_liq",
    "vl_fncd_liq",
    "vl_grvm_liq",
    "vl_sfra_prjt",
    "pc_liq_1d",
    "pc_liq_90d",
    "pc_rbrp",
    "pc_fncd",
    "pc_fncd_cilo",
    "pc_grvm",
] 