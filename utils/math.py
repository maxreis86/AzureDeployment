import typing

import pyspark.sql.functions as F
from pyspark.sql import DataFrame
from pyspark.sql.window import Window



def aplica_peso_a_dif_datas(df: DataFrame, dt_col_1: str, dt_col_2: str, tipo_peso: str) -> DataFrame:
    """
    Aplica um calculo de peso para a diferença entre duas data presentes em um dataframe

    :param df: dataframe a ter o peso calculado
    :param dt_col_1: valor da menor data
    :param dt_col_2: valor da maior data
    :param tipo_peso: tipo de peso a ser aplicado
    :return: df com a coluna "peso"
    """
    date_diff = F.datediff(F.col(dt_col_2), F.col(dt_col_1))
    if tipo_peso == "none":
        return df.select("*", F.lit(1).alias("peso"))
    elif tipo_peso == "linear":
        return df.select("*", (date_diff + 1).alias("peso"))
    elif tipo_peso == "log":
        return df.select("*", F.log(date_diff + 1).alias("peso"))
    elif tipo_peso == "exp":
        return df.select("*", F.exp(date_diff).alias("peso"))
    else:
        raise ValueError(f"Não conhecemos o peso do tipo {tipo_peso}")
        
        
def fill_linear_interpolation(df: DataFrame, id_cols: list, order_col: str, value_col: str) -> DataFrame:
    """ 
    Apply linear interpolation to dataframe to fill gaps. 

    :param df: spark dataframe
    :param id_cols: string or list of column names to partition by the window function 
    :param order_col: column to use to order by the window function
    :param value_col: column to be filled
    :returns: spark dataframe updated with interpolated values
    """
    # create row number over window and a column with row number only for non missing values
    w = Window.partitionBy(id_cols).orderBy(order_col)
    new_df = df.withColumn("rn", F.row_number().over(w))
    new_df = new_df.withColumn("rn_not_null", F.when(F.col(value_col).isNotNull(), F.col("rn")))

    # create relative references to the start value (last value not missing)
    w_start = Window.partitionBy(id_cols).orderBy(order_col).rowsBetween(Window.unboundedPreceding, -1)
    new_df = new_df.withColumn("start_val", F.last(value_col, True).over(w_start))
    new_df = new_df.withColumn("start_rn", F.last("rn_not_null", True).over(w_start))

    # create relative references to the end value (first value not missing)
    w_end = Window.partitionBy(id_cols).orderBy(order_col).rowsBetween(0, Window.unboundedFollowing)
    new_df = new_df.withColumn("end_val", F.first(value_col, True).over(w_end))
    new_df = new_df.withColumn("end_rn", F.first("rn_not_null", True).over(w_end))

    # create references to gap length and current gap position  
    new_df = new_df.withColumn("diff_rn", F.col("end_rn") - F.col("start_rn"))
    new_df = new_df.withColumn("curr_rn", F.col("diff_rn") - (F.col("end_rn") - F.col("rn")))

    # calculate linear interpolation value
    lin_interp_func = (F.col("start_val") + (F.col("end_val") - F.col("start_val")) / F.col("diff_rn") * F.col("curr_rn"))
    new_df = new_df.withColumn(value_col, F.when(F.col(value_col).isNull(), lin_interp_func).otherwise(F.col(value_col)))
    
    return new_df.drop(
        "rn", "rn_not_null", "start_val", "start_rn", "end_val", "end_rn", "diff_rn", "curr_rn"
    )


def melt(
    df: DataFrame, 
    id_vars: typing.Optional[typing.Iterable[str]] = None, 
    value_vars: typing.Optional[typing.Iterable[str]] = None, 
    var_name: str = "variable", 
    value_name: str = "value",
) -> DataFrame:
    """
    Unpivot a DataFrame from wide to long format, optionally leaving identifiers set.
    This function is useful to massage a DataFrame into a format where one or more columns are identifier variables (id_vars), 
    while all other columns, considered measured variables (value_vars), are “unpivoted” to the row axis, leaving just two 
    non-identifier columns, 'variable' and 'value'.
    
    :param df: data frame to be melted
    :param id_vars: scalar, tuple, list, or ndarray, optional - Column(s) to use as identifier variables.
    :param value_vars: scalar, tuple, list, or ndarray, optional - Column(s) to unpivot. If not specified, uses all columns that are not set as id_vars.
    :param var_name: scalar, default 'variable' - Name to use for the variable column.
    :param value_name: scalar, default 'value' - Name to use for the value column, can't be an existing column label.
    :return: melted version of data frame
    """
    if id_vars is None:
        id_vars = list()
    if value_vars is None:
        value_vars = list(set(df.columns) - set(id_vars))

    # Create array<struct<variable: str, value: ...>>
    _vars_and_vals = F.array(
        *(
            F.struct(F.lit(c).alias(var_name), F.col(c).alias(value_name)) 
            for c in value_vars
        )
    )

    # Add to the DataFrame and explode
    _tmp = df.withColumn("_vars_and_vals", F.explode(_vars_and_vals))

    cols = id_vars + [
        F.col("_vars_and_vals")[x].alias(x) for x in [var_name, value_name]
    ]
    return _tmp.select(*cols)
